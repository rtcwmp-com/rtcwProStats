#!/usr/bin/env python3

import aws_cdk as cdk
import aws_cdk.aws_lambda as _lambda

from stacks.api import APIStack
from stacks.storage import StorageStack
from stacks.dns import DNSStack
from stacks.periodical import PeriodicalStack
from stacks.database import DatabaseStack
from stacks.delivery_retriever import DeliveryRetrieverStack
from stacks.delivery_writer import DeliveryWriterStack
from stacks.delivery import DeliveryStack
from stacks.postprocess import PostProcessStack
from stacks.read_match_lambda import ReadMatchStack
from stacks.taskfunnel import TaskFunnelStack
from stacks.custom_bus import CustomBusStack
from stacks.gamelog_lambda import GamelogLambdaStack
from stacks.observability import ObservabilityStack

import aws_cdk.aws_iam as iam

from stacks.settings import (
    cert_arn,
    api_key,
    env,
    enable_tracing,
    dns_resource_name,
    hosted_zone_id,
    zone_name,
    prompt_id
)

def print_cdk_tree(construct_, level=0):
    """Recursively print nodes of constructs in the app."""
    level = level + 1
    try:
        for lower_construct in construct_.node.children:
            try:
                print(("Level " + str(level) + ":").rjust(9 + level * 4) + lower_construct.node.id)
                print_cdk_tree(lower_construct, level)
            except Exception as ex:
                print("Could not print something under " + construct_.node.id)
                print(ex)
    except Exception as ex:
        print("!!!!!!!!!!Could not iterate over " + construct_.node.id)
        print(ex)

app = cdk.App()

lambda_tracing = _lambda.Tracing.DISABLED
if enable_tracing:
    lambda_tracing = _lambda.Tracing.ACTIVE

database = DatabaseStack(app, "rtcwprostats-database", env=env)

storage = StorageStack(app, "rtcwprostats-storage", env=env)

custom_bus_stack = CustomBusStack(app, "rtcwprostats-custom-bus", lambda_tracing=lambda_tracing, 
                                  ddb_table=database.ddb_table, 
                                  env=env)

gamelog_lambda_stack = GamelogLambdaStack(app, "rtcwprostats-gamelog",
                                          ddb_table=database.ddb_table,
                                          custom_event_bus=custom_bus_stack.custom_bus,
                                          lambda_tracing=lambda_tracing, env=env)

task_funnel_stack = TaskFunnelStack(app, "rtcwprostats-taskfunnel", lambda_tracing=lambda_tracing,
                                    ddb_table=database.ddb_table,
                                    gamelog_lambda = gamelog_lambda_stack.gamelog_lambda,
                                    custom_event_bus=custom_bus_stack.custom_bus,
                                    prompt_id=prompt_id,
                                    env=env)

post_process_stack = PostProcessStack(app, "rtcwprostats-postprocess", 
                                      lambda_tracing=lambda_tracing, 
                                      ddb_table=database.ddb_table, 
                                      gamelog_lambda = gamelog_lambda_stack.gamelog_lambda,
                                      custom_event_bus=custom_bus_stack.custom_bus,
                                      env=env)

reader = ReadMatchStack(app, "rtcwprostats-reader", 
                        storage_bucket=storage.storage_bucket, 
                        ddb_table=database.ddb_table,
                        read_queue=storage.read_queue,
                        read_dlq=storage.read_dlq,
                        postproc_state_machine=post_process_stack.postproc_state_machine, 
                        custom_event_bus=custom_bus_stack.custom_bus, 
                        lambda_tracing=lambda_tracing, 
                        env=env)

retriever = DeliveryRetrieverStack(app, "rtcwprostats-retriever", 
                                   ddb_table=database.ddb_table, 
                                   env=env, lambda_tracing=lambda_tracing)

delivery_writer = DeliveryWriterStack(app, "rtcwprostats-delivery-writer",
                                      ddb_table=database.ddb_table,
                                      funnel_sf=task_funnel_stack.funnel_state_machine,
                                      custom_event_bus=custom_bus_stack.custom_bus,
                                      env=env, lambda_tracing=lambda_tracing)

apistack = APIStack(app, "rtcwprostats-API", 
                    cert_arn=cert_arn,
                    api_key=api_key,
                    storage_bucket=storage.storage_bucket,
                    env=env, lambda_tracing=lambda_tracing)

DNSStack(app, "rtcwprostats-DNS", 
         api=apistack.api, 
         env=env, 
         dns_resource_name=dns_resource_name, 
         hosted_zone_id=hosted_zone_id, 
         zone_name=zone_name)

PeriodicalStack(app, "rtcwprostats-periodical", 
                ddb_table=database.ddb_table,
                funnel_sf=task_funnel_stack.funnel_state_machine,
                custom_event_bus=custom_bus_stack.custom_bus,
                env=env, lambda_tracing=lambda_tracing)

DeliveryStack(app, "rtcwprostats-delivery", 
              api=apistack.api,
              retriever=retriever.retriever_lambda,
              server_query=retriever.server_query_lambda,
              delivery_writer=delivery_writer.delivery_writer_lambda,
              env=env)

ObservabilityStack(app, "rtcwprostats-observe",
                   sns_topic=storage.ops_topic,
                   save_payload_lambda=apistack.save_payload,
                   read_match_lambda=reader.read_match,
                   gamelog_lambda=gamelog_lambda_stack.gamelog_lambda,
                   summary_lambda=post_process_stack.summary_lambda,
                   elo_lambda=post_process_stack.elo_lambda,
                   env=env
                   )

def strip_permissions(node):
    for child in node.children:
        if(isinstance(child, cdk.aws_apigateway.Method)):
            method = child
            for method_child in method.node.children:
                if(isinstance(method_child, cdk.aws_lambda.CfnPermission)):
                    # print(method_child)
                    # print("DELETE THIS")
                    method.node.try_remove_child(method_child.node.id)
                    # print("new method.node")
                    # print(method.node.children)
        else:
            # print(child)
            # print("recurse")
            strip_permissions(child.node)

print("Deleting lambda resource policies from API lambdas")
strip_permissions(apistack.node)
print("Adding lambda resource policies for API lambdas")
retriever.retriever_lambda.grant_invoke(iam.ServicePrincipal("apigateway.amazonaws.com"))
retriever.server_query_lambda.grant_invoke(iam.ServicePrincipal("apigateway.amazonaws.com"))
apistack.save_payload.grant_invoke(iam.ServicePrincipal("apigateway.amazonaws.com"))
delivery_writer.delivery_writer_lambda.grant_invoke(iam.ServicePrincipal("apigateway.amazonaws.com"))


cdk.Tags.of(app).add("purpose", "rtcwpro")

""" This recursive function traverses CDK app class and displays its nested contents."""

# print_cdk_tree(app)

app.synth()
