from aws_cdk import Stack, Duration, RemovalPolicy
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_sns as sns
# import aws_cdk.aws_lambda_python_alpha as _alambda
import aws_cdk.aws_bedrock as bedrock
import aws_cdk.aws_events as events

from aws_cdk.aws_dynamodb import Table


class TaskFunnelStack(Stack):
    """Make a step function state machine with lambdas doing the work."""

    def __init__(self, scope: Construct, id: str, 
        lambda_tracing, 
        ddb_table: Table, 
        gamelog_lambda: _lambda.Function, 
        custom_event_bus: events.IEventBus,
        prompt_id: str,
        **kwargs) -> None:

        super().__init__(scope, id, **kwargs)

        fail_topic = sns.Topic(self, "Funnel Failure Topic")

        funnel_lambda_role = iam.Role(self, "Lambda-funnel-role",
                                      role_name='rtcwpro-lambda-funnel-role',
                                      assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                      )
        funnel_lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        ddb_table.grant_read_write_data(funnel_lambda_role)

        group_cacher_lambda = _lambda.Function(
            self, 'gather-cacher-lambda',
            function_name='rtcwpro-gather-cacher',
            code=_lambda.Code.from_asset('lambdas/tasks/group_cacher'),
            handler='group_cacher.handler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            role=funnel_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(90),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
                }
            )

        group_reporter_lambda_role = iam.Role(self, "group-reporter-lambda-role",
                                              role_name='rtcwpro-lambda-group-reporter',
                                              assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                              )
        group_reporter_lambda_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        group_reporter_lambda_role.add_to_policy(iam.PolicyStatement(
            resources=["*"],
            actions=[
                "bedrock:InvokeModel"
            ],
        ))
        custom_event_bus.grant_put_events_to(group_reporter_lambda_role)

        group_reporter_lambda = _lambda.Function(
            self, 'group-reporter-lambda',
            function_name='rtcwpro-group-reporter',
            code=_lambda.Code.from_asset('lambdas/tasks/group_genai_reporter'),
            handler='group_genai_reporter.handler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            role=group_reporter_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(60),
            environment={
                'PROMPT_ID': prompt_id,
                'RTCWPROSTATS_CUSTOM_BUS_ARN': custom_event_bus.event_bus_arn
            }
            )

        send_failure_notification = tasks.SnsPublish(self, "Funnel Failure",
                                                     topic=fail_topic,
                                                     integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
                                                     message=sfn.TaskInput.from_text("Process Failure")
                                                     )

        group_ai_reporter_task = tasks.LambdaInvoke(self, "GenAI Group Report", input_path="$.Payload.group_name", lambda_function=group_reporter_lambda)
        gamelog_lambda_task = tasks.LambdaInvoke(self, "Group Cache Awards", input_path="$.Payload.group_name", lambda_function=gamelog_lambda)
        group_cacher_task = tasks.LambdaInvoke(self, "Group Cache Task", lambda_function=group_cacher_lambda)

        choice = sfn.Choice(self, "Task type")
        choice.when(sfn.Condition.string_equals("$.tasktype", "group_cacher"), group_cacher_task)
        choice.otherwise(send_failure_notification)

        group_cacher_task.add_catch(send_failure_notification)
        gamelog_lambda_task.add_catch(send_failure_notification)
        group_ai_reporter_task.add_catch(send_failure_notification)

        group_cacher_task.next(gamelog_lambda_task)
        gamelog_lambda_task.next(group_ai_reporter_task)

        funnel_state_machine = sfn.StateMachine(self, "Task Funnel",
                                                definition=choice,
                                                timeout=Duration.minutes(5),
                                                state_machine_type=sfn.StateMachineType.EXPRESS
                                                )

        self.funnel_state_machine = funnel_state_machine
