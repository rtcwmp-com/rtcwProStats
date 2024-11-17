from aws_cdk import Stack, Duration, RemovalPolicy
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_sns as sns
# import aws_cdk.aws_lambda_python_alpha as _alambda

from aws_cdk.aws_dynamodb import Table


class TaskFunnelStack(Stack):
    """Make a step function state machine with lambdas doing the work."""

    def __init__(self, scope: Construct, id: str, lambda_tracing, ddb_table: Table, gamelog_lambda: _lambda.Function, **kwargs) -> None:
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

        # boto3_lambda_layer = _alambda.PythonLayerVersion(self,
        #                                                  'boto3-lambda-layer',
        #                                                  entry='./rtcwProStats/lambdas/layer/boto3_latest/',
        #                                                  compatible_architectures=[_lambda.Architecture.ARM_64],
        #                                                  compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
        #                                                  )

        group_reporter_lambda = _lambda.Function(
            self, 'group-reporter-lambda',
            function_name='rtcwpro-group-reporter',
            code=_lambda.Code.from_asset('lambdas/tasks/group_genai_reporter'),
            handler='group_genai_reporter.handler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            role=group_reporter_lambda_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(30),
            layers=[
                _lambda.LayerVersion.from_layer_version_arn(self, "boto3layer",
                                                            layer_version_arn="arn:aws:lambda:" + self.region + ":" + self.account + ":layer:boto3_layer1_34_33:1"
                                                            )
                ]
            )

        send_failure_notification = tasks.SnsPublish(self, "Funnel Failure",
                                                     topic=fail_topic,
                                                     integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
                                                     message=sfn.TaskInput.from_text("Process Failure")
                                                     )

        group_ai_reporter_lambda_task = tasks.LambdaInvoke(self, "GenAI Group Report", input_path="$.Payload.match_results", lambda_function=group_reporter_lambda)
        gamelog_lambda_task = tasks.LambdaInvoke(self, "Group Cache Awards", input_path="$.Payload.group_name", lambda_function=gamelog_lambda)
        group_cacher_task = tasks.LambdaInvoke(self, "Group Cache Task", lambda_function=group_cacher_lambda)

        choice = sfn.Choice(self, "Task type")
        choice.when(sfn.Condition.string_equals("$.tasktype", "group_cacher"), group_cacher_task)
        choice.otherwise(send_failure_notification)

        group_cacher_task.add_catch(send_failure_notification)

        cache_processing_parallel = sfn.Parallel(self, "Post-cache processing")
        cache_processing_parallel.branch(gamelog_lambda_task)
        cache_processing_parallel.branch(group_ai_reporter_lambda_task)
        cache_processing_parallel.add_catch(send_failure_notification)

        group_cacher_task.next(cache_processing_parallel)

        funnel_state_machine = sfn.StateMachine(self, "Task Funnel",
                                                definition=choice,
                                                timeout=Duration.minutes(5),
                                                state_machine_type=sfn.StateMachineType.EXPRESS
                                                )

        self.funnel_state_machine = funnel_state_machine
