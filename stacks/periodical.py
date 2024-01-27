from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_lambda as _lambda
import aws_cdk.aws_events as events
import aws_cdk.aws_events_targets as targets
import aws_cdk.aws_iam as iam

import aws_cdk.aws_stepfunctions as sfn
from aws_cdk.aws_dynamodb import Table


class PeriodicalStack(Stack):
    """Processes that take place after the match had been saved."""

    def __init__(self, scope: Construct, construct_id: str, 
                 ddb_table: Table, 
                 funnel_sf: sfn.StateMachine,
                 custom_event_bus: events.IEventBus,
                 lambda_tracing, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        classifier_role = iam.Role(self, "ClassifierRole",
                                   role_name='rtcwpro-lambda-classifier-role',
                                   assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                   )

        classifier_lambda = _lambda.Function(
            self, 'classifier',
            function_name='rtcwpro-classifier',
            code=_lambda.Code.from_asset('lambdas/periodical/classifier'),
            handler='classifier.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=classifier_role,
            tracing=lambda_tracing
        )

        # Run every hour
        # See https://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
        rule = events.Rule(
            self, "Rule0",
            rule_name="hourly_cleanser",
            schedule=events.Schedule.cron(
                minute='0',
                hour='0',  # change this to */1 for hourly
                month='*/1',  # tmp
                week_day='*',
                year='*'),
        )
        rule.add_target(targets.LambdaFunction(classifier_lambda))
        
        # Monthly summaries
        period_grouper_role = iam.Role(self, "PeriodGroupLambdaRole",
                                   role_name='rtcwpro-lambda-period-grouper',
                                   assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                   )
        period_grouper_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        ddb_table.grant_read_write_data(period_grouper_role)
        custom_event_bus.grant_put_events_to(period_grouper_role)
        funnel_sf.grant_start_execution(period_grouper_role)

        period_grouper = _lambda.Function(
            self, 'period_grouper',
            function_name='period-grouper',
            code=_lambda.Code.from_asset('lambdas/periodical/period_grouper'),
            handler='period_grouper.handler',
            runtime=_lambda.Runtime.PYTHON_3_8,
            role=period_grouper_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(60),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
                'RTCWPROSTATS_FUNNEL_STATE_MACHINE': funnel_sf.state_machine_arn,
                'RTCWPROSTATS_CUSTOM_BUS_ARN': custom_event_bus.event_bus_arn
            }
        )

        season_maker_role = iam.Role(self, "SeasonMakerLambdaRole",
                                     role_name='rtcwpro-lambda-season_maker',
                                     assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                     )
        season_maker_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSLambdaBasicExecutionRole'))
        ddb_table.grant_read_write_data(season_maker_role)
        custom_event_bus.grant_put_events_to(season_maker_role)

        season_maker = _lambda.Function(
            self, 'season_maker',
            function_name='season_maker',
            code=_lambda.Code.from_asset('lambdas/periodical/season_maker'),
            handler='season_maker.handler',
            runtime=_lambda.Runtime.PYTHON_3_9,
            role=season_maker_role,
            tracing=lambda_tracing,
            timeout=Duration.seconds(60),
            environment={
                'RTCWPROSTATS_TABLE_NAME': ddb_table.table_name,
                'RTCWPROSTATS_CUSTOM_BUS_ARN': custom_event_bus.event_bus_arn
            }
        )

        # Run on day 1 at some hour
        regiontypes_hours = {
            "na#6": "23",
            "eu#6": "17",
            "sa#6": "21",
            "na#3": "22",
            "eu#3": "16",
            "sa#3": "21",
            }
        i = 1
        for regiontype, hour in regiontypes_hours.items():
            rule2 = events.Rule(
                self, "Rule" + str(i),
                rule_name="monthly_group_maker" + regiontype.replace("#",""),
                schedule=events.Schedule.cron(
                    minute='0',
                    hour=hour,  
                    day='1'),
            )
            rule2.add_target(targets.LambdaFunction(period_grouper,
                            event=events.RuleTargetInput.from_object({"regiontype": regiontype}))
                            )
            i += 1

        # Season maker
            rule3 = events.Rule(
                self, "SeasonRule" + str(i),
                rule_name="season_maker" + regiontype.replace("#", ""),
                schedule=events.Schedule.cron(
                    month="*/3",
                    minute='30',
                    hour=hour,
                    day='1'),
            )
            rule3.add_target(targets.LambdaFunction(season_maker,
                                                    event=events.RuleTargetInput.from_object(
                                                        {"regiontype": regiontype}))
                             )