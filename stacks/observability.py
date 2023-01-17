from aws_cdk import Stack, Duration
from constructs import Construct

import aws_cdk.aws_iam as iam
import aws_cdk.aws_sns as sns
import aws_cdk.aws_cloudwatch as cloudwatch
import aws_cdk.aws_cloudwatch_actions as cw_actions
import aws_cdk.aws_lambda as _lambda


class ObservabilityStack(Stack):
    """Make a step function state machine with lambdas doing the work."""

    def __init__(self, scope: Construct, id: str,
                 sns_topic: sns.Topic,
                 save_payload_lambda: _lambda.Function,
                 read_match_lambda: _lambda.Function,
                 gamelog_lambda: _lambda.Function,
                 summary_lambda: _lambda.Function,
                 elo_lambda: _lambda.Function,
                 **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        worker_functions = {"save_payload": save_payload_lambda,
                            "read_match": read_match_lambda,
                            "gamelog": gamelog_lambda,
                            "summary": summary_lambda,
                            "elo": elo_lambda}
        for name, fn in worker_functions.items():
            alarm = cloudwatch.Alarm(self, name,
                                     metric=fn.metric_errors(),
                                     threshold=1,
                                     evaluation_periods=1
                                     )
            alarm.add_alarm_action(cw_actions.SnsAction(sns_topic))


