import boto3
import logging
import os
from summary_calc import process_rtcwpro_summary

# for local testing use actual table
# for lambda execution runtime use dynamic reference
if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
    CUSTOM_BUS = "fake"
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
    CUSTOM_BUS = os.environ['RTCWPROSTATS_CUSTOM_BUS_ARN']
    
dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')
event_client = boto3.client('events')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('summary')
logger.setLevel(log_level)


def handler(event, context):
    """Merge summaries for a player including match."""
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    if not isinstance(event, int):
        logger.warning("Received unexpected input for match_id " + str(event))
        return {"error": "Exiting function due to bad input."}
    else:
        match_id = str(event)

    logger.info("Processing match id " + match_id)
    
    message = process_rtcwpro_summary(ddb_table, ddb_client, event_client, match_id, log_stream_name, CUSTOM_BUS)

    logger.info(message)


if __name__ == "__main__":
    event = 1673935466
    handler(event, None)
