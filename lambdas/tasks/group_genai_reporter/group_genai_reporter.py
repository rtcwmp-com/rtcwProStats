import boto3
import logging
import os
import json
import time as _time


log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('group_reporter')
logger.setLevel(log_level)


def handler(event, context):
    """Merge summaries for a player including match."""
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    # group_name = event["taskdetail"]
    match_results = event["match_results"]
    logger.info("Processing group description")
    process_group_description(match_results)
    # return {"group_name": json.dumps(group_name)}

def process_group_description(match_results):
    t1 = _time.time()
    logger.info(match_results)
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to process summaries is {time_to_write} s")


if __name__ == "__main__":
    event = {
        "match_results":
    }
    handler(event, None)
