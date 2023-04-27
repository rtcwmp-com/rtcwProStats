# -*- coding: utf-8 -*-

import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging
import os
import sys
import traceback
import time as _time
from datetime import datetime, timedelta
from notify_discord import post_custom_bus_event

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("retriever")
logger.setLevel(log_level)
logger.info("test")

if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
    print("\n\n\nSet state machine and custom bus values in console\n\n\n")
    # MATCH_STATE_MACHINE = ""
    # CUSTOM_BUS = ""
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
    MATCH_STATE_MACHINE = os.environ['RTCWPROSTATS_FUNNEL_STATE_MACHINE']
    CUSTOM_BUS = os.environ['RTCWPROSTATS_CUSTOM_BUS_ARN']

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')
# sf_client = boto3.client('stepfunctions')
event_client = boto3.client('events')


def handler(event, context):
    """Todo."""
    logger.info('request: {}'.format(json.dumps(event)))
    region_match_type = event["regiontype"]

    try:
        process_new_season(region_match_type)
    except Exception as ex:
        print(type(ex).__name__)
        print(ex.args)
        template = "A general error of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to complete new season function " + "\n" + error_msg
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=5, file=sys.stdout)
        logger.error(message)
    else:
        logger.info("Created a new season.")


def process_new_season(region_match_type):
    logger.info("Starting the new season sequence.")
    old_season_name = get_season_name()
    player_metrics = copy_current_metrics_to_archive(old_season_name, region_match_type)
    delete_current_metrics(player_metrics)
    logger.info("Created a new season successfully.")

    logger.info("Sending notification to discord.")
    events = announce_new_season(old_season_name, region_match_type)
    post_custom_bus_event(event_client, events)
    logger.info("Sent notification to discord.")

def get_season_name():
    logger.info("Getting the new season name.")
    old_season_name = "1"
    logger.info("Got the new season name: " + old_season_name)
    return old_season_name

def copy_current_metrics_to_archive(old_season, region_match_type):
    logger.info("Retrieving current metrics for " + region_match_type)
    try:
        player_metrics = get_current_metrics(region_match_type)
        copy_current_metrics(player_metrics, old_season, region_match_type)
    except Exception as ex:
        message = "Failed to copy current metrics for " + region_match_type + "\n"
        logger.error(message)
        raise
    return player_metrics


def delete_current_metrics(player_metrics):
    logger.info("Deleting current metrics.")
    delete_keys = []
    for player_metric in player_metrics:
        delete_keys.append({"pk": player_metric["pk"], "sk": player_metric["sk"]})
    print("These items will be deleted")
    print(json.dumps(delete_keys))
    logger.info("Deletion complete.")


def get_current_metrics(region_match_type):
    logger.info("Getting current metrics for " + region_match_type)
    player_metrics = []
    categories = ["acc", "kdr", "acc", "Sharpshooter"]
    for category in categories:
        leaders_category = get_leaders(category, region_match_type)
        player_metrics.extend(leaders_category)

    len_player_metrics = len(player_metrics)
    logger.info(f"Got {len_player_metrics} metrics.")
    return player_metrics


def get_leaders(category, region_match_type):

    pound_goes_here = "#"
    if category.lower() in ["elo", "kdr", "acc"]:
        pound_goes_here = ""
    pk = "leader" + pound_goes_here + category + "#" + region_match_type

    response = ddb_table.query(IndexName='gsi1', KeyConditionExpression=Key('gsi1pk').eq(pk))
    return response["Items"]


def copy_current_metrics(player_metrics, old_season, region_match_type):
    logger.info("Copying current metrics.")
    new_player_metrics = []
    # old_season = "Season000"
    for player_metric in player_metrics:
        new_item = player_metric.copy()
        new_item["pk"] = old_season + "#" + new_item["pk"]
        new_item["gsi1pk"] = old_season + "#" + new_item["gsi1pk"]
        new_player_metrics.append(new_item)

    try:
        ddb_batch_write(ddb_client, ddb_table.name, new_player_metrics)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to insert player metrics to archive " + old_season + "#" + region_match_type + "\n" + error_msg
        logger.error(message)
        raise

    logger.info("Copied current metrics.")


def announce_new_season(old_season_name, region_match_type):
    """Prepare an event about a new season for discord announcement."""
    events = []

    event_template = {
        'Source': 'rtcwpro-pipeline',
        'DetailType': 'Discord notification',
        'Detail': '',
        'EventBusName': CUSTOM_BUS
    }

    tmp_event = event_template.copy()
    tmp_event["Detail"] = json.dumps({"notification_type": "Season complete",
                                      "old_season_name": old_season_name,
                                      "region_match_type": region_match_type})
    events.append(tmp_event)
    return events


def create_batch_write_structure(table_name, items, start_num, batch_size):
    """
    Create item structure for passing to batch_write_item
    :param table_name: DynamoDB table name
    :param items: large collection of items
    :param start_num: Start index
    :param num_items: Number of items
    :return: dictionary of tables to write to
    """

    serializer = boto3.dynamodb.types.TypeSerializer()
    item_batch = {table_name: []}
    item_batch_list = items[start_num: start_num + batch_size]
    if len(item_batch_list) < 1:
        return None
    for item in item_batch_list:
        item_serialized = {k: serializer.serialize(v) for k, v in item.items()}
        item_batch[table_name].append({'PutRequest': {'Item': item_serialized}})

    return item_batch


def ddb_batch_write(client, table_name, items):
    messages = ""
    num_items = len(items)
    logger.info(f'Performing ddb_batch_write to dynamo with {num_items} items.')
    start = 0
    batch_size = 25
    while True:
        # Loop adding 25 items to dynamo at a time
        request_items = create_batch_write_structure(table_name, items, start, batch_size)
        if not request_items:
            break
        try:
            response = client.batch_write_item(RequestItems=request_items)
        except ClientError as err:
            logger.error(err.response['Error']['Message'])
            logger.error("Failed to run full batch_write_item")
            raise
        if len(response['UnprocessedItems']) == 0:
            logger.info(f'Wrote a batch of about {batch_size} items to dynamo')
        else:
            # Hit the provisioned write limit
            logger.warning('Hit write limit, backing off then retrying')
            sleep_time = 5  # seconds
            logger.warning(f"Sleeping for {sleep_time} seconds")
            _time.sleep(sleep_time)

            # Items left over that haven't been inserted
            unprocessed_items = response['UnprocessedItems']
            logger.warning('Resubmitting items')
            # Loop until unprocessed items are written
            while len(unprocessed_items) > 0:
                response = client.batch_write_item(RequestItems=unprocessed_items)
                # If any items are still left over, add them to the
                # list to be written
                unprocessed_items = response['UnprocessedItems']

                # If there are items left over, we could do with
                # sleeping some more
                if len(unprocessed_items) > 0:
                    sleep_time = 5  # seconds
                    logger.warning(f"Sleeping for {sleep_time} seconds")
                    _time.sleep(sleep_time)

            # Inserted all the unprocessed items, exit loop
            logger.warning('Unprocessed items successfully inserted')
            break
        if response["ResponseMetadata"]['HTTPStatusCode'] != 200:
            messages += f"\nBatch {start} returned non 200 code"
        start += 25

if __name__ == "__main__":
    event = {
        "regiontype": "eu#3"
    }
    handler(event, None)