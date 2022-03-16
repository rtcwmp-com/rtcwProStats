# -*- coding: utf-8 -*-

import json
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging
import os
import time
from datetime import datetime, timedelta
from notify_discord import post_custom_bus_event

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("retriever")
logger.setLevel(log_level)

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
sf_client = boto3.client('stepfunctions')
event_client = boto3.client('events')

def handler(event, context):
    """Todo."""
    print('request: {}'.format(json.dumps(event)))
    region_match_type = event["regiontype"]
    
    skhigh = int(time.time())
    sklow = region_match_type + "#" + str(skhigh - 60 * 60 * 24 * 30)
    skhigh = region_match_type + "#" + str(skhigh)
    responses = get_range(sklow, skhigh, ddb_table)
    matches_dedup = {}  # for deduplication
    for response in responses:
        match_no_round = list(response.values())[0].replace(region_match_type + "#", "")[0:-1]
        matches_dedup[match_no_round] = 1
    matches = list(matches_dedup.keys())
        
    logger.info("Number of matches: " + str(len(matches)))
    
    try:
        group_name, Item = ddb_prepare_group_item(region_match_type, matches, "0.0.0.0")
        ddb_put_item(Item, ddb_table)
        events = announce_new_group(group_name.split("#")[0], region_match_type)
        execute_state_machine(group_name)
        post_custom_bus_event(event_client, events)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to complete a process for periodical group: " + group_name + "\n" + error_msg
        logger.error(message)
    else:
        logger.info("Added group to the database.")
        
    return "Finished without critical errors"

def get_range(sklow, skhigh, ddb_table):
    """Get several items by pk and range of sk."""
    item_info = "match" + ":" + sklow + " to " + skhigh
    try:
        logger.info("Looking for " + item_info)
        response = ddb_table.query(IndexName='lsi', KeyConditionExpression=Key('pk').eq("match") & Key('lsipk').between(sklow, skhigh), ProjectionExpression="lsipk")
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            logger.warning("Request returned no results")
            result = {}
    return result

def ddb_prepare_group_item(region_match_type, matches, submitter_ip):
    """Make a ddb item for a match group."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prev_month_dt = datetime.now() - timedelta(days=28)
    group_name = "monthly-" + region_match_type.replace("#","-") + "-" + prev_month_dt.strftime("%Y%B")
    group_name_ts = group_name + "#" + ts
    group_item = {
        'pk'    : 'group',
        'sk'    : group_name_ts,
        'lsipk' : region_match_type + "#" + group_name_ts,
        'gsi1pk': "group",
        'gsi1sk': region_match_type + "#" + ts,
        'data'  : json.dumps(matches),
        "submitter_ip": submitter_ip
    }
    logger.info("Adding to group " + group_name)
    return group_name, group_item

def ddb_put_item(Item, table):
    """Put a single item in ddb."""
    try:
        response = table.put_item(Item=Item)
    except ClientError as err:
        logger.error(err.response['Error']['Message'])
        logger.error("Item was: " + Item["pk"] + ":" + Item["sk"])
        raise
    else:
        pk = Item["pk"]
        sk = Item["sk"]
        http_code = "No http code"

        try:
            http_code = response['ResponseMetadata']['HTTPStatusCode']
        except: 
            http_code = "Could not retrieve http code"
            logger.error("Could not retrieve http code from response\n" + str(response))

        if http_code != 200:
            logger.error("Unhandled HTTP Code " + str(http_code) + " while submitting item " + pk + ":" + sk + "\n" + str(response))
    return response

def announce_new_group(group_name, region_match_type):
    """Prepare an event about new group for discord announcement."""
    events = []

    event_template = {
        'Source': 'rtcwpro-pipeline',
        'DetailType': 'Discord notification',
        'Detail': '',
        'EventBusName': CUSTOM_BUS
    }

    tmp_event = event_template.copy()
    tmp_event["Detail"] = json.dumps({"notification_type": "new group",
                                      "group_name": group_name,
                                      "match_type" : region_match_type})
    events.append(tmp_event)
    return events

def execute_state_machine(group_name):
    """Execute step functions with group caching."""
    try:
        response = sf_client.start_execution(
            stateMachineArn=MATCH_STATE_MACHINE,
            input='{"tasktype": "group_cacher","taskdetail": "' + group_name + '"}'
        )
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to start state machine for group_cacher group: " + group_name + "\n" + error_msg
        logger.error(message)
    else:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Started state machine " + response['executionArn'])
        else:
            logger.warning("Bad response from state machine " + str(response))
            message += "\nState machine failed."
            logger.error(message)

if __name__ == "__main__":
    event = {
      "regiontype": "eu#3"
    } 
    print(handler(event, None))