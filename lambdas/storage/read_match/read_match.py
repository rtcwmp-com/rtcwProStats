import logging
import boto3
import json
import time as _time
import os
import datetime

from reader_writeddb import (
    # ddb_put_item,
    ddb_prepare_match_item,
    ddb_prepare_stats_items,
    ddb_prepare_statsall_item,
    ddb_prepare_gamelog_item,
    ddb_prepare_wstat_items,
    ddb_prepare_wstatsall_item,
    ddb_prepare_player_items,
    ddb_prepare_log_item,
    ddb_batch_write,
    ddb_update_server_record,
    ddb_prepare_server_item,
    ddb_get_server
)

# pip install --target ./ sqlalchemy
# import sqlalchemy

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("read_match")
logger.setLevel(log_level)

if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
    MATCH_STATE_MACHINE = ""  # set this at debug time
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
    MATCH_STATE_MACHINE = os.environ['RTCWPROSTATS_MATCH_STATE_MACHINE']

dynamodb = boto3.resource('dynamodb')
ddb_client = boto3.client('dynamodb')


table = dynamodb.Table(TABLE_NAME)

s3 = boto3.client('s3')
sf_client = boto3.client('stepfunctions')

def handler(event, context):
    """Read new incoming json and submit it to the DB."""
    # s3 event source
    # bucket_name = event['Records'][0]['s3']['bucket']['name']
    # file_key = event['Records'][0]['s3']['object']['key']
    
    # sqs event source
    s3_request_from_sqs = json.loads(event['Records'][0]["body"])
    bucket_name = s3_request_from_sqs["Records"][0]['s3']['bucket']['name']
    file_key    = s3_request_from_sqs["Records"][0]['s3']['object']['key']

    logger.info('Reading {} from {}'.format(file_key, bucket_name))

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    except s3.exceptions.ClientError as err:
        if err.response['Error']['Code'] == 'EndpointConnectionError':
           logger.error("Connection could not be established to AWS. Possible firewall or proxy issue. " + str(err))
        elif err.response['Error']['Code'] == 'ExpiredToken':
            logger.error("Credentials for AWS S3 are not valid. " + str(err))
        elif err.response['Error']['Code'] == 'AccessDenied':
            logger.error("Current credentials to not provide access to read the file. " + str(err))
        elif err.response['Error']['Code'] == 'NoSuchKey':
            logger.error("File was not found: " + file_key)
        else:
            print("[x] Unexpected error: %s" % err)
        return None

    message = "Nothing was processed"
    try:
        content = obj['Body'].read().decode('UTF-8')
        gamestats = json.loads(content)
        logger.info("Number of keys in the file: " + str(len(gamestats.keys())))
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        # Todo expand these
        message = "Failed to read content from " + file_key + "\n" + error_msg
        return message

    integrity, message = integrity_checks(gamestats)
    if not integrity:
        logger.error("Failed integrity check:" + message)
        return message
    
    date_time_human = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        date_time_human = datetime.datetime.fromtimestamp(int(gamestats["gameinfo"]["match_id"])).strftime("%Y-%m-%d %H:%M:%S")
    except:
        logger.warning("Could not convert epoch time to timestamp: " + gamestats["gameinfo"]["match_id"])
     
    gamestats["gameinfo"]["date_time_human"] = date_time_human

    server = ddb_get_server(gamestats['serverinfo']['serverName'], table)
    region = ""
    server_item = None
    if server:
        logger.info("Updating the server with +1 match")
        region = server["region"]
        ddb_update_server_record(gamestats, table, region, date_time_human)
    else:
        server_item = ddb_prepare_server_item(gamestats)

    if region == "":
        region = "na"  # TODO: insert region logic

    if len(gamestats.get("stats", 0)) == 2:
        team1_size = len(gamestats["stats"][0].keys())
        team2_size = len(gamestats["stats"][1].keys())
        total_size = team1_size + team2_size
    else: 
        total_size = len(gamestats["stats"])
                         
    gametype = "notype"
    if 5 < total_size <= 7:  # normally 6, but shit happens 5-8
        gametype = "3"
    if 7 < total_size <= 14:  # normally 12, but shit happens 5-8
        gametype = "6"
    if 14 < total_size:
        gametype = "6plus"

    match_type = region + "#" + gametype
    logger.info("Setting the match_type to " + match_type)
    gamestats["match_type"] = match_type
    gamestats["gameinfo"]["server_name"] = gamestats['serverinfo']['serverName']

    submitter_ip = gamestats.get("submitter_ip", "no.ip.in.file")

    items = []
    match_item = ddb_prepare_match_item(gamestats)
    match_id = str(match_item["sk"])
    stats_items = ddb_prepare_stats_items(gamestats)
    statsall_item = ddb_prepare_statsall_item(gamestats)
    gamelog_item = ddb_prepare_gamelog_item(gamestats)
    wstats_items = ddb_prepare_wstat_items(gamestats)
    wstatsall_item = ddb_prepare_wstatsall_item(gamestats)
    player_items = ddb_prepare_player_items(gamestats)
    log_item = ddb_prepare_log_item(match_id, file_key,
                                    len(match_item["data"]),
                                    len(stats_items),
                                    len(statsall_item["data"]),
                                    len(gamelog_item["data"]),
                                    len(wstats_items),
                                    len(wstatsall_item["data"]),
                                    len(player_items),
                                    # timestamp,
                                    submitter_ip)

    items.append(match_item)
    items.extend(stats_items)
    items.append(statsall_item)
    items.append(gamelog_item)
    items.extend(wstats_items)
    items.append(wstatsall_item)
    items.extend(player_items)
    items.append(log_item)
    if server_item:
        items.append(server_item)

    total_items = str(len(items))
    message = ""
    t1 = _time.time()
# =============================================================================
#     for Item in items:
#         response = None
#         try:
#             response = ddb_put_item(Item, table)
#         except Exception as ex:
#             template = "An exception of type {0} occurred. Arguments:\n{1!r}"
#             error_msg = template.format(type(ex).__name__, ex.args)
#             message = "Failed to load all records for a match " + file_key + "\n" + error_msg
#
#         if response["ResponseMetadata"]['HTTPStatusCode'] != 200:
#             message += "\nItem returned non 200 code " + Item["pk"] + ":" + Item["sk"]
# =============================================================================

    try:
        ddb_batch_write(ddb_client, table.name, items)
        message = f"Sent {file_key} to database with {total_items} items. pk = match, sk = {match_id}"
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to load all records for a match " + file_key + "\n" + error_msg
        
    try:
        response = sf_client.start_execution(
                stateMachineArn=MATCH_STATE_MACHINE,
                input='{"matchid": "' + gamestats["gameinfo"]["match_id"] + '","roundid": ' + gamestats["gameinfo"]["round"] + '}'
                )
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to start state machine for " + gamestats["gameinfo"]["match_id"] + "\n" + error_msg
    else:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Started state machine " + response['executionArn'])
        else:
            logger.warning("Bad response from state machine " + str(response))
            message += "\nState machine failed."
            
    logger.info(message)
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to write {total_items} items is {time_to_write} s")
    return message


def integrity_checks(gamestats):
    """Check if gamestats valid for any known things."""
    message = "Started integrity checks"
    integrity = True

    if "gameinfo" not in gamestats:
        integrity = False
        if "map_restart" in json.dumps(gamestats):
            message = "No gameinfo. Map was restarted. Aborting."
        else:
            message = "No gameinfo found."
        return integrity, message

    if "stats" not in gamestats:
        integrity = False
        message = "No stats in " + gamestats["gameinfo"].get('match_id', 'na')
        return integrity, message
    
    if len(gamestats["stats"]) < 5:
        integrity = False
        message = "Number of players is less than 3v3, not saving."
        return integrity, message

    if isinstance(gamestats["stats"], dict):
        integrity = False
        message = gamestats["gameinfo"].get('match_id', 'na') + " stats is a dict. Expecting a list."
        return integrity, message

    return integrity, message


if __name__ == "__main__":
    #not working since sqs was introduced. Finniky json double quotes
    event = {
    "Records": [
                    {
                    "body": "{\"Records\":[{\"s3\":{\"bucket\":{\"name\":\"rtcwprostats\"},\"object\":{\"key\":\"intake/20210803-144348-1628001413.txt\"}}}]}"
                    }
        ]
    }
    print("Test result" + handler(event, None))
