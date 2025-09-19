import logging
import boto3
import json
import time as _time
import os
import datetime
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
from read_match_matchinfo import build_teams, convert_stats_to_dict, build_new_match_summary

from reader_writeddb import (
    ddb_prepare_match_item,
    ddb_prepare_stats_items,
    ddb_prepare_statsall_item,
    ddb_prepare_gamelog_item,
    ddb_prepare_wstat_items,
    ddb_prepare_wstatsall_item,
    ddb_prepare_log_item,
    ddb_batch_write,
    ddb_update_server_record,
    ddb_prepare_server_item,
    ddb_get_server,
    ddb_prepare_alias_items_v2,
    ddb_prepare_real_name_update,
    ddb_update_user_records,
    ddb_update_map_records
)

dry_run_server_update = False
dry_run_batch_load = False
dry_run_user_records = False
dry_run_map_records = False
dry_run_step_functions = False
dry_run_event_bridge = False

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("read_match")
logger.setLevel(log_level)

if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
    print("\n\n\nSet state machine and custom bus values in console\n\n\n")
    MATCH_STATE_MACHINE = ""  # set this at debug time
    CUSTOM_BUS = ""
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
    MATCH_STATE_MACHINE = os.environ['RTCWPROSTATS_MATCH_STATE_MACHINE']
    CUSTOM_BUS = os.environ['RTCWPROSTATS_CUSTOM_BUS_ARN']

dynamodb = boto3.resource('dynamodb')
ddb_client = boto3.client('dynamodb')
event_client = boto3.client('events')


table = dynamodb.Table(TABLE_NAME)

s3 = boto3.client('s3')
sf_client = boto3.client('stepfunctions')

event_template = {
    'Source': 'rtcwpro-pipeline',
    'DetailType': 'Discord notification',
    'Detail': '',
    'EventBusName': CUSTOM_BUS
}


def handler(event, context):
    """Read new incoming json and submit it to the DB."""
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
            logger.error("[x] Unexpected error: " + str(err))
        return None

    message = "Nothing was processed"
    try:
        content = obj['Body'].read().decode('UTF-8')
        gamestats = json.loads(content)
        logger.debug("Number of keys in the file: " + str(len(gamestats.keys())))
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        # Todo expand these
        message = "Failed to read content from " + file_key + "\n" + error_msg
        logger.error(message)
        raise

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
        if dry_run_server_update:
            logger.info("Skipped update_server_record due to dry run")
        else:
            ddb_update_server_record(gamestats, table, region, date_time_human)
    else:
        server_item = ddb_prepare_server_item(gamestats)

    if region == "":
        region = "unk"

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
    teamA, teamB, aliases, team_mapping, alias_team_str = add_team_info(gamestats)
    gamestats["gameinfo"]["teams"] = alias_team_str

    match_id = gamestats["gameinfo"]["match_id"] 
    round_id = gamestats["gameinfo"]["round"]
    round2 = True if round_id == "2" else False
    
    if round2:
        match_dict = {match_id + "2": gamestats["gameinfo"]}
        
        #fake round 1 to fit another function evaluation logic
        try:
            time_limit_sec = int(gamestats["gameinfo"]["time_limit"].split(":")[0])*60 + int(gamestats["gameinfo"]["time_limit"].split(":")[1])
        except Exception as ex:
            logger.warning("time_limit_sec conversion failed")
            time_limit_sec = 600

        match_dict[match_id + "1"] = {
            "winner": "", 
            "round_start":match_id, 
            "round_end":str(int(match_id) + time_limit_sec), 
            "round":"1",
            "map": gamestats["gameinfo"]["map"]}
        #end fake
        match_summary = build_new_match_summary(match_dict, team_mapping)       
        gamestats["gameinfo"]["match_summary"] = match_summary
        # gamestats["gameinfo"]["TeamA"] = teamA
        # gamestats["gameinfo"]["TeamB"] = teamB
    else:
        match_summary = {"results": {}}
        teamA, teamB = None, None

    submitter_ip = gamestats.get("submitter_ip", "no.ip.in.file")

    stats = convert_stats_to_dict(gamestats["stats"])

    real_name_item_list = prepare_playerinfo_list(stats, "realname")
    response = get_batch_items(real_name_item_list, table, dynamodb, "real_names for match in file " + file_key)
    real_names = {}
    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            real_names[guid] = result["data"]

    items = []
    match_item = ddb_prepare_match_item(gamestats)
    tmp_stats_unnested = fix_stats_nesting(gamestats)

    win_loss_dict = make_win_loss_dict(match_id, tmp_stats_unnested, match_summary, teamA, teamB)
    stats_items = ddb_prepare_stats_items(gamestats, tmp_stats_unnested, win_loss_dict)

    statsall_item = ddb_prepare_statsall_item(gamestats)
    
    gamelog_item = ddb_prepare_gamelog_item(gamestats)
    wstats_items = ddb_prepare_wstat_items(gamestats)
    wstatsall_item = ddb_prepare_wstatsall_item(gamestats)
    aliasv2_items = ddb_prepare_alias_items_v2(gamestats, real_names)
    new_player_items, old_player_items = ddb_prepare_real_name_update(gamestats, real_names)
    log_item = ddb_prepare_log_item(match_id + round_id, file_key,
                                    len(match_item["data"]),
                                    len(stats_items),
                                    len(statsall_item["data"]),
                                    len(gamelog_item["data"]),
                                    len(wstats_items),
                                    len(wstatsall_item["data"]),
                                    len(aliasv2_items),
                                    # timestamp,
                                    submitter_ip)

    items.append(match_item)
    items.extend(stats_items)
    items.append(statsall_item)
    items.append(gamelog_item)
    items.extend(wstats_items)
    items.append(wstatsall_item)
    items.extend(aliasv2_items)
    items.extend(new_player_items)
    items.append(log_item)
    if server_item:
        items.append(server_item)

    total_items = str(len(items))
    message = ""
    
    t1 = _time.time()
    if dry_run_batch_load:
        logger.info("Skipped ddb_batch_write due to dry run")
    else:
        try:
            ddb_batch_write(ddb_client, table.name, items)
            message = f"Sent {file_key} to database with {total_items} items. pk = match, sk = {match_id}"
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            message = "Failed to load all records for a match " + file_key + "\n" + error_msg
            logger.error(message)
            return message

    if dry_run_user_records:
        logger.info("Skipped user_records due to dry run")
    else:
        try:
            ddb_update_user_records(old_player_items, table)
            logger.info(f"Updated player dates for {match_id}")  
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            message = "Failed to update all player dates for a match " + match_id + "\n" + error_msg
            logger.warning(message)

    if dry_run_map_records:
        logger.info("Skipped map_records due to dry run")
    else:
        if round2: 
            try:
                ddb_update_map_records(gamestats, tmp_stats_unnested, real_names, win_loss_dict, table)
                logger.info(f"Added mapstats for {match_id}")
            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                error_msg = template.format(type(ex).__name__, ex.args)
                message = "Failed to update map records for a match " + match_id + "\n" + error_msg
                logger.warning(message)

    if dry_run_step_functions:
        logger.info("Skipped step functions due to dry run")
    else:
        try:
            response = sf_client.start_execution(stateMachineArn=MATCH_STATE_MACHINE,
                                                 input='{"matchid": "' + match_id + '","roundid": ' + gamestats["gameinfo"]["round"] + '}'
                                                 )
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            message = "Failed to start state machine for " + match_id + "\n" + error_msg
            logger.error(message)
            return message
        else:
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.info("Started state machine " + response['executionArn'])
            else:
                logger.warning("Bad response from state machine " + str(response))
                message += "\nState machine failed."
                logger.error(message)
                return message
    
    if dry_run_event_bridge:
        logger.info("Skipped event bridge due to dry run")
    else:
        try:
            events = []
            new_player_events = announce_new_players(gamestats, real_names, match_type)
            new_server_events = announce_new_server(server_item)
            events.extend(new_player_events)
            events.extend(new_server_events)

            if len(events) > 0:
                response = event_client.put_events(Entries=events)
        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            message = "Failed to announce new players via event bridge in " + match_id + "\n" + error_msg
            logger.error(message)
        else:
            if response and response['ResponseMetadata']['HTTPStatusCode'] == 200:
                logger.info("Submitted new player event(s)")
            else:
                logger.warning("Bad response from event bridge " + str(response))
                message += "Failed to announce new players\n."
                logger.error(message)

        logger.info(message)
        time_to_write = str(round((_time.time() - t1), 3))
        logger.info(f"Time to write {total_items} items is {time_to_write} s")
        
    return message


def announce_new_players(gamestats, real_names, match_type):
    """Put event about new players for discord announcement."""
    events = []
    for player_wrapper in gamestats["stats"]:
        for playerguid, stat in player_wrapper.items():
            if playerguid not in real_names:
                logger.info("New guid event: " + playerguid + " as " + stat["alias"])
                tmp_event = event_template.copy()
                tmp_event["Detail"] = json.dumps({"notification_type": "new player",
                                                  "guid": playerguid,
                                                  "alias": stat["alias"],
                                                  "match_type" : match_type})
                events.append(tmp_event)
    return events


def announce_new_server(server_item):
    """Put event about new server for discord announcement."""
    events = []

    if server_item:
        logger.info("New server event")
        tmp_event = event_template.copy()
        tmp_event["Detail"] = json.dumps({"notification_type": "new server",
                                          "server_name": server_item["sk"],
                                          "server_region": server_item["region"]})
        events.append(tmp_event)
    return events


def prepare_playerinfo_list(stats, sk):
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    for guid, player_stats in stats.items():
        item_list.append({"pk": "player#" + guid, "sk": sk})
    return item_list


def get_batch_items(item_list, ddb_table, dynamodb, log_stream_name):
    """Get items in a batch."""
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list, 'ProjectionExpression': 'pk, sk, #data_value', 'ExpressionAttributeNames': {'#data_value': 'data'}}})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            logger.warning("Items do not exist" + item_info)
    return result


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


def add_team_info(gamestats):
    """Add a new element with teams and players in one string."""
    teams = "TeamA:error;TeamB:error"

    try:
        new_total_stats = {}
        match_id = gamestats['gameinfo']['match_id']
        new_total_stats[match_id] = convert_stats_to_dict(gamestats["stats"])
        teamA, teamB, aliases, team_mapping, alias_team_str = build_teams(new_total_stats)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to make teams" + error_msg
        logger.warning(message)

    return teamA, teamB, aliases, team_mapping, alias_team_str


def fix_stats_nesting(gamestats):
    stats_new_object = []
    if len(gamestats["stats"]) == 2:
            logger.info("Number of items in stats: 2")
            for k,v in gamestats["stats"][0].items():
                stats_new_object.append({k:v})
            for k,v in gamestats["stats"][1].items():
                stats_new_object.append({k:v})
            logger.info("New statsall has " + str(len(stats_new_object)) + " players")
    else:
        stats_new_object = gamestats["stats"]
    return stats_new_object


def make_win_loss_dict(match_id, tmp_stats_unnested, match_summary, teamA, teamB):
    win_loss_dict = {}
    player_win_loss_ind = "Draw"

    for player_item in tmp_stats_unnested:
        for playerguid, stat in player_item.items():
            if "round2" in match_summary["results"].get(match_id, {}):
                        winnerAB = match_summary["results"].get(match_id).get("winnerAB")
                        if winnerAB == "Draw":
                            player_win_loss_ind = "Draw"
                        elif winnerAB == "TeamA":
                            if playerguid in teamA:
                                player_win_loss_ind = "Win"
                            else:
                                player_win_loss_ind = "Loss"
                        elif winnerAB == "TeamB":
                            if playerguid in teamB:
                                player_win_loss_ind = "Win"
                            else:
                                player_win_loss_ind = "Loss"
            else:
                player_win_loss_ind = "R1MSB"
            win_loss_dict.setdefault(playerguid, player_win_loss_ind)
    return win_loss_dict
            

def test_get_log_records(num_of_files, more_recent_sk):
    """Test function to retrieve DynamoDB records with pk='match' and sk beginning with specified prefix."""
    try:
        # Create range for between operation - from prefix to next lexicographic value
        start_key = 'log#00000000000#intake/20250516-005744-1747356459.txt'
        # Increment the last character to create upper bound
        end_key = more_recent_sk
        
        # Query DynamoDB for records with pk="match" and sk between the range
        response = table.query(
            KeyConditionExpression=Key('pk').eq('match') & Key('sk').between(start_key, end_key),
            ProjectionExpression='sk',
            Limit=num_of_files,
            ScanIndexForward=False  # Get most recent records first
        )
        
        # Extract sk values into test_files array
        test_files = []
        for item in response.get('Items', []):
            test_files.append(item["sk"].split("#")[2])
        
        logger.info(f"Retrieved {len(test_files)} records")        
        return test_files
        
    except ClientError as e:
        logger.error(f"Error querying DynamoDB: {e.response['Error']['Message']}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return []


if __name__ == "__main__":
    # Test the log records retrieval function
    test_files = test_get_log_records(14, "log#9999999999#intake/20240620-034750-1718854749.txt")
    
    # Original test code
    test_files = ["intake/20250913-062548-1757743496.txt"]
    
    print("first " + test_files[0])
    print("last " + test_files[-1])
    for test_file_name in test_files:
        event = {"Records":
             [
                 {
                     "body": "{\"Records\":[{\"s3\":{\"bucket\":{\"name\":\"rtcwprostats\"},\"object\":{\"key\":\"@filename\"}}}]}"
                 }
             ]
        }
        event["Records"][0]["body"] = event["Records"][0]["body"].replace("@filename", test_file_name)
        handler(event, None)
