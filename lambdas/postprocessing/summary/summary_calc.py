import logging
from botocore.exceptions import ClientError
import json
import boto3
import time as _time
from datetime import datetime

from summary_achievements import Achievements
from notify_discord import post_custom_bus_event

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("summary_calc")
logger.setLevel(log_level)
  
def process_rtcwpro_summary(ddb_table, ddb_client, event_client, match_id, log_stream_name, CUSTOM_BUS):
    "RTCWPro pipeline specific logic."
    t1 = _time.time()
    sk = match_id
    message = ""
    
    response = get_item("statsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        stats = json.loads(response["data"])
        match_region_type = response['gsi1pk'].replace("statsall#","")
        logger.info("Retrieved statsall for " + str(len(stats)) + " players")
        stats = convert_stats_to_dict(stats)
    else:
        logger.error("Failed to retrieve statsall: " + sk)
        logger.error(json.dumps(response))
        message += "Error in getting stats" + response["error"]
        return message
    
    response = get_item("wstatsall", sk, ddb_table, log_stream_name)
    if "error" not in response:
        wstats = json.loads(response["data"])
        logger.info("Retrieved wstatsall for " + str(len(wstats)) + " players")
    else:
        logger.error("Failed to retrieve wstatsall: " + sk)
        logger.error(json.dumps(response))
        message += "Error in getting wstats" + response["error"]
        return message
    
    item_list = []
    real_name_list = prepare_playerinfo_list(stats, "realname")
    item_list.extend(real_name_list)
    
    aggstats_item_list = prepare_playerinfo_list(stats, "aggstats#" + match_region_type)
    item_list.extend(aggstats_item_list)
    
    aggwstats_item_list = prepare_playerinfo_list(stats, "aggwstats#" + match_region_type)
    item_list.extend(aggwstats_item_list)
    
    response = get_batch_items(item_list, ddb_table, log_stream_name)
    
    stats_old = {}
    real_names = {}
    wstats_old = {}

    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            
            if "data" in result:
                if "aggstats" in result["sk"]:
                    stats_old[guid] = result["data"]
                if result["sk"] == "realname":
                    real_names[guid] = result["data"]
                if "aggwstats" in result["sk"]:
                    wstats_old[guid] = result["data"]
    else:
        if "Items do not exist" in response["error"]:
            logger.warning("Starting fresh.")
        else:   
            logger.error("Failed to retrieve any batch records.")
            logger.error(json.dumps(response))
            message += "Error in summary_calc.get_batch_items" + response["error"]
            return message
   
    new_wstats = {}
    for wplayer_wrap in wstats:
        for wplayer_guid, wplayer in wplayer_wrap.items():
            new_wplayer = {}
            for weapon in wplayer:
                new_wplayer[weapon["weapon"]] = weapon
            new_wstats[wplayer_guid] = new_wplayer
    
    # build updated stats summaries
    stats_dict_updated = build_new_stats_summary(stats, stats_old)
    
    games_dict = {}
    for s in stats_dict_updated:
        games_dict[s] = int(stats_dict_updated[s]["games"])
    
    stats_items = ddb_prepare_statswstats_items("stats", stats_dict_updated, match_region_type, real_names, games_dict)
    
    # build updated wtats summaries
    wstats = new_wstats
    wstats_dict_updated = build_new_wstats_summary(wstats, wstats_old)
    wstats_items = ddb_prepare_statswstats_items("wstats", wstats_dict_updated, match_region_type, real_names, games_dict)

    try:
        achievements_class = Achievements(stats)
        potential_achievements = achievements_class.potential_achievements
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to process summary achievements for a match " + match_id + "\n" + error_msg
        logger.info(message)
    
    potential_achievements_item_list = prepare_old_achievements_list(potential_achievements, match_region_type)
    item_list = []
    item_list.extend(potential_achievements_item_list)
    achievments_old_response = get_batch_items(item_list, ddb_table, log_stream_name)
    
    achievements_old = {}
    if "error" in achievments_old_response:
        if "Items do not exist" not in achievments_old_response["error"]:
             message = "Failed to retrieve old achievements for " + match_id
             logger.error(message)   
        # else ok
    else:
        for achievement_item in achievments_old_response:
            guid = achievement_item["pk"].split("#")[1]
            achievement_code = achievement_item["sk"].split("#")[1]
            achievements_old[guid + "#" + achievement_code] = float(achievement_item["gsi1sk"])
    
    achievement_items = ddb_prepare_achievement_items(potential_achievements, achievements_old, real_names, match_region_type, match_id)

    # submit updated summaries
    items = []
    items.extend(stats_items)
    items.extend(wstats_items)
    items.extend(achievement_items)

    try:
        ddb_batch_write(ddb_client, ddb_table.name, items)
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to load aggregate stats for a match " + match_id + "\n" + error_msg
        logger.error(message)
    else:
        message = "Elo progress records inserted.\n"
    
    
    
    time_to_write = str(round((_time.time() - t1), 3))
    logger.info(f"Time to process summaries is {time_to_write} s")
    message += "Records were summarized"
    
    if len(achievement_items) > 0:
        try:
            events = announce_new_achievements(achievement_items, match_region_type, CUSTOM_BUS)
            post_custom_bus_event(event_client, events)
        except Exception as ex:
            template = "gamelog_calc.ddb_batch_write: An exception of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            message = "Failed to insert/announce achievements for a match.\n" + error_msg
        else:
            message = "Achievements records inserted."
    else:
        message = "No achievements to insert this time."
    
    return message

def announce_new_achievements(achievement_items, match_region_type, CUSTOM_BUS):
    """Prepare an event about new group for discord announcement."""
    events = []

    event_template = {
        'Source': 'rtcwpro-pipeline',
        'DetailType': 'Discord notification',
        'Detail': '',
        'EventBusName': CUSTOM_BUS
    }

    achievements = {}
    for achievement in achievement_items:
        achievements_key = achievement['real_name'] + "#" + achievement['sk'].split("#")[1]
        achievements_value = int(achievement['gsi1sk'])
        achievements[achievements_key] = achievements_value
    tmp_event = event_template.copy()
    tmp_event["Detail"] = json.dumps({"notification_type": "new achievements",
                                      "achievements": achievements,
                                      "match_type": match_region_type})
    events.append(tmp_event)
    return events

def convert_stats_to_dict(stats):
    if len(stats) == 2 and len(stats[0]) > 1: #stats grouped in teams in a list of 2 teams , each team over 1 player
        logger.info("Number of stats entries 2, trying to merge teams")
        stats_tmp = stats[0].copy()
        stats_tmp.update(stats[1])
    else:
        logger.info("Merging list into dict.")
        stats_tmp = {}
        for player in stats:
            stats_tmp.update(player)
    logger.info("New statsall has " + str(len(stats_tmp)) + " players in a " + str(type(stats_tmp)))
    return stats_tmp

def build_new_stats_summary(stats, stats_old):
    """Add up new and old stats."""
    stats_dict_updated = {}
    for guid in stats:
        metrics = stats[guid]["categories"]
        stats_dict_updated[guid] = {}
        for metric in metrics:
            if guid not in stats_old:
                stats_dict_updated[guid][metric] = int(metrics[metric])
                continue
            if metric in stats_old[guid]:
                if metric not in ["accuracy","efficiency", "killpeak"]:
                    stats_dict_updated[guid][metric] = int(stats_old[guid][metric]) + int(metrics[metric])
            else:
                stats_dict_updated[guid][metric] = int(metrics[metric])
                
        new_acc = metrics["hits"]/metrics["shots"]
        stats_dict_updated[guid]["accuracy"] = int(new_acc)
        
        efficiency = 100*stats_dict_updated[guid]["kills"]/(stats_dict_updated[guid]["kills"] + stats_dict_updated[guid]["deaths"])                
        stats_dict_updated[guid]["efficiency"] = int(efficiency)
        stats_dict_updated[guid]["killpeak"] = max(stats_dict_updated[guid].get("killpeak",0),metrics.get("killpeak",0))
                
                
        stats_dict_updated[guid]["games"] = stats_old.get(guid,{}).get("games",0) + 1
    return stats_dict_updated

def build_new_wstats_summary(wstats, wstats_old):
    """Add up new and old stats."""
    wstats_dict_updated = {}
    for guid, wstat in wstats.items():
        wstats_dict_updated[guid] = {}
        for weapon, metrics in wstat.items():
            wstats_dict_updated[guid][weapon] = {}

            for metric in metrics:
                if metric == "weapon":
                    continue
                if guid not in wstats_old:
                    wstats_dict_updated[guid][weapon][metric] = int(metrics[metric])
                    continue
                if weapon not in wstats_old[guid]:
                    wstats_dict_updated[guid][weapon][metric] = int(metrics[metric])
                    continue
                if metric in wstats_old[guid][weapon]:
                    wstats_dict_updated[guid][weapon][metric] = int(wstats_old[guid][weapon][metric]) + int(metrics[metric])
                else:
                    wstats_dict_updated[guid][weapon][metric] = int(metrics[metric])
            wstats_dict_updated[guid][weapon]["games"] = wstats_old.get(guid,{}).get(weapon, {}).get("games",0) + 1
    return wstats_dict_updated
        
def wstat(new_wstats, guid, weapon, metric):
    """Safely get a number from a deeply nested dict."""
    if guid not in new_wstats:
        value = 0
    elif weapon not in new_wstats[guid]:
        value = 0
    elif metric not in new_wstats[guid][weapon]:
        value = 0
    else:
        value = new_wstats[guid][weapon][metric]
    return value


def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}


def get_item(pk, sk, table, log_stream_name):
    """Get one dynamodb item."""
    item_info = pk + ":" + sk + ". Logstream: " + log_stream_name
    try:
        response = table.get_item(Key={'pk': pk, 'sk': sk})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if "Item" in response:
            result = response['Item']
        else:
            result = make_error_dict("[x] Item does not exist: ", item_info)
    return result


def prepare_playerinfo_list(stats, sk):
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    for guid, player_stats in stats.items():
        item_list.append({"pk": "player#" + guid, "sk": sk})
    return item_list

def prepare_old_achievements_list(potential_achievements, match_region_type):
    """Make a list of achievements to retrieve from ddb."""
    """Make a list of guids to retrieve from ddb."""
    item_list = []
    for achievement, achievement_items in potential_achievements.items():
        for guid, value in achievement_items.items():
            item_list.append({"pk": "player#" + guid, "sk": "achievement#" + achievement + "#" + match_region_type,})
    return item_list


def get_batch_items(item_list, ddb_table, log_stream_name):
    """Get items in a batch."""
    dynamodb = boto3.resource('dynamodb')
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list, 'ProjectionExpression': 'pk, sk, #data_value, gsi1sk', 'ExpressionAttributeNames': {'#data_value': 'data'}}})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result


def update_player_info_stats(ddb_table, stats_dict_updated, stats_type):
    """Does not work because missing keys like "na#6#elo throw errors."""
    # Example: ddb_table.update_item(Key=Key, UpdateExpression="set elos.#eloname = :elo, elos.#gamesname = :games", ExpressionAttributeNames={"#eloname": "na#6#elo", "#gamesname": "na#6#games"}, ExpressionAttributeValues={':elo': 134, ':games' : 135})
    update_expression="set " + stats_type + " = :stat"
    for guid, new_stat in stats_dict_updated.items():
        logger.info("Updating player " + stats_type + ": " + guid)
        key = { "pk": "player" , "sk": "playerinfo#" + guid }
            
        expression_values = {':stat': new_stat}
        ddb_table.update_item(Key=key, 
                              UpdateExpression=update_expression, 
                              ExpressionAttributeValues=expression_values)
        
        
def ddb_prepare_statswstats_items(stat_type, dict_, match_region_type, real_names, games_dict):
    items = []
    for guid, player_stat in dict_.items():
        item = ddb_prepare_stat_item(stat_type, guid, match_region_type, player_stat, real_names, games_dict.get(guid, 0))
        items.append(item)
    return items

def ddb_prepare_stat_item(stat_type, guid, match_region_type, player_stat, real_names, real_games): 
    if stat_type == "stats":
        sk = "aggstats#" + match_region_type
        gsi1pk = "leaderkdr#" + match_region_type
        try:
            deaths = player_stat["deaths"] if player_stat["deaths"] > 0 else 1
            kdr = player_stat["kills"]/deaths
            games = player_stat["games"]
        except: 
            logger.warning("Could not calculate KDR for guid")
            kdr = 0.0  
            games = 0
        kdr_str = str(round(kdr,1)).zfill(3)
        gsi1sk = kdr_str
        logger.debug("Setting new agg stats for " + guid + " with kdr of " + str(kdr_str))
    elif stat_type == "wstats":
        sk = "aggwstats#" + match_region_type
        gsi1pk = "leaderacc#" + match_region_type
        try:
            acc = calculate_accuracy(player_stat)
            games = real_games
        except: 
            logger.warning("Could not calculate KDR for guid")
            acc = 0.0
            games = 0
        acc_str = str(round(acc,1)).zfill(4)
        gsi1sk =  acc_str
        logger.debug("Setting new agg stats for " + guid + " with acc of " + str(acc_str))
    
    
    real_name = real_names.get(guid, "no_name#")
    
    item = {
            'pk'            : "player"+ "#" + guid,
            'sk'            : sk,
            # 'lsipk'         : "",
            'gsi1pk'        : gsi1pk,
            'gsi1sk'        : gsi1sk,
            'data'          : player_stat,
            'games'         : games,
            "real_name"     : real_name
        }
    return item

def ddb_prepare_achievement_items(potential_achievements, achievements_old, real_names, match_region_type, match_id):
    """Figure out which potential achievements will make the cut and cast them to dynamodb format."""
    items = []
    ts = datetime.now().isoformat()
    for achievement, achievement_table in potential_achievements.items():
        for guid, player_value in achievement_table.items():
            if int(player_value) > int(achievements_old.get(guid + "#" + achievement,0)):
                item = {
                    'pk'            : "player" + "#" + guid,
                    'sk'            : "achievement#" + achievement + "#" + match_region_type,
                    'lsipk'         : "achievement#" + ts,
                    'gsi1pk'        : "leader#" + achievement + "#" + match_region_type,
                    'gsi1sk'        : str(player_value).zfill(6),
                    "real_name"     : real_names.get(guid, "no_name#"),
                    "match_id"      : match_id
                }
                items.append(item)
    return items

def calculate_accuracy(player_stat):
    try:
        hits = shots = 0
        for weapon, wstat in player_stat.items():
            if weapon in ['MP-40','Thompson','Sten', 'Colt', 'Luger']:
                hits += wstat["hits"]
                s = wstat["shots"] if wstat["shots"] > 0 else 1
                shots += s
        acc = round(hits/shots*100, 1)
    except:
        acc = 0.0
    return acc


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
    item_batch = { table_name: []}
    item_batch_list = items[start_num : start_num + batch_size]
    if len(item_batch_list) < 1:
        return None
    for item in item_batch_list:
        item_serialized = {k: serializer.serialize(v) for k,v in item.items()}
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
            request_items = create_batch_write_structure(table_name,items, start, batch_size)
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
                sleep_time = 5 #seconds
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
                        sleep_time = 5 #seconds
                        logger.warning(f"Sleeping for {sleep_time} seconds")
                        _time.sleep(sleep_time)

                # Inserted all the unprocessed items, exit loop
                logger.warning('Unprocessed items successfully inserted')
                break
            if response["ResponseMetadata"]['HTTPStatusCode'] != 200:
                messages += f"\nBatch {start} returned non 200 code"
            start += 25