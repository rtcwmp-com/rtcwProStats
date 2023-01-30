import requests
import os
import time
import boto3
import json
import logging
from boto3.dynamodb.conditions import Attr, Key

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("test_submit")
logger.setLevel(log_level)

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table("rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE")

'''
post this to git on windows or just linux
curl --location --request POST 'https://rtcwproapi.donkanator.com/serverquery' \
--header "Content-Type: application/json" \
--data '{"a":"b"}'
'''

def get_matches(starting_key, region, type_, limit):
    match_type = region + "#" + type_ + "#"
    response = table.query(IndexName="lsi",
                           KeyConditionExpression=Key("pk").eq("match") & Key("lsipk").begins_with(match_type),
                           Limit=limit,
                           ScanIndexForward=False)
                           # ,ExclusiveStartKey={"pk": {"S": "match"}, "lsipk": {"S": match_type + "16748768752"}})

    if "Items" not in response:
        logger.warning("Items not found.")
    else:
        matches = response['Items']
        logger.info(f"Found matches: {str(len(matches))}")
    return matches


def get_stats(match_id):
    logger.debug("Getting stats for " + match_id)
    response = table.get_item(Key={"pk": "statsall", 'sk': match_id}, ReturnConsumedCapacity='TOTAL')
    if "Item" not in response:
        logger.warning("Item statsall not found.")
    else:
        logger.info("Cost of the call: " + str(response['ConsumedCapacity']['CapacityUnits']) + " capacity units")
        statsall = json.loads(response['Item']["data"])
        num_players = len(statsall)
        logger.info(f"Found statsall for {match_id} with {num_players} players")
    return statsall


def server_query(server_status_json):
    str_len = str(float(len(server_status_json) / 1024))
    logger.info(f"Submitting string of {str_len} KB")

    url_api = 'https://rtcwproapi.donkanator.com/'
    url_path_submit = 'serverquery'
    url = url_api + url_path_submit
    response = requests.post(url, headers={'Content-Type': 'application/json'}, data=json.dumps(server_status_json))
    print(response.text)


def make_test_jsons(limit):
    matches = get_matches("1123321", "na", "6", limit)

    test_jsons = []
    server_status = {}
    server_status["format"] = "v1"
    try:
        logger.info("Scanning through " + str(len(matches)) + " matches.")
        for match in matches:
            match_data = json.loads(match["data"])
            logger.info("looping through " + match_data["match_id"])

            server_status["server_name"] = match_data.get("server_name", "no_server_name")
            server_status["map"] = match_data.get("map", "no_server_name")
            try:
                stats_all = get_stats(match_data["match_id"])
                server_status["caller"] = list(stats_all[0].keys())[0]
                server_status["current_round"] = match_data.get("round", "2")
                # fake value of match + 100 seconds
                server_status["unix_time"] = match_data["match_id"] + "100"
                server_status["players"] = {}
                for stat in stats_all:
                    guid = list(stat.keys())[0]
                    server_status["players"][guid] = {}
                    server_status["players"][guid]["alias"] = stat[guid].get("alias", "noname")
                    server_status["players"][guid]["team"] = stat[guid].get("team", "spec")
                test_jsons.append(server_status)
            except ConnectionError as ex:
                logger.error("Connection error.")
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                error_msg = template.format(type(ex).__name__, ex.args)
                logger.error(error_msg)
                time.sleep(10)
            except Exception as ex:
                template = "A submit exception of type {0} occurred. Arguments:\n{1!r}"
                error_msg = template.format(type(ex).__name__, ex.args)
                logger.error(error_msg)

            time.sleep(2)
        print("Out of matches cycle.")
    except Exception as ex:
        template = "A general loop error of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to match " + match_data["match_id"] + "\n" + error_msg
        logger.error(message)
    return test_jsons

if __name__ == "__main__":
    server_json_calls = make_test_jsons(1)

    for i in server_json_calls:
        i["command"] = "whois"

    for j in server_json_calls:
        print(json.dumps(j))
        server_query(j)






