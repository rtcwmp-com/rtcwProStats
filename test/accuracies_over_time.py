import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr
import json

TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(log_level)

log_stream_name = "local"

def get_stats_per_match_collection(matches, period):
    match_ids = {}

    for match in matches["Items"]:
        match_ids[match['lsipk'].split("#")[2][0:-1]] = 1


    item_list = []
    for match in match_ids:
        item_list.append({"pk": "wstatsall", "sk": match})

    wstats_response_1 = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list[0:100]}})
    wstats_response_2 = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list[100:]}})

    wstats_response = []
    wstats_response.extend(wstats_response_1["Responses"]["rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"])
    wstats_response.extend(wstats_response_2["Responses"]["rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"])

    players = []
    for wstats in wstats_response:
        for player in json.loads(wstats["data"]):
            for player_guid, player_weapons in player.items():
                for weapon in player_weapons:
                    if weapon["weapon"] in ["MP-40","Thompson"]:
                        players.append([period, player_guid, weapon["weapon"], weapon["hits"], weapon["shots"], weapon["headshots"]])

    return players

matches_1 = ddb_table.query(IndexName='lsi', KeyConditionExpression=Key('pk').eq("match") & Key("lsipk").between("eu#6#1675209600", "eu#6#1676160000")) # 2/1 0am to 2/12 0am
matches_2 = ddb_table.query(IndexName='lsi', KeyConditionExpression=Key('pk').eq("match") & Key("lsipk").between("eu#6#1676678400", "eu#6#1677628500")) # 2/18 0am to 2/28 23:55
players_1 = get_stats_per_match_collection(matches_1, "first")
players_2 = get_stats_per_match_collection(matches_2, "second")

players_all = players_1 + players_2

import csv
with open('players.csv', 'w', newline='') as file:
    writer = csv.writer(file)

    writer.writerow(["period","guid","weapon","hits","shots","headshots"])
    for player in players_all:
        writer.writerow(player)








