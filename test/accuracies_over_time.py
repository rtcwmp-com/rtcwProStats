import math

import boto3
import logging
from boto3.dynamodb.conditions import Key
import json
import csv
import datetime

TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(log_level)

log_stream_name = "local"


def get_stats_per_match_collection(matches):
    match_ids = {}

    for match in matches["Items"]:
        match_ids[match['lsipk'].split("#")[2][0:-1]] = 1

    item_list = []
    for match in match_ids:
        item_list.append({"pk": "wstatsall", "sk": match})

    chunks = int(len(item_list) / 100) + 1

    wstats_responses = []
    for chunk in range(chunks):
        range_low = chunk*100
        range_high = (chunk+1)*100
        print("querying wstats chunks", range_low, range_high)
        wstats_response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list[range_low:range_high]}})
        wstats_responses.extend(wstats_response["Responses"]["rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"])

    print("Processing all wstats of length:", len(wstats_responses))
    unique_guids = []
    real_name_items = []
    for wstats in wstats_responses:
        for player in json.loads(wstats["data"]):
            for player_guid in player:
                if player_guid not in unique_guids:
                    unique_guids.append(player_guid)
                    real_name_items.append({"pk": "player#" + player_guid, "sk": "realname"})
    names_response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': real_name_items, 'ProjectionExpression': 'pk, gsi1sk'}})

    name_dict = {}
    for name_item in names_response["Responses"]["rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"]:
        name_dict[name_item['pk'][7:]] = name_item['gsi1sk'][9:]

    players = []
    for wstats in wstats_responses:
        for player in json.loads(wstats["data"]):
            for player_guid, player_weapons in player.items():
                for weapon in player_weapons:
                    if weapon["weapon"] in ["MP-40", "Thompson"]:
                        players.append([datetime.datetime.fromtimestamp(int(wstats['sk'])).strftime('%Y-%m-%d %H:%M:%S'), wstats['sk'], player_guid, name_dict[player_guid], weapon["weapon"], weapon["hits"], weapon["shots"], weapon["headshots"]])

    return players


matches = ddb_table.query(IndexName='lsi', KeyConditionExpression=Key('pk').eq("match") & Key("lsipk").between("na#6#1735689600", "na#6#1743705313"))
players = get_stats_per_match_collection(matches)

with open('players.csv', 'w', newline='') as file:
    writer = csv.writer(file)

    writer.writerow(["datetime", "match", "guid", "name", "weapon", "hits", "shots", "headshots"])
    for player in players:
        writer.writerow(player)
