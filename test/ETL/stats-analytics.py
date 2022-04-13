import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr
import json
import numpy as np
import csv
from datetime import datetime
from botocore.exceptions import ClientError
import time as _time
import pandas as pd

# for local testing use actual table
# for lambda execution runtime use dynamic reference
TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger()
logger.setLevel(log_level)

log_stream_name = "local"


def get_stats(ddb_table, region, type_, num_rows):
    pk = "statsall#" + region + "#" + type_
    responses = ddb_table.query(IndexName='gsi1',
                               KeyConditionExpression=Key('gsi1pk').eq(pk),
                               Limit=num_rows,
                               ScanIndexForward=False)
    
    values_array = []
    for response in responses['Items']:
        for player in json.loads(response['data']):
            for player_guid, player_props in player.items():
                row = []
                row.append(region + "#" + type_)
                row.append(response['sk'])
                row.append(player_props['alias'])
                row.append(player_props['categories']['kills'])
                row.append(player_props['categories']['gibs'])
                row.append(player_props['categories']['headshots'])
                row.append(player_props['categories']['damagegiven'])
                row.append(player_props['categories']['revives'])
                row.append(player_props['categories']['ammogiven'])
                row.append(player_props['categories']['healthgiven'])
                row.append(player_props['categories']['obj_captured'])
                row.append(player_props['categories']['obj_destroyed'])
                values_array.append(row)
    df = pd.DataFrame(values_array)
    df.columns = ["regiontype", "match","alias", "kills","gibs","headshots","damagegiven","revives","ammogiven","healthgiven","obj_captured","obj_destroyed"]
        
    return df



if __name__ == "__main__":
    regions = ["na", "eu", "sa", "unk"]
    types = ["6", "3", "6plus", "unk"]
    df_na6 = get_stats(ddb_table, regions[0], types[0], num_rows=400)
    df_eu6 = get_stats(ddb_table, regions[1], types[0], num_rows=400)
    df_all = df_na6.append(df_eu6)
    
    quantile = .75
    df_all[["kills","gibs","headshots","obj_captured","obj_destroyed"]].quantile(quantile)
    df_all[df_all['revives']>0]['revives'].quantile(quantile)
    #non-panzer gibs
    df_all[df_all['ammogiven']>0]['gibs'].quantile(quantile)
    df_all[df_all['healthgiven']>0]['gibs'].quantile(quantile)
     
    df_all[df_all['ammogiven']>0]['ammogiven'].quantile(quantile)
    df_all[df_all['healthgiven']>0]['healthgiven'].quantile(quantile)
    df_all[df_all['obj_destroyed']>0]['obj_destroyed'].quantile(quantile)
    print("Final number of players: " + str(len(df_all)))
    df_all.to_csv("player_stats.csv")



