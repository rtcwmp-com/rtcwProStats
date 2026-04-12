import boto3
import logging
from boto3.dynamodb.conditions import Key, Attr
import json
import numpy as np
import csv
from datetime import datetime
from botocore.exceptions import ClientError
import time as _time

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


def get_groups(ddb_table, region, type_, num_rows):
    pk = "group"
    begins_with = region + "#" + type_
    response = ddb_table.query(IndexName='gsi1',
                               KeyConditionExpression=Key('gsi1pk').eq(pk) & Key("gsi1sk").begins_with(begins_with),
                               FilterExpression=Attr('games').eq(4) | Attr('games').eq(3),
                               Limit=num_rows,
                               ScanIndexForward=False)

    group_match_keys = {}
    dedup_response = []
    for record in response["Items"]:
        matches_arr = json.loads(record['data'])
        tmp_group_matches = []
        for match in matches_arr:
            tmp_group_matches.append(str(match))
        group_key = ".".join(sorted(tmp_group_matches))
        if group_key in group_match_keys:
            print("Skip duplicate group")
            continue
        else:
            group_match_keys[group_key] = 1
            dedup_response.append(record)

    group_items = []
    group_item_dups = []
    for record in dedup_response:
        group_key = record["sk"]
        if group_key.split("#")[0] not in group_item_dups:
            group_items.append({"pk": "groupcache#stats", "sk": group_key.split("#")[0]})
            group_item_dups.append(group_key.split("#")[0])

    num_items = len(group_items)
    start = 0
    batch_size = 100
    batches = int(np.ceil(num_items / batch_size))

    item_list_list = []
    for i in range(1, batches + 1):
        item_list_list.append(group_items[start: start + batch_size])
        start += batch_size

    big_response = []
    for item_list in item_list_list:
        print("Getting batch of " + str(len(item_list)))
        response2 = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list}})
        big_response.extend(response2["Responses"][ddb_table.name])

    rows = []
    guids = {}
    skipped = 0
    try:
        for group_cache in big_response:
            js = json.loads(group_cache['data'])
            group_id = js["match_id"].split(" ")[1]

            teama = []
            teamb = []
            teamapanz = 0
            teambpanz = 0
            team_a_result = ""
            stats_tmp = convert_stats_to_dict(js["statsall"])
            keep_row = True

            if "elos" not in js:
                print("Skipping. No elos found for " + group_id)
                skipped += 1
                continue
            elif "classes" not in js:
                print("Skipping. No classes found for " + group_id)
                skipped += 1
                continue

            for guid, values in stats_tmp.items():
                cells = []
                panz = False

                if "classes" in js:
                    if guid in js["classes"] and js["classes"][guid] == "Panzer":
                        panz = True

                if guid in js["elos"]:
                    player_elo = js["elos"][guid][1]
                    guids[guid] = [js["elos"][guid][0], js["elos"][guid][1]]
                else:
                    print("Skipping. No elo found for guid " + guid)
                    keep_row = False

                if values["team"] == "TeamA":
                    if panz and teamapanz == 0:
                        teamapanz = player_elo
                    else:
                        teama.append(player_elo)
                if values["team"] == "TeamB":
                    if panz and teambpanz == 0:
                        teambpanz = player_elo
                    else:
                        teamb.append(player_elo)

            if "match_summary" not in js:
                print("Skipping. No match summary " + group_id)
                keep_row = False

            scoreA = 0
            scoreB = 0
            results = js["match_summary"]["results"]
            for match_id, match_res in results.items():
                if match_res["winnerAB"] == "TeamA":
                    scoreA += 1
                elif match_res["winnerAB"] == "TeamB":
                    scoreB += 1
                elif match_res["winnerAB"] == "Draw":
                    scoreA += 1
                    scoreB += 1
                else:
                    print("Who is the winner??" + match_res["winnerAB"])

            if scoreA > scoreB:
                team_a_result = "WIN"
            elif scoreA < scoreB:
                team_a_result = "LOSS"
            else:
                print("Skipping a draw " + group_id)
                keep_row = False

            if teamapanz == 0 or teambpanz ==0:
                keep_row = False
                print("Skipping no panzer " + group_id)


            if keep_row:
                print("Adding " + group_id)
                cells.append(np.mean(teama))
                cells.append(np.median(teama))
                cells.append(np.max(teama))
                cells.append(np.min(teama))
                cells.append(teamapanz if teamapanz > 0 else np.mean(teama))

                cells.append(np.mean(teamb))
                cells.append(np.median(teamb))
                cells.append(np.max(teamb))
                cells.append(np.min(teamb))
                cells.append(teambpanz if teambpanz > 0 else np.mean(teamb))
                cells.append(team_a_result)
                rows.append(cells)
            else:
                skipped +=1
    except:
        print("Something didnt work out for " + group_id)
        skipped += 1

    total_per_region = len(big_response)
    print(f"Skipped {skipped} out of {total_per_region} groups for this region")
    return rows


def guids_to_csv(guids):
    fields = ["guid", "real_name", "current_elo"]
    guid_rows = []
    for guid, vals in guids.items():
        guid_rows.append([guid, vals[0], vals[1]])
    with open("rt_guids.csv", 'w') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile, lineterminator='\n')
        csvwriter.writerow(fields)
        csvwriter.writerows(guid_rows)


def matches_to_csv(matches, type="training"):
    fields = ["mean_a", "median_a", "max_a", "min_a", "panz_a", "mean_b", "median_b", "max_b", "min_b", "panz_b"]
    if type == "training":
        fields.append("result")
    with open("rt_" + type + ".csv", 'w') as csvfile:
        # creating a csv writer object
        csvwriter = csv.writer(csvfile, lineterminator='\n')

        # columns
        csvwriter.writerow(fields)

        # data
        if type == "training":
            csvwriter.writerows(matches)
        else:
            matches_no_win = []
            for match in matches:
                matches_no_win.append(match[0:-1])
            csvwriter.writerows(matches_no_win)


def convert_stats_to_dict(stats):
    stats_tmp = {}
    for player in stats:
        stats_tmp.update(player)
    return stats_tmp

if __name__ == "__main__":
    regions = ["na", "eu", "sa", "unk"]
    types = ["6", "3", "6plus", "unk"]
    matches_na = get_groups(ddb_table, regions[0], types[0], num_rows=1000)
    # matches_eu = get_groups(ddb_table, regions[1], types[0], num_rows=400)
    matches = matches_na # + matches_eu
    print("Final number of matches: " + str(len(matches)))
    training_rows_cutoff = int(len(matches)*0.8)
    matches_to_csv(matches[0:training_rows_cutoff], type="training")
    matches_to_csv(matches[training_rows_cutoff:], type="testing")
