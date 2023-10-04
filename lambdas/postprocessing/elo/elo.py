import boto3
import logging
import os
from elo_calc import process_rtcwpro_elo
import datetime

# for local testing use actual table
# for lambda execution runtime use dynamic reference
if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
ddb_client = boto3.client('dynamodb')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('elo')
logger.setLevel(log_level)


def handler(event, context):
    """Calculate elo changes for this match."""
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    if not isinstance(event, int):
        logger.warning("Received unexpected input for match_id " + str(event))
        return {"error": "Exiting function due to bad input."}
    else:
        match_id = str(event)

    logger.info("Processing match id " + match_id)
    
    process_rtcwpro_elo(ddb_table, ddb_client, match_id, log_stream_name)

    return { "matchid": int(match_id) }


if __name__ == "__main__":
    event = 1675102720
    handler(event, None)

    # we don't want to runs this more than once...
    if datetime.datetime.now().month == 2 and datetime.datetime.now().day == 1:

        write_once = False
        if write_once:

            from boto3.dynamodb.conditions import Attr, Key

            matches_response1 = ddb_table.query(
                KeyConditionExpression=Key('pk').eq("match") & Key("sk").between("16709628642", "16752168362"),
                ProjectionExpression="pk,sk",
                ScanIndexForward=True)

            matches_response2 = ddb_table.query(
                KeyConditionExpression=Key('pk').eq("match") & Key("sk").between("16745511142", "16752168362"),
                ProjectionExpression="pk,sk",
                ScanIndexForward=True)

            matches_responses = []
            matches_responses.extend(matches_response1["Items"])
            matches_responses.extend(matches_response2["Items"])

            match_array = []
            for match_item in matches_responses:
                # print(datetime.fromtimestamp(int(match_item["sk"][0:-1])))
                if match_item["sk"][-1:] == "2":
                    match_array.append(match_item["sk"][0:-1])
            with open('elo_reprocess.txt', 'w') as f:
                for line in match_array:
                    f.write(f"{line}\n")


        with open("elo_reprocess.txt") as file:
            matches = file.read().splitlines()

        with open("elo_reprocessed.txt") as file:
            matches_processed = file.read().splitlines()

        matches_processed_file = open("elo_reprocessed.txt", "a")
        matches_errored_file = open("elo_errored.txt", "a")

        for match in matches:
            if match not in matches_processed:
                match_round_id = match + "2"
                try:
                    print("Processing match " + match_round_id)
                    handler(int(match), None)
                except Exception as ex:
                    matches_errored_file.write(match + "\n" + str(ex))
                    print("Failed " + match_round_id + "\n" + str(ex))
                finally:
                    matches_processed_file.write(match+ "\n")
            else:
                print("Skipping match " + match)

        matches_processed_file.close()
        matches_errored_file.close()




