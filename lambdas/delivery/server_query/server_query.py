import sys
import json
import boto3
from boto3.dynamodb.conditions import Attr, Key
from botocore.exceptions import ClientError
import botocore.config
import logging
import os
import decimal
import datetime
import traceback

if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("server_query")
logger.setLevel(log_level)


def handler(event, context):
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    # print('request: {}'.format(json.dumps(event)))
    api_path = event["resource"]
    logger.info("incoming request " + api_path)
    data = make_error_dict("Unhandled path: ", api_path)
    response = ""

    if api_path == "/serverquery":
        try:
            hacked_request = False
            body_str = event["body"]
            try:
                server_status = json.loads(body_str)
            except:
                logger.warning("Failed to load string as is. Trying hackery.")
                last_comma_index = body_str.rfind("},")
                hacked_string = body_str[0:last_comma_index + 1] + "}}"
                logger.info("New hacked_string")
                logger.info(hacked_string)
                server_status = json.loads(hacked_string)
                hacked_request = True
            logger.info("Received server status")
            logger.info(server_status)
            server_status["caller_ip"] = event["headers"].get("X-Forwarded-For", "1.1.1.1")
            logger.info("Caller IP " + server_status["caller_ip"])
            command_text = server_status.get("command", "")
            command_tokens = command_text.split(" ")
            command = command_tokens[0].lower()
            logger.info("incoming command " + command_tokens[0])

            parameter1 = None
            if len(command_tokens) > 1:
                logger.info("incoming param1 " + command_tokens[1])
                parameter1 = command_tokens[1].lower()

            if command == "help":
                response = prepare_help_response()
            elif command == "test":
                response = server_status
            elif command == "whois":
                if parameter1:
                    logger.info("incoming param1 " + parameter1)
                    player_match_str = parameter1
                    response = prepare_player_whois_response(server_status, player_match_str, log_stream_name,
                                                             hacked_request)
                else:
                    response = prepare_whois_response(server_status, log_stream_name, hacked_request)
            elif command == "last":
                type_ = "6"
                if parameter1:
                    logger.info("incoming param1 " + parameter1)
                    if parameter1 in ["3", "6"]:
                        type_ = parameter1
                response = prepare_last_match_response(server_status, type_, log_stream_name, hacked_request)
            elif command == "servers":
                region = None
                if parameter1:
                    logger.info("incoming param1 " + parameter1)
                    if parameter1 in ["na", "eu", "sa"]:
                        region = parameter1
                response = prepare_servers_response(server_status, region, log_stream_name, hacked_request)
            elif command == "ai":
                question = " ".join(command_tokens[1:])
                logger.info("incoming AI question: " + question)
                response = [call_managed_prompt(question)]
            elif command.strip() == "":
                response = ["An empty string was supplied after /api. Try /api help"]
            else:
                response = ["Unknown command " + command_text]
        except ValueError as ex:
            logger.info("Still failing to read json string: ")
            logger.info(event["body"])
            logger.warning(str(ex))
            response = [str(ex)]  # this looks like json
        except Exception as ex:
            template = "A general error of type {0} occurred. Arguments:\n{1!r}"
            error_msg = template.format(type(ex).__name__, ex.args)
            message = "Failed to process request " + "\n" + error_msg
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback, limit=5, file=sys.stdout)
            logger.info(event["body"])
            logger.error(message)
            response = "Request failed terribly."

    logger.info("The following response was sent.")
    logger.info(response)
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response, default=default_type_error_handler)
    }


def prepare_help_response():
    response_2darr = []
    headers = ["Available commands (type in /api command argument)"]
    response_2darr.append(headers)
    # response_2darr.append(['test', '', '', ''])
    response_2darr.append(["whois - display player's real names and elos. Arguments: partial string"])
    response_2darr.append(["last - display last match elo deltas. Arguments: 3 or 6"])
    response_2darr.append(["servers - display active servers. Arguments: na or eu or sa"])
    response_2darr.append(["ai - send a question to AI. Arguments - text_with_underscores"])
    response_string = format_response(response_2darr, ["3"], [60], True, True)
    return response_string


def prepare_whois_response(server_status, log_stream_name, hacked_request):
    response_2darr = []

    headers = ["alias", "real name", "elo", "games"]

    response_2darr.append(headers)
    if "players" not in server_status:
        raise ValueError('Player list is not part of the payload.')

    if len(server_status["players"]) == 0:
        raise ValueError('Player list is empty.')

    players = server_status["players"]
    region_code = get_server_region_code(server_status.get("server_name", "no_server"))

    item_list = []
    logger.info("Getting real names and elos.")
    query_list = prepare_playerinfo_list(players, region_code)
    item_list.extend(query_list)

    response = get_batch_items(item_list, ddb_table, log_stream_name)
    real_names, elos, games = process_whois_batch_response(response)

    for guid, player_props in players.items():
        col1 = player_props["alias"]
        col2 = real_names.get(guid, "noname2")
        col3 = elos.get(guid, "1111")
        col4 = str(games.get(guid, 0))
        line = [col1, col2, col3, col4]
        response_2darr.append(line)

    response_string = format_response(response_2darr, ["1", "2", "3", "4"], [15, 15, 6, 8], True, True)
    if hacked_request:
        footer_str = format_response([["Request was hacked!"]], ["3"], [30], True, False)
        response_all_json = json.loads(response_string) + json.loads(footer_str)
        response_string = json.dumps(response_all_json)

    return response_string


def prepare_last_match_response(server_status, type_, log_stream_name, hacked_request):
    if "players" not in server_status:
        raise ValueError('Player list is not part of the payload.')

    if len(server_status["players"]) == 0:
        raise ValueError('Player list is empty.')

    players = server_status["players"]
    region_code = get_server_region_code(server_status.get("server_name", "no_server"))

    logger.info("Getting last match.")
    last_three_matches = ddb_table.query(IndexName='lsi',
                                         KeyConditionExpression=Key('pk').eq('match') & Key('lsipk').begins_with(
                                             region_code + "#" + type_ + "#"), Limit=3, ScanIndexForward=False)
    last_match = None
    if "Items" in last_three_matches:
        for match_info in last_three_matches["Items"]:
            if match_info["sk"][-1:] == "2":
                last_match = match_info
                break

        if not last_match:
            raise ValueError("Could not find a full match.")

    match_data = json.loads(last_match["data"])
    match_id = last_match["sk"][:-1]
    logger.info("Got last match_rnd_id " + last_match["sk"])
    round_ = match_data.get("round", "0")
    map_ = match_data.get("map", "map")
    time_limit = match_data.get("time_limit", "8:00")
    date_time_human = match_data.get("date_time_human", "2021-07-31")
    last_server_name = match_data.get("server_name", "server_")[0:20]
    winner = "r1msb" if round_ == "1" else match_data.get("winner", "")
    winner = "unknown" if winner == "" else winner

    header1 = [f"Match: {match_id} Round {round_}"]
    header2 = [f"Played on {last_server_name} at {date_time_human} UTC(EST+5)"]
    header3 = [f"Map: {map_}. Time to beat: {time_limit}. Winner: {winner}"]
    match_headers = [header1, header2, header3]

    logger.info("Getting elo progress for " + match_id)
    elo_progress = ddb_table.query(IndexName='gsi1', KeyConditionExpression=Key('gsi1pk').eq('eloprogressmatch') & Key(
        'gsi1sk').begins_with(match_id))

    response_2darr = []
    headers = ["Player", "*Score", "Elo+-", "New Elo"]
    response_2darr.append(headers)
    if "Items" in elo_progress:
        logger.info("Got elo progress items " + str(elo_progress['Count']))
        for progress_item in elo_progress["Items"]:
            col1 = progress_item.get("real_name", "n/a")
            col2 = str(progress_item.get("performance_score", "n/a"))
            col3 = str(progress_item.get("data", "n/a")).rjust(2)
            col4 = str(progress_item.get("elo", "n/a")).rjust(3)
            line = [col1, col2, col3, col4]
            response_2darr.append(line)

    response_string1 = format_response(match_headers, ["3"], [60], True, False)
    response_string2 = format_response(response_2darr, ["1", "2", "3", "4"], [20, 8, 8, 8], True, True)
    response_all_json = json.loads(response_string1) + json.loads(response_string2)

    # no need for this because response does not depend on request quality
    # if hacked_request:
    #     footer_str = format_response([["Request was hacked!"]], ["3"], [60], True, False)
    #     response_all_json = response_all_json + json.loads(footer_str)

    response_string = json.dumps(response_all_json)

    cut_to_length = True
    while cut_to_length:
        if len(response_string) < 1024:
            cut_to_length = False
        else:
            logger.warning("Response too long. Cutting")
            response_all_json = response_all_json[0:-1]
            response_string = json.dumps(response_all_json)

    return response_string


def prepare_player_whois_response(server_status, player_match_str, log_stream_name, hacked_request):
    if "players" in server_status:
        players = server_status["players"]
    else:
        raise ValueError('Player list is not part of the payload.')

    if len(players) == 0:
        raise ValueError('Player list is empty.')

    if len(player_match_str) > 5:
        player_match_str = player_match_str[0:5]

    logger.info("Matching string to player.")
    player_guid = None
    for guid in players:
        if player_match_str in players[guid]["alias"]:
            logger.info("String matched player: " + players[guid]["alias"])
            player_guid = guid
            player_alias = players[guid]["alias"]
            break

    if not player_guid:
        response_string = ["^3Could not match anyone to ^1" + player_match_str]
    else:
        region_code = get_server_region_code(server_status.get("server_name", "no_server"))
        response_string = player_guid

        skoal = get_skoal()

        if player_guid in skoal:
            player_guid = "22b0e88467093a63d5dd979eec2631d1"
            logger.info("Guid replaced due to being on skoal")

        pk = "player" + "#" + player_guid

        response = get_items_pk(pk, ddb_table, log_stream_name)
        player_data = process_player_response(response)
        real_name = player_data.get("real_name","noname3")
        region_type = region_code + "#6"
        elo = player_data["elos"][region_type]["elo"]
        games = player_data["elos"][region_type]["games"]
        kdr = player_data["kdr"][region_type]
        accuracy = player_data["acc"][region_type]
        damage_ratio = round(player_data["aggstats"][region_type]["damagegiven"] / (
                    player_data["aggstats"][region_type]["damagereceived"] + 1) * 100, 1)

        rows = []
        rows.append([f"String ^1{player_match_str} ^3matched {player_alias}"])
        rows.append([f"Real name   : {real_name}"])
        rows.append([f"Stats region: NA/6v6"])
        rows.append([f"ELO         : {elo}"])
        rows.append([f"Games       : {games}"])
        rows.append([f"KDR         : {kdr}"])
        rows.append([f"Acc. perc.  : {accuracy}"])
        rows.append([f"DMG/DMR perc: {damage_ratio}"])

        response_string = format_response(rows, ["3"], [50], True, False)

    return response_string


def prepare_servers_response(server_status, region, log_stream_name, hacked_request):
    if "server_name" in server_status:
        server_name = server_status["server_name"]
    else:
        raise ValueError('Server is not part of the payload.')

    logger.info("Looking up server name.")
    if region:
        region_code = region
    else:
        region_code = get_server_region_code(server_status.get("server_name", "no_server"))

    logger.info("Looking up active servers in region: " + region_code)
    response = get_rest_api_response("servers", {"region_code": region_code})
    logger.info("Got response json of length " + str(len(response)))

    header1 = [f"Active servers in ^1{region_code}"]
    server_headers = [header1]

    response_2darr = []
    headers = ["IP", "Name", "Last Game"]
    response_2darr.append(headers)
    for server in response:
        col1 = server.get("IP", "Ip.Not.Found")
        col2 = server.get("server_name", "Server name not found")[0:28]
        col3 = server.get("last_submission", "No-date")[0:10]
        line = [col1, col2, col3]
        response_2darr.append(line)

    response_string1 = format_response(server_headers, ["3"], [60], True, False)
    response_string2 = format_response(response_2darr, ["1", "3", "3"], [18, 30, 10], True, True)
    response_all_json = json.loads(response_string1) + json.loads(response_string2)
    response_string = json.dumps(response_all_json)
    return response_string


def format_response(response_2darr, colors, spaces, json_format, format_headers):
    response_lines = []
    endline = "/n"

    if len(response_2darr[0]) == len(colors) == len(spaces):
        header_line = ""

        next_index = 0
        if format_headers:
            next_index = 1
            for i, header in enumerate(response_2darr[0]):
                header_line += "^" + colors[i] + header.ljust(spaces[i])
            response_lines.append(header_line)

            headers_sep_line = "-" * (len(header_line) - len(colors) * 2)
            response_lines.append(headers_sep_line)

        for i, row in enumerate(response_2darr[next_index:]):
            response_line = ""
            for j, cell in enumerate(row):
                response_line += "^" + colors[j] + cell.ljust(spaces[j])
            response_lines.append(response_line)

    response_string = json.dumps(response_lines) if json_format else endline.join(response_lines)

    # print(response_string)
    return response_string


def prepare_playerinfo_list(players, region, type_=None):
    """Make a list of guids to retrieve from ddb."""
    if not type_:
        type_ = "6"
    item_list = []
    # this can be a list of guids or dict of players where keys are guids
    for guid in players:
        item_list.append({"pk": "player#" + guid, "sk": "realname"})
        item_list.append({"pk": "player#" + guid, "sk": "elo" + "#" + region + "#" + type_})
    return item_list


def get_server_region_code(server_name):
    region = ""
    server = None
    logger.info("Getting region code for " + server_name)
    if server_name:
        server = ddb_get_server(server_name, ddb_table)
    else:
        logger.warning("No server name is found in the server payload.")
        region = "na"

    if server:
        region = server.get("region", "na")
        if region == "unk":
            region = "na"
    else:
        region = "na"
    logger.info("Returning region " + region)
    return region


def ddb_get_server(sk, table):
    result = None
    try:
        response = table.get_item(Key={'pk': "server", 'sk': sk})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        raise
    else:
        if "Item" in response:
            result = response['Item']
        else:
            logger.warning(f"Server {sk} was not found.")
    return result


def process_whois_batch_response(response):
    real_names = {}
    elos = {}
    games = {}
    if "error" not in response:
        for result in response:
            guid = result["pk"].split("#")[1]
            if "realname" in result["sk"]:
                real_names[guid] = result.get("data", "noname1")
            if "elo" in result["sk"]:
                elo = result["pk"].split("#")[1]
                elos[guid] = result.get("data", "1111")
                games[guid] = result.get("games", "0")
    else:
        logger.warning("Failed to retrieve any real names.")
    return real_names, elos, games


# https://stackoverflow.com/questions/63278737/object-of-type-decimal-is-not-json-serializable
def default_type_error_handler(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj)
    else:
        logger.error("Default type handler could not handle object" + str(obj))
    raise TypeError


def get_skoal():
    """
    Get list of players that wish not to be on ladders and personal profiles.
    """
    pk = "skoal"
    sk = "v0"
    skoal = []
    try:
        skoal_response = get_item(pk, sk, ddb_table, "skoal_get")
        skoal = json.loads(skoal_response.get("skoal", "[]"))
    except:
        logger.error("Could not get skoal.")
    return skoal


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


def get_items_pk(pk, table, log_stream_name):
    """Get several items by pk."""
    item_info = pk + " Logstream: " + log_stream_name
    try:
        response = table.query(KeyConditionExpression=Key('pk').eq(pk), Limit=100, ReturnConsumedCapacity='NONE')
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result


def get_query_all(pk_name, pk, ddb_table, log_stream_name, limit):
    """Get several items by pk."""
    item_info = pk + ". Logstream: " + log_stream_name
    try:
        response = ddb_table.query(KeyConditionExpression=Key(pk_name).eq(pk), Limit=limit)
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response['Count'] > 0:
            result = response['Items']
        else:
            result = make_error_dict("[x] Items do not exist: ", item_info)
    return result


def get_batch_items(item_list, ddb_table, log_stream_name):
    """Get items in a batch."""
    dynamodb = boto3.resource('dynamodb')
    item_info = "get_batch_items. Logstream: " + log_stream_name
    try:
        response = dynamodb.batch_get_item(RequestItems={ddb_table.name: {'Keys': item_list}})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if len(response["Responses"][ddb_table.name]) > 0:
            result = response["Responses"][ddb_table.name]
        else:
            result = make_error_dict("[x] Item does not exist: ", item_info)
    return result


def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}


def process_event_responses(api_path, responses):
    if "error" not in responses:
        # logic specific to /events
        data = []
        for event in responses:
            data_line = {}

            data_line["eventtype"] = event["eventtype"]
            data_line["eventdesc"] = event.get("eventdesc", "n#a")
            data_line["timestamp"] = event["gsi2sk"]

            data.append(data_line)
    else:
        data = responses
    return data


def process_group_responses(responses):
    if "error" in responses:
        data = responses
    else:
        data = {}
        for response in responses:  # these should be sorted in order
            group_name = response["sk"].split("#")[0]
            group = {}
            group["matches"] = json.loads(response["data"])
            group["cached"] = response.get("cached", "No")
            group["teams"] = response.get("teams", "A: player1, player2 B: player3, player4")
            group["games"] = int(response.get("games", len(group["matches"])))
            group["finish_human"] = response.get("finish_human", "2021-07-31 23:59:59")
            group["duration_nice"] = response.get("duration_nice", "00:00")
            data[group_name] = group
    return data


def process_player_response(response):
    data = {}
    data["elos"] = {}
    data["aggstats"] = {}
    data["aggwstats"] = {}
    data["kdr"] = {}
    data["acc"] = {}
    data["achievements"] = {}
    data["real_name"] = ""
    data["last_seen"] = ""

    if "error" in response:
        data = response
    else:
        try:
            for item in response:
                if "elo#" in item["sk"]:
                    data["elos"][item["sk"].replace("elo#", "")] = {}
                    data["elos"][item["sk"].replace("elo#", "")]["elo"] = int(item["data"])
                    data["elos"][item["sk"].replace("elo#", "")]["games"] = int(item["games"])
                if "realname" in item["sk"]:
                    data["real_name"] = item["data"]
                    data["last_seen"] = item.get("updated", "2021-07-31T22:21:34.211247")
                if "aggstats#" in item["sk"]:
                    data["aggstats"][item["sk"].replace("aggstats#", "")] = item["data"]
                    data["kdr"][item["gsi1pk"].replace("leaderkdr#", "")] = float(item["gsi1sk"])
                if "aggwstats#" in item["sk"]:
                    data["aggwstats"][item["sk"].replace("aggwstats#", "")] = item["data"]
                    data["acc"][item["gsi1pk"].replace("leaderacc#", "")] = float(item["gsi1sk"])
                if "achievement#" in item["sk"]:
                    data["achievements"][item["sk"].replace("achievement#", "")] = float(item["gsi1sk"])
        except:
            item_info = "unkown"
            if len(response) > 0:
                if "pk" in response[0]:
                    item_info = item["pk"]
            data["error"] = "Could not process player response for " + item_info
            logger.error(data["error"])
    return data


def process_eloprogress_response(response, filter_old_elos):
    data = []
    if "error" in response:
        data = response
    else:
        try:
            for item in response:
                elo_delta = {}
                # leader_line['updated'] = item["updated"]
                elo_delta['value'] = int(item["data"])
                elo_delta['elo'] = int(item["elo"])
                elo_delta['match_id'] = int(item.get("gsi1sk", 0))
                elo_delta['real_name'] = item.get("real_name", "no_name#")
                if filter_old_elos:
                    if int(item["elo"]) > 500:  # leave off old values
                        data.append(elo_delta)
                else:
                    data.append(elo_delta)
        except:
            item_info = "unknown"
            if len(response) > 0:
                if "pk" in response[0]:
                    item_info = item["pk"]
            data = make_error_dict("Could not process leader response.", item_info)
            logger.error(data["error"])
    if filter_old_elos and len(data) == 0:
        data = process_eloprogress_response(response, False)
    return data

def get_rest_api_response(domain, params):
    import urllib3

    base_url = "https://rtcwproapi.donkanator.com/"
    response = []

    if domain == "servers":
        region_code = params.get("region_code", "na")
        api_url = f"{base_url}servers/region/{region_code}/active"
    else:
        logger.error("Bad api domain" + str(domain))
        return response

    logger.info("Contacting url: " + api_url)
    http = urllib3.PoolManager()
    response_http = http.request('GET', api_url)
    if response_http.status == 200:
        response = json.loads(response_http.data.decode('utf-8'))  # body
    else:
        logger.error("Bad response status from API call " + str(response_http.status))
    return response


def call_managed_prompt(question):
    model_id = "arn:aws:bedrock:us-east-1:793070529856:prompt/FDDU5BIOUF:5"
    bedrock_runtime = boto3.client('bedrock-runtime', region_name="us-east-1",
                                   config=botocore.config.Config(read_timeout=30, retries={'max_attempts': 1}))
    body = {"promptVariables":{"query":{"text":question.replace("_", " ")}}}  # TODO: maybe next versions of RTCW server will allow more than 2 tokens
    response = bedrock_runtime.invoke_model(
        body=json.dumps(body),
        modelId=model_id,
        accept="application/json",
        contentType="application/json"
    )

    response_content = response.get('body').read().decode('utf-8')
    response_data = json.loads(response_content)
    response_answer = response_data["output"]["message"]["content"][0]["text"]

    finish_reason = response_data.get("error")

    if finish_reason is not None:
        raise Exception(f"Text generation error. Error is {finish_reason}")

    logger.info("Successfully generated text with model %s", model_id)
    response_safe = ''.join(e for e in response_answer if e.isalnum() or e == " ")
    return response_safe


if __name__ == "__main__":
    event_whois = {
        "resource": "/serverquery",
        "headers": {"X-Forwarded-For": "127.0.0.1"},
        "body": "{\"server_name\":\"Virginia RtCWPro na\",\"command\":\"whois\",\"players\":{\"b3465bff43fe40ea76f9e522d3314809\":{\"alias\":\"wolfprayer\",\"team\":\"Axis\"}}}"
    }
    event_whois_player = {
        "resource": "/serverquery",
        "headers": {"X-Forwarded-For": "127.0.0.1"},
        "body": "{\"server_name\":\"Virginia RtCWPro na\",\"command\":\"whois wolf\",\"players\":{\"b3465bff43fe40ea76f9e522d3314809\":{\"alias\":\"wolfprayer\",\"team\":\"Axis\"}}}"
    }
    event_help = {
        "resource": "/serverquery",
        "headers": {"X-Forwarded-For": "127.0.0.1"},
        "body": "{\"server_name\":\"Virginia RtCWPro na\",\"command\":\"help\",\"players\":{\"b3465bff43fe40ea76f9e522d3314809\":{\"alias\":\"wolfprayer\",\"team\":\"Axis\"}}}"
    }

    event_last = {
        "resource": "/serverquery",
        "headers": {"X-Forwarded-For": "127.0.0.1"},
        "body": "{\"server_name\":\"Virginia RtCWPro na\",\"command\":\"last 3\",\"players\":{\"b3465bff43fe40ea76f9e522d3314809\":{\"alias\":\"wolfprayer\",\"team\":\"Axis\"}}}"
    }

    event_servers_default = {
        "resource": "/serverquery",
        "headers": {"X-Forwarded-For": "127.0.0.1"},
        "body": "{\"server_name\":\"Virginia RtCWPro na\",\"command\":\"servers\",\"players\":{\"b3465bff43fe40ea76f9e522d3314809\":{\"alias\":\"wolfprayer\",\"team\":\"Axis\"}}}"
    }

    event_servers_region = {
        "resource": "/serverquery",
        "headers": {"X-Forwarded-For": "127.0.0.1"},
        "body": "{\"server_name\":\"Virginia RtCWPro na\",\"command\":\"servers sa\",\"players\":{\"b3465bff43fe40ea76f9e522d3314809\":{\"alias\":\"wolfprayer\",\"team\":\"Axis\"}}}"
    }

    event_ai = {
        "resource": "/serverquery",
        "headers": {"X-Forwarded-For": "127.0.0.1"},
        "body": "{\"server_name\":\"Virginia RtCWPro na\",\"command\":\"ai who is john mullins\",\"players\":{\"b3465bff43fe40ea76f9e522d3314809\":{\"alias\":\"wolfprayer\",\"team\":\"Axis\"}}}"
    }
    handler(event_ai, None)
