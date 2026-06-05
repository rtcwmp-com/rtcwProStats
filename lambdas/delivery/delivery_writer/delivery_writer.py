import json
import boto3
from botocore.exceptions import ClientError
import logging
import os
import decimal
import datetime

from boto3.dynamodb.conditions import Attr, Key

from notify_discord import post_custom_bus_event

if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
    print("\n\n\nSet state machine and custom bus values in console\n\n\n")
    # CUSTOM_BUS = ""
    # MATCH_STATE_MACHINE = ""  # set this at debug time
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']
    MATCH_STATE_MACHINE = os.environ['RTCWPROSTATS_FUNNEL_STATE_MACHINE']
    CUSTOM_BUS = os.environ['RTCWPROSTATS_CUSTOM_BUS_ARN']

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)
sf_client = boto3.client('stepfunctions')
event_client = boto3.client('events')

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger("retriever")
logger.setLevel(log_level)


def handler(event, context):
    """AWS Lambda handler."""
    # print('request: {}'.format(json.dumps(event)))
    api_path = event["resource"]
    logger.info("incoming request " + api_path)
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    if api_path == "/groups/add":

        data = {}
        submitter_ip = event.get("headers", {}).get("X-Forwarded-For", "")

        try:
            body = event["body"]
            body_json = json.loads(body)
            logger.info("submitted info: " + body)
        except:
            logger.error("Event was")
            logger.error(event)
            message = "Could not get body from the event"
            logger.error(message)
            data = make_error_dict(message, "")

        region = body_json.get("region", None)
        match_type = body_json.get("type", None)
        group_name = body_json.get("group_name", None)
        matches = body_json.get("matches", None)
        if not isinstance(matches, list):
            matches = None

        if not region or not match_type or not group_name or not matches:
            message = "One or more required parameters are missing or incorrect."
            logger.error(message)
            data = make_error_dict(message, "")

        if "error" not in data:
            Item = ddb_prepare_group_item(region, match_type, group_name.replace("#", ""), matches, submitter_ip)
            ddb_put_item(Item, ddb_table)
            message = "Matches added to group " + str(group_name)
            data["response"] = message
            logger.info(message)
            execute_state_machine(group_name)
            region_match_type = region + "#" + match_type

            events = announce_new_group(group_name, region_match_type)
            post_custom_bus_event(event_client, events)
    

    if api_path == "/prediction/add":
        data = {}
        submitter_ip = event.get("headers", {}).get("X-Forwarded-For", "")

        try:
            body = event["body"]
            body_json = json.loads(body)
            logger.info("submitted info: " + body)
        except:
            logger.error("Event was")
            logger.error(event)
            message = "Could not get body from the event"
            logger.error(message)
            data = make_error_dict(message, "")

        session_id = body_json.get("session_id", "123")
        groupgsi1sk = body_json.get("groupgsi1sk", None)
        prediction = body_json.get("prediction", None)

        if not isinstance(session_id, str) or not isinstance(groupgsi1sk, str):
            message = "session_id and groupgsi1sk must be strings."
            logger.error(message)
            data = make_error_dict(message, "")

        if prediction is not None:
            try:
                prediction = int(prediction)
            except (ValueError, TypeError):
                message = "prediction must be a signed integer."
                logger.error(message)
                data = make_error_dict(message, "")
        else:
            message = "prediction is required."
            logger.error(message)
            data = make_error_dict(message, "")
        
        #retireve group details
        group_cache, sk = get_group_cache(groupgsi1sk, ddb_table, log_stream_name)
        

        actual_result = 0
        scorea = 0
        scoreb = 0
        match_summary = group_cache["match_summary"]
        for match_id, match_result in match_summary["results"].items():
            if match_result["winnerAB"] == "TeamA":
                actual_result = actual_result - 1
                scorea = scorea + 1
            elif match_result["winnerAB"] == "TeamB":
                actual_result = actual_result + 1
                scoreb = scoreb + 1
            elif match_result["winnerAB"] == "Draw":
                actual_result = actual_result + 0
                scorea = scorea + 1
                scoreb = scoreb + 1
        logger.info(f"Actual result {actual_result}")
        logger.info("Score a:b " + str(scorea) + ":" + str(scoreb))
        logger.info(f"Prediction {prediction}")

        if (prediction < 0 and actual_result < 0) or \
            (prediction > 0 and actual_result > 0) or \
            (prediction == 0 and actual_result == 0):
                correct = True
        else:
            correct = False

        # --- Accuracy Calculation ---
        error_dist = abs(prediction - actual_result)          # 0 to 6
        accuracy = 1 - (error_dist / 6)                 # 0.0 to 1.0
        accuracy_percent = accuracy * 100          # 0 to 100%

        print("Correct pick:", correct)
        print("Error:", error_dist)
        print("Accuracy %:", round(accuracy_percent, 1))

        total_dist = 6
        data = update_session_score(ddb_table, session_id, correct, error_dist, total_dist)
        write_prediction_record(ddb_table, session_id, sk, correct, error_dist)
        data["scorea"] = scorea
        data["scoreb"] = scoreb
        data["accuracy_percent"] = accuracy_percent

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(data, default=default_type_error_handler)
    }

def write_prediction_record(ddb_table, session_id, sk, correct, error_dist):
    try:
        pred_item = {
                'pk': 'prediction_result',
                'sk': sk + "#" + session_id,
                'session_correct': decimal.Decimal(int(correct))
            }
        ddb_table.put_item(Item=pred_item, ReturnValues="NONE")
        logger.info(f"write_prediction_record failed for {sk} by {session_id}")
    except Exception as e:
        logger.warning(f"write_prediction_record failed for {sk} by {session_id}")


def update_session_score(ddb_table, session_id, current_correct, error_dist, total_dist):
    key = {"pk": "predictions", "sk": session_id}
    update_expression = "SET session_correct = session_correct + :current_correct, session_guesses = session_guesses + :one, session_error_dist = session_error_dist + :error_dist, session_total_dist = session_total_dist + :total_dist"
    expression_values = {
        ':current_correct': decimal.Decimal(int(current_correct)),
        ':one': decimal.Decimal(1),
        ':error_dist': decimal.Decimal(str(error_dist)),
        ':total_dist': decimal.Decimal(str(total_dist))
    }

    condition_expression = "attribute_exists(pk)"
    try:
        response = ddb_table.update_item(Key=key,
                              UpdateExpression=update_expression,
                              ExpressionAttributeValues=expression_values,
                              ConditionExpression=condition_expression,
                              ReturnValues="UPDATED_NEW")
        result = {k: int(v) if isinstance(v, decimal.Decimal) else v for k, v in response.get("Attributes", {}).items()}
        logger.info(f"Updated predicitons for session {session_id}")
        logger.info(result)
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
            # Item doesn't exist yet, create it with initial values
            init_item = {
                'pk': 'predictions',
                'sk': session_id,
                'session_correct': decimal.Decimal(int(current_correct)),
                'session_guesses': decimal.Decimal(1),
                'session_error_dist': decimal.Decimal(str(error_dist)),
                'session_total_dist': decimal.Decimal(str(total_dist))
            }
            response = ddb_table.put_item(Item=init_item, ReturnValues="NONE")
            result = {k: int(v) if isinstance(v, decimal.Decimal) else v for k, v in init_item.items()}
            del result['pk']
            del result['sk']
            logger.info(f"Inserted predicitons for session {session_id}")
            logger.info(result)
        else:
            raise
    
    result["current_correct"] = int(current_correct)
    return result

def get_group_cache(gsi1sk, table, log_stream_name):
    """Get one dynamodb item."""
    gsi1pk = "group"
    item_info = gsi1pk + ":" + gsi1sk + ". Logstream: " + log_stream_name

    try:
        response = table.query(
            IndexName='gsi1',
            KeyConditionExpression=Key('gsi1pk').eq(gsi1pk) & Key('gsi1sk').eq(gsi1sk),
            Limit=1,
            ReturnConsumedCapacity='NONE'
        )
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        result = make_error_dict("[x] Client error calling database: ", item_info)
    else:
        if response["Count"] > 0:
            sk = response['Items'][0]["sk"].split("#")[0]
            logger.info("Identified sk as: " + sk)
        else:
            logger.error("Item does not exist: " + item_info)
            return None
    
    pk = "groupcache#stats"
    item_info = pk + ":" + sk + ". Logstream: " + log_stream_name

    try:
        response = table.get_item(Key={'pk': pk, 'sk': sk})
    except ClientError as e:
        logger.warning("Exception occurred: " + e.response['Error']['Message'])
        logger.error("Client error calling database: " + item_info)
        return None
    else:
        if "Item" in response:
            group_cache = json.loads(response['Item']["data"])
        else:
            logger.error("Item does not exist: " + item_info)
            return None
    return group_cache, sk
    

def execute_state_machine(group_name):
    """Execute step functions with group caching."""
    try:
        response = sf_client.start_execution(
            stateMachineArn=MATCH_STATE_MACHINE,
            input='{"tasktype": "group_cacher","taskdetail": "' + group_name + '"}'
        )
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to start state machine for group_cacher group: " + group_name + "\n" + error_msg
        logger.error(message)
    else:
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            logger.info("Started state machine " + response['executionArn'])
        else:
            logger.warning("Bad response from state machine " + str(response))
            message += "\nState machine failed."
            logger.error(message)


# https://stackoverflow.com/questions/63278737/object-of-type-decimal-is-not-json-serializable
def default_type_error_handler(obj):
    """See comment."""
    if isinstance(obj, decimal.Decimal):
        return int(obj)
    else:
        logger.error("Default type handler could not handle object" + str(obj))
    raise TypeError


def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}


def ddb_put_item(Item, table):
    """Put a single item in ddb."""
    try:
        response = table.put_item(Item=Item)
    except ClientError as err:
        logger.error(err.response['Error']['Message'])
        logger.error("Item was: " + Item["pk"] + ":" + Item["sk"])
        raise
    else:
        pk = Item["pk"]
        sk = Item["sk"]
        http_code = "No http code"

        try:
            http_code = response['ResponseMetadata']['HTTPStatusCode']
        except: 
            http_code = "Could not retrieve http code"
            logger.error("Could not retrieve http code from response\n" + str(response))

        if http_code != 200:
            logger.error("Unhandled HTTP Code " + str(http_code) + " while submitting item " + pk + ":" + sk + "\n" + str(response))
    return response


def ddb_prepare_group_item(region, match_type, group_name, matches, submitter_ip):
    """Make a ddb item for a match group."""
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    group_item = {
        'pk'    : 'group',
        'sk'    : group_name + "#" + ts,
        'lsipk' : region + "#" + match_type + "#" + group_name + "#" + ts,
        'gsi1pk': "group",
        'gsi1sk': region + "#" + match_type + "#" + ts,
        'data'  : json.dumps(matches),
        "submitter_ip": submitter_ip
    }
    return group_item


def announce_new_group(group_name, region_match_type):
    """Prepare an event about new group for discord announcement."""
    events = []

    event_template = {
        'Source': 'rtcwpro-pipeline',
        'DetailType': 'Discord notification',
        'Detail': '',
        'EventBusName': CUSTOM_BUS
    }

    tmp_event = event_template.copy()
    tmp_event["Detail"] = json.dumps({"notification_type": "new group",
                                      "group_name": group_name,
                                      "match_type" : region_match_type})
    events.append(tmp_event)
    return events


# curl -X POST "https://rtcwproapi.donkanator.com/groups/add" -H "content-type: application/json" -H "pass: 123" -d "{ \"region\": \"eu\" , \"type\": \"6\", \"group_name\": \"gather16904\", \"matches\": [1634234950,1634236160,1634238542,1634238932] }"
if __name__ == "__main__":
    group_add_event = {
        "resource": "/groups/add",
        "headers": {
            "X-Forwarded-For": "199.247.45.106",
        },
        "body": "{ \"region\": \"eu\" , \"type\": \"6\", \"group_name\": \"TavernLan\", \"matches\": [1650704272,1650705209,1650705680,1650706311,1650710108,1650710424,1650711397,1650711801,1650712051,1650712414,1650713049,1650713530,1650715757,1650715891,1650716294,1650716516,1650717913,1650718272,1650718771,1650719559,1650723830,1650724474,1650724756,1650725740,1650726148,1650727192,1650727321,1650728553,1650730913,1650731488,1650731810,1650732247,1650732925,1650733548,1650734082,1650734719,1650737576,1650737739,1650738342,1650738663,1650739065,1650739729,1650739769,1650740799,1650744317,1650745118,1650745342,1650745546,1650745703,1650746074,1650746682,1650748077,1650750759,1650751196,1650752000,1650752512,1650752545,1650753609,1650753749,1650754000,1650792166,1650793332,1650793354,1650794553,1650795758,1650796100,1650796186,1650797594,1650804106,1650804382,1650804905,1650804979,1650805499,1650806725,1650806820,1650808429,1650811605,1650812225,1650813040,1650813493,1650815406,1650819146,1650819983,1650820852,1650821774]  }"
    }
    prediction_event = {
        "resource": "/prediction/add",
        "headers": {
            "X-Forwarded-For": "199.247.45.106",
        },
        "body": "{ \"session_id\": \"1234\" , \"groupgsi1sk\": \"na#6#2023-09-29 02:59:02\", \"prediction\": \"-2\"}"
    }
    print(handler(prediction_event, None))
