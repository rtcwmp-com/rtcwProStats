import json
import boto3
from botocore.exceptions import ClientError
import logging
import os
import decimal
import datetime

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

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(data, default=default_type_error_handler)
    }


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
    event = {
        "resource": "/groups/add",
        "headers": {
            "X-Forwarded-For": "199.247.45.106",
        },
        "body": "{ \"region\": \"eu\" , \"type\": \"6\", \"group_name\": \"TavernLan\", \"matches\": [1650704272,1650705209,1650705680,1650706311,1650710108,1650710424,1650711397,1650711801,1650712051,1650712414,1650713049,1650713530,1650715757,1650715891,1650716294,1650716516,1650717913,1650718272,1650718771,1650719559,1650723830,1650724474,1650724756,1650725740,1650726148,1650727192,1650727321,1650728553,1650730913,1650731488,1650731810,1650732247,1650732925,1650733548,1650734082,1650734719,1650737576,1650737739,1650738342,1650738663,1650739065,1650739729,1650739769,1650740799,1650744317,1650745118,1650745342,1650745546,1650745703,1650746074,1650746682,1650748077,1650750759,1650751196,1650752000,1650752512,1650752545,1650753609,1650753749,1650754000,1650792166,1650793332,1650793354,1650794553,1650795758,1650796100,1650796186,1650797594,1650804106,1650804382,1650804905,1650804979,1650805499,1650806725,1650806820,1650808429,1650811605,1650812225,1650813040,1650813493,1650815406,1650819146,1650819983,1650820852,1650821774]  }"
    }
    print(handler(event, None))
