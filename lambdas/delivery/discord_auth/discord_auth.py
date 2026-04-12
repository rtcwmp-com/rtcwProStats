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
import urllib3
from datetime import datetime, timedelta, UTC
import secrets

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

DISCORD_CLIENT_ID = "1464528354937143378"
DISCORD_CLIENT_SECRET = "GZJL8TCnIvOvhQBSa8OU17ucD4QtWM4i"
DISCORD_REDIRECT_URI = "http://localhost:3000/admin/callback"


def handler(event, context):
    if __name__ == "__main__":
        log_stream_name = "local"
    else:
        log_stream_name = context.log_stream_name

    print('request: {}'.format(json.dumps(event)))
    api_path = event["resource"]
    logger.info("incoming request " + api_path)
    data = []

    try:

        if api_path == "/discordauth/token":
            logger.info("Processing " + api_path)
            data = handle_token_exchange(event, log_stream_name)

        elif api_path == "/admin/tasks":
            logger.info("Processing " + api_path)

            is_admin = False
            if "Authorization" in event["headers"]:
                auth_header = event["headers"].get("Authorization", "")[7:]  # skip "Bearer "
                is_admin = verify_discord_user(auth_header, log_stream_name)

            data = get_task_list(is_admin, log_stream_name)

        elif api_path == "/admin/tasks/{task_id}/status":
            logger.info("Processing " + api_path)
            auth_header = event["headers"].get("Authorization", "")[7:]  # skip "Bearer "
            body_json = json.loads(event["body"])
            status = body_json.get("approval_status")
            task_id = event["pathParameters"]["task_id"]

            updated = update_task_status(task_id, status)

            if updated:
                data = {
                    "id": "1",
                    "approval_status": status
                }
            else:
                data = make_error_dict("Failed to update task status", task_id)
        elif api_path == "/discordauth/logout":
            logger.info("Processing " + api_path)
            auth_header = event["headers"].get("Authorization", "")[7:]  # skip "Bearer "
            data = logout(auth_header, log_stream_name)
        else:
            logger.error("Unhandled path: " + api_path)
            data = make_error_dict("Unhandled path: ", api_path)

    except Exception as ex:
        template = "A general error of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        message = "Failed to process request " + "\n" + error_msg
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_exception(exc_type, exc_value, exc_traceback, limit=5, file=sys.stdout)
        if "body" in event:
            logger.error(event["body"])
        logger.error(message)
        response = "Request failed terribly."

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(data, default=default_type_error_handler)
    }


def logout(auth_header, log_stream_name):
    """Remove a Discord session token from the database."""
    pk = "session_token"
    sk = auth_header

    try:
        response = ddb_table.delete_item(Key={'pk': pk, 'sk': sk})
        print(response)
        logger.info("Deleted token from db")
        return True
    except ClientError as e:
        logger.error(e.response['Error']['Message'])
        return False


def handle_token_exchange(event, log_stream_name):
    body = json.loads(event.get("body", "{}"))
    code = body.get("code")
    state = body.get("state")

    if not code:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing OAuth code"})
        }

    logger.info("Received Discord OAuth code")

    http = urllib3.PoolManager()

    # Step 1: Exchange code for Discord access token
    token_url = "https://discord.com/api/oauth2/token"
    payload = {
        "client_id": DISCORD_CLIENT_ID,
        "client_secret": DISCORD_CLIENT_SECRET,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": DISCORD_REDIRECT_URI
    }

    encoded_payload = urllib3.request.urlencode(payload)

    token_response = http.request(
        "POST",
        token_url,
        body=encoded_payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"}
    )

    if token_response.status != 200:
        logger.error("Discord token exchange failed")
        logger.error("Status code: " + str(token_response.status))
        logger.error(token_response.data.decode("utf-8"))
        data = {"error": "Failed to exchange OAuth code"}
        return {"statusCode": 401, "body": json.dumps(data)}

    discord_tokens = json.loads(token_response.data.decode("utf-8"))
    access_token = discord_tokens.get("access_token")

    logger.info("Discord access token acquired")

    # Step 2: Fetch Discord user info
    user_response = http.request(
        "GET",
        "https://discord.com/api/users/@me",
        headers={"Authorization": f"Bearer {access_token}"}
    )

    if user_response.status != 200:
        logger.error("Failed to fetch Discord user info")
        data = {"error": "Failed to fetch Discord user info"}
        return {"statusCode": 401, "body": json.dumps(data)}

    discord_user = json.loads(user_response.data.decode("utf-8"))
    discord_id = discord_user["id"]
    logger.info("Discord user ID: " + discord_id)

    # # Step 3: Check if user is an admin
    pk = "adminuser"
    sk = discord_id
    response = get_item(pk, sk, ddb_table, log_stream_name)
    admin_ind = "N"

    if "error" in response:
        logger.warning("User is not an admin")
    else:
        logger.info("Admin user validated")
        admin_ind = "Y"

    # Step 4: Generate admin token
    session_token = generate_admin_token()

    # Store session
    session_token_item = ddb_prepare_token_item(session_token, discord_id, admin_ind)
    ddb_put_item(session_token_item, ddb_table)
    logger.info("Saved token to db")

    # Step 5: Build response
    data = {
        "token": session_token,
        "user": {
            "discordId": discord_id,
            "username": discord_user.get("username"),
            "avatar": discord_user.get("avatar")
        }
    }
    logger.info("Returning token and user info")
    return data


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
            logger.error(
                "Unhandled HTTP Code " + str(http_code) + " while submitting item " + pk + ":" + sk + "\n" + str(
                    response))
    return response


def ddb_prepare_token_item(session_token, discord_id, admin_ind):
    """Make a ddb item for a match group."""
    ts = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
    expire_at = datetime.now(UTC) + timedelta(days=30)
    expire_at_unix = int(expire_at.timestamp())
    group_item = {
        'pk': 'session_token',
        'sk': session_token,
        'discord_id': discord_id,
        'admin_ind': admin_ind,
        'ExpirationTime': int(expire_at_unix)
    }
    return group_item


def generate_admin_token():
    return secrets.token_urlsafe(32)


def verify_discord_user(token, log_stream_name):
    """Verify that a user is a Discord admin."""
    pk = "session_token"
    sk = token
    response = get_item(pk, sk, ddb_table, log_stream_name)

    if "error" in response:
        logger.info("session_token not found in database")
        return False
    else:
        if response.get("admin_ind", "N") == "Y":
            logger.info("User is admin")
            return True
        else:
            logger.info("User is not an admin")
            return False


def get_task_list(is_admin, log_stream_name):
    pk = "admintask"
    response = get_items_pk(pk, ddb_table, log_stream_name)

    data = []
    if "error" in response:
        data = []
    else:
        try:
            for item in response:
                task = {}
                task["id"] = item.get("sk", "")
                task["name"] = item.get("name", "")
                task["description"] = item.get("description", "")
                task["approval_status"] = item.get("approval_status", "")
                task["createdAt"] = item.get("createdAt", "")
                task["createdBy"] = item.get("createdBy", "")
                task["isAdmin"] = is_admin
                data.append(task)
        except:
            item_info = "unknown"
            if len(response) > 0:
                if "pk" in response[0]:
                    item_info = item["pk"]
            data = make_error_dict("Could not process task response.", item_info)
            logger.error(data["error"])
    return data


def update_task_status(task_id, status):
    key = {"pk": "admintask", "sk": task_id}
    update_expression = "set approval_status = :approval_status"
    expression_values = {':approval_status': status}
    ddb_table.update_item(Key=key,
                          UpdateExpression=update_expression,
                          ExpressionAttributeValues=expression_values)
    return True


# https://stackoverflow.com/questions/63278737/object-of-type-decimal-is-not-json-serializable
def default_type_error_handler(obj):
    if isinstance(obj, decimal.Decimal):
        return int(obj)
    else:
        logger.error("Default type handler could not handle object" + str(obj))
    raise TypeError


def make_error_dict(message, item_info):
    """Make an error message for API gateway."""
    return {"error": message + " " + item_info}


if __name__ == "__main__":
    event_token = {
        "resource": "/discordauth/token",
        "headers": {"X-Forwarded-For": "127.0.0.1"},
        "body": "{\"server_name\":\"Virginia RtCWPro na\"}"
    }
    event_tasks = {
        "resource": "/admin/tasks",
        "headers": {"Authorization": "kek"}
    }
    event_logout = {
        "resource": "/discordauth/logout",
        "headers": {"Authorization": "kek"}
    }
    handler(event_token, None)
