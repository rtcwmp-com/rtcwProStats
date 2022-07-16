import boto3
import logging
import json
import os
import urllib3
import urllib.parse

# for local testing use actual table
# for lambda execution runtime use dynamic reference
if __name__ == "__main__":
    TABLE_NAME = "rtcwprostats-database-DDBTable2F2A2F95-1BCIOU7IE3DSE"
else:
    TABLE_NAME = os.environ['RTCWPROSTATS_TABLE_NAME']

dynamodb = boto3.resource('dynamodb')
ddb_table = dynamodb.Table(TABLE_NAME)

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('discordhook')
logger.setLevel(log_level)


def handler(event, context):
    """Receive an event."""
    logger.info(json.dumps(event["detail"]))

    hook_url = get_hook_url(event)

    if hook_url:
        payload = build_discord_message(event)
        send_notification(hook_url, payload)
    else:
        logger.info("No hook URL. Exiting without sending a message.")

    logger.info("Done.")


def get_hook_url(event):
    """Get discord hook URL."""
    hook_url = None

    match_type = "test#6"
    if "match_type" in event['detail']:
        match_type = event['detail']['match_type']

    sk = ""
    if event['detail']['notification_type'] in ['new player', 'new server', "new group", "new achievements"]:
        sk = match_type + "#default"

    try:
        logger.info("Looking for hook info on " + sk)
        response_hook = ddb_table.get_item(Key={'pk': "discordhook", 'sk': sk})
        hook_url = response_hook.get("Item", {}).get("discordhook", None)
        hook_active = response_hook.get("Item", {}).get("hookactive", "no")
        if hook_active != "yes":
            hook_url = None
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        error_msg = template.format(type(ex).__name__, ex.args)
        logger.error("Failed to send match notification to discord + \n" + error_msg)
    return hook_url


def build_discord_message(event):
    """Dispatch a content building event."""
    payload = None
    if event["detail"]["notification_type"] == "new player":
        payload = build_new_user_payload(event)
    if event["detail"]["notification_type"] == "new server":
        payload = build_server_payload(event)
    if event["detail"]["notification_type"] == "new group":
        payload = build_new_group_payload(event)
    if event["detail"]["notification_type"] == "new achievements":
        payload = build_new_achievements_payload(event)

    if not payload:
        raise ValueError("No content was created.")
    return payload

def build_new_achievements_payload(event):
    """Create a message about a new server."""
    achievements = event["detail"]["achievements"]
    
    colwith_1 = 15
    colwith_2 = 20
    colwith_3 = 6
    
    headers = ""
    headers += "Name".ljust(colwith_1)
    headers += "Achievement".ljust(colwith_2)
    headers += "Value".ljust(colwith_3)

    content = headers + "\n"
    for key, value in achievements.items():
        row = ""
        row += key.split("#")[0][0:15].ljust(colwith_1)
        row += key.split("#")[1][0:20].ljust(colwith_2)
        row += str(value).ljust(colwith_3)
        content += row + "\n"

    payload = {
        "embeds": [
            {
                "author": {
                    "name": "RTCWProAPI",
                    "icon_url": "https://rtcwpro.com/images/RtCWProVector.png"
                },
                "description": "The following players received new achievements! `\n" + content + "`",
                "color": 15258703
            }]
    }
    return payload

def build_new_user_payload(event):
    """Create a message about a new user."""
    guid = event["detail"]["guid"]
    discord_message = "New user had been added - **" + event["detail"]["alias"] + "**\n" + \
        f"User page: [https://stats.rtcwpro.com/player/{guid}](https://stats.rtcwpro.com/player/{guid})"

    payload = {
        "embeds": [
            {
                "author": {
                    "name": "RTCWProAPI",
                    "icon_url": "https://rtcwpro.com/images/RtCWProVector.png"
                },
                "description": discord_message
            }]
    }
    return payload


def build_new_group_payload(event):
    """Create a message about a new group."""
    group_name = event["detail"]["group_name"]
    link = "https://stats.rtcwpro.com/groups/" + urllib.parse.quote(group_name)
    discord_message = "New group had been created - **" + group_name + "**\n" + \
        f"Link: [{group_name}]({link})"

    payload = {
        "embeds": [
            {
                "author": {
                    "name": "RTCWProAPI",
                    "icon_url": "https://rtcwpro.com/images/RtCWProVector.png"
                },
                "description": discord_message
            }]
    }
    return payload


def build_server_payload(event):
    """Create a message about a new server."""
    discord_message = "New server started to submit stats! " + \
        event["detail"]["server_name"] + " in " + \
        event["detail"]["match_type"].split("#")[0].upper() + " region."

    payload = {"content" : discord_message}
    return payload


def send_notification(hook_url, payload):
    """Post payload to discord hook url."""
    http = urllib3.PoolManager()
    response = http.request('POST', hook_url,
                            headers={'Content-Type': 'application/json'},
                            body=json.dumps(payload).encode('utf-8'))

    # response = requests.post(hook_url, json=payload)

    if response.status == 204:
        logger.info("Discord hook call successfull")
    else:
        logger.info("Discord hook call abnormal return code " + str(response.status))
        logger.info(response._body)


if __name__ == "__main__":
    event_new_server = {'detail-type': 'Discord notification',
                        'source': 'rtcwpro-pipeline',
                        'detail': {'notification_type': 'new server',
                                   'server_name': 'kek',
                                   'match_type': 'test#6'
                                   }
                        }
    event_new_player = {'detail-type': 'Discord notification',
                        'source': 'rtcwpro-pipeline',
                        'detail': {'notification_type': 'new player',
                                   'guid': '10352b3aab1331dafbc842131738f874',
                                   'alias': 'kiz',
                                   'match_type': 'test#6'
                                   }
                        }
    event_new_group = {'detail-type': 'Discord notification',
                       'source': 'rtcwpro-pipeline',
                       'detail': {"notification_type": "new group",
                                  "group_name": "gather-1636791197",
                                  "match_type": "test#6"
                                  }
                       }
    
    event_new_achievements = {'source': 'rtcwpro-pipeline',
                              'detail-type': 'Discord notification',
                              'detail': {"notification_type": "new achievements", 
                                         "achievements": {"ryder#Longest Kill": 2004, 
                                                          "consecro#MegaKill": 3}, 
                                         "match_type": "test#6"
                                         },
                              }
    event = event_new_achievements
    handler(event, None)
