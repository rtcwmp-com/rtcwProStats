import boto3
import botocore.config
import logging
import os
import json
import time as _time
from botocore.exceptions import ClientError
from notify_discord import post_custom_bus_event

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('group_reporter')
logger.setLevel(log_level)
PROMPT_ID = os.environ['PROMPT_ID']
CUSTOM_BUS = os.environ['RTCWPROSTATS_CUSTOM_BUS_ARN']
event_client = boto3.client('events')



def handler(event, context):
    logger.info(event)
    """Get the match results dictionary and generate report using gen AI."""
    group_name = event
    logger.info("Generating group report.")
    process_group_results(group_name)
    logger.info("Process finished.")

def get_rest_api_group_info(group_name):
    import urllib3

    base_url = "https://rtcwproapi.donkanator.com/"
    api_url = f"{base_url}stats/group/{group_name}"
    
    response = []

    logger.info("Contacting url: " + api_url)
    http = urllib3.PoolManager()
    response_http = http.request('GET', api_url)
    if response_http.status == 200:
        response = json.loads(response_http.data.decode('utf-8'))  # body
        logger.info("Contacted url successfully")
    else:
        logger.error("Bad response status from API call " + str(response_http.status))
    return response

def process_group_results(group_name):
  domain = "stats/group/{group_name}"
  params = {"group_name": group_name}
  match_result = get_rest_api_group_info(group_name)
  match_info, match_results_summary, region_match_type = digest_group_stats(match_result)
    
  # match_results_summary = f"""
  # 1. Player "fromy:o(" got the most kills:91
  # 2. Players "fro, source, pepe, n3ngo" had a biggest streak of 3 kills in an instant.
  # 3. TeamA took rounds on maps: te_frostbite.
  # 4. TeamB took rounds on maps: te_adlernest_b1, te_frostbite, te_adlernest_b1.
  # 5. TeamA had 328 kills and 76 revives.
  # 6. TeamB had 341 kills and 67 revives.
  # 7. The biggest confrontation was between players "source" and "parcher" with 33 kills between them.
  # """
  

  t1 = _time.time()
  facts = match_results_summary
  ai_response = invoke_prompt(facts)
  discord_summary = f"""
  Group Report for {group_name}
  {match_info}

  {ai_response}
  """
  logger.info(discord_summary)


  # region_match_type = "test#6"
  events = make_summary_event(discord_summary, region_match_type)
  post_custom_bus_event(event_client, events)

  time_to_execute = str(round((_time.time() - t1), 3))
  logger.info(f"Time to run process_group_description is {time_to_execute} s")

def make_summary_event(discord_summary, region_match_type):
    """Prepare an event about new group for discord announcement."""
    events = []

    event_template = {
        'Source': 'rtcwpro-pipeline',
        'DetailType': 'Discord notification',
        'Detail': '',
        'EventBusName': CUSTOM_BUS
    }

    tmp_event = event_template.copy()
    tmp_event["Detail"] = json.dumps({"notification_type": "group ai summary",
                                      "group_summary": discord_summary,
                                      "match_type" : region_match_type})
    events.append(tmp_event)
    return events

# https://github.com/awsdocs/aws-doc-sdk-examples/blob/main/python/example_code/bedrock-agent/prompts/run_prompt.py
def invoke_prompt(facts):
    bedrock_runtime = boto3.client('bedrock-runtime', region_name="us-east-1", config=botocore.config.Config(read_timeout=30, retries={'max_attempts': 1}))
    try:
        logger.info("Generating response with prompt: %s", PROMPT_ID)

        # Create promptVariables dictionary dynamically
        prompt_variables = {
            "facts": {"text": facts}
        }

        response = bedrock_runtime.converse(
            modelId=PROMPT_ID,
            promptVariables=prompt_variables
        )

        # Extract the response text
        message = response['output']['message']
        result = ""
        for content in message['content']:
            result += content['text']

        logger.info("Finished generating response with prompt: %s", PROMPT_ID)
    
        return result
    
    except ClientError as e:
        logger.exception("Client error invoking prompt version: %s", str(e))
        raise
    except Exception as e:
        logger.error("Error invoking prompt: %s", str(e))
        raise

def digest_group_stats(match_result):
    
    logger.info("Digesting group stats.")

    match_region_type = match_result.get("type", "test#6")
    important_results = {}
    valid_teams = ["TeamA", "TeamB"]
    
    important_results[valid_teams[0]] = {}
    important_results[valid_teams[1]] = {}
    important_results[valid_teams[0]]["roster"] = []
    important_results[valid_teams[1]]["roster"] = []
    important_results["most_headshots"] = 0
    important_results["most_kills"] = 0
    important_results["obj_captured"] = []
    
    statsall = match_result.get("statsall")
    if statsall is None:
      raise ValueError("Statsall is missing.")
    else:
      for stat in statsall:
        for guid, player_stat in stat.items():
          team_key = player_stat.get("team")
          if team_key in valid_teams:
            important_results[team_key]["roster"].append(player_stat.get("alias", "noname")[0:15])
            important_results[team_key]["kills"] = important_results[team_key].get("kills",0) + player_stat["categories"].get("kills",0)
            important_results[team_key]["revives"] = important_results[team_key].get("revives",0) + player_stat["categories"].get("revives",0)
            if player_stat["categories"].get("headshots",0) > important_results["most_headshots"]:
              important_results["most_headshots"] = player_stat["categories"].get("headshots",0)
              important_results["most_hs_alias"] = player_stat.get("alias", "noname")[0:15]
            if player_stat["categories"].get("kills",0) > important_results["most_kills"]:
              important_results["most_kills"] = player_stat["categories"].get("kills",0)
              important_results["most_kills_guid"] = guid
            if player_stat["categories"].get("obj_captured",0) > 0:
              important_results["obj_captured"].append(player_stat.get("alias", "noname")[0:15])
    
    real_names = {}
    for guid, param_arr in match_result.get("elos",{}).items():
      real_names[guid] = param_arr[0]

    match_summary = match_result.get("match_summary")
    if match_summary is None:
      raise ValueError("match_summary is missing.")
    else:
      important_results["match_time"] = match_summary.get("finish_human", "notime")
      important_results["duration"] = match_summary.get("duration_nice", "notime")
      important_results["games"] = match_summary.get("games", "#games")
      
    match_summary = match_result.get("match_summary")
    if match_summary is None:
      raise ValueError("match_summary is missing.")
    else:
      important_results["match_time"] = match_summary.get("finish_human", "notime")
      important_results["duration"] = match_summary.get("duration_nice", "notime")
      important_results["games"] = match_summary.get("games", "#games")
      
      match_results = match_summary.get("results")
      if match_results is None:
        raise ValueError("Results is missing.")
      else:
        for match_id, round_result in match_results.items():
          if round_result.get("winnerAB") is not None:
            if round_result.get("winnerAB") == "Draw":
              if "Draws" in important_results:
                important_results["Draws"].append(round_result.get("map", "nomap"))
              else:
                important_results["Draws"] = [round_result.get("map", "nomap")]
            else:
              if "maps" in important_results[round_result.get("winnerAB")]:
                important_results[round_result.get("winnerAB")]["maps"].append(round_result.get("map", "nomap"))
              else:
                important_results[round_result.get("winnerAB")]["maps"] = [round_result.get("map", "nomap")]
    
    awards = match_result.get("awards")
    if awards is not None:
      if "MegaKill" in awards:
        mk_names = []
        for name, amount in awards["MegaKill"].items():
          mk_amount = amount
          mk_names.append(name)
        important_results["MegaKill"] = { "kills": mk_amount, "names": ", ".join(mk_names)}
    
    feuds = match_result.get("top_feuds")
    if feuds is not None:
      new_feuds = {}
      for feud in feuds:
        new_feuds[feud[0] + "%" + feud[1]] = feud[2] + feud[3]
      important_results["super_feud"] = [max(new_feuds, key=new_feuds.get), new_feuds[max(new_feuds, key=new_feuds.get)]]
    
    all_maps = []
    for team in valid_teams:
      if "maps" in important_results[team]:
        all_maps += important_results[team]["maps"]
    # make all_maps unique
    all_maps = list(set(all_maps))

    match_info = "**Maps**: " + ", ".join(all_maps) + ". **Rounds**: " + str(important_results["games"]) + "\n"
    match_info += "**TeamA**: " + " ,".join(important_results["TeamA"]["roster"]) + "\n"
    match_info += "**TeamB**: " + " ,".join(important_results["TeamB"]["roster"])
    
    match_results_summary = ""
    i = 1
    match_results_summary += str(i) + ". Player \"" + real_names.get(important_results['most_kills_guid'], "GuidNotFound") + "\" got the most kills:" + str(important_results['most_kills']) + "\n"
    i += 1
    if "MegaKill" in important_results:
      match_results_summary += str(i) + ". Players \"" + important_results['MegaKill']['names'] + "\" had a biggest streak of " + str(important_results['MegaKill']['kills']) + " kills in an instant." + "\n"
      i += 1
    if "maps" in important_results['TeamA']:
      match_results_summary += str(i) + ". TeamA took rounds on maps: " + ", ".join(important_results['TeamA']['maps']) + "." + "\n"
      i += 1
    else:
      match_results_summary += str(i) + ". TeamB won games on all maps."
      i += 1
    if "maps" in important_results['TeamB']:
      match_results_summary += str(i) + ". TeamB took rounds on maps: " + ", ".join(important_results['TeamB']['maps']) + "." + "\n"
      i += 1
    else:
      match_results_summary += str(i) + ". TeamA won games on all maps."
      i += 1
    match_results_summary += str(i) + ". TeamA had " + str(important_results['TeamA']['kills']) + " kills and " + str(important_results['TeamA']['revives']) + " revives." + "\n"
    i += 1
    match_results_summary += str(i) + ". TeamB had " + str(important_results['TeamB']['kills']) + " kills and " + str(important_results['TeamB']['revives']) + " revives." + "\n"
    i += 1
    if "super_feud" in important_results:
      match_results_summary += str(i) + ". The biggest confrontation was between players \"" + important_results["super_feud"][0].split("%")[0] + "\" and \"" + important_results["super_feud"][0].split("%")[1] + "\" with " + str(important_results["super_feud"][1]) + " kills between them." + "\n"
    
    return match_info, match_results_summary, match_region_type

if __name__ == "__main__":
    event = "group_name"
    handler(event, None)
