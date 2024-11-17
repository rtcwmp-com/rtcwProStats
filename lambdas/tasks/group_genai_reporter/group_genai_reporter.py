import boto3
import botocore.config
import logging
import os
import json
import time as _time

log_level = logging.INFO
logging.basicConfig(format='%(name)s:%(levelname)s:%(message)s')
logger = logging.getLogger('group_reporter')
logger.setLevel(log_level)

class ImageError(Exception):
    "Custom exception for errors returned by Amazon &titan-text-express; model"

    def __init__(self, message):
        self.message = message


def handler(event, context):
    logger.info(event)
    """Get the match results dictionary and generate report using gen AI."""
    match_results = event
    logger.info("Generating group report.")
    process_group_description(match_results)
    # digest_group_stats()


def process_group_description(match_results):
    

    match_results_str = json.dumps(match_results)
    dominant_margin_str = "10%"
    language_style = "sassy"

    prompt_text = f"""
    A match finished with the following results:
    {match_results_str}\n
    Write a game summary highlighting the winner team based on higher scores and best players on each team. Determine if one of the teams was more dominant in points within {dominant_margin_str} margin and mention if it helped to get a win. Give total points per team inline. Use {language_style} language.
    """
    
#     prompt_text = """A game of RTCW had been played. Team1 with firehot, vulki, abdul kehali played a match against team2 with deadeye, toeknee, and roevswade. Team 1 won with 3 points against 1. Team 1 had 10% more points The best players were firehot with 16 kills and vulki with 11. 
    
#     List teams.
#     List scores.
#     Write a professional sports game summary.
#     List best players.
#     """
    prompt_text = f"""
    Context: A computer game called "RTCW" had a match. Write a 400 word match description using all of the following facts in order:
      0. TeamA was the winner.
      1. Player "parcher" got the most kills - 29.
      2. Player "parcher" had a biggest streak of 4 kills in an instant.
      3. TeamA took rounds on maps: mp_beach.
      4. TeamB took rounds on maps: mp_ice.
      5. TeamA had 223 kills and 58 revives.
      6. TeamB had 209 kills and 60 revives.
      7. The biggest confrontation was between players "yyz" and "jimmy" with 25 kills between them.
    """
    
    

    t1 = _time.time()
    # call AI
    # call_titan(prompt_text)
    call_ai21(prompt_text)
    time_to_execute = str(round((_time.time() - t1), 3))
    logger.info(f"Time to run process_group_description is {time_to_execute} s")

def call_ai21(prompt_text):
    model_id = "ai21.j2-mid-v1"
    body = {
      "prompt": prompt_text,
      "maxTokens": 400,
      "temperature": 0.8,
      "topP": 1,
      # "stopSequences": [],
      "countPenalty": {
        "scale": 0
      },
      "presencePenalty": {
        "scale": 0
      },
      "frequencyPenalty": {
        "scale": 0
        }
      }

    try:
        response_data = call_bedrock_model(model_id, body)
        report = response_data.get("completions")[0].get("data").get("text")
        logger.info(report)

    except Exception as e:
        print(f"Error generating the code: {e}")
        return ""



def call_titan(prompt_text):
    model_id = "amazon.titan-text-lite-v1"
    body = {
      "inputText": prompt_text,
      "textGenerationConfig": {
          "maxTokenCount": 400,
          # "stopSequences": [[]],
          "temperature": 0.7,
          "topP": 1
          }
    }
    

    try:
        response_data = call_bedrock_model(model_id, body)
        logger.info(f"inputTextTokenCount: {response_data['inputTextTokenCount']}")

        i = 0
        for result in response_data['results']:
            i = i + 1
            print("Response number: " + str(i))
            print(f"Token count: {result['tokenCount']}")
            print(f"Output text: {result['outputText']}")
            print(f"Completion reason: {result['completionReason']}")
        
        # # report = response_data.get("results")[0].get("outputText")
        # # logger.info(report) 

    except Exception as e:
        print(f"Error generating the code: {e}")
        return ""

def call_bedrock_model(model_id, body):
    bedrock_runtime = boto3.client('bedrock-runtime', region_name="us-east-1", config=botocore.config.Config(read_timeout=30, retries={'max_attempts': 1}))
    response = bedrock_runtime.invoke_model(
        body=json.dumps(body),
        modelId=model_id,
        accept="application/json",
        contentType="application/json"
        )
  
    response_content = response.get('body').read().decode('utf-8')
    response_data = json.loads(response_content)
    
    finish_reason = response_data.get("error")
  
    if finish_reason is not None:
        raise ImageError(f"Text generation error. Error is {finish_reason}")
  
    logger.info("Successfully generated text with model %s", model_id)
    return response_data

def digest_group_stats():
    match_result = {
      "statsall": [
        {
          "bf8fc5ff8f0222e3ff243f0c7678b036": {
            "alias": "ra!ser",
            "alias_colored": "^7Ra^4!s^7er",
            "team": "TeamA",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 43,
              "deaths": 31,
              "gibs": 12,
              "suicides": 8,
              "teamkills": 1,
              "headshots": 37,
              "damagegiven": 7655,
              "damagereceived": 6678,
              "damageteam": 132,
              "hits": 351,
              "shots": 888,
              "revives": 6,
              "ammogiven": 0,
              "healthgiven": 4,
              "knifekills": 0,
              "score": 102,
              "dyn_planted": 1,
              "dyn_defused": 0,
              "obj_captured": 2,
              "obj_destroyed": 1,
              "obj_returned": 2,
              "obj_taken": 5,
              "obj_checkpoint": 1,
              "obj_killcarrier": 1,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 58,
              "killpeak": 1,
              "games": 3,
              "alias_colored": "^7Ra^4!s^7er"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "e36178cfe46d84198b5f369b920f9592": {
            "alias": "souldriver",
            "alias_colored": "^bSoulDri^0V^ber",
            "team": "TeamA",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 27,
              "deaths": 35,
              "gibs": 8,
              "suicides": 9,
              "teamkills": 3,
              "headshots": 32,
              "damagegiven": 5563,
              "damagereceived": 6104,
              "damageteam": 735,
              "hits": 228,
              "shots": 933,
              "revives": 7,
              "ammogiven": 18,
              "healthgiven": 9,
              "knifekills": 0,
              "score": 5,
              "dyn_planted": 2,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 2,
              "obj_returned": 0,
              "obj_taken": 1,
              "obj_checkpoint": 0,
              "obj_killcarrier": 0,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 43,
              "killpeak": 1,
              "games": 3,
              "alias_colored": "^bSoulDri^0V^ber"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "55e5e503624a1913d3d2419c734471b4": {
            "alias": "jun1or",
            "alias_colored": "^1Jun^01oR",
            "team": "TeamB",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 24,
              "deaths": 28,
              "gibs": 8,
              "suicides": 3,
              "teamkills": 2,
              "headshots": 19,
              "damagegiven": 3618,
              "damagereceived": 6650,
              "damageteam": 82,
              "hits": 170,
              "shots": 584,
              "revives": 23,
              "ammogiven": 0,
              "healthgiven": 6,
              "knifekills": 0,
              "score": 41,
              "dyn_planted": 0,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 0,
              "obj_returned": 2,
              "obj_taken": 0,
              "obj_checkpoint": 3,
              "obj_killcarrier": 0,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 46,
              "killpeak": 2,
              "games": 3,
              "alias_colored": "^1Jun^01oR"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "827157b27cd60814a1700515531aaccb": {
            "alias": "parcher",
            "alias_colored": "parcher",
            "team": "TeamA",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 50,
              "deaths": 28,
              "gibs": 8,
              "suicides": 7,
              "teamkills": 0,
              "headshots": 65,
              "damagegiven": 9170,
              "damagereceived": 6688,
              "damageteam": 124,
              "hits": 447,
              "shots": 972,
              "revives": 10,
              "ammogiven": 0,
              "healthgiven": 15,
              "knifekills": 0,
              "score": 32,
              "dyn_planted": 1,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 1,
              "obj_returned": 0,
              "obj_taken": 1,
              "obj_checkpoint": 2,
              "obj_killcarrier": 0,
              "obj_protectflag": 1,
              "accuracy": 0,
              "efficiency": 64,
              "killpeak": 3,
              "games": 3,
              "alias_colored": "parcher"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "611d79d1b08949e650c4c299aadd0fde": {
            "alias": "spaztik",
            "alias_colored": "spaztik",
            "team": "TeamB",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 35,
              "deaths": 33,
              "gibs": 15,
              "suicides": 9,
              "teamkills": 0,
              "headshots": 16,
              "damagegiven": 8440,
              "damagereceived": 5416,
              "damageteam": 121,
              "hits": 79,
              "shots": 164,
              "revives": 2,
              "ammogiven": 1,
              "healthgiven": 3,
              "knifekills": 0,
              "score": 17,
              "dyn_planted": 0,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 0,
              "obj_returned": 0,
              "obj_taken": 1,
              "obj_checkpoint": 1,
              "obj_killcarrier": 0,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 51,
              "killpeak": 3,
              "games": 3,
              "alias_colored": "spaztik"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "c606fb49242ad759e87575e5a04d05a3": {
            "alias": "!!!joep",
            "alias_colored": "^5!!!^7Joep",
            "team": "TeamB",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 44,
              "deaths": 37,
              "gibs": 14,
              "suicides": 8,
              "teamkills": 3,
              "headshots": 41,
              "damagegiven": 7187,
              "damagereceived": 7244,
              "damageteam": 400,
              "hits": 350,
              "shots": 1006,
              "revives": 8,
              "ammogiven": 13,
              "healthgiven": 4,
              "knifekills": 0,
              "score": 26,
              "dyn_planted": 1,
              "dyn_defused": 0,
              "obj_captured": 1,
              "obj_destroyed": 1,
              "obj_returned": 0,
              "obj_taken": 2,
              "obj_checkpoint": 1,
              "obj_killcarrier": 0,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 54,
              "killpeak": 2,
              "games": 3,
              "alias_colored": "^5!!!^7Joep"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "8555e87b759ae51d36b5030fcdd08a23": {
            "alias": "jimmy",
            "alias_colored": "^7J^7i^4mm^7y",
            "team": "TeamA",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 48,
              "deaths": 46,
              "gibs": 6,
              "suicides": 12,
              "teamkills": 2,
              "headshots": 43,
              "damagegiven": 8299,
              "damagereceived": 7477,
              "damageteam": 865,
              "hits": 358,
              "shots": 1231,
              "revives": 2,
              "ammogiven": 34,
              "healthgiven": 0,
              "knifekills": 0,
              "score": 10,
              "dyn_planted": 0,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 0,
              "obj_returned": 0,
              "obj_taken": 0,
              "obj_checkpoint": 2,
              "obj_killcarrier": 0,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 51,
              "killpeak": 2,
              "games": 3,
              "alias_colored": "^7J^7i^4mm^7y"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "18a519162abddc7638d3c44a50b124dc": {
            "alias": "fistermiagi",
            "alias_colored": "FisterMiagi",
            "team": "TeamA",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 17,
              "deaths": 37,
              "gibs": 3,
              "suicides": 3,
              "teamkills": 0,
              "headshots": 21,
              "damagegiven": 4622,
              "damagereceived": 7573,
              "damageteam": 25,
              "hits": 147,
              "shots": 477,
              "revives": 4,
              "ammogiven": 0,
              "healthgiven": 4,
              "knifekills": 0,
              "score": 26,
              "dyn_planted": 2,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 2,
              "obj_returned": 0,
              "obj_taken": 1,
              "obj_checkpoint": 0,
              "obj_killcarrier": 0,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 31,
              "killpeak": 1,
              "games": 3,
              "alias_colored": "FisterMiagi"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "49d64b3c0fcd5512a8e87b6287b29b21": {
            "alias": "xill",
            "alias_colored": "^0X^4i^7ll",
            "team": "TeamA",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 38,
              "deaths": 40,
              "gibs": 23,
              "suicides": 12,
              "teamkills": 2,
              "headshots": 13,
              "damagegiven": 10634,
              "damagereceived": 6789,
              "damageteam": 555,
              "hits": 106,
              "shots": 199,
              "revives": 0,
              "ammogiven": 0,
              "healthgiven": 0,
              "knifekills": 0,
              "score": 7,
              "dyn_planted": 0,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 0,
              "obj_returned": 0,
              "obj_taken": 0,
              "obj_checkpoint": 5,
              "obj_killcarrier": 0,
              "obj_protectflag": 1,
              "accuracy": 0,
              "efficiency": 48,
              "killpeak": 2,
              "games": 3,
              "alias_colored": "^0X^4i^7ll"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "41e30e5dd230f4469712df0f4c3e60c3": {
            "alias": "0274(/)",
            "alias_colored": "^70274(^0/^7)",
            "team": "TeamB",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 52,
              "deaths": 44,
              "gibs": 17,
              "suicides": 12,
              "teamkills": 2,
              "headshots": 52,
              "damagegiven": 9250,
              "damagereceived": 9824,
              "damageteam": 450,
              "hits": 359,
              "shots": 1023,
              "revives": 16,
              "ammogiven": 0,
              "healthgiven": 3,
              "knifekills": 0,
              "score": 5,
              "dyn_planted": 1,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 1,
              "obj_returned": 1,
              "obj_taken": 0,
              "obj_checkpoint": 2,
              "obj_killcarrier": 0,
              "obj_protectflag": 3,
              "accuracy": 0,
              "efficiency": 54,
              "killpeak": 3,
              "games": 3,
              "alias_colored": "^70274(^0/^7)"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "459bdc2f8af877c7781c426993383f5f": {
            "alias": "eln3ngo",
            "alias_colored": "^eE^dl^eN^d3n^eGo",
            "team": "TeamB",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 26,
              "deaths": 49,
              "gibs": 11,
              "suicides": 17,
              "teamkills": 0,
              "headshots": 31,
              "damagegiven": 5586,
              "damagereceived": 8653,
              "damageteam": 146,
              "hits": 257,
              "shots": 943,
              "revives": 2,
              "ammogiven": 20,
              "healthgiven": 0,
              "knifekills": 0,
              "score": -3,
              "dyn_planted": 2,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 2,
              "obj_returned": 1,
              "obj_taken": 0,
              "obj_checkpoint": 2,
              "obj_killcarrier": 0,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 34,
              "killpeak": 1,
              "games": 3,
              "alias_colored": "^eE^dl^eN^d3n^eGo"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        },
        {
          "613a7ede908f8c427e847dbbf71103ab": {
            "alias": "john_mullins",
            "alias_colored": "^>Joh^4n_M^dull^uins",
            "team": "TeamB",
            "start_time": 0,
            "num_rounds": 1,
            "categories": {
              "kills": 28,
              "deaths": 40,
              "gibs": 3,
              "suicides": 12,
              "teamkills": 1,
              "headshots": 38,
              "damagegiven": 7228,
              "damagereceived": 8156,
              "damageteam": 263,
              "hits": 272,
              "shots": 786,
              "revives": 7,
              "ammogiven": 8,
              "healthgiven": 7,
              "knifekills": 0,
              "score": 1,
              "dyn_planted": 0,
              "dyn_defused": 0,
              "obj_captured": 0,
              "obj_destroyed": 0,
              "obj_returned": 2,
              "obj_taken": 1,
              "obj_checkpoint": 0,
              "obj_killcarrier": 0,
              "obj_protectflag": 0,
              "accuracy": 0,
              "efficiency": 41,
              "killpeak": 1,
              "games": 3,
              "alias_colored": "^>Joh^4n_M^dull^uins"
            },
            "jsonGameStatVersion": "0.1.3"
          }
        }
      ],
      "match_id": "group gather-1707108305",
      "type": "na#6",
      "match_summary": {
        "duration": 2305,
        "duration_nice": "38:25",
        "finish_human": "2024-02-05 04:41:54",
        "games": 3,
        "results": {
          "1707105941": {
            "round1": {
              "duration": 350,
              "duration_nice": "05:50"
            },
            "map": "mp_beach",
            "round2": {
              "duration": 241,
              "duration_nice": "04:01"
            },
            "winner": "Allied",
            "winnerAB": "TeamA"
          },
          "1707106734": {
            "round1": {
              "duration": 600,
              "duration_nice": "10:00"
            },
            "map": "mp_beach",
            "round2": {
              "duration": 600,
              "duration_nice": "10:00"
            },
            "winner": "Draw",
            "winnerAB": "Draw"
          },
          "1707108114": {
            "round1": {
              "duration": 132,
              "duration_nice": "02:12"
            },
            "map": "mp_ice",
            "winner": "Allied",
            "winnerAB": "TeamB"
          }
        }
      },
      "elos": {
        "613a7ede908f8c427e847dbbf71103ab": [
          "john_mullins",
          1594
        ],
        "55e5e503624a1913d3d2419c734471b4": [
          "jun1or",
          1635
        ],
        "8555e87b759ae51d36b5030fcdd08a23": [
          "jimmy mullins",
          1683
        ],
        "611d79d1b08949e650c4c299aadd0fde": [
          "spaztik",
          1420
        ],
        "c606fb49242ad759e87575e5a04d05a3": [
          "joep",
          1709
        ],
        "49d64b3c0fcd5512a8e87b6287b29b21": [
          "xill",
          1787
        ],
        "827157b27cd60814a1700515531aaccb": [
          "rebellion",
          1776
        ],
        "459bdc2f8af877c7781c426993383f5f": [
          "n3ngo",
          1707
        ],
        "41e30e5dd230f4469712df0f4c3e60c3": [
          "yyz",
          1741
        ],
        "e36178cfe46d84198b5f369b920f9592": [
          "souldriver",
          1621
        ],
        "18a519162abddc7638d3c44a50b124dc": [
          "fistermiagi",
          1593
        ],
        "bf8fc5ff8f0222e3ff243f0c7678b036": [
          "ra!ser",
          1692
        ]
      },
      "classes": {
        "bf8fc5ff8f0222e3ff243f0c7678b036": "LT",
        "e36178cfe46d84198b5f369b920f9592": "LT",
        "55e5e503624a1913d3d2419c734471b4": "Medic",
        "827157b27cd60814a1700515531aaccb": "Medic",
        "611d79d1b08949e650c4c299aadd0fde": "Panzer",
        "c606fb49242ad759e87575e5a04d05a3": "LT",
        "8555e87b759ae51d36b5030fcdd08a23": "LT",
        "18a519162abddc7638d3c44a50b124dc": "Sniper",
        "49d64b3c0fcd5512a8e87b6287b29b21": "Panzer",
        "41e30e5dd230f4469712df0f4c3e60c3": "Medic",
        "459bdc2f8af877c7781c426993383f5f": "LT",
        "613a7ede908f8c427e847dbbf71103ab": "Sniper"
      },
      "wstatsall": [
        {
          "bf8fc5ff8f0222e3ff243f0c7678b036": [
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 3,
              "shots": 54,
              "games": 2,
              "weapon": "Knife"
            },
            {
              "kills": 2,
              "deaths": 0,
              "headshots": 0,
              "hits": 7,
              "shots": 11,
              "games": 2,
              "weapon": "Colt"
            },
            {
              "kills": 22,
              "deaths": 16,
              "headshots": 26,
              "hits": 229,
              "shots": 616,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 15,
              "deaths": 9,
              "headshots": 11,
              "hits": 112,
              "shots": 302,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 0,
              "deaths": 2,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 2,
              "weapon": "Panzer"
            },
            {
              "kills": 3,
              "deaths": 1,
              "headshots": 0,
              "hits": 5,
              "shots": 13,
              "games": 2,
              "weapon": "Grenade"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 1,
              "games": 1,
              "weapon": "Dynamite"
            },
            {
              "kills": 1,
              "deaths": 0,
              "headshots": 0,
              "hits": 1,
              "shots": 3,
              "games": 2,
              "weapon": "Artillery"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 6,
              "shots": 7,
              "games": 3,
              "weapon": "Syringe"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Mauser"
            },
            {
              "kills": 0,
              "deaths": 2,
              "headshots": 0,
              "hits": 3,
              "shots": 20,
              "games": 2,
              "weapon": "Luger"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 1,
              "shots": 10,
              "games": 1,
              "weapon": "Airstrike"
            }
          ]
        },
        {
          "e36178cfe46d84198b5f369b920f9592": [
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 16,
              "games": 2,
              "weapon": "Knife"
            },
            {
              "kills": 1,
              "deaths": 4,
              "headshots": 0,
              "hits": 1,
              "shots": 7,
              "games": 3,
              "weapon": "Luger"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 4,
              "shots": 6,
              "games": 2,
              "weapon": "Colt"
            },
            {
              "kills": 12,
              "deaths": 12,
              "headshots": 23,
              "hits": 151,
              "shots": 653,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 8,
              "deaths": 9,
              "headshots": 9,
              "hits": 72,
              "shots": 335,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 0,
              "deaths": 2,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 2,
              "weapon": "Panzer"
            },
            {
              "kills": 4,
              "deaths": 3,
              "headshots": 0,
              "hits": 5,
              "shots": 33,
              "games": 3,
              "weapon": "Grenade"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 4,
              "games": 2,
              "weapon": "Dynamite"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 110,
              "games": 2,
              "weapon": "Airstrike"
            },
            {
              "kills": 2,
              "deaths": 0,
              "headshots": 0,
              "hits": 2,
              "shots": 16,
              "games": 2,
              "weapon": "Artillery"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 7,
              "shots": 7,
              "games": 3,
              "weapon": "Syringe"
            },
            {
              "kills": 0,
              "deaths": 2,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Mauser"
            }
          ]
        },
        {
          "55e5e503624a1913d3d2419c734471b4": [
            {
              "kills": 1,
              "deaths": 2,
              "headshots": 2,
              "hits": 9,
              "shots": 27,
              "games": 3,
              "weapon": "Colt"
            },
            {
              "kills": 10,
              "deaths": 12,
              "headshots": 8,
              "hits": 83,
              "shots": 307,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 10,
              "deaths": 6,
              "headshots": 8,
              "hits": 70,
              "shots": 305,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 0,
              "deaths": 4,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 2,
              "weapon": "Panzer"
            },
            {
              "kills": 2,
              "deaths": 1,
              "headshots": 0,
              "hits": 2,
              "shots": 5,
              "games": 2,
              "weapon": "Grenade"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Artillery"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 23,
              "shots": 25,
              "games": 3,
              "weapon": "Syringe"
            },
            {
              "kills": 1,
              "deaths": 1,
              "headshots": 1,
              "hits": 8,
              "shots": 18,
              "games": 2,
              "weapon": "Luger"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 6,
              "games": 1,
              "weapon": "Knife"
            }
          ]
        },
        {
          "827157b27cd60814a1700515531aaccb": [
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 29,
              "games": 3,
              "weapon": "Knife"
            },
            {
              "kills": 1,
              "deaths": 1,
              "headshots": 1,
              "hits": 5,
              "shots": 13,
              "games": 2,
              "weapon": "Luger"
            },
            {
              "kills": 30,
              "deaths": 8,
              "headshots": 40,
              "hits": 280,
              "shots": 595,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 16,
              "deaths": 12,
              "headshots": 18,
              "hits": 144,
              "shots": 367,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Sten"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Panzer"
            },
            {
              "kills": 0,
              "deaths": 3,
              "headshots": 0,
              "hits": 1,
              "shots": 4,
              "games": 3,
              "weapon": "Grenade"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 1,
              "games": 1,
              "weapon": "Dynamite"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 10,
              "shots": 13,
              "games": 3,
              "weapon": "Syringe"
            },
            {
              "kills": 3,
              "deaths": 0,
              "headshots": 6,
              "hits": 18,
              "shots": 31,
              "games": 1,
              "weapon": "Colt"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Airstrike"
            }
          ]
        },
        {
          "611d79d1b08949e650c4c299aadd0fde": [
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 16,
              "games": 2,
              "weapon": "Knife"
            },
            {
              "kills": 1,
              "deaths": 0,
              "headshots": 0,
              "hits": 3,
              "shots": 18,
              "games": 2,
              "weapon": "Luger"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 7,
              "games": 2,
              "weapon": "Colt"
            },
            {
              "kills": 0,
              "deaths": 17,
              "headshots": 0,
              "hits": 1,
              "shots": 5,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 4,
              "deaths": 5,
              "headshots": 3,
              "hits": 26,
              "shots": 69,
              "games": 2,
              "weapon": "Thompson"
            },
            {
              "kills": 9,
              "deaths": 2,
              "headshots": 0,
              "hits": 10,
              "shots": 16,
              "games": 3,
              "weapon": "Panzer"
            },
            {
              "kills": 1,
              "deaths": 1,
              "headshots": 0,
              "hits": 3,
              "shots": 29,
              "games": 3,
              "weapon": "Grenade"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 2,
              "shots": 2,
              "games": 2,
              "weapon": "Syringe"
            },
            {
              "kills": 20,
              "deaths": 6,
              "headshots": 13,
              "hits": 39,
              "shots": 57,
              "games": 2,
              "weapon": "Mauser"
            }
          ]
        },
        {
          "c606fb49242ad759e87575e5a04d05a3": [
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 30,
              "games": 2,
              "weapon": "Knife"
            },
            {
              "kills": 1,
              "deaths": 1,
              "headshots": 1,
              "hits": 11,
              "shots": 34,
              "games": 2,
              "weapon": "Luger"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 1,
              "shots": 28,
              "games": 2,
              "weapon": "Colt"
            },
            {
              "kills": 28,
              "deaths": 18,
              "headshots": 33,
              "hits": 258,
              "shots": 870,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 13,
              "deaths": 8,
              "headshots": 7,
              "hits": 80,
              "shots": 221,
              "games": 2,
              "weapon": "Thompson"
            },
            {
              "kills": 2,
              "deaths": 1,
              "headshots": 0,
              "hits": 3,
              "shots": 5,
              "games": 2,
              "weapon": "Grenade"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 2,
              "games": 2,
              "weapon": "Dynamite"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 2,
              "games": 1,
              "weapon": "Artillery"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 8,
              "shots": 8,
              "games": 2,
              "weapon": "Syringe"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Mauser"
            },
            {
              "kills": 0,
              "deaths": 6,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 2,
              "weapon": "Panzer"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 10,
              "games": 1,
              "weapon": "Airstrike"
            }
          ]
        },
        {
          "8555e87b759ae51d36b5030fcdd08a23": [
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 10,
              "games": 1,
              "weapon": "Colt"
            },
            {
              "kills": 40,
              "deaths": 15,
              "headshots": 43,
              "hits": 352,
              "shots": 1281,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 1,
              "deaths": 13,
              "headshots": 0,
              "hits": 6,
              "shots": 30,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 0,
              "deaths": 2,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 2,
              "weapon": "Panzer"
            },
            {
              "kills": 3,
              "deaths": 2,
              "headshots": 0,
              "hits": 5,
              "shots": 9,
              "games": 3,
              "weapon": "Grenade"
            },
            {
              "kills": 1,
              "deaths": 1,
              "headshots": 0,
              "hits": 1,
              "shots": 190,
              "games": 3,
              "weapon": "Airstrike"
            },
            {
              "kills": 3,
              "deaths": 1,
              "headshots": 0,
              "hits": 4,
              "shots": 20,
              "games": 3,
              "weapon": "Artillery"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 2,
              "shots": 2,
              "games": 2,
              "weapon": "Syringe"
            },
            {
              "kills": 0,
              "deaths": 6,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 2,
              "weapon": "Mauser"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Luger"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Sten"
            }
          ]
        },
        {
          "18a519162abddc7638d3c44a50b124dc": [
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 10,
              "games": 2,
              "weapon": "Luger"
            },
            {
              "kills": 8,
              "deaths": 15,
              "headshots": 10,
              "hits": 97,
              "shots": 368,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 2,
              "deaths": 9,
              "headshots": 5,
              "hits": 37,
              "shots": 106,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 0,
              "deaths": 2,
              "headshots": 0,
              "hits": 1,
              "shots": 3,
              "games": 2,
              "weapon": "Grenade"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 4,
              "shots": 4,
              "games": 2,
              "weapon": "Syringe"
            },
            {
              "kills": 7,
              "deaths": 6,
              "headshots": 6,
              "hits": 12,
              "shots": 21,
              "games": 2,
              "weapon": "Mauser"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 2,
              "games": 1,
              "weapon": "Dynamite"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 1,
              "shots": 6,
              "games": 1,
              "weapon": "Colt"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Panzer"
            },
            {
              "kills": 0,
              "deaths": 2,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Airstrike"
            }
          ]
        },
        {
          "49d64b3c0fcd5512a8e87b6287b29b21": [
            {
              "kills": 2,
              "deaths": 0,
              "headshots": 8,
              "hits": 34,
              "shots": 78,
              "games": 3,
              "weapon": "Luger"
            },
            {
              "kills": 1,
              "deaths": 1,
              "headshots": 4,
              "hits": 19,
              "shots": 39,
              "games": 2,
              "weapon": "Colt"
            },
            {
              "kills": 1,
              "deaths": 19,
              "headshots": 1,
              "hits": 8,
              "shots": 24,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 0,
              "deaths": 7,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 27,
              "deaths": 1,
              "headshots": 0,
              "hits": 33,
              "shots": 43,
              "games": 3,
              "weapon": "Panzer"
            },
            {
              "kills": 4,
              "deaths": 2,
              "headshots": 0,
              "hits": 7,
              "shots": 35,
              "games": 3,
              "weapon": "Grenade"
            },
            {
              "kills": 3,
              "deaths": 8,
              "headshots": 0,
              "hits": 12,
              "shots": 23,
              "games": 2,
              "weapon": "Mauser"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Artillery"
            }
          ]
        },
        {
          "41e30e5dd230f4469712df0f4c3e60c3": [
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 42,
              "games": 2,
              "weapon": "Knife"
            },
            {
              "kills": 6,
              "deaths": 1,
              "headshots": 4,
              "hits": 17,
              "shots": 51,
              "games": 2,
              "weapon": "Luger"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 13,
              "shots": 53,
              "games": 3,
              "weapon": "Colt"
            },
            {
              "kills": 21,
              "deaths": 25,
              "headshots": 31,
              "hits": 195,
              "shots": 573,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 17,
              "deaths": 8,
              "headshots": 17,
              "hits": 134,
              "shots": 479,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 0,
              "deaths": 4,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 3,
              "weapon": "Panzer"
            },
            {
              "kills": 8,
              "deaths": 3,
              "headshots": 0,
              "hits": 14,
              "shots": 51,
              "games": 3,
              "weapon": "Grenade"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 4,
              "games": 2,
              "weapon": "Dynamite"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 1,
              "weapon": "Artillery"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 16,
              "shots": 18,
              "games": 2,
              "weapon": "Syringe"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 1,
              "games": 2,
              "weapon": "Mauser"
            }
          ]
        },
        {
          "459bdc2f8af877c7781c426993383f5f": [
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 1,
              "games": 1,
              "weapon": "Luger"
            },
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 1,
              "hits": 1,
              "shots": 6,
              "games": 2,
              "weapon": "Colt"
            },
            {
              "kills": 19,
              "deaths": 25,
              "headshots": 23,
              "hits": 210,
              "shots": 909,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 4,
              "deaths": 8,
              "headshots": 7,
              "hits": 46,
              "shots": 147,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 0,
              "deaths": 6,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 2,
              "weapon": "Panzer"
            },
            {
              "kills": 0,
              "deaths": 4,
              "headshots": 0,
              "hits": 1,
              "shots": 15,
              "games": 2,
              "weapon": "Grenade"
            },
            {
              "kills": 3,
              "deaths": 0,
              "headshots": 0,
              "hits": 5,
              "shots": 170,
              "games": 2,
              "weapon": "Airstrike"
            },
            {
              "kills": 0,
              "deaths": 3,
              "headshots": 0,
              "hits": 1,
              "shots": 21,
              "games": 2,
              "weapon": "Artillery"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 2,
              "shots": 2,
              "games": 1,
              "weapon": "Syringe"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 45,
              "games": 2,
              "weapon": "Knife"
            }
          ]
        },
        {
          "613a7ede908f8c427e847dbbf71103ab": [
            {
              "kills": 0,
              "deaths": 1,
              "headshots": 0,
              "hits": 0,
              "shots": 1,
              "games": 2,
              "weapon": "Luger"
            },
            {
              "kills": 7,
              "deaths": 16,
              "headshots": 13,
              "hits": 104,
              "shots": 343,
              "games": 3,
              "weapon": "MP-40"
            },
            {
              "kills": 11,
              "deaths": 7,
              "headshots": 20,
              "hits": 110,
              "shots": 349,
              "games": 3,
              "weapon": "Thompson"
            },
            {
              "kills": 2,
              "deaths": 0,
              "headshots": 3,
              "hits": 37,
              "shots": 114,
              "games": 2,
              "weapon": "Sten"
            },
            {
              "kills": 0,
              "deaths": 5,
              "headshots": 0,
              "hits": 0,
              "shots": 0,
              "games": 2,
              "weapon": "Panzer"
            },
            {
              "kills": 0,
              "deaths": 4,
              "headshots": 0,
              "hits": 2,
              "shots": 10,
              "games": 2,
              "weapon": "Grenade"
            },
            {
              "kills": 2,
              "deaths": 1,
              "headshots": 0,
              "hits": 7,
              "shots": 120,
              "games": 2,
              "weapon": "Airstrike"
            },
            {
              "kills": 2,
              "deaths": 1,
              "headshots": 0,
              "hits": 2,
              "shots": 11,
              "games": 2,
              "weapon": "Artillery"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 7,
              "shots": 8,
              "games": 2,
              "weapon": "Syringe"
            },
            {
              "kills": 3,
              "deaths": 2,
              "headshots": 0,
              "hits": 10,
              "shots": 17,
              "games": 2,
              "weapon": "Mauser"
            },
            {
              "kills": 1,
              "deaths": 2,
              "headshots": 2,
              "hits": 10,
              "shots": 35,
              "games": 2,
              "weapon": "Colt"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 1,
              "games": 1,
              "weapon": "Dynamite"
            },
            {
              "kills": 0,
              "deaths": 0,
              "headshots": 0,
              "hits": 0,
              "shots": 1,
              "games": 1,
              "weapon": "Knife"
            }
          ]
        }
      ],
      "awards": {
        "Chicken": {
          "yyz": 30
        },
        "Backstabber": {
          "rebellion": 28,
          "jimmy mullins": 28
        },
        "Longest Kill": {
          "n3ngo": 5268
        },
        "MegaKill": {
          "rebellion": 4
        },
        "Frontliner": {
          "spaztik": 6
        }
      },
      "top_feuds": [
        [
          "spaztik",
          "fistermiagi",
          6,
          9
        ],
        [
          "spaztik",
          "jimmy mullins",
          7,
          7
        ],
        [
          "rebellion",
          "yyz",
          10,
          5
        ],
        [
          "jun1or",
          "rebellion",
          9,
          7
        ],
        [
          "xill",
          "joep",
          8,
          12
        ],
        [
          "jimmy mullins",
          "yyz",
          12,
          13
        ],
        [
          "rebellion",
          "joep",
          11,
          6
        ],
        [
          "xill",
          "n3ngo",
          8,
          5
        ],
        [
          "ra!ser",
          "joep",
          8,
          7
        ],
        [
          "n3ngo",
          "jimmy mullins",
          9,
          12
        ],
        [
          "yyz",
          "ra!ser",
          9,
          8
        ],
        [
          "xill",
          "john_mullins",
          7,
          6
        ],
        [
          "souldriver",
          "yyz",
          6,
          11
        ],
        [
          "jimmy mullins",
          "john_mullins",
          8,
          5
        ]
      ]
    }
    logger.info(str(len(match_result)))
    
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
              important_results["most_kills_alias"] = player_stat.get("alias", "noname")[0:15]
            if player_stat["categories"].get("obj_captured",0) > 0:
              important_results["obj_captured"].append(player_stat.get("alias", "noname")[0:15])
    
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
        important_results["MegaKill"] = awards["MegaKill"]
    
    feuds = match_result.get("top_feuds")
    if feuds is not None:
      new_feuds = {}
      for feud in feuds:
        new_feuds[feud[0] + "-vs-" + feud[1]] = feud[2] + feud[3]
        important_results["super_feud"] = {max(new_feuds, key=new_feuds.get), new_feuds[max(new_feuds, key=new_feuds.get)]}

    print(important_results)
    return important_results

if __name__ == "__main__":
    event = {
        "points": {
            "red team": {
                "mark": 3,
                "twain": 20,
                "heck": 24,
                "finn": 2,
                "tom": 15,
                "sawyer": 12
                },
            "blue team": {
                "jules": 34,
                "vern": 10,
                "fenimore": 24,
                "james": 22,
                "cooper": 15,
                "Osceola": 12
                }
            },
        "scores": {"red team": 3, "blue team": 1}

        }
    handler(event, None)
