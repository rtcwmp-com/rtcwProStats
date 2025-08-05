import json
from collections import Counter

file_name = "gamestats5/gameStats_match_1610684722_round_2_mp_base.json"
with open(file_name) as file:
    content = file.read()

content_json = json.loads(content)

teams = {"team1guids":[], "team2guids":[], "team1names": [], "team2names": []}
for stats in content_json.get("stats", []):
    for guid, player_stats in stats.items():
        if player_stats["team"] == "Axis":
            teams["team1guids"].append(guid)
            teams["team1names"].append(player_stats["alias"])
        elif player_stats["team"] == "Allied":
            teams["team2guids"].append(guid)
            teams["team2names"].append(player_stats["alias"])

scores = {
    "pk":"scores",
    "sk": "server1",
    "teamA":['8EE5313A2172CD', '934D5F55971F99', 'FDE32828995C0A', 'FFF4EF1FD439BB', '2918F80471E175', '23A888044AAD96', 'E79B8A18A9DBB7', 'FDA55D6797CBBE'],
    "teamB":['CBF7EA94F52F1D', '31CC569471C123', 'C16F54F4CB35C3', '1CEEDAB80C53DD', 'C241209BDE5BB0', '44D0B08B78E9F2', '1441314A80B76F'],
    "teamAname":"pek",
    "teamBname":"kek",
    "teamAscore":1,
    "teamBscore":0,
    "expiretime":1000
    }

scores["teamA"] = scores["teamA"][0:5]


def match_guids_to_team(guids):
    teamAmatches = teamBmatches = 0
    for guid in guids:
        if guid in scores["teamA"]:
            teamAmatches += 1
        elif guid in scores["teamB"]:
            teamBmatches += 1
    if teamAmatches > teamBmatches and teamAmatches/len(guids) > 0.6:
        return "teamA"
    elif teamAmatches < teamBmatches and teamBmatches/len(guids) > 0.6:
        return "teamB"
    else:
        raise Exception("Failed to match teams for match xyx")


def team_name_nchar(team_names):
    all_tokens = []
    for n in [2,3,4,5,6]:
        for name in team_names:
            name = name.replace(" ","_")
            tokens = [name[i:i+n] for i in range(0, len(name), 1)]
            tokens = [x for x in tokens if len(x) >= n]
            tokens = list(set(tokens))
            all_tokens.extend(tokens)

    tokens_counter = Counter(all_tokens)

    clan_tag = ""
    most_common_count = tokens_counter.most_common(1)[0][1]
    if most_common_count < int(len(team_names)*0.67):
        # print("Not enough matches")
        return clan_tag
    for popular_token in tokens_counter.most_common():
        if popular_token[1] == most_common_count:
            if len(clan_tag) < len(popular_token[0]):
                clan_tag = popular_token[0]
    return clan_tag


teams["team1match"] = match_guids_to_team(teams["team1guids"])
teams["team2match"] = match_guids_to_team(teams["team2guids"])

teams["team1name"] = team_name_nchar(teams["team1names"])
teams["team2name"] = team_name_nchar(teams["team2names"])


teamAmatches = teamBmatches = 0

for guid in teams["team2guids"]:
    print("Team 2 guid " + guid)
    if guid in scores["teamA"]:
        print("teamA")
    elif guid in scores["teamB"]:
        print("teamB")



players = ["exe-donkz",
           "exe-wiza4d",
           "exe-flog",
           "exe-blog",
           "exe-bizz",
           "exe-pschonic",
           "ex-tard"]

#similar chars, but not too many
players2 = ["colgate",
           "wiza4d",
           "flog",
           "blog",
           "bolg",
           "mogul",
           ]

#one letter somewhat repeats
players3 = ["donkz",
           "kittens",
           "caffeine",
           "blan",
           "clan",
           "brazilian flan"]

#suffix clan tag
players4 = ["luigi-x-",
           "eternal-x-",
           "slaya-x-",
           "ringer-x-",
           "elutard",
           "cky-x-",
           "mario-x-"
           ]

t1 = [':+:Lun4tic>:O','Cypher','John_Mullins','Kittens','illkilla','DillWeed']   #   too many L's
t2 = [')rek uranus(','DillWeed','Lunatic ????','Renta-Rek'] # character ? repetition
t3 = ['CHUCK_donka','Lunatic ????','SOURCE','illkilla'] # too many L's
t4 = ['CyyyyDUCK','Fister Miagi','donka'] # too many Y's
t5 = ['KrAzYkAzE','Lunatic ????','VirUs047','bru','caff*****','rek tum'] # too many *'s
t6 = ['----E Spuddy', '----E caffeine', '----E copserr', '----E jaytee', '----E oreo', '----E v1k!ng']
t7 = ['/mute ABomB', '/mute Op!o', '/mute doNka', '/mute eternal', '/mute nigel', '/mute sem']

test_strings = [players, players2, players3, players4,t1,t2,t3,t4,t5,t6,t7]

def test_all(test_strings):
    for team in test_strings:
        print("Team: " + ",".join(team))
        print("Result: " + team_name_nchar(team))

test_all(test_strings)
