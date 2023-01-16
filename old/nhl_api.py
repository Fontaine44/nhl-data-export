import requests
import export


BASE_URL = "https://statsapi.web.nhl.com/api/v1/"
SEASON = 20222023


# Get a list of team objects
def get_teams(id):
    response = requests.get(f"{BASE_URL}teams")
    data = response.json()
    teams = [get_team(team) for team in data['teams']]
    if id is not None:
        return [team for team in teams if team["id"] == id]
    else:
        return teams


# Get a team object
def get_team(team):
    return {
        "id": team['id'],
        "name": team['name'],
        "abb": team['abbreviation']
    }


# Get a list of game ids for a team
def get_game_ids(team_id, append):
    response = requests.get(
        f"{BASE_URL}schedule?season={SEASON}&teamId={team_id}&gameType=R")
    data = response.json()
    played_games = [i["games"][0]["gamePk"] for i in data["dates"] if i["games"]
                    [0]["status"]["statusCode"] == "7"]   # Get played games
    if not append:
        return played_games
    else:
        exported = export.get_exported_games(team_id)
        return [e for e in played_games if e not in exported]


# Get a list of shots from the NHL api for a list of games
def get_api_shots(game_ids, team_id, include_shootout=False):
    shots = []
    if len(game_ids) > 1:
        for id in game_ids:
            shots += get_game_shots(id, team_id)
    elif len(game_ids) == 1:
        shots = get_game_shots(game_ids[0], team_id)

    if not include_shootout:
        return api_remove_shootout(shots)
    else:
        return shots


# Get a list of shots from the NHL api for a game
def get_game_shots(game_id: list, team_id) -> list:
    response = requests.get(f"{BASE_URL}game/{game_id}/feed/live")
    data = response.json()
    plays = data["liveData"]["plays"]["allPlays"]
    game_shots = [i for i in plays if i["result"]["event"]
                  in ["Goal", "Shot"]]     # Get goals and shots
    team_shots = [i for i in game_shots if i["team"]["id"] == team_id]
    for sh in team_shots:
        sh["gameId"] = game_id
    return team_shots


# Remove shootout shots
def api_remove_shootout(shots: list) -> list:
    return [shot for shot in shots if shot["about"]["periodType"] != "SHOOTOUT"]
