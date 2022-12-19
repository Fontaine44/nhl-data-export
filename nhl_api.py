import requests

BASE_URL = "https://statsapi.web.nhl.com/api/v1/"
SEASON = 20222023

def get_teams() -> list:
    response = requests.get(f"{BASE_URL}teams")
    data = response.json()
    return [
        {
            "id": team['id'],
            "name": team['name'],
            "abb": team['abbreviation']
        }
        for team in data['teams']
    ]


def get_game_ids(team_id: int) -> list:
    response = requests.get(f"{BASE_URL}schedule?season={SEASON}&teamId={team_id}&gameType=R")
    data = response.json()
    games = [ i["games"][0]["gamePk"] for i in data["dates"] if i["games"][0]["status"]["statusCode"] == "7"]   # Get played games
    return games


def get_api_shots(game_ids: list, team_id, include_shootout=False) -> list:
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


def get_game_shots(game_id: list, team_id) -> list:
        response = requests.get(f"{BASE_URL}game/{game_id}/feed/live")
        data = response.json()
        plays = data["liveData"]["plays"]["allPlays"]
        game_shots = [i for i in plays if i["result"]["event"] in ["Goal", "Shot"]]     # Get goals and shots
        team_shots = [i for i in game_shots if i["team"]["id"] == team_id]
        for sh in team_shots:
            sh["gameId"] = game_id
        return team_shots

# remove shootout shots from api shots
def api_remove_shootout(shots: list) -> list:
    return [shot for shot in shots if shot["about"]["periodType"] != "SHOOTOUT"]
