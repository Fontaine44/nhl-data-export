from bs4 import BeautifulSoup
from shots import convert_time
import requests

BASE_URL = "https://www.nhl.com/scores/htmlreports/"
SEASON = 20222023
YEAR = 2022


def get_pbp_shots(game_ids: list, team_id, team_abb) -> list:
    if len(game_ids) > 1:
        shots = []
        for id in game_ids:
            shots += get_game_shots(id, team_id, team_abb)
        return shots
    elif len(game_ids) == 1:
        return get_game_shots(game_ids[0], team_id, team_abb)
    else:
        return None


def get_game_shots(game_id: list, team_id, team_abb) -> list:
    page = requests.get(f"{BASE_URL}{SEASON}/PL{str(game_id)[-6:]}.HTM")
    soup = BeautifulSoup(page.content, "html.parser")
    tables = soup.find_all("table", class_="tablewidth")

    shots = []
    for table in tables:
        rows = table.find_all("tr", {"class": ["evenColor", "oddColor"]})
        for row in rows:
            col = row.find_all("td", recursive=False)
            if col[4].text in ["SHOT", "GOAL"] and col[5].text[:3] == team_abb and col[1].text != "5":
                shots.append({
                    "strength": col[2].text,
                    "time": convert_time(col[3].get_text(strip=True, separator='\n').splitlines()[0], int(col[1].text)),
                    "distance": get_distance(col[5].text),
                    "gameId": game_id
                })
    return shots


def get_distance(des: str):
    return int(des[:des.find("ft.")].split()[-1])

