import pandas as pd
import requests
import zipfile
import io

YEAR = 2022
BASE_URL = f"https://peter-tanner.com/moneypuck/downloads/shots_{YEAR}.zip"


# Retrieve moneypuck data
def get_moneypuck_shots():
    r = requests.get(BASE_URL)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall()
    df = pd.read_csv(f"shots_{YEAR}.csv")

    # Add shotID column
    df = df.drop(columns=["shotID", "id"], axis=1)
    df["shotId"] = range(1, len(df) + 1)

    return df


# Remove old shots from df
def get_new_shots(df, last_shot_id):
    return df.query(f"shotId > {last_shot_id}")


# Add strength value
def add_strength(df):
    df["strength"] = df.apply(lambda row: get_strength(row), axis=1)
    return df


# Decide strength
def get_strength(shot):
    home_players = shot["homeSkatersOnIce"] - shot["homeEmptyNet"]
    away_players = shot["awaySkatersOnIce"] - shot["awayEmptyNet"]

    is_home = bool(shot["isHomeTeam"])

    if home_players == away_players:
        return "EVEN"
    elif is_home and home_players > away_players:
        return "PP"
    elif is_home and home_players < away_players:
        return "PK"
    elif not is_home and home_players < away_players:
        return "PP"
    elif not is_home and home_players > away_players:
        return "PK"
