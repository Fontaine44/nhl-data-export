from pandas import DataFrame
import pandas as pd
import json
import math


# convert api data to dataframe
def api_to_dataframe(shots: list) -> DataFrame:
    events = []
    for shot in shots:
        events.append({
            "playerId": shot["players"][0]["player"]["id"],
            "playerName": shot["players"][0]["player"]["fullName"],
            "eventTypeId": shot["result"]["eventTypeId"],
            "secondaryType":  shot["result"].get("secondaryType"),
            "time": convert_time(shot["about"]["periodTime"], shot["about"]["period"]),
            "period": shot["about"]["period"],
            "x": abs(shot["coordinates"]["x"]),
            "y": get_y(shot["coordinates"]["x"], shot["coordinates"]["y"]),
            "teamId": shot["team"]["id"],
            "team": shot["team"]["triCode"],
            "gameId": shot["gameId"]
        })
    return pd.DataFrame(events)


# convert pbp data to dataframe
def pbp_to_dataframe(shots: list) -> DataFrame:
    return pd.DataFrame(shots)


# writes a dataframe to a json file
def dataframe_to_json(df: DataFrame, filename="shots.json") -> None:
    with open(filename, "w") as f:
        df.to_json(f, orient="records", indent=2)


def merge_dataframes(df1: DataFrame, df2: DataFrame) -> DataFrame: 
    merged = pd.merge(df1, df2, left_index=True, right_index=True, how='outer', suffixes=('', '_DROP'))
    return merged.filter(regex='^(?!.*_DROP)')


def convert_time(time, period):
    time = time.split(":")
    seconds = int(time[0])*60 + int(time[1])
    seconds += (period-1)*1200
    return seconds


def time_to_game_time(time: int, period: int):
    seconds = time%60
    print(seconds)
    minutes = (time-seconds-(period-1)*1200)//60
    print(type(minutes))
    return f"{minutes}:{seconds}"

def get_y(x, y):
    if x < 0:
        return -y
    else:
        return y

def add_side(df):
    df = df.reset_index()
    cross_middle = []
    for index, row in df.iterrows():
        if row["distance"] < 89:
            cross_middle.append(True)
        elif row["distance"] > 99:
            cross_middle.append(False)
        else:
            max_distance = 89**2+row["y"]**2
            cross_middle.append(row['distance']**2 <= max_distance)
    return cross_middle
