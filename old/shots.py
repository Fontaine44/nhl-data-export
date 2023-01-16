import pandas as pd
import nhl_api
import nhl_scraping


# Get a dataframe of a team's shots
def get_team_shots(game_ids, team):

    api_shots = nhl_api.get_api_shots(game_ids, team['id'])
    api_shots_df = api_to_dataframe(api_shots)

    pbp_shots = nhl_scraping.get_pbp_shots(game_ids, team['abb'])
    pbp_shots_df = nhl_scraping.pbp_to_dataframe(pbp_shots)

    merged_df = merge_dataframes(api_shots_df, pbp_shots_df)

    merged_df["offensiveSide"] = add_side(merged_df)

    return merged_df


# Convert api data to dataframe
def api_to_dataframe(shots):
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


# Merge the shots from the api with pbp
def merge_dataframes(df1, df2):
    merged = pd.merge(df1, df2, left_index=True,
                      right_index=True, how='outer', suffixes=('', '_DROP'))
    return merged.filter(regex='^(?!.*_DROP)')


# Convert game time to seconds
def convert_time(time, period):
    time = time.split(":")
    seconds = int(time[0])*60 + int(time[1])
    seconds += (period-1)*1200
    return seconds


# Convert seconds to game time
def time_to_game_time(time: int, period: int):
    seconds = time % 60
    minutes = (time-seconds-(period-1)*1200)//60
    return f"{minutes}:{seconds}"


# Get the adjusted y coordinate
def get_y(x, y):
    if x < 0:
        return -y
    else:
        return y


# Add the side attribute
def add_side(df):
    df = df.reset_index()
    off_side = []
    for index, row in df.iterrows():
        if row["distance"] < 89:
            off_side.append(True)
        elif row["distance"] > 99:
            off_side.append(False)
        else:
            max_distance = 89**2+row["y"]**2
            off_side.append(row['distance']**2 <= max_distance)
    return off_side
