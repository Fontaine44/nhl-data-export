from nhl_api import get_game_ids, get_api_shots, get_teams
from nhl_scraping import get_pbp_shots

from shots import api_to_dataframe, pbp_to_dataframe, dataframe_to_json, merge_dataframes, add_side
from export import clear_container, export_dataframe
import time

YEAR = 20222023
MTL_ID = 8

def main(start_time):
    teams = get_teams()
    teams = [{"id": 10, "abb": "TOR", "name":"Toronto"}]
    # endpoint = get_password("azure-nhl-data", "URI")
    # key = get_password("azure-nhl-data", "Primary Key")

    # with CosmosClient(url=endpoint, credential=key) as cc:
    #     db = cc.get_database_client("nhl-data")
    #     clear_container(db, 'nhl-shots')

    for team in teams:
        print("exporting: "+team['name'])
        df = export_team_shots(team)
        # export_dataframe(df)
        dataframe_to_json(df)
        tim = time.time() - start_time
        start_time += tim
        print("exported: "+team['name']+" in "+("%s seconds" % tim))


def export_team_shots(team):
    game_ids = get_game_ids(team['id'])

    api_shots = get_api_shots(game_ids, team['id'])
    api_shots_df = api_to_dataframe(api_shots)

    pbp_shots = get_pbp_shots(game_ids, team['id'], team['abb'])
    pbp_shots_df = pbp_to_dataframe(pbp_shots)

    merged_df = merge_dataframes(api_shots_df, pbp_shots_df)

    merged_df["crossRed"] = add_side(merged_df)

    print("total shots: ", len(merged_df))
    
    return merged_df

if __name__ == "__main__":
    start_time = time.time()
    main(start_time)
    print("--- %s seconds ---" % (time.time() - start_time))

