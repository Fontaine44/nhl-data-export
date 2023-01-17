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
