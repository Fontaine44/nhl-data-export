from azure.cosmos import CosmosClient, PartitionKey
from keyring import get_password
import os
import datetime


DATABASE = "nhl-data"
CONTAINER = "nhl-shots"
LOG_CONTAINER = "export-log"
ENDPOINT = "https://nhl-data.documents.azure.com:443/"


# Export a dataframe to a Cosmos container
def export_dataframe(dataframe):
    key = get_key()
    with CosmosClient(url=ENDPOINT, credential=key, request_timeout=3000) as cc:
        db = cc.get_database_client(DATABASE)
        container_client = db.get_container_client(CONTAINER)
        for row in dataframe.to_dict(orient="records"):
            container_client.create_item(
                row, enable_automatic_id_generation=True)


# Get the Cosmos primary key
def get_key():
    key = os.getenv('COSMOS_PK')
    if key is None:
        key = get_password("azure-nhl-data", "Primary Key")
    return key


# Clear a container
def clear_container(name):
    key = get_key()
    with CosmosClient(url=ENDPOINT, credential=key) as cc:
        db = cc.get_database_client(DATABASE)
        try:
            db.delete_container(name)
        finally:
            db.create_container(name, partition_key=PartitionKey(path='/id'))


# Clear the shots container
def clear_shots():
    clear_container(CONTAINER)


# Store the log file in the database
def export_log(success):
    key = get_key()
    date = str(datetime.date.today())
    current_time = str(datetime.datetime.now().strftime("%H:%M:%S"))
    with open("trace.log", mode="r") as f:
        log = {
            "log": "".join(f.readlines()),
            "date": date,
            "time": current_time,
            "success": success
        }
        with CosmosClient(url=ENDPOINT, credential=key) as cc:
            db = cc.get_database_client(DATABASE)
            container_client = db.get_container_client(LOG_CONTAINER)
            container_client.create_item(
                log, enable_automatic_id_generation=True)


# Get the games present in the db for a team
def get_exported_games(team_id):
    key = get_key()
    with CosmosClient(url=ENDPOINT, credential=key) as cc:
        db = cc.get_database_client(DATABASE)
        container_client = db.get_container_client(CONTAINER)
        query = "SELECT DISTINCT VALUE c.gameId FROM c WHERE c.teamId = @teamId"
        parameters = [{
            "name": "@teamId",
            "value": team_id
        }]
        return list(container_client.query_items(
            query=query, parameters=parameters, enable_cross_partition_query=True))


# Get the id of the last shot in the database
def get_last_shot_id():
    key = get_key()
    with CosmosClient(url=ENDPOINT, credential=key) as cc:
        db = cc.get_database_client(DATABASE)
        container_client = db.get_container_client(CONTAINER)
        query = "SELECT value MAX(c.shotId) FROM c"
        items_iterator = container_client.query_items(
            query=query, enable_cross_partition_query=True)
        try:
            return list(items_iterator)[0]
        except:
            return -1
