from azure.cosmos import CosmosClient, PartitionKey
from keyring import get_password
import os


DATABASE = "nhl-data"
CONTAINER = "nhl-shots"
LOG_CONTAINER = "export-log"
ENDPOINT = "https://nhl-data.documents.azure.com:443/"


# Export a dataframe to a Cosmos container
def export_dataframe(dataframe):
    key = get_key()
    with CosmosClient(url=ENDPOINT, credential=key) as cc:
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
def export_log():
    key = get_key()
    clear_container(LOG_CONTAINER)
    with open("trace.log", mode="r") as f:
        log = {"log": "".join(f.readlines())}
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
