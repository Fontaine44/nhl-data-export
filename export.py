from keyring import get_password
import json
from azure.cosmos import CosmosClient, PartitionKey, ContainerProxy, DatabaseProxy


def clear_container(db_client, container_name):
    try:
        db_client.delete_container(container_name)
    finally:
        db_client.create_container(container_name, partition_key=PartitionKey(path='/id'))


# def get_database_client():
#     endpoint = get_password("azure-nhl-data", "URI")
#     key = get_password("azure-nhl-data", "Primary Key")

#     client = CosmosClient(url=endpoint, credential=key)
#     return client.get_database_client("nhl-data")


# def get_container_client(db_client, container_name):
#     return db_client.get_container_client(container_name)


def export_dataframe(dataframe):
    endpoint = get_password("azure-nhl-data", "URI")
    key = get_password("azure-nhl-data", "Primary Key")

    with CosmosClient(url=endpoint, credential=key) as cc:
        db = cc.get_database_client("nhl-data")
        container_client = db.get_container_client('nhl-shots')
        for row in dataframe.to_dict(orient="records"):
            container_client.create_item(row, enable_automatic_id_generation=True)

    
























def run(dataframe):


    drop_data(database, dataframe)




def drop_data(database: DatabaseProxy, dataframe):
    try:
        database.delete_container("nhl-shots")
    finally:
        database.create_container("nhl-shots", partition_key=PartitionKey(path='/id'))
        container = database.get_container_client("nhl-shots")
        
        for row in dataframe.to_dict(orient="records"):
            container.create_item(row, enable_automatic_id_generation=True)


def query_all_shots(container: ContainerProxy):
    qu = """SELECT 
            c.playerId,
            c.playerName,
            c.eventTypeId,
            c.secondaryType,
            c["time"],
            c.period,
            c.x,
            c.y,
            c.teamId,
            c.team,
            c.gameId,
            c.strength,
            c.distance
            FROM c
            WHERE c.strength=@strength
            AND c.playerName=@playerName
            """

    params = [
        {
            "name":"@strength",
            "value":"PP"
        },
        {
            "name":"@playerName",
            "value":"Cole Caufield"
        }
    ]

    items = container.query_items(query=qu, parameters=params, enable_cross_partition_query=True)
    it = list(items)

if __name__ == "__main__":
    endpoint = get_password("azure-nhl-data", "URI")
    key = get_password("azure-nhl-data", "Primary Key")

    client = CosmosClient(url=endpoint, credential=key)
    database = client.get_database_client("nhl-data")
    drop_data(database)
