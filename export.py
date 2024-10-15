import os
import datetime
import oracledb


ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
CONNECTION_STR='''(description= (retry_count=1)(retry_delay=3)(address=(protocol=tcps)(port=1521)(host=adb.ca-montreal-1.oraclecloud.com))(connect_data=(service_name=g8776c1047b3446_fkmjnxbscms692ba_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'''


# Export a dataframe to an Oracle database
def export_dataframe(df):
    with oracledb.connect(
        user="NHL_API",
        password=ORACLE_PASSWORD,
        dsn=CONNECTION_STR) as conn:
    
        cursor = conn.cursor()
        insert_query = 'INSERT INTO Shots VALUES(:1'
        df_list = df.values.tolist()

        if len(df_list) == 0:
            return

        for i in range(2, len(df_list[0])+1):
            insert_query += f',:{i}'
        insert_query += ')'

        for i in range(df.shape[0]):
            cursor.execute(insert_query, df_list[i])
        
        conn.commit()


# Store the log file in the database
def export_log(success):
    with oracledb.connect(
        user="NHL_API",
        password=ORACLE_PASSWORD,
        dsn=CONNECTION_STR) as conn:
            
        date = datetime.datetime.today()

        with open("trace.log", "r") as f:
            log = "".join(f.readlines())

        cursor = conn.cursor()
        query = 'INSERT INTO EXPORT_LOGS VALUES(:1, :2, :3)'
        cursor.execute(query, (log, date, success))

        conn.commit()


# Get the games present in the db for a team
def get_exported_games(team_code):
    with oracledb.connect(
        user="NHL_API",
        password=ORACLE_PASSWORD,
        dsn=CONNECTION_STR) as conn:
    
        cursor = conn.cursor()
        query = 'SELECT DISTINCT game_id FROM NHL_API.Shots WHERE teamCode = :1'
        cursor.execute(query, (team_code,))

        return cursor.fetchall()


# Get the id of the last shot in the database
def get_last_shot_id():
    with oracledb.connect(
        user="NHL_API",
        password=ORACLE_PASSWORD,
        dsn=CONNECTION_STR) as conn:
    
        cursor = conn.cursor()
        query = 'SELECT MAX(ShotId) AS max_shot_id FROM NHL_API.SHOTS'

        cursor.execute(query)
        
        return cursor.fetchone()[0]
