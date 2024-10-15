import oracledb
import moneypuck
import numpy

password = "rzsx9fJPHywe7Z9"

cs='''(description= (retry_count=1)(retry_delay=3)(address=(protocol=tcps)(port=1521)(host=adb.ca-montreal-1.oraclecloud.com))(connect_data=(service_name=g8776c1047b3446_fkmjnxbscms692ba_high.adb.oraclecloud.com))(security=(ssl_server_dn_match=yes)))'''

with oracledb.connect(
     user="NHL_API",
     password=password,
     dsn=cs) as conn:
    
    cursor = conn.cursor()

    # df = moneypuck.get_moneypuck_shots()

    # # # Remove regular season shots
    # # df = df[df['isPlayoffGame'] != 0]

    # # Remove playoffs shots
    # df = df[df['isPlayoffGame'] == 0]

    # # Add shotID column
    # df = df.drop(columns=["shotID", "id"], axis=1)
    # df["shotId"] = range(1, len(df) + 1)                # TODO: why start at 1

    # df = moneypuck.add_strength(df)

    # df = moneypuck.add_zone(df)

    # # Sort columns alphabetically
    # df = df.reindex(sorted(df.columns), axis=1)

    # # Shift column 'shotId' to first position 
    # first_column = df.pop('shotId') 
    # df.insert(0, 'shotId', first_column)

    # # Sort by Id
    # df = df.sort_values(by='shotId', ascending=True)

    # df.replace(to_replace=[numpy.nan], value=None, inplace=True)

    # df = df.reset_index(drop=True)

    # insert_query = 'INSERT INTO Shots VALUES(:1'
    # df_list = df.values.tolist()

    # for i in range(2, len(df_list[0])+1):
    #     insert_query += f',:{i}'
    # insert_query += ')'

    # for i in range(df.shape[0]):
    #     cursor.execute(insert_query, df_list[i])
    
    # conn.commit()



    # # Determine column data types
    # column_types = {col: df[col].dtype for col in df.columns}

    # # Add zone and strength columns
    # column_types['zone'] = 'object'
    # column_types['strength'] = 'object'

    # # Sort columns alphabetically
    # column_types = dict(sorted(column_types.items()))

    # # Create table schema
    # create_table_query = "CREATE TABLE Shots ("

    # create_table_query += "shotID INTEGER PRIMARY KEY, "

    # for col, dtype in column_types.items():
    #     if dtype == 'object':
    #         sql_type = 'VARCHAR2(255)'
    #     elif dtype == 'int64':
    #         sql_type = 'INTEGER'
    #     elif dtype == 'float64':
    #         sql_type = 'BINARY_DOUBLE'
            
    #     if col == 'id' or col == 'shotID':
    #         pass
    #     else:
    #         create_table_query += f"{col} {sql_type}, "
    
    # create_table_query = create_table_query[:-2] + ")"

    # print(create_table_query)


    # # Execute table creation query
    # cursor.execute(create_table_query)

    # conn.commit()

    # Create table for logs
    query = "CREATE TABLE EXPORT_LOGS (log VARCHAR2(4000), export_date TIMESTAMP, success INTEGER)"

    cursor.execute(query)
    conn.commit()



# Clear with TRUNCATE TABLE NHL_API.Shots
# CREATE TABLE NHL_API.Shots (shotId INTEGER PRIMARY KEY, arenaAdjustedShotDistance BINARY_DOUBLE, arenaAdjustedXCord BINARY_DOUBLE, arenaAdjustedXCordABS BINARY_DOUBLE, arenaAdjustedYCord BINARY_DOUBLE, arenaAdjustedYCordAbs BINARY_DOUBLE, averageRestDifference BINARY_DOUBLE, awayEmptyNet INTEGER, awayPenalty1Length INTEGER, awayPenalty1TimeLeft INTEGER, awaySkatersOnIce INTEGER, awayTeamCode VARCHAR2(255), awayTeamGoals INTEGER, defendingTeamAverageTimeOnIce BINARY_DOUBLE, defendingTeamAverageTimeOnIceOfDefencemen BINARY_DOUBLE, defendingTeamAverageTimeOnIceOfDefencemenSinceFaceoff BINARY_DOUBLE, defendingTeamAverageTimeOnIceOfForwards BINARY_DOUBLE, defendingTeamAverageTimeOnIceOfForwardsSinceFaceoff BINARY_DOUBLE, defendingTeamAverageTimeOnIceSinceFaceoff BINARY_DOUBLE, defendingTeamDefencemenOnIce INTEGER, defendingTeamForwardsOnIce INTEGER, defendingTeamMaxTimeOnIce INTEGER, defendingTeamMaxTimeOnIceOfDefencemen INTEGER, defendingTeamMaxTimeOnIceOfDefencemenSinceFaceoff INTEGER, defendingTeamMaxTimeOnIceOfForwards INTEGER, defendingTeamMaxTimeOnIceOfForwardsSinceFaceoff INTEGER, defendingTeamMaxTimeOnIceSinceFaceoff INTEGER, defendingTeamMinTimeOnIce INTEGER, defendingTeamMinTimeOnIceOfDefencemen INTEGER, defendingTeamMinTimeOnIceOfDefencemenSinceFaceoff INTEGER, defendingTeamMinTimeOnIceOfForwards INTEGER, defendingTeamMinTimeOnIceOfForwardsSinceFaceoff INTEGER, defendingTeamMinTimeOnIceSinceFaceoff INTEGER, distanceFromLastEvent BINARY_DOUBLE, event VARCHAR2(255), game_id INTEGER, goal INTEGER, goalieIdForShot INTEGER, goalieNameForShot VARCHAR2(255), homeEmptyNet INTEGER, homePenalty1Length INTEGER, homePenalty1TimeLeft INTEGER, homeSkatersOnIce INTEGER, homeTeamCode VARCHAR2(255), homeTeamGoals INTEGER, homeTeamWon INTEGER, isHomeTeam INTEGER, isPlayoffGame INTEGER, lastEventCategory VARCHAR2(255), lastEventShotAngle BINARY_DOUBLE, lastEventShotDistance BINARY_DOUBLE, lastEventTeam VARCHAR2(255), lastEventxCord INTEGER, lastEventxCord_adjusted INTEGER, lastEventyCord INTEGER, lastEventyCord_adjusted INTEGER, location VARCHAR2(255), offWing INTEGER, period INTEGER, playerNumThatDidEvent INTEGER, playerNumThatDidLastEvent INTEGER, playerPositionThatDidEvent VARCHAR2(255), season INTEGER, shooterLeftRight VARCHAR2(255), shooterName VARCHAR2(255), shooterPlayerId BINARY_DOUBLE, shooterTimeOnIce INTEGER, shooterTimeOnIceSinceFaceoff INTEGER, shootingTeamAverageTimeOnIce BINARY_DOUBLE, shootingTeamAverageTimeOnIceOfDefencemen BINARY_DOUBLE, shootingTeamAverageTimeOnIceOfDefencemenSinceFaceoff BINARY_DOUBLE, shootingTeamAverageTimeOnIceOfForwards BINARY_DOUBLE, shootingTeamAverageTimeOnIceOfForwardsSinceFaceoff BINARY_DOUBLE, shootingTeamAverageTimeOnIceSinceFaceoff BINARY_DOUBLE, shootingTeamDefencemenOnIce INTEGER, shootingTeamForwardsOnIce INTEGER, shootingTeamMaxTimeOnIce INTEGER, shootingTeamMaxTimeOnIceOfDefencemen INTEGER, shootingTeamMaxTimeOnIceOfDefencemenSinceFaceoff INTEGER, shootingTeamMaxTimeOnIceOfForwards INTEGER, shootingTeamMaxTimeOnIceOfForwardsSinceFaceoff INTEGER, shootingTeamMaxTimeOnIceSinceFaceoff INTEGER, shootingTeamMinTimeOnIce INTEGER, shootingTeamMinTimeOnIceOfDefencemen INTEGER, shootingTeamMinTimeOnIceOfDefencemenSinceFaceoff INTEGER, shootingTeamMinTimeOnIceOfForwards INTEGER, shootingTeamMinTimeOnIceOfForwardsSinceFaceoff INTEGER, shootingTeamMinTimeOnIceSinceFaceoff INTEGER, shotAngle BINARY_DOUBLE, shotAngleAdjusted BINARY_DOUBLE, shotAnglePlusRebound BINARY_DOUBLE, shotAnglePlusReboundSpeed BINARY_DOUBLE, shotAngleReboundRoyalRoad INTEGER, shotDistance BINARY_DOUBLE, shotGeneratedRebound INTEGER, shotGoalieFroze INTEGER, shotOnEmptyNet INTEGER, shotPlayContinuedInZone INTEGER, shotPlayContinuedOutsideZone INTEGER, shotPlayStopped INTEGER, shotRebound INTEGER, shotRush INTEGER, shotType VARCHAR2(255), shotWasOnGoal INTEGER, speedFromLastEvent BINARY_DOUBLE, strength VARCHAR2(255), team VARCHAR2(255), teamCode VARCHAR2(255), time INTEGER, timeDifferenceSinceChange INTEGER, timeSinceFaceoff INTEGER, timeSinceLastEvent INTEGER, timeUntilNextEvent INTEGER, xCord INTEGER, xCordAdjusted INTEGER, xFroze BINARY_DOUBLE, xGoal BINARY_DOUBLE, xPlayContinuedInZone BINARY_DOUBLE, xPlayContinuedOutsideZone BINARY_DOUBLE, xPlayStopped BINARY_DOUBLE, xRebound BINARY_DOUBLE, xShotWasOnGoal BINARY_DOUBLE, yCord INTEGER, yCordAdjusted INTEGER, zone VARCHAR2(255))

# DROP TABLE NHL_API.Shots

# Creat user NHL_API and give this:
# ALTER USER NHL_API quota unlimited on DATA;

# 2024 create
# CREATE TABLE Shots (shotID INTEGER PRIMARY KEY, arenaAdjustedShotDistance BINARY_DOUBLE, arenaAdjustedXCord BINARY_DOUBLE, arenaAdjustedXCordABS BINARY_DOUBLE, arenaAdjustedYCord BINARY_DOUBLE, arenaAdjustedYCordAbs BINARY_DOUBLE, averageRestDifference BINARY_DOUBLE, awayEmptyNet INTEGER, awayPenalty1Length INTEGER, awayPenalty1TimeLeft INTEGER, awaySkatersOnIce INTEGER, awayTeamCode VARCHAR2(255), awayTeamGoals INTEGER, defendingTeamAverageTimeOnIce BINARY_DOUBLE, defendingTeamAverageTimeOnIceOfDefencemen BINARY_DOUBLE, defendingTeamAverageTimeOnIceOfDefencemenSinceFaceoff BINARY_DOUBLE, defendingTeamAverageTimeOnIceOfForwards BINARY_DOUBLE, defendingTeamAverageTimeOnIceOfForwardsSinceFaceoff BINARY_DOUBLE, defendingTeamAverageTimeOnIceSinceFaceoff BINARY_DOUBLE, defendingTeamDefencemenOnIce INTEGER, defendingTeamForwardsOnIce INTEGER, defendingTeamMaxTimeOnIce INTEGER, defendingTeamMaxTimeOnIceOfDefencemen INTEGER, defendingTeamMaxTimeOnIceOfDefencemenSinceFaceoff INTEGER, defendingTeamMaxTimeOnIceOfForwards INTEGER, defendingTeamMaxTimeOnIceOfForwardsSinceFaceoff INTEGER, defendingTeamMaxTimeOnIceSinceFaceoff INTEGER, defendingTeamMinTimeOnIce INTEGER, defendingTeamMinTimeOnIceOfDefencemen INTEGER, defendingTeamMinTimeOnIceOfDefencemenSinceFaceoff INTEGER, defendingTeamMinTimeOnIceOfForwards INTEGER, defendingTeamMinTimeOnIceOfForwardsSinceFaceoff INTEGER, defendingTeamMinTimeOnIceSinceFaceoff INTEGER, distanceFromLastEvent BINARY_DOUBLE, event VARCHAR2(255), gameOver BINARY_DOUBLE, game_id INTEGER, goal INTEGER, goalieIdForShot INTEGER, goalieNameForShot VARCHAR2(255), homeEmptyNet INTEGER, homePenalty1Length INTEGER, homePenalty1TimeLeft INTEGER, homeSkatersOnIce INTEGER, homeTeamCode VARCHAR2(255), homeTeamGoals INTEGER, homeTeamScore BINARY_DOUBLE, homeTeamWon INTEGER, homeWinProbability BINARY_DOUBLE, isHomeTeam BINARY_DOUBLE, isPlayoffGame INTEGER, lastEventCategory VARCHAR2(255), lastEventShotAngle BINARY_DOUBLE, lastEventShotDistance BINARY_DOUBLE, lastEventTeam VARCHAR2(255), lastEventxCord INTEGER, lastEventxCord_adjusted INTEGER, lastEventyCord INTEGER, lastEventyCord_adjusted INTEGER, location VARCHAR2(255), offWing INTEGER, penaltyLength BINARY_DOUBLE, period INTEGER, playerNumThatDidEvent INTEGER, playerNumThatDidLastEvent INTEGER, playerPositionThatDidEvent VARCHAR2(255), playoffGame BINARY_DOUBLE, roadTeamCode BINARY_DOUBLE, roadTeamScore BINARY_DOUBLE, season INTEGER, shooterLeftRight VARCHAR2(255), shooterName VARCHAR2(255), shooterPlayerId INTEGER, shooterTimeOnIce INTEGER, shooterTimeOnIceSinceFaceoff INTEGER, shootingTeamAverageTimeOnIce BINARY_DOUBLE, shootingTeamAverageTimeOnIceOfDefencemen BINARY_DOUBLE, shootingTeamAverageTimeOnIceOfDefencemenSinceFaceoff BINARY_DOUBLE, shootingTeamAverageTimeOnIceOfForwards BINARY_DOUBLE, shootingTeamAverageTimeOnIceOfForwardsSinceFaceoff BINARY_DOUBLE, shootingTeamAverageTimeOnIceSinceFaceoff BINARY_DOUBLE, shootingTeamDefencemenOnIce INTEGER, shootingTeamForwardsOnIce INTEGER, shootingTeamMaxTimeOnIce INTEGER, shootingTeamMaxTimeOnIceOfDefencemen INTEGER, shootingTeamMaxTimeOnIceOfDefencemenSinceFaceoff INTEGER, shootingTeamMaxTimeOnIceOfForwards INTEGER, shootingTeamMaxTimeOnIceOfForwardsSinceFaceoff INTEGER, shootingTeamMaxTimeOnIceSinceFaceoff INTEGER, shootingTeamMinTimeOnIce INTEGER, shootingTeamMinTimeOnIceOfDefencemen INTEGER, shootingTeamMinTimeOnIceOfDefencemenSinceFaceoff INTEGER, shootingTeamMinTimeOnIceOfForwards INTEGER, shootingTeamMinTimeOnIceOfForwardsSinceFaceoff INTEGER, shootingTeamMinTimeOnIceSinceFaceoff INTEGER, shotAngle BINARY_DOUBLE, shotAngleAdjusted BINARY_DOUBLE, shotAnglePlusRebound BINARY_DOUBLE, shotAnglePlusReboundSpeed BINARY_DOUBLE, shotAngleReboundRoyalRoad INTEGER, shotDistance BINARY_DOUBLE, shotGeneratedRebound INTEGER, shotGoalProbability BINARY_DOUBLE, shotGoalieFroze INTEGER, shotOnEmptyNet INTEGER, shotPlayContinued BINARY_DOUBLE, shotPlayContinuedInZone INTEGER, shotPlayContinuedOutsideZone INTEGER, shotPlayStopped INTEGER, shotRebound INTEGER, shotRush INTEGER, shotType VARCHAR2(255), shotWasOnGoal BINARY_DOUBLE, speedFromLastEvent BINARY_DOUBLE, strength VARCHAR2(255), team VARCHAR2(255), teamCode VARCHAR2(255), time INTEGER, timeBetweenEvents BINARY_DOUBLE, timeDifferenceSinceChange INTEGER, timeLeft BINARY_DOUBLE, timeSinceFaceoff INTEGER, timeSinceLastEvent INTEGER, timeUntilNextEvent INTEGER, wentToOT BINARY_DOUBLE, wentToShootout BINARY_DOUBLE, xCord INTEGER, xCordAdjusted INTEGER, xFroze BINARY_DOUBLE, xGoal BINARY_DOUBLE, xPlayContinuedInZone BINARY_DOUBLE, xPlayContinuedOutsideZone BINARY_DOUBLE, xPlayStopped BINARY_DOUBLE, xRebound BINARY_DOUBLE, xShotWasOnGoal BINARY_DOUBLE, yCord INTEGER, yCordAdjusted INTEGER, zone VARCHAR2(255))