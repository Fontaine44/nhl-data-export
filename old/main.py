import datetime
import logging
import sys
import argparse
import time
import nhl_api
import shots
import export


YEAR = 20222023


# Entry point
def main(argv):
    exit_code = 0
    logger = set_logger()
    log_start_time(logger)
    start_time = time.time()
    try:
        namespace = parse_args(argv)
        clear = namespace.clear
        team_id = namespace.id

        teams = nhl_api.get_teams(team_id)

        if clear:
            export.clear_shots()
            logger.info("Container nhl_shots cleared")

        for team in teams:
            team_start = time.time()
            logger.info(f"-> Retrieving shots for the {team['name']}")

            game_ids = nhl_api.get_game_ids(team['id'], not clear)
            logger.info(f"Number of games to update: {len(game_ids)}")

            if len(game_ids) > 0:
                df = shots.get_team_shots(game_ids, team)
                logger.info(f"Number of shots: {len(df)}")
                export.export_dataframe(df)

            team_time = int(time.time() - team_start)
            logger.info(f"Exported in {team_time} seconds")

    except:
        logger.exception("An error has occured.")
        exit_code = 1

    log_exit_time(logger, start_time)
    export.export_log()
    logger.info(f"Log file exported")
    sys.exit(exit_code)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog='NHL SHOTS EXPORTER',
        description='Export NHL shots to a COSMOS DB')
    parser.add_argument('-i', '--id', type=int)
    parser.add_argument('-c', '--clear', action='store_true')
    return parser.parse_args(argv)


def log_start_time(logger):
    date = str(datetime.date.today())
    current_time = str(datetime.datetime.now().strftime("%H:%M:%S"))
    logger.info("----------------------------------------")
    logger.info(f"DATE: {date}")
    logger.info(f"TIME: {current_time}")
    logger.info("----------------------------------------")


def log_exit_time(logger, start_time):
    time_elapsed = time.time() - start_time
    minutes = int(time_elapsed // 60)
    seconds = int(time_elapsed % 60)
    logger.info("----------------------------------------")
    logger.info(f"TOTAL ELAPSED TIME: {minutes} min | {seconds} sec")
    logger.info("----------------------------------------")


def set_logger():
    logger = logging.getLogger('trace')
    logger.setLevel(logging.INFO)
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('trace.log', mode='w')
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    return logger


if __name__ == "__main__":
    main(sys.argv[1:])
