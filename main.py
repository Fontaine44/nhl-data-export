import time
import argparse
import sys
import logging
import datetime
import numpy
import export
import moneypuck


YEAR = 20242025


# Entry point
def main():
    exit_code = 0
    success = True
    logger = set_logger()
    log_start_time(logger)
    start_time = time.time()
    try:
        df = moneypuck.get_moneypuck_shots()
        logger.info("Retrieved data from moneypuck")

        # Add shotID column
        df = df.drop(columns=["shotID", "id"], axis=1)
        df["shotId"] = range(1, len(df) + 1)

        last_shot_id = export.get_last_shot_id()
        df = moneypuck.get_new_shots(df, last_shot_id)
        logger.info("Removed old shots")

        df = moneypuck.add_strength(df)
        logger.info("Added strength")

        df = moneypuck.add_zone(df)
        logger.info("Added zone")

        # Sort columns alphabetically
        df = df.reindex(sorted(df.columns), axis=1)

        # Shift column 'shotId' to first position 
        first_column = df.pop('shotId') 
        df.insert(0, 'shotId', first_column)

        logger.info("Cleaned data")

        df.replace(to_replace=[numpy.nan], value=None, inplace=True)
        logger.info("Data is ready, exporting to database...")

        export.export_dataframe(df)
        logger.info(f"Sucessfully exported {len(df)} shots")

    except:
        logger.exception("An error has occured.")
        exit_code = 1
        success = False

    log_exit_time(logger, start_time)
    export.export_log(success)
    logger.info(f"Log file exported")
    sys.exit(exit_code)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog='NHL SHOTS EXPORTER',
        description='Export NHL shots to a ORACLE DB')
    parser.add_argument('-i', '--id', type=int)
    parser.add_argument('-c', '--clear', action='store_true')
    return parser.parse_args(argv)


def log_start_time(logger):
    date = str(datetime.date.today())
    current_time = str(datetime.datetime.now().strftime("%H:%M:%S"))
    logger.info(
        "--------------------------------------------------------------------------------")
    logger.info(f"DATE: {date}")
    logger.info(f"TIME: {current_time}")
    logger.info(
        "--------------------------------------------------------------------------------")


def log_exit_time(logger, start_time):
    time_elapsed = time.time() - start_time
    minutes = int(time_elapsed // 60)
    seconds = int(time_elapsed % 60)
    logger.info(
        "--------------------------------------------------------------------------------")
    logger.info(f"TOTAL ELAPSED TIME: {minutes} min | {seconds} sec")
    logger.info(
        "--------------------------------------------------------------------------------")


def set_logger():
    logger = logging.getLogger('trace')
    logger.setLevel(logging.INFO)
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('trace.log', mode='w')
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    return logger


if __name__ == "__main__":
    main()