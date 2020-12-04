import logging
import sys
import os
import yaml
from pathlib import Path

from sql_database_client import SQLDatabaseClient


def create_directories(config):
    """
    Creates directories if not present yet.
    """
    for directory in config:
        if not os.path.exists(config[directory]):
            os.mkdir(config[directory])
            logging.debug("Directory {} created".format(config[directory]))
        else:
            logging.debug("Directory {} already exists".format(config[directory]))


def main():
    """
    Run query to get data from database specified in the config-file, manipulate the data and store
    it again in another table in the database.
    """
    # Load configurations
    job_config = yaml.full_load(Path("project_config.yaml").read_text())

    # Create directories if not present
    create_directories(job_config["directories"])

    # Initiate sql database client
    sql_database_client = SQLDatabaseClient(job_config)

    # load data from database as a pandas DataFrame
    df = sql_database_client.load_data(
        job_config["load_query"],
        index_cols=job_config["index_columns"],
        use_cache=job_config["use_cache"],
    )
    # write dataframe to csv-file in "datadump" directory (if you like)
    # df.to_csv(os.path.join(job_config["directories"]["datadump_dir"], "raw_data.csv"))

    # print head of dataframe and summary statistics to have a quick check
    print(df.head())
    print(df.describe())

    # do your stuff here to manipulate the dataframe
    # ...
    # df_adjusted = ...
    # ...

    # write to database
    # sql_database_client.write_data(df_adjusted, "table_name", if_exists="replace")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stdout,
        format="%(asctime)s %(name)-4s: %(module)-4s: %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    main()
