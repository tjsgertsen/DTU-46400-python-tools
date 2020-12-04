import logging
import os
import pickle
from datetime import datetime
from pathlib import Path
import pandas as pd
import pymysql.cursors
import sqlalchemy

from sqlalchemy.engine import url
from tenacity import retry, stop_after_attempt, wait_exponential, before_sleep_log
import sqlalchemy.types as sql_dtype

LOGGER = logging.getLogger(__name__)


class SQLDatabaseClient:
    """
    A class to facilitate easy querying to different SQL databases. It also generates
    a cache folder in order to store (large) data files so that you do not have to
    load the data each time you run the script.
    """

    def __init__(self, job_config):
        """
        Initialize an object.
        Args:
            job_config: Config file to specify project specific details.
        """
        self.db_client = None
        self.db_engine = None

        self.load_config = job_config["load"]
        try:
            self.credentials_store = job_config["store"]
            self.engine_encoding = self.credentials_store["encoding"]
            self.engine_dict = {
                "drivername": self.credentials_store["drivername"],
                "username": self.credentials_store["username"],
                "password": self.credentials_store["password"],
                "host": self.credentials_store["host"],
                "port": self.credentials_store["port"],
                "database": self.credentials_store["database"],
                "query": {"charset": self.engine_encoding},
            }
        except (KeyError, TypeError, ValueError, OverflowError):
            self.credentials_store = None
            LOGGER.debug(
                "No destination is given, in local_config.yaml, to store data."
            )

        self.cache_dir = job_config["directories"]["cache_dir"]
        self.query_dir = job_config["directories"]["query_dir"]
        self.suffix = datetime.utcnow().strftime("%Y%m%d")

        self.query_file_fmt = "{}.sql"
        self.cache_file_fmt = "{}_cache_{}.pickle"

    def clear_cached_files(self, filename):
        """
        Delete any earlier pickle files from this query in order to update it with the latest.
        Args:
            filename (str): filename of the query that is executed.
        """
        for file in os.listdir(self.cache_dir):
            file_path = os.path.join(self.cache_dir, file)
            if file.startswith(filename):
                os.remove(file_path)

    @retry(
        wait=wait_exponential(multiplier=1, min=5, max=10),
        stop=stop_after_attempt(10),
        before_sleep=before_sleep_log(LOGGER, logging.INFO),
    )
    def load_data(self, filename, index_cols=0, use_cache=False):
        """
        Load data from specified database.
        Args:
            filename (str): filename of the query that has to be executed.
            use_cache (bool): using cache True or False (default=False).

        Returns:
            df (DataFrame): pandas DataFrame with queried data.
        """
        cache_filename = os.path.join(
            self.cache_dir,
            self.cache_file_fmt.format(filename, self.suffix),
        )

        if os.path.exists(cache_filename) and use_cache:
            LOGGER.info("Loading {} data from cache.".format(filename))
            with open(cache_filename, "rb") as f:
                df = pickle.load(f)

        else:
            LOGGER.info("Querying {} from database.".format(filename))
            query = Path(
                self.query_dir, self.query_file_fmt.format(filename)
            ).read_text()

            if self.db_client is None:
                self.db_client = pymysql.connect(
                    host=self.load_config["host"],
                    port=self.load_config["port"],
                    user=self.load_config["username"],
                    password=self.load_config["password"],
                    database=self.load_config["database"],
                    charset="utf8",
                )

            cursor = self.db_client.cursor()
            cursor.execute(query=query)
            df = pd.DataFrame(cursor.fetchall())
            df.columns = [i[0] for i in cursor.description]
            df.columns = df.columns.str.lower()
            if index_cols > 0:
                df.set_index(list(df.columns[list(range(index_cols))]), inplace=True)

            cursor.close()
            self.db_client.close()

            self.clear_cached_files(filename)

            if use_cache:
                with open(cache_filename, "wb") as f:
                    pickle.dump(df, f)

        return df

    def dtype_to_sqldtype(self, df):
        """
        Creates a dict of SQL-dtypes to pass to the pd.to_sql() command.
        Args:
            df (DataFrame): Dataframe with data to be converted.

        Returns:
            sqldtype_dict (dict): Dict with SQL-dtype per column.
        """
        sqldtype_dict = {}
        for i, j in zip(df.columns, df.dtypes):
            if "object" in str(j):
                sqldtype_dict.update({i: sql_dtype.VARCHAR(12)})

            if "datetime" in str(j):
                sqldtype_dict.update({i: sql_dtype.DateTime()})

            if "float" in str(j):
                sqldtype_dict.update({i: sql_dtype.FLOAT(precision=12)})

            if "int" in str(j):
                sqldtype_dict.update({i: sql_dtype.INT()})

        return sqldtype_dict

    @retry(
        wait=wait_exponential(multiplier=1, min=5, max=10),
        stop=stop_after_attempt(10),
        before_sleep=before_sleep_log(LOGGER, logging.INFO),
    )
    def write_data(self, df, table_name, if_exists="fail"):
        """
        Write data from specified database.
        Args:
            if_exists (str): argument on what to do if there is already a table with "table_name".
            table_name (str): table name in database to write the data to.
            df (pd.DataFrame): filename of the query that has to be executed.
        """
        LOGGER.info(f"Writing dataframe to database.")

        if self.credentials_store is None:
            raise ValueError(
                "No destination is given, in local_config.yaml, to store data."
            )

        if self.db_engine is None:
            engine_url = url.URL(**self.engine_dict)
            LOGGER.debug(engine_url)

            self.db_engine = sqlalchemy.create_engine(
                engine_url, encoding=self.engine_encoding
            )

        sql_dtypes = self.dtype_to_sqldtype(df.reset_index())
        df.to_sql(
            table_name,
            con=self.db_engine,
            if_exists=if_exists,
            dtype=sql_dtypes,
        )
