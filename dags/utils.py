import logging
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine
from pathlib import Path
from typing import Literal
import sys
from dotenv import load_dotenv, find_dotenv
from os import getenv
from pymongo.mongo_client import MongoClient
from typing import Union

from objs import CredentialHolder, DotEnvError, DatabaseConnectionError


def get_logger(logger_name: str) -> logging.Logger:

    logger = logging.getLogger(name=logger_name)

    logger.setLevel(level=logging.INFO)

    formatter = logging.Formatter(
        fmt="[%(asctime)s] {%(name)s:%(lineno)d} %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger_handler = logging.StreamHandler(stream=sys.stdout)
    logger_handler.setFormatter(formatter)
    logger.addHandler(logger_handler)

    return logger


logger = get_logger(logger_name=str(Path(Path(__file__).name)))


class DatabaseConnector:
    def __init__(self, db: Literal["pg_dwh", "pg_source", "mongo_source"]) -> None:
        self.db = db

    def _get_creds_holder(self) -> CredentialHolder:
        logger.info("Loading .env variables.")
        try:
            find_dotenv(raise_error_if_not_found=True)
            load_dotenv(verbose=True, override=True)
            logger.info(".env was loaded successfully.")
        except Exception:
            logger.exception("Unable to load .env! Please check if its accessible.")
            raise DotEnvError

        if self.db == "pg_dwh":
            try:
                holder = CredentialHolder(
                    host=getenv("PG_DWH_HOST"),
                    port=getenv("PG_DWH_PORT"),
                    user=getenv("PG_DWH_USER"),
                    password=getenv("PG_DWH_PASSWORD"),
                    database=getenv("PG_DWH_DB"),
                )
                logger.info(f"All variables recieved for `{self.db}`.")
            except Exception:
                logger.exception(
                    f"Unable to get one of or all variables for `{self.db}`! See last recent traceback call."
                )
                raise DotEnvError

        if self.db == "pg_source":
            try:
                holder = CredentialHolder(
                    host=getenv("PG_SOURCE_HOST"),
                    port=getenv("PG_SOURCE_PORT"),
                    user=getenv("PG_SOURCE_USER"),
                    password=getenv("PG_SOURCE_PASSWORD"),
                    database=getenv("PG_SOURCE_DB"),
                )
                logger.info(f"All variables recieved for `{self.db}`.")
            except Exception:
                logger.exception(
                    f"Unable to get one of or all variables for `{self.db}`! See last recent traceback call."
                )
                raise DotEnvError

        if self.db == "mongo_source":
            try:
                holder = CredentialHolder(
                    host=getenv("MONGO_SOURCE_HOST"),
                    port=getenv("MONGO_SOURCE_PORT"),
                    user=getenv("MONGO_SOURCE_USER"),
                    password=getenv("MONGO_SOURCE_PASSWORD"),
                    database=getenv("MONGO_SOURCE_DB"),
                    ca_path=getenv("MONGO_SOURCE_CA_PATH"),
                    repl_set=getenv("MONGO_SOURCE_REPL_SET"),
                )
                logger.info(f"All variables recieved for `{self.db}`.")
            except Exception:
                logger.exception(
                    f"Unable to get one of or all variables for `{self.db}`! See last recent traceback call."
                )
                raise DotEnvError

        return holder

    def connect_to_database(self) -> Union[Engine, MongoClient]:

        holder = self._get_creds_holder()

        if self.db == "pg_dwh" or self.db == "pg_source":
            logger.info(f"Connecting to `{holder.database}` database.")
            conn_obj = create_engine(
                f"postgresql+psycopg2://{holder.user}:{holder.password}@{holder.host}:{holder.port}/{holder.database}"
            )
            try:
                conn = conn_obj.connect()
                conn.close()
                logger.info(f"Succesfully connected to `{holder.database}` database.")
            except Exception:
                logger.exception(f"Connection to `{holder.database}` database failed!")
                raise DatabaseConnectionError

        if self.db == "mongo_source":
            logger.info(f"Connecting to `{holder.database}` database.")
            try:
                conn_obj = MongoClient(
                    f"mongodb://{holder.user}:{holder.password}@{holder.host}:{holder.port}/?replicaSet={holder.repl_set}&authSource={holder.database}",
                    tlscafile=holder.ca_path,
                    connect=True,
                )[holder.database]

                logger.info(f"Succesfully connected to `{holder.database}` database.")
            except Exception:
                logger.exception(f"Connection to `{holder.database}` database failed!")
                raise DatabaseConnectionError

        return conn_obj


if __name__ == "__main__":

    conn = DatabaseConnector(db="mongo_source").connect_to_database()
