import json
from datetime import datetime
import sys

# logging
import logging

# fs
from pathlib import Path
from dotenv import load_dotenv, find_dotenv
from os import getenv

# datebases
from sqlalchemy import create_engine, text
from pymongo.mongo_client import MongoClient

# type hints
from typing import Literal, Union, List, Any
from sqlalchemy.engine import Engine

# package
sys.path.append(str(Path(__file__).parent.parent))

from pkg.objs import CredentialHolder, EtlObj, OrdersystemObj
from pkg.errors import DotEnvError, DatabaseConnectionError, SQLError

logger = logging.getLogger(name="airflow.task")


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


class StgEtlSyncer:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_latest_sync(
        self, etl_key: str, type: Literal["latest_loaded_id", "latest_loaded_ts"]
    ) -> EtlObj:
        logger.info(f"Getting latest sync data for `{etl_key}` key.")

        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    statement=text(
                        f""" 
                        SELECT
                            id,
                            workflow_key,
                            workflow_settings
                        FROM stg.srv_wf_settings
                        WHERE workflow_key = '{etl_key}';
                    """
                    )
                ).fetchone()

            obj = EtlObj(
                id=result[0], workflow_key=result[1], workflow_settings=result[2]
            )
            logger.info(f"Sync data for `{etl_key}` key recieved.")

        except Exception:
            logger.info(
                f"There is no workflow key with name `{etl_key}`! Returning default value."
            )
            if type == "latest_loaded_ts":
                obj = EtlObj(
                    id=0,
                    workflow_key=etl_key,
                    workflow_settings={
                        "latest_loaded_ts": datetime(1900, 1, 1).isoformat()
                    },
                )
            if type == "latest_loaded_id":
                obj = EtlObj(
                    id=0,
                    workflow_key=etl_key,
                    workflow_settings={"latest_loaded_id": -1},
                )

        return obj

    def save_pg_sync(
        self,
        etl_key: str,
        collection: List[Any],
        type: Literal["latest_loaded_id", "latest_loaded_ts"],
    ) -> None:

        if type == "latest_loaded_id":
            try:
                latest_loaded_id = max([row.id for row in collection])
                etl_dict = json.dumps(
                    obj={type: latest_loaded_id}, sort_keys=True, ensure_ascii=False
                )

                try:
                    with self.engine.begin() as conn:
                        conn.execute(
                            text(
                                f"""
                                INSERT INTO
                                    stg.srv_wf_settings(workflow_key, workflow_settings)
                                VALUES
                                    ('{etl_key}', '{etl_dict}')
                                ON CONFLICT (workflow_key) DO UPDATE
                                    SET
                                        workflow_settings = excluded.workflow_settings;
                                """
                            )
                        )
                    logger.info(f"Succesfully saved `{etl_key}` ETL key data.")

                except Exception:
                    logger.exception("Unable to save sync data!")
                    raise SQLError

            except Exception:
                logger.info("Nothing to update.")

        if type == "latest_loaded_ts":
            try:
                latest_loaded_ts = str(datetime.today())
                etl_dict = json.dumps(
                    obj={type: latest_loaded_ts}, sort_keys=True, ensure_ascii=False
                )
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO
                                stg.srv_wf_settings(workflow_key, workflow_settings)
                            VALUES
                                ('{etl_key}', '{etl_dict}')
                            ON CONFLICT (workflow_key) DO UPDATE
                                SET
                                    workflow_settings = excluded.workflow_settings;
                            """
                        )
                    )
                logger.info(f"Succesfully saved `{etl_key}` ETL key data.")

            except Exception:
                logger.exception(f"Unable to save `{etl_key}` ETL key data!")
                raise SQLError

    def save_mongo_sync(self, etl_key: str, collection: List[OrdersystemObj]) -> None:
        logger.info(f"Trying to save `{etl_key}` ETL key data.")
        type = "latest_loaded_ts"

        try:
            latest_loaded_ts = str(max([row.update_ts for row in collection]))
            etl_dict = json.dumps(
                obj={type: latest_loaded_ts}, sort_keys=True, ensure_ascii=False
            )
            try:
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"""
                                INSERT INTO
                                    stg.srv_wf_settings(workflow_key, workflow_settings)
                                VALUES
                                    ('{etl_key}', '{etl_dict}')
                                ON CONFLICT (workflow_key) DO UPDATE
                                    SET
                                        workflow_settings = excluded.workflow_settings;
                                """
                        )
                    )
                logger.info(f"Succesfully saved `{etl_key}` ETL key data.")

            except Exception:
                logger.exception("Unable to save sync data!")
                raise SQLError

        except Exception:
            logger.info("Nothing to update.")


class DDSEtlSyncer:
    def __init__(self, dwh_conn: Engine) -> None:
        self.dwh_conn = dwh_conn

    def get_latest_sync(
        self, etl_key: str, type: Literal["latest_loaded_id", "latest_loaded_ts"]
    ) -> EtlObj:
        logger.info(f"Getting latest sync data for `{etl_key}` key.")

        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    statement=text(
                        f""" 
                        SELECT
                            id,
                            workflow_key,
                            workflow_settings
                        FROM stg.srv_wf_settings
                        WHERE workflow_key = '{etl_key}';
                    """
                    )
                ).fetchone()

            obj = EtlObj(
                id=result[0], workflow_key=result[1], workflow_settings=result[2]
            )
            logger.info(f"Sync data for `{etl_key}` key recieved.")

        except Exception:
            logger.info(
                f"There is no workflow key with name `{etl_key}`! Returning default value."
            )
            if type == "latest_loaded_ts":
                obj = EtlObj(
                    id=0,
                    workflow_key=etl_key,
                    workflow_settings={
                        "latest_loaded_ts": datetime(1900, 1, 1).isoformat()
                    },
                )
            if type == "latest_loaded_id":
                obj = EtlObj(
                    id=0,
                    workflow_key=etl_key,
                    workflow_settings={"latest_loaded_id": -1},
                )

        return obj

    def save_sync(
        self,
        etl_key: str,
        collection: List[Any],
        type: Literal["latest_loaded_id", "latest_loaded_ts"],
    ) -> None:

        if type == "latest_loaded_id":
            try:
                latest_loaded_id = max([row.id for row in collection])
                etl_dict = json.dumps(
                    obj={type: latest_loaded_id}, sort_keys=True, ensure_ascii=False
                )

                try:
                    with self.engine.begin() as conn:
                        conn.execute(
                            text(
                                f"""
                                INSERT INTO
                                    stg.srv_wf_settings(workflow_key, workflow_settings)
                                VALUES
                                    ('{etl_key}', '{etl_dict}')
                                ON CONFLICT (workflow_key) DO UPDATE
                                    SET
                                        workflow_settings = excluded.workflow_settings;
                                """
                            )
                        )
                    logger.info(f"Succesfully saved `{etl_key}` ETL key data.")

                except Exception:
                    logger.exception("Unable to save sync data!")
                    raise SQLError

            except Exception:
                logger.info("Nothing to update.")

        if type == "latest_loaded_ts":
            try:
                latest_loaded_ts = max([row.update_ts for row in collection])
                etl_dict = json.dumps(
                    obj={type: latest_loaded_ts}, sort_keys=True, ensure_ascii=False
                )
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO
                                stg.srv_wf_settings(workflow_key, workflow_settings)
                            VALUES
                                ('{etl_key}', '{etl_dict}')
                            ON CONFLICT (workflow_key) DO UPDATE
                                SET
                                    workflow_settings = excluded.workflow_settings;
                            """
                        )
                    )
                logger.info(f"Succesfully saved `{etl_key}` ETL key data.")

            except Exception:
                logger.exception(f"Unable to save `{etl_key}` ETL key data!")
                raise SQLError


if __name__ == "__main__":

    pass
