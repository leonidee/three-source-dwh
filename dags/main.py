from datetime import datetime
from typing import List, Any, Literal
from sqlalchemy.engine import Engine
from sqlalchemy import text
from datetime import datetime
import os
from pathlib import Path
import json


from utils import get_logger
from dataobjs import (
    EtlObj,
    BonussystemRankObj,
    BonussystemUserObj,
    BonussystemOutboxObj,
)


logger = get_logger(logger_name=str(Path(Path(__file__).name)))


class EtlWarehouseSyncer:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def get_latest_sync(self, etl_key: str) -> EtlObj:
        logger.info(f"Getting latest sync data for `{etl_key}` key.")

        try:
            with self.engine.begin() as conn:
                result = conn.execute(
                    statement=text(
                        f""" 
                        SELECT
                            id,
                            workflow_key,
                            workflow_setting
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
            obj = EtlObj(
                id=0, workflow_key=etl_key, workflow_settings={"latest_loaded_id": -1}
            )

        return obj

    def save_sync(
        self,
        etl_key: str,
        collection: List[Any],
        type: Literal["latest_loaded_id", "latest_sync_ts"],
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
                                    stg.srv_wf_settings(workflow_key, workflow_setting)
                                VALUES
                                    ('{etl_key}', '{etl_dict}')
                                ON CONFLICT (workflow_key) DO UPDATE
                                    SET
                                        workflow_setting = excluded.workflow_setting;
                                """
                            )
                        )
                    # conn.commit()
                    logger.info(f"Succesfully saved `{etl_key}` ETL key data.")

                except Exception:
                    # conn.rollback()
                    logger.exception("Unable to save sync data!")

            except Exception:
                logger.info("Nothing to update.")

        if type == "latest_sync_ts":
            try:
                latest_sync_ts = str(datetime.today())
                etl_dict = json.dumps(
                    obj={type: latest_sync_ts}, sort_keys=True, ensure_ascii=False
                )
                with self.engine.begin() as conn:
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO
                                stg.srv_wf_settings(workflow_key, workflow_setting)
                            VALUES
                                ('{etl_key}', '{etl_dict}')
                            ON CONFLICT (workflow_key) DO UPDATE
                                SET
                                    workflow_setting = excluded.workflow_setting;
                            """
                        )
                    )
                logger.info(f"Succesfully saved `{etl_key}` ETL key data.")

            except Exception:
                logger.exception("Unable to save sync data!")


class BonussystemDataMover:
    def __init__(self, origin_engine: Engine, dwh_engine: Engine) -> None:
        self.origin_engine = origin_engine
        self.dwh_engine = dwh_engine
        self.etl_syncer = EtlWarehouseSyncer(engine=dwh_engine)

    def _get_data_from_source(
        self, query: str, etl_key: str, type: Literal["snapshot", "increment"]
    ) -> List[Any]:
        logger.info(f"Getting `{etl_key}` data from source.")

        if type == "increment":
            etl_obj = self.etl_syncer.get_latest_sync(etl_key=etl_key)

            try:
                with self.origin_engine.begin() as conn:
                    result = conn.execute(
                        statement=text(
                            query.format(
                                latest_loaded_id=etl_obj.workflow_settings[
                                    "latest_loaded_id"
                                ]
                            )
                        )
                    ).fetchall()
                logger.info(f"{len(result)} rows recieved from source.")

            except Exception:
                logger.exception(
                    f"Unable to get `{etl_key}` data from source! Something went wrong."
                )
        if type == "snapshot":
            try:
                with self.origin_engine.begin() as conn:
                    result = conn.execute(statement=text(query)).fetchall()
                logger.info(f"{len(result)} rows recieved from source.")

            except Exception:
                logger.exception(
                    f"Unable to get `{etl_key}` data from source! Something went wrong."
                )
        return result

    def load_ranks_data(self) -> None:
        """Snapshot update"""

        etl_key = "ranks"

        get_query = """ 
            SELECT
                id,
                name,
                bonus_percent,
                min_payment_threshold
            FROM public.ranks;
        """

        collection = [
            BonussystemRankObj(
                id=row[0],
                name=row[1],
                bonus_percent=row[2],
                min_payment_threshold=row[3],
            )
            for row in self._get_data_from_source(
                query=get_query, etl_key=etl_key, type="snapshot"
            )
        ]

        logger.info(
            "Trying to insert public.ranks source data to stg.bonussystem_ranks table."
        )
        try:
            with self.dwh_engine.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.bonussystem_ranks(id, name, bonus_percent, min_payment_threshold)
                            VALUES
                                ({row.id}, '{row.name}', {row.bonus_percent}, {row.min_payment_threshold})
                            ON CONFLICT (id) DO UPDATE
                            SET
                                name    = excluded.name,
                                bonus_percent  = excluded.bonus_percent,
                                min_payment_threshold = excluded.min_payment_threshold;
                        """
                        )
                    )

            self.etl_syncer.save_sync(
                etl_key=etl_key, collection=collection, type="latest_sync_ts"
            )
            logger.info(
                f"stg.bonusystem_ranks table was succesfully updated. {len(collection)} rows were updated."
            )

        except Exception:
            logger.exception("Unable to insert data to stg.bonussystem_ranks table.")

    def load_users_data(self) -> None:
        """Snapshot update"""

        etl_key = "users"

        get_query = """ 
            SELECT
                id,
                order_user_id
            FROM public.users;
        """

        collection = [
            BonussystemUserObj(id=row[0], order_user_id=row[1])
            for row in self._get_data_from_source(
                query=get_query, etl_key=etl_key, type="snapshot"
            )
        ]

        logger.info(
            "Trying to insert public.users source data to stg.bonussystem_users table."
        )
        try:
            with self.dwh_engine.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.bonussystem_users(id, order_user_id)
                            VALUES
                                ({row.id}, '{row.order_user_id}')
                            ON CONFLICT (id) DO UPDATE
                            SET
                                order_user_id = excluded.order_user_id
                        """
                        )
                    )
            self.etl_syncer.save_sync(
                etl_key=etl_key, type="latest_sync_ts", collection=collection
            )
            logger.info(
                f"stg.bonusystem_users table was succesfully updated. {len(collection)} rows were updated."
            )

        except Exception:
            logger.exception("Unable to insert data to stg.bonussystem_users table.")

    def load_outbox_data(self) -> None:
        """Increment update"""

        etl_key = "outbox"

        get_query = """ 
            SELECT
                id,
                event_ts,
                event_type,
                event_value
            FROM public.outbox
            WHERE id > {latest_loaded_id};
        """

        collection = [
            BonussystemOutboxObj(
                id=row[0], event_ts=row[1], event_type=row[2], event_value=row[3]
            )
            for row in self._get_data_from_source(
                query=get_query, etl_key=etl_key, type="increment"
            )
        ]

        logger.info(
            "Trying to insert public.outbox source data to stg.bonussystem_events table."
        )
        try:
            with self.dwh_engine.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.bonussystem_events(id, event_ts, event_type, event_value)
                            VALUES
                                ({row.id}, '{row.event_ts}', '{row.event_type}', '{row.event_value}')
                            ON CONFLICT (id) DO UPDATE
                            SET
                                event_ts    = excluded.event_ts,
                                event_type  = excluded.event_type,
                                event_value = excluded.event_value;
                        """
                        )
                    )
            self.etl_syncer.save_sync(
                etl_key=etl_key, type="latest_loaded_id", collection=collection
            )
            logger.info(
                f"stg.bonusystem_events table was succesfully updated. {len(collection)} rows were updated."
            )

        except Exception:
            logger.exception("Unable to insert data to stg.bonussystem_events table.")


if __name__ == "__main__":

    from dotenv import load_dotenv
    from utils import connect_to_database

    load_dotenv()

    pg_origin_creds = {
        "host": os.getenv("PG_ORIGIN_HOST"),
        "port": os.getenv("PG_ORIGIN_PORT"),
        "user": os.getenv("PG_ORIGIN_USER"),
        "password": os.getenv("PG_ORIGIN_PASSWORD"),
        "database": "de-public",
    }

    pg_dwh_creds = {
        "host": os.getenv("PG_DWH_HOST"),
        "port": os.getenv("PG_DWH_PORT"),
        "user": os.getenv("PG_DWH_USER"),
        "password": os.getenv("PG_DWH_PASSWORD"),
        "database": "dwh",
    }

    pg_dwh_engine = connect_to_database(creds=pg_dwh_creds)
    pg_origin_engine = connect_to_database(creds=pg_origin_creds)

    data_mover = BonussystemDataMover(
        origin_engine=pg_origin_engine, dwh_engine=pg_dwh_engine
    )

    data_mover.load_ranks_data()
    data_mover.load_users_data()
    data_mover.load_outbox_data()
