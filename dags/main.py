from datetime import datetime
from typing import List, Any, Literal
from datetime import datetime
import json
from pathlib import Path

from pymongo.cursor import Cursor
from sqlalchemy import text

from utils import get_logger, DatabaseConnector, StgEtlSyncer, DDSEtlSyncer
from objs import (
    BonussystemRankObj,
    BonussystemUserObj,
    BonussystemOutboxObj,
    OrdersystemObj,
    DDSUser,
)
from errors import SQLError, MongoServiceError


logger = get_logger(logger_name=str(Path(Path(__file__).name)))


class BonussystemStgDataMover:
    """Gets data from Postgres bonus system and loads to STG layer."""

    def __init__(self) -> None:
        self.source_conn = DatabaseConnector(db="pg_source").connect_to_database()
        self.dwh_conn = DatabaseConnector(db="pg_dwh").connect_to_database()
        self.etl_syncer = StgEtlSyncer(engine=self.dwh_conn)

    def _get_data_from_source(
        self, query: str, etl_key: str, type: Literal["snapshot", "increment"]
    ) -> List[Any]:
        logger.info(f"Getting `{etl_key}` data from source.")

        if type == "increment":
            etl_obj = self.etl_syncer.get_latest_sync(
                etl_key=etl_key, type="latest_loaded_id"
            )

            try:
                with self.source_conn.begin() as conn:
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
                with self.source_conn.begin() as conn:
                    result = conn.execute(statement=text(query)).fetchall()
                logger.info(f"{len(result)} rows recieved from source.")

            except Exception:
                logger.exception(
                    f"Unable to get `{etl_key}` data from source! Something went wrong."
                )
        return result

    def load_ranks_data(self) -> None:
        """Snapshot update"""

        ETL_KEY = "bonus_system_ranks"

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
                query=get_query, etl_key=ETL_KEY, type="snapshot"
            )
        ]

        logger.info(
            "Trying to insert public.ranks source data to stg.bonussystem_ranks table."
        )
        try:
            with self.dwh_conn.begin() as conn:
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

            self.etl_syncer.save_pg_sync(
                etl_key=ETL_KEY, collection=collection, type="latest_loaded_ts"
            )
            logger.info(
                f"stg.bonusystem_ranks table was succesfully updated. {len(collection)} rows were updated."
            )

        except Exception:
            logger.exception("Unable to insert data to stg.bonussystem_ranks table.")

    def load_users_data(self) -> None:
        """Snapshot update"""

        ETL_KEY = "bonus_system_users"

        get_query = """ 
            SELECT
                id,
                order_user_id
            FROM public.users;
        """

        collection = [
            BonussystemUserObj(id=row[0], order_user_id=row[1])
            for row in self._get_data_from_source(
                query=get_query, etl_key=ETL_KEY, type="snapshot"
            )
        ]

        logger.info(
            "Trying to insert public.users source data to stg.bonussystem_users table."
        )
        try:
            with self.dwh_conn.begin() as conn:
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
            self.etl_syncer.save_pg_sync(
                etl_key=ETL_KEY, type="latest_loaded_ts", collection=collection
            )
            logger.info(
                f"stg.bonusystem_users table was succesfully updated. {len(collection)} rows were updated."
            )

        except Exception:
            logger.exception("Unable to insert data to stg.bonussystem_users table.")

    def load_outbox_data(self) -> None:
        """Increment update"""

        ETL_KEY = "bonus_system_outbox"

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
                query=get_query, etl_key=ETL_KEY, type="increment"
            )
        ]

        logger.info(
            "Trying to insert public.outbox source data to stg.bonussystem_events table."
        )
        try:
            with self.dwh_conn.begin() as conn:
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
            self.etl_syncer.save_pg_sync(
                etl_key=ETL_KEY, type="latest_loaded_id", collection=collection
            )
            logger.info(
                f"stg.bonusystem_events table was succesfully updated. {len(collection)} rows were updated."
            )

        except Exception:
            logger.exception("Unable to insert data to stg.bonussystem_events table.")


class OrdersystemStgDataMover:
    """Gets data from MongoDB order system and loads to STG layer."""

    def __init__(self) -> None:
        self.source_conn = DatabaseConnector(db="mongo_source").connect_to_database()
        self.dwh_conn = DatabaseConnector(db="pg_dwh").connect_to_database()
        self.etl_syncer = StgEtlSyncer(engine=self.dwh_conn)

    def _get_data_from_source(self, etl_key: str, collection_name: str) -> Cursor:
        logger.info(f"Getting `{etl_key}` data from source.")

        etl_obj = self.etl_syncer.get_latest_sync(
            etl_key=etl_key, type="latest_loaded_ts"
        )
        latest_loaded_ts = datetime.fromisoformat(
            etl_obj.workflow_settings["latest_loaded_ts"]
        )
        mongo_filter = {"update_ts": {"$gt": latest_loaded_ts}}

        try:
            cur = self.source_conn.get_collection(collection_name).find(
                filter=mongo_filter, sort=[("update_ts", 1)]
            )

            logger.info("Data was recieved from mongo source.")
        except Exception:
            logger.exception(
                f"Unable to get `{etl_key}` data from source! Something went wrong."
            )
            raise MongoServiceError

        return cur

    def load_restaurants(self) -> None:
        ETL_KEY = "order_system_restaurants"
        COLLECTION_NAME = "restaurants"

        collection = [
            OrdersystemObj(
                object_id=str(row["_id"]),
                object_value=json.dumps(
                    obj=dict(
                        name=row["name"],
                        menu=[
                            dict(
                                _id=str(r["_id"]),
                                name=r["name"],
                                price=str(r["price"]),
                                category=r["category"],
                            )
                            for r in row["menu"]
                        ],
                    ),
                    ensure_ascii=False,
                ),
                update_ts=row["update_ts"],
            )
            for row in self._get_data_from_source(
                etl_key=ETL_KEY, collection_name=COLLECTION_NAME
            )
        ]
        logger.info(
            "Trying to insert users collection source data to stg.ordersystem_users table."
        )

        try:
            with self.dwh_conn.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.ordersystem_restaurants(object_id, object_value, update_ts)
                            VALUES
                                ('{row.object_id}', '{row.object_value}', '{row.update_ts}')
                            ON CONFLICT (object_id) DO UPDATE
                            SET
                                object_value  = excluded.object_value,
                                update_ts = excluded.update_ts;
                        """
                        )
                    )
                self.etl_syncer.save_mongo_sync(etl_key=ETL_KEY, collection=collection)
                logger.info(
                    f"stg.ordersystem_restaurants table was succesfully updated. {len(collection)} rows were updated."
                )
        except Exception:
            logger.exception(
                "Unable to insert data to stg.ordersystem_restaurants table."
            )
            raise SQLError

    def load_users(self) -> None:

        ETL_KEY = "order_system_users"
        COLLECTION_NAME = "users"

        collection = [
            OrdersystemObj(
                object_id=str(row["_id"]),
                object_value=json.dumps(
                    obj=dict(name=row["name"], login=row["login"]),
                    ensure_ascii=False,
                ),
                update_ts=row["update_ts"],
            )
            for row in self._get_data_from_source(
                etl_key=ETL_KEY, collection_name=COLLECTION_NAME
            )
        ]
        logger.info(
            "Trying to insert users collection source data to stg.ordersystem_users table."
        )
        try:
            with self.dwh_conn.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.ordersystem_users(object_id, object_value, update_ts)
                            VALUES
                                ('{row.object_id}', '{row.object_value}', '{row.update_ts}')
                            ON CONFLICT (object_id) DO UPDATE
                            SET
                                object_value  = excluded.object_value,
                                update_ts = excluded.update_ts;
                        """
                        )
                    )
                self.etl_syncer.save_mongo_sync(etl_key=ETL_KEY, collection=collection)
                logger.info(
                    f"stg.ordersystem_users table was succesfully updated. {len(collection)} rows were updated."
                )
        except Exception:
            logger.exception("Unable to insert data to stg.ordersystem_users table.")
            raise SQLError

    def load_orders(self) -> None:

        ETL_KEY = "order_system_orders"
        COLLECTION_NAME = "orders"

        collection = [
            OrdersystemObj(
                object_id=str(row["_id"]),
                object_value=json.dumps(
                    obj=dict(
                        bonus_grant=row["bonus_grant"],
                        bonus_payment=row["bonus_payment"],
                        cost=row["cost"],
                        date=str(row["date"]),
                        final_status=row["final_status"],
                        order_items=[
                            dict(
                                id=str(r["id"]),
                                name=r["name"],
                                price=str(r["price"]),
                                quantity=str(r["quantity"]),
                            )
                            for r in row["order_items"]
                        ],
                        payment=row["payment"],
                        restaurant=dict(id=str(row["restaurant"]["id"])),
                        statuses=[
                            dict(status=r["status"], dttm=str(r["dttm"]))
                            for r in row["statuses"]
                        ],
                        user=dict(id=str(row["user"]["id"])),
                    ),
                    ensure_ascii=False,
                ),
                update_ts=row["update_ts"],
            )
            for row in self._get_data_from_source(
                etl_key=ETL_KEY, collection_name=COLLECTION_NAME
            )
        ]

        logger.info(
            "Trying to insert users collection source data to stg.ordersystem_orders table."
        )
        try:
            with self.dwh_conn.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.ordersystem_orders(object_id, object_value, update_ts)
                            VALUES
                                ('{row.object_id}', '{row.object_value}', '{row.update_ts}')
                            ON CONFLICT (object_id) DO UPDATE
                            SET
                                object_value  = excluded.object_value,
                                update_ts = excluded.update_ts;
                        """
                        )
                    )
                self.etl_syncer.save_mongo_sync(etl_key=ETL_KEY, collection=collection)
                logger.info(
                    f"stg.ordersystem_orders table was succesfully updated. {len(collection)} rows were updated."
                )
        except Exception:
            logger.exception("Unable to insert data to stg.ordersystem_orders table.")
            raise SQLError


class DDSDataMover:
    def __init__(self) -> None:
        self.dwh_conn = DatabaseConnector(db="pg_dwh").connect_to_database()
        self.etl_syncer = DDSEtlSyncer(dwh_conn=self.dwh_conn)

    def load_users(self) -> None:
        logger.info(
            "Getting data from stg.bonussystem_users and stg.ordersystem_users."
        )

        try:
            with self.dwh_conn.begin() as conn:
                bonus_users = conn.execute(
                    statement=text(
                        f"""
                        select id, order_user_id
                        from stg.bonussystem_users
                        where id > -1;
                        """
                    )
                ).fetchall()
                order_users = conn.execute(
                    statement=text(
                        f"""
                        select id, object_id, object_value, update_ts
                        from stg.ordersystem_users
                        where update_ts > '1900-01-01 00:00:00';
                        """
                    )
                ).fetchall()
        except Exception:
            raise SQLError

        users = [
            DDSUser(
                user_id=bl[1],
                user_name=json.loads(ol[2])["name"],
                user_login=json.loads(ol[2])["login"],
            )
            for ol in order_users
            for bl in bonus_users
            if ol[1] == bl[1]
        ]

        try:
            with self.dwh_conn.begin() as conn:
                for row in users:
                    conn.execute(
                        statement=text(
                            f""" 
                            INSERT INTO
                                dds.dm_users(user_id, user_name, user_login)
                            VALUES
                                ('{row.user_id}', '{row.user_name}', '{row.user_login}')
                            ON CONFLICT (user_id) DO UPDATE
                            SET
                                user_name = excluded.user_name,
                                user_login = excluded.user_login
                            """
                        )
                    )
        except Exception:
            raise SQLError


class CDMDataMover:
    pass


if __name__ == "__main__":

    data_mover = DDSDataMover()
    data_mover.load_users()
