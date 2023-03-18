from datetime import datetime
import json
from pathlib import Path
import sys
import logging

from sqlalchemy import text

# typing
from pymongo.cursor import Cursor
from typing import List, Any, Literal

# package
sys.path.append(str(Path(__file__).parent.parent))

from pkg.utils import DatabaseConnector, StgEtlSyncer, DDSEtlSyncer
from pkg.objs import (
    BonussystemRankObj,
    BonussystemUserObj,
    BonussystemOutboxObj,
    OrdersystemObj,
    DDSUser,
    DDSRestaurant,
    DDSTimestamp,
    DDSProduct,
    DDSOrder,
    DDSFactProductSale,
)
from pkg.errors import SQLError, MongoServiceError

# gets airflow default logger and use it 
logger = logging.getLogger("airflow.task")


class STGBonussystemDataLoader:
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


class STGOrdersystemDataLoader:
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


class DDSDataLoader:
    def __init__(self) -> None:
        self.dwh_conn = DatabaseConnector(db="pg_dwh").connect_to_database()
        self.etl_syncer = DDSEtlSyncer(dwh_conn=self.dwh_conn)

    def load_users(self) -> None:
        logger.info("Loading dds.dm_users table.")
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
            logger.info("Data recieved from stg.")
        except Exception:
            logger.exception("Unable to get data from stg! Updating failed.")
            raise SQLError
        logger.info("Collecting `DDSUser` object.")
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
        logger.info("Inserting data into dds.dm_users.")
        try:
            with self.dwh_conn.begin() as conn:
                logger.info("Processing...")
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
            logger.info(
                f"dds.dm_users table was succesfully updated with {len(users)} rows."
            )
        except Exception:
            logger.exception(
                "Unable to insert data to dds.dm_users table! Updating failed."
            )
            raise SQLError

    def load_restaurants(self) -> None:
        logger.info("Loading dds.dm_restaurants table.")
        logger.info("Getting data from stg.ordersystem_rastaurants.")

        try:
            with self.dwh_conn.begin() as conn:
                restaurants = conn.execute(
                    statement=text(
                        f"""
                        select id, object_id, object_value, update_ts
                        from stg.ordersystem_restaurants
                        where update_ts > '1900-01-01 00:00:00';
                        """
                    )
                ).fetchall()
                logger.info("Data recieved from stg.")
        except Exception:
            raise SQLError

        logger.info("Collecting `DDSRestaurant` object.")
        restaurants = [
            DDSRestaurant(
                restaurant_id=row[1],
                restaurant_name=json.loads(row[2])["name"],
                active_from=row[3],
                active_to=datetime(2099, 12, 31, 00, 00, 00, 000),
            )
            for row in restaurants
        ]
        logger.info("Inserting data into dds.dm_restaurants.")
        try:
            with self.dwh_conn.begin() as conn:
                logger.info("Processing...")
                for row in restaurants:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                            VALUES
                                ('{row.restaurant_id}', '{row.restaurant_name}', '{row.active_from}', '{row.active_to}')
                            ON CONFLICT (restaurant_id) DO UPDATE
                            SET
                                restaurant_name = excluded.restaurant_name,
                                active_from = excluded.active_from,
                                active_to = excluded.active_to;
                            """
                        )
                    )
            logger.info(
                f"dds.dm_products table was succesfully updated with {len(restaurants)} rows."
            )
        except Exception:
            logger.exception(
                "Unable to insert data to dds.dm_restaurants table! Updating failed."
            )
            raise SQLError

    def load_timestamps(self) -> None:
        logger.info("Loading dds.dm_timestamps table.")
        logger.info("Getting data from stg.ordersystem_orders.")

        try:
            with self.dwh_conn.begin() as conn:
                timestamps = conn.execute(
                    statement=text(
                        f"""
                        select object_value
                        from stg.ordersystem_orders
                        where update_ts > '1900-01-01 00:00:00';
                        """
                    )
                ).fetchall()
            logger.info("Data recieved from stg.")
        except Exception:
            logger.exception("Unable to get data from stg! Updating failed.")
            raise SQLError

        logger.info("Collecting `DDSTimestamp` object.")
        timestamps = [
            dict(
                final_status=json.loads(row[0])["final_status"],
                date=datetime.fromisoformat(json.loads(row[0])["date"]),
            )
            for row in timestamps
            if json.loads(row[0])["final_status"] in ("CANCELLED", "CLOSED")
        ]

        timestamps = [
            DDSTimestamp(
                ts=row["date"].strftime("%Y-%m-%d %H:%M:%S"),
                year=row["date"].year,
                month=row["date"].month,
                day=row["date"].day,
                time=row["date"].strftime("%H:%M:%S"),
                date=str(row["date"].date()),
            )
            for row in timestamps
        ]
        logger.info("Inserting data into dds.dm_timestamps.")
        try:
            with self.dwh_conn.begin() as conn:
                logger.info("Processing...")
                for row in timestamps:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                dds.dm_timestamps(ts, year, month, day, time, date)
                            VALUES
                                ('{row.ts}', {row.year}, {row.month}, {row.day}, '{row.time}', '{row.date}')
                            ON CONFLICT (ts) DO UPDATE
                            SET
                               year = excluded.year,
                               month = excluded.month,
                               day = excluded.day,
                               time = excluded.time,
                               date = excluded.date;
                            """
                        )
                    )
            logger.info(
                f"dds.dm_timestamps table was succesfully updated with {len(timestamps)} rows."
            )

        except Exception:
            logger.exception(
                "Unable to insert data to dds.dm_timestamps table! Updating failed."
            )
            raise SQLError

    def load_products(self) -> None:
        logger.info("Loading dds.dm_products table.")
        logger.info("Getting data from stg.ordersystem_rastaurants.")

        try:
            with self.dwh_conn.begin() as conn:
                products = conn.execute(
                    statement=text(
                        f"""
                        select object_id, object_value, update_ts
                        from stg.ordersystem_restaurants
                        where update_ts > '1900-01-01 00:00:00';
                        """
                    )  # TODO remove hardcode
                ).fetchall()
            logger.info("Data recieved from stg.")
        except Exception:
            logger.exception("Unable to get data from stg! Updating failed.")
            raise SQLError

        logger.info("Collecting `DDSProduct` object.")
        products = [
            DDSProduct(
                restaurant_id=row[0],
                product_id=r["_id"],
                product_name=r["name"],
                product_price=r["price"],
                active_from=row[2],
                active_to=datetime(2099, 12, 31, 00, 00, 00, 000),
            )
            for row in products
            for r in json.loads(row[1])["menu"]
        ]
        logger.info("Starting updating process.")
        with self.dwh_conn.begin() as conn:
            logger.info("Processing...")
            try:
                logger.info("Creating temp table.")
                conn.execute(
                    text(
                        """
                        DROP TABLE IF EXISTS dm_products_tmp;
                        CREATE LOCAL TEMP TABLE dm_products_tmp
                        (
                            restaurant_source_id varchar,
                            product_id           varchar,
                            product_name         varchar,
                            product_price        float,
                            active_from          timestamp,
                            active_to            timestamp
                        )
                            ON COMMIT PRESERVE ROWS;
                        """
                    )
                )
                logger.info("Temp table created.")
            except Exception:
                logger.exception("Unable to create temp table!")
                raise SQLError
            try:
                logger.info("Inserting data into temp table.")
                for row in products:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                dm_products_tmp (restaurant_source_id, product_id, product_name, product_price, active_from, active_to)
                            VALUES
                                ('{row.restaurant_id}', '{row.product_id}', '{row.product_name}', {row.product_price}, '{row.active_from}', '{row.active_to}');
                            """
                        )
                    )
                logger.info("Data was inserted.")
            except Exception:
                logger.exception("Unable to insert data into temp table!")

            try:
                logger.info("Inserting data into dds.dm_products.")
                conn.execute(
                    statement=text(
                        """
                        INSERT INTO
                            dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                        SELECT
                            r.id AS restaurant_id,
                            tmp.product_id,
                            tmp.product_name,
                            tmp.product_price,
                            tmp.active_from,
                            tmp.active_to
                        FROM
                            dm_products_tmp tmp
                            JOIN (
                                SELECT
                                    id,
                                    restaurant_id
                                FROM dds.dm_restaurants
                                ) r ON tmp.restaurant_source_id = r.restaurant_id
                        ON CONFLICT (restaurant_id, product_id) DO UPDATE
                            SET
                                product_name  = excluded.product_name,
                                product_price = excluded.product_price,
                                active_from   = excluded.active_from,
                                active_to     = excluded.active_to;

                        DROP TABLE IF EXISTS dm_products_tmp;
                        """
                    )
                )
                logger.info("Data was inserted successfully.")
            except Exception:
                logger.exception(
                    "Unable to insert data to dds.dm_products table! Updating failed."
                )
                raise SQLError
        logger.info(
            f"dds.dm_products table was succesfully updated with {len(products)} rows."
        )

    def load_orders(self) -> None:
        logger.info("Loading dds.dm_orders table.")
        logger.info("Getting data from stg.ordersystem_orders.")

        try:
            with self.dwh_conn.begin() as conn:
                orders = conn.execute(
                    statement=text(
                        f"""
                        select object_id, object_value, update_ts
                        from stg.ordersystem_orders
                        where update_ts > '1900-01-01 00:00:00';
                        """
                    )  # TODO remove hardcode
                ).fetchall()
            logger.info("Data recieved from stg.")
        except Exception:
            logger.exception("Unable to get data from stg! Updating failed.")
            raise SQLError

        logger.info("Collecting `DDSOrder` object.")
        orders = [
            DDSOrder(
                order_key=row[0],
                order_status=json.loads(row[1])["final_status"],
                restaurant_id=json.loads(row[1])["restaurant"]["id"],
                date=datetime.fromisoformat(json.loads(row[1])["date"]).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                user_id=json.loads(row[1])["user"]["id"],
            )
            for row in orders
        ]

        logger.info("Starting updating process.")
        with self.dwh_conn.begin() as conn:
            logger.info("Processing...")
            try:
                logger.info("Creating temp table.")
                conn.execute(
                    statement=text(
                        """
                        DROP TABLE IF EXISTS dm_orders_tmp;
                        CREATE TEMP TABLE dm_orders_tmp
                        (
                            order_key            varchar,
                            order_status         varchar,
                            restaurant_source_id varchar,
                            date_source          timestamp,
                            user_source_id       varchar
                        )
                            ON COMMIT PRESERVE ROWS;
                        """
                    )
                )
                logger.info("Temp table created.")
            except Exception:
                logger.exception("Unable to create temp table!")
                raise SQLError
            try:
                logger.info("Inserting data into temp table.")
                for row in orders:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                dm_orders_tmp (order_key, order_status, restaurant_source_id, date_source, user_source_id)
                            VALUES
                                ('{row.order_key}', '{row.order_status}', '{row.restaurant_id}', '{row.date}', '{row.user_id}');
                            """
                        )
                    )
                logger.info("Data was inserted.")
            except Exception:
                logger.exception("Unable to insert data into temp table!")

            try:
                logger.info("Inserting data into dds.dm_orders.")
                conn.execute(
                    statement=text(
                        """
                        INSERT INTO
                            dds.dm_orders(order_key, order_status, user_id, restaurant_id, timestamp_id)
                        SELECT
                            DISTINCT
                            tmp.order_key as order_key,
                            tmp.order_status as order_status,
                            u.id AS user_id,
                            r.id AS restaurant_id,
                            t.id AS timestamp_id
                        FROM
                            dm_orders_tmp AS tmp
                            JOIN (
                                SELECT
                                    id,
                                    user_id
                                FROM dds.dm_users
                                ) u ON tmp.user_source_id = u.user_id
                            JOIN (
                                SELECT
                                    id,
                                    restaurant_id
                                FROM dds.dm_restaurants
                                ) r ON tmp.restaurant_source_id = r.restaurant_id
                            JOIN (
                                SELECT
                                    id,
                                    ts
                                FROM dds.dm_timestamps
                                ) t ON tmp.date_source = t.ts
                        ON CONFLICT (order_key) DO UPDATE
                            SET
                                order_status  = excluded.order_status,
                                user_id       = excluded.user_id,
                                restaurant_id = excluded.restaurant_id,
                                timestamp_id  = excluded.timestamp_id;

                        DROP TABLE IF EXISTS dm_orders_tmp;
                        """
                    )
                )
                logger.info("Data was inserted successfully.")
            except Exception:
                logger.exception(
                    "Unable to insert data to dds.dm_orders table! Updating failed."
                )
                raise SQLError

        logger.info(
            f"dds.dm_orders table was succesfully updated with {len(orders)} rows."
        )

    def load_fct_product_sales(self) -> None:
        logger.info("Loading dds.fct_product_sales table.")

        with self.dwh_conn.begin() as conn:
            try:
                logger.info(
                    "Getting bonus transactions data from stg.bonussystem_events."
                )
                bonus_raw = conn.execute(
                    statement=text(
                        f"""
                        SELECT
                            event_value,
                            event_ts
                        FROM stg.bonussystem_events
                        WHERE 1 = 1
                        AND event_type = 'bonus_transaction'
                        AND event_ts > '1900-01-01 00:00:00';
                        """
                    )
                ).fetchall()
                logger.info("Bonus transactions data recieved from stg.")
            except Exception:
                logger.exception("Unable to get data from stg! Updating failed.")
                raise SQLError

            logger.info("Collecting `DDSFactProductSale` object.")
            bonuses = [
                DDSFactProductSale(
                    order_id=json.loads(row[0])["order_id"],
                    product_id=r["product_id"],
                    price=r["price"],
                    quantity=r["quantity"],
                    bonus_payment=float(r["bonus_payment"]),
                    bonus_grant=int(r["bonus_grant"]),
                )
                for row in bonus_raw
                for r in json.loads(row[0])["product_payments"]
            ]

        logger.info("Starting updating process.")
        with self.dwh_conn.begin() as conn:
            logger.info("Processing...")
            try:
                logger.info("Creating temp table.")
                conn.execute(
                    statement=text(
                        """
                        DROP TABLE IF EXISTS fct_product_sales_tmp;
                        CREATE TEMP TABLE fct_product_sales_tmp
                        (
                            order_source_id   varchar,
                            product_source_id varchar,
                            price             numeric(19, 5),
                            quantity          int,
                            bonus_payment     numeric(19, 5),
                            bonus_grant       numeric(19, 5)
                        )
                            ON COMMIT PRESERVE ROWS;
                        """
                    )
                )
            except Exception:
                logger.exception("Unable to create temp table!")
                raise SQLError
            try:
                logger.info("Inserting data into temp table.")
                for row in bonuses:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                fct_product_sales_tmp(order_source_id, product_source_id, price, quantity, bonus_payment, bonus_grant)
                            VALUES
                                ('{row.order_id}', '{row.product_id}', {row.price}, {row.quantity}, {row.bonus_payment}, {row.bonus_grant});
                            """
                        )
                    )
                logger.info("Data was inserted.")
            except Exception:
                logger.exception("Unable to insert data into temp table!")

            try:
                logger.info("Inserting data into dds.fct_product_sales.")
                conn.execute(
                    statement=text(
                        """
                            INSERT INTO
                                dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                            SELECT
                                p.id                     AS product_id,
                                o.id                     AS order_id,
                                tmp.quantity             AS count,
                                tmp.price                AS price,
                                tmp.quantity * tmp.price AS total_sum,
                                tmp.bonus_payment,
                                tmp.bonus_grant
                            FROM fct_product_sales_tmp tmp
                                    JOIN (
                                        SELECT
                                            id,
                                            product_id
                                        FROM dds.dm_products
                                        ) p ON tmp.product_source_id = p.product_id
                                    JOIN (
                                        SELECT
                                            id,
                                            order_key,
                                            order_status
                                        FROM dds.dm_orders
                                        ) o ON tmp.order_source_id = o.order_key
                            WHERE 1=1
                            AND o.order_status = 'CLOSED'
                            ON CONFLICT (product_id, order_id) DO UPDATE
                                SET
                                    count         = excluded.count,
                                    price         = excluded.price,
                                    total_sum     = excluded.total_sum,
                                    bonus_payment = excluded.bonus_payment,
                                    bonus_grant   = excluded.bonus_grant;

                            DROP TABLE IF EXISTS fct_product_sales_tmp;
                            """
                    )
                )
                logger.info("Data was inserted successfully.")
            except Exception:
                logger.exception(
                    "Unable to insert data to dds.fct_product_sales table! Updating failed."
                )
                raise SQLError
        logger.info(
            f"dds.fct_product_sales table was succesfully updated with {len(bonuses)} rows."
        )


class CDMDataLoader:
    def __init__(self) -> None:
        self.dwh_conn = DatabaseConnector(db="pg_dwh").connect_to_database()

    def load_settlement_report(self) -> None:
        logger.info("Starting data mart load process.")
        try:
            logger.info("Executing postgres query...")
            with self.dwh_conn.begin() as conn:
                conn.execute(
                    statement=text(
                        """
                        INSERT INTO
                            cdm.dm_settlement_report(restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum,
                                                    orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                        SELECT
                            dmr.id                                                                      AS restaurant_id,
                            dmr.restaurant_name,
                            dmt.date                                                                    AS settlement_date,
                            COUNT(DISTINCT fct.order_id)                                                AS orders_count,
                            SUM(fct.total_sum)                                                          AS orders_total_sum,
                            SUM(fct.bonus_payment)                                                      AS orders_bonus_payment_sum,
                            SUM(fct.bonus_grant)                                                        AS orders_bonus_granted_sum,
                            SUM(fct.total_sum) * 0.25                                                   AS order_processing_fee,
                            (SUM(fct.total_sum) - (SUM(fct.total_sum) * 0.25) - SUM(fct.bonus_payment)) AS restaurant_reward_sum
                        FROM
                            dds.dm_orders dmo
                            LEFT JOIN dds.fct_product_sales fct ON dmo.id = fct.order_id
                            LEFT JOIN (
                                SELECT
                                    id,
                                    date
                                FROM dds.dm_timestamps
                                ) dmt ON dmo.timestamp_id = dmt.id
                            LEFT JOIN (
                                SELECT
                                    id,
                                    restaurant_id,
                                    restaurant_name
                                FROM dds.dm_restaurants
                                WHERE 1 = 1
                                AND active_to > CURRENT_TIMESTAMP::timestamp WITHOUT TIME ZONE
                                ) dmr ON dmo.restaurant_id = dmr.id
                        WHERE 1 = 1
                        AND dmo.order_status = 'CLOSED'
                        GROUP BY 1, 2, 3
                        ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                            SET
                                restaurant_name          = excluded.restaurant_name,
                                orders_count             = excluded.orders_count,
                                orders_total_sum         = excluded.orders_total_sum,
                                orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
                                orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
                                order_processing_fee     = excluded.order_processing_fee,
                                restaurant_reward_sum    = excluded.restaurant_reward_sum;
                        """
                    )
                )
            logger.info("Data mart was updated successfully.")
        except Exception:
            logger.exception("Unable to execute query! Updating failed.")


if __name__ == "__main__":

    data_mover = CDMDataLoader()
    # data_mover.load_users()
    # data_mover.load_restaurants()
    # data_mover.load_timestamps()
    # data_mover.load_products()
    # data_mover.load_orders()
    data_mover.load_settlement_report()
