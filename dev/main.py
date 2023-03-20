import sys

# logging
import logging

# fs
from pathlib import Path
import coloredlogs


def get_dev_logger(logger_name: str) -> logging.Logger:
    """logger for development and testing. don't use it in airflow."""

    logger = logging.getLogger(name=logger_name)

    coloredlogs.install(logger=logger, level="INFO")
    logger.setLevel(level=logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    logger_handler = logging.StreamHandler(stream=sys.stdout)

    colored_formatter = coloredlogs.ColoredFormatter(
        fmt="[%(asctime)s UTC] {%(name)s:%(lineno)d} %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level_styles=dict(
            info=dict(color="green"),
            error=dict(color="red", bold=False, bright=True),
        ),
        field_styles=dict(
            asctime=dict(color="magenta"),
            name=dict(color="cyan"),
            levelname=dict(color="yellow", bold=True, bright=True),
            lineno=dict(color="white"),
        ),
    )

    logger_handler.setFormatter(fmt=colored_formatter)
    logger.addHandler(logger_handler)
    logger.propagate = False  # makes sure that coloredlogs doesn’t pass our log events to the root logger. This prevents that we log every event double.

    return logger


logger = get_dev_logger(logger_name=str(Path(Path(__file__).name)))


from os import getenv

from dataclasses import dataclass
from dotenv import load_dotenv, find_dotenv
import requests
from sqlalchemy import text
from itertools import chain
from typing import Any
import time
from datetime import datetime, timedelta
from pprint import pprint
import json

sys.path.append(str(Path(__file__).parent.parent))
from pkg.utils import DatabaseConnector
from pkg.errors import DotEnvError, SQLError
from pkg.objs import DDSTimestamp


class APIServiceError(Exception):
    pass


@dataclass
class HeadersHolder:
    apikey: str
    nickname: str
    cohort: int


@dataclass
class DeliverySystemObj:
    object_id: str
    object_value: dict
    update_ts: datetime


class HeadersGetter:
    def _get_headers() -> dict:

        logger.info("Loading .env variables.")
        try:
            find_dotenv(raise_error_if_not_found=True)
            load_dotenv(verbose=True, override=True)
            logger.info(".env was loaded successfully.")
        except Exception:
            logger.exception("Unable to load .env! Please check if its accessible.")
            raise DotEnvError
        try:
            HEADERS = {
                "X-API-KEY": getenv("APIKEY"),
                "X-Nickname": getenv("NICKNAME"),
                "X-Cohort": getenv("COHORT"),
            }
            logger.info("All variables recieved for API connection.")
        except Exception:
            logger.exception("Unable to get one or more variables!")
            raise DotEnvError

        return HEADERS


class STGDeliverySystemDataLoader:
    def __init__(self) -> None:
        self.dwh_conn = DatabaseConnector(db="pg_dwh").connect_to_database()
        self.headers = HeadersGetter._get_headers()

    def load_restaurants(self) -> None:
        logger.info("Starting loading process for restaurants data.")

        SORT_FIELD = "_id"  # field to sort by. values: 'id', 'name'
        SORT_DIRECTION = "asc"  # values: 'acs', 'desc'
        LIMIT = 50  # int from 0 to 50 exclusive
        OFFSET = 0
        METHOD = "restaurants"

        attempt = 1
        max_attempts = 8

        for _ in range(max_attempts + 1):
            logger.info(f"Sending request to API. Attempt {attempt}.")
            try:
                response = requests.get(
                    url=f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{METHOD}?sort_field={SORT_FIELD}&sort_direction={SORT_DIRECTION}&limit={LIMIT}&offset={OFFSET}",
                    headers=self.headers,
                ).json()
                logger.info("Response recieved. 😎")
                break
            except:
                logger.exception("Response wasn't recieved!")
                attempt += 1

                if attempt == max_attempts:
                    logger.info("API is not responding! No more attempts left.")
                    raise APIServiceError

                time.sleep(10)
                logger.info("Give me another try.")

        logger.info("Collecting `DeliverySystemObj` objects.")
        collection = [
            DeliverySystemObj(
                object_id=row["_id"],
                object_value=json.dumps(
                    obj=(dict(name=row["name"])), ensure_ascii=False
                ),
                update_ts=datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
            )
            for row in response
        ]
        logger.info("Inserting data into `stg.deliverysystem_restaurants` table.")
        try:
            with self.dwh_conn.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.deliverysystem_restaurants(object_id, object_value, update_ts)
                            VALUES
                                ('{row.object_id}', '{row.object_value}', '{row.update_ts}')
                            ON CONFLICT (object_id) DO UPDATE
                                SET
                                    object_value = excluded.object_value,
                                    update_ts    = excluded.update_ts;
                            """
                        )
                    )
            logger.info("Data was inserted successfully!")
        except Exception:
            logger.exception(
                "Unable to insert data into `stg.deliverysystem_restaurants` table. Loading process failed."
            )
            raise SQLError

    def load_couriers(self) -> None:
        logger.info("Starting loading process for couriers data.")

        SORT_FIELD = "_id"  # field to sort by. values: 'id', 'name'
        SORT_DIRECTION = "asc"  # values: 'acs', 'desc'
        LIMIT = 50  # int from 0 to 50 exclusive
        OFFSET = 0
        METHOD = "couriers"

        attempt = 1
        max_attempts = 8

        collection = []

        for _ in range(max_attempts + 1):
            logger.info(f"Sending request to API. Attempt {attempt}.")
            try:
                for i in range(5):
                    response = requests.get(
                        url=f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{METHOD}?sort_field={SORT_FIELD}&sort_direction={SORT_DIRECTION}&limit={LIMIT}&offset={OFFSET}",
                        headers=self.headers,
                    ).json()
                    x = [
                        DeliverySystemObj(
                            object_id=row["_id"],
                            object_value=json.dumps(
                                dict(name=row["name"]), ensure_ascii=False
                            ),
                            update_ts=datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        for row in response
                    ]
                    collection.append(x)
                    OFFSET += 50
                logger.info("Response recieved. 😎")
                break
            except:
                logger.exception("Response wasn't recieved!")
                attempt += 1

                if attempt == max_attempts:
                    logger.info("API is not responding! No more attempts left.")
                    raise APIServiceError

                time.sleep(10)
                logger.info("Give me another try.")

        logger.info("Collecting `DeliverySystemObj` objects.")

        collection = list(chain(*collection))
        logger.info(f"Objects collected with {len(collection)} elements.")

        logger.info("Insering data into `stg.deliverysystem_couriers` table.")
        try:
            with self.dwh_conn.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.deliverysystem_couriers(object_id, object_value, update_ts)
                            VALUES
                                ('{row.object_id}', '{row.object_value}', '{row.update_ts}')
                            ON CONFLICT (object_id) DO UPDATE
                                SET
                                    object_value = excluded.object_value,
                                    update_ts    = excluded.update_ts;
                            """
                        )
                    )
            logger.info("Data was inserted successfully!")
        except Exception:
            logger.exception(
                "Unable to insert data into `stg.deliverysystem_couriers` table. Loading process failed."
            )
            raise SQLError

    def load_deliveries(self) -> None:
        logger.info("Starting loading process for deliveries data.")

        SORT_FIELD = "date"  # field to sort by. values: 'id', 'name'
        SORT_DIRECTION = "asc"  # values: 'acs', 'desc'
        FROM = (datetime.today().date() - timedelta(days=7)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        TO = datetime.today().date().strftime("%Y-%m-%d %H:%M:%S")
        LIMIT = 50  # int from 0 to 50 exclusive
        OFFSET = 0
        METHOD = "deliveries"

        attempt = 1
        max_attempts = 8

        collection = []

        for _ in range(max_attempts + 1):
            logger.info(f"Sending request to API. Attempt {attempt}.")
            try:
                for i in range(50):
                    response = requests.get(
                        url=f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{METHOD}?from={FROM}&to={TO}&sort_field={SORT_FIELD}&sort_direction={SORT_DIRECTION}&limit={LIMIT}&offset={OFFSET}",
                        headers=self.headers,
                    ).json()
                    x = [
                        DeliverySystemObj(
                            object_id=row["order_id"],
                            object_value=json.dumps(
                                dict(
                                    order_ts=row["order_ts"],
                                    delivery_id=row["delivery_id"],
                                    courier_id=row["courier_id"],
                                    address=row["address"],
                                    delivery_ts=row["delivery_ts"],
                                    rate=row["rate"],
                                    sum=row["sum"],
                                    tip_sum=row["tip_sum"],
                                ),
                                ensure_ascii=False,
                            ),
                            update_ts=datetime.today().strftime("%Y-%m-%d %H:%M:%S"),
                        )
                        for row in response
                    ]
                    collection.append(x)
                    OFFSET += 50
                logger.info("Response recieved. 😎")
                break
            except:
                logger.exception("Response wasn't recieved!")
                attempt += 1

                if attempt == max_attempts:
                    logger.info("API is not responding! No more attempts left.")
                    raise APIServiceError

                time.sleep(10)
                logger.info("Give me another try.")

        logger.info("Collecting `DeliverySystemObj` objects.")

        collection = list(chain(*collection))
        logger.info(f"Objects collected with {len(collection)} elements.")

        logger.info("Insering data into `stg.deliverysystem_deliveries` table.")
        try:
            with self.dwh_conn.begin() as conn:
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                stg.deliverysystem_deliveries(object_id, object_value, update_ts)
                            VALUES
                                ('{row.object_id}', '{row.object_value}', '{row.update_ts}')
                            ON CONFLICT (object_id) DO UPDATE
                                SET
                                    object_value = excluded.object_value,
                                    update_ts    = excluded.update_ts;
                            """
                        )
                    )
            logger.info("Data was inserted successfully!")
        except Exception:
            logger.exception(
                "Unable to insert data into `stg.deliverysystem_deliveries` table. Loading process failed."
            )
            raise SQLError


@dataclass
class DDSCourier:
    courier_id: str
    courier_name: str
    active_from: datetime
    active_to: datetime


@dataclass
class DDSDimDeliveries:
    delivery_id: str
    delivery_ts: datetime
    courier_id: str
    order_id: str


@dataclass
class DDSFctDeliveries:
    delivery_id: str
    address: str
    rate: int
    order_sum: float
    tip_sum: float


class DDSDeliverySystemDataLoader:
    def __init__(self) -> None:
        self.dwh_conn = DatabaseConnector(db="pg_dwh").connect_to_database()

    def load_couriers(self) -> None:
        logger.info("Loading dds.dm_couriers table.")
        logger.info("Getting data from stg.deliverysystem_couriers.")

        try:
            with self.dwh_conn.begin() as conn:
                raw = conn.execute(
                    statement=text(
                        f"""
                        SELECT
                            object_id,
                            object_value,
                            update_ts
                        FROM stg.deliverysystem_couriers
                        WHERE 1 = 1
                        AND update_ts > '1900-01-01 00:00:00';
                        """
                    )
                ).fetchall()
            logger.info("Data recieved from stg.")
        except Exception:
            logger.exception("Unable to get data from stg! Updating failed.")
            raise SQLError

        logger.info("Collecting `DDSCourier` objects.")
        collection = [
            DDSCourier(
                courier_id=row[0],
                courier_name=row[1]["name"],
                active_from=row[2],
                active_to=datetime(
                    2099, 12, 31, 00, 00, 00, 000  # TODO can we get rid of hardcode?
                ),
            )
            for row in raw
        ]
        logger.info("Inserting data into dds.dm_couriers")
        try:
            with self.dwh_conn.begin() as conn:
                logger.info("Processing...")
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                dds.dm_couriers(courier_id, courier_name, active_from, active_to)
                            VALUES
                                ('{row.courier_id}', '{row.courier_name}', '{row.active_from}', '{row.active_to}')
                            ON CONFLICT (courier_id) DO UPDATE
                                SET
                                    courier_name = excluded.courier_name,
                                    active_from  = excluded.active_from,
                                    active_to    = excluded.active_to;
                            """
                        )
                    )
            logger.info(
                f"dds.dm_couriers table was succesfully updated with {len(collection)} rows."
            )

        except Exception:
            logger.exception(
                "Unable to insert data to dds.dm_couriers table! Updating failed."
            )
            raise SQLError

    def load_dim_deliveries(self) -> None:
        logger.info("Loading dds.dm_deliveries table.")

        try:
            logger.info("Getting data from stg.deliverysystem_deliveries.")
            with self.dwh_conn.begin() as conn:
                raw = conn.execute(
                    statement=text(
                        f"""
                        SELECT
                            object_id,
                            object_value,
                            update_ts
                        FROM stg.deliverysystem_deliveries
                        WHERE 1 = 1
                        AND update_ts > '1900-01-01 00:00:00';
                        """
                    )  # TODO remove hardcode
                ).fetchall()
            logger.info("Data recieved from stg.")
        except Exception:
            logger.exception("Unable to get data from stg! Updating failed.")
            raise SQLError

        logger.info("Collecting `DDSDimDeliveries` object.")

        collection = [
            DDSDimDeliveries(
                delivery_id=row[1]["delivery_id"],
                delivery_ts=datetime.fromisoformat(row[1]["delivery_ts"]).strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                courier_id=row[1]["courier_id"],
                order_id=row[0],
            )
            for row in raw
        ]
        logger.info("Starting updating process.")
        with self.dwh_conn.begin() as conn:
            logger.info("Processing...")
            try:
                logger.info("Creating temp table.")
                conn.execute(
                    text(
                        """
                        DROP TABLE IF EXISTS dm_deliveries_tmp;
                        CREATE TEMP TABLE dm_deliveries_tmp
                        (
                            delivery_source_id varchar,
                            delivery_ts        timestamp,
                            courier_source_id  varchar,
                            order_source_id    varchar
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
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                dm_deliveries_tmp(delivery_source_id, delivery_ts, courier_source_id, order_source_id)
                            VALUES
                                ('{row.delivery_id}', '{row.delivery_ts}', '{row.courier_id}', '{row.order_id}');
                            """
                        )
                    )
                logger.info("Data was inserted.")
            except Exception:
                logger.exception("Unable to insert data into temp table!")
                raise SQLError

            try:
                logger.info("Inserting data into dds.dm_deliveries.")
                conn.execute(
                    statement=text(
                        """
                        INSERT INTO
                            dds.dm_deliveries(delivery_id, timestamp_id, courier_id, order_id)
                        SELECT
                            tmp.delivery_source_id AS delivery_id,
                            ts.id                  AS timestamp_id,
                            c.id                   AS courier_id,
                            o.id                   AS order_id
                        FROM
                            dm_deliveries_tmp tmp
                            LEFT JOIN (
                                SELECT
                                    id,
                                    ts
                                FROM dds.dm_timestamps
                                ) ts ON tmp.delivery_ts = ts.ts
                            LEFT JOIN (
                                SELECT
                                    id,
                                    courier_id
                                FROM dds.dm_couriers
                                ) c ON tmp.courier_source_id = c.courier_id
                            LEFT JOIN (
                                SELECT
                                    id,
                                    order_key
                                FROM dds.dm_orders
                                ) o ON tmp.order_source_id = o.order_key
                        ON CONFLICT (delivery_id) DO UPDATE
                            SET
                                timestamp_id = excluded.timestamp_id,
                                courier_id   = excluded.courier_id,
                                order_id     = excluded.order_id;
                        """
                    )
                )
                logger.info("Data was inserted successfully.")
            except Exception:
                logger.exception(
                    "Unable to insert data to dds.dm_deliveries table! Updating failed."
                )
                raise SQLError

        logger.info(
            f"dds.dm_deliveries table was succesfully updated with {len(collection)} rows."
        )

    def load_fct_deliveries(self) -> None:
        logger.info("Loading dds.fct_deliveries table.")

        try:
            logger.info("Getting data from stg.deliverysystem_deliveries.")
            with self.dwh_conn.begin() as conn:
                raw = conn.execute(
                    statement=text(
                        f"""
                        SELECT
                            object_value,
                            update_ts
                        FROM stg.deliverysystem_deliveries
                        WHERE 1 = 1
                        AND update_ts > '1900-01-01 00:00:00';
                        """
                    )  # TODO remove hardcode
                ).fetchall()
            logger.info("Data recieved from stg.")
        except Exception:
            logger.exception("Unable to get data from stg! Updating failed.")
            raise SQLError

        logger.info("Collecting `DDSFctDeliveries` object.")

        collection = [
            DDSFctDeliveries(
                delivery_id=row[0]["delivery_id"],
                address=row[0]["address"],
                rate=row[0]["rate"],
                order_sum=row[0]["sum"],
                tip_sum=row[0]["tip_sum"],
            )
            for row in raw
        ]
        logger.info("Starting updating process.")
        with self.dwh_conn.begin() as conn:
            logger.info("Processing...")
            try:
                logger.info("Creating temp table.")
                conn.execute(
                    statement=text(
                        """
                        DROP TABLE IF EXISTS fct_deliveries_tmp;
                        CREATE TEMP TABLE fct_deliveries_tmp
                        (
                            delivery_sourse_id varchar,
                            address            varchar,
                            rate               int,
                            order_sum          numeric(14, 5),
                            tip_sum            numeric(14, 5)
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
                for row in collection:
                    conn.execute(
                        statement=text(
                            f"""
                            INSERT INTO
                                fct_deliveries_tmp(delivery_sourse_id, address, rate, order_sum, tip_sum)
                            VALUES
                                ('{row.delivery_id}', '{row.address}', {row.rate}, {row.order_sum}, {row.tip_sum});
                            """
                        )
                    )
                logger.info("Data was inserted.")
            except Exception:
                logger.exception("Unable to insert data into temp table!")
                raise SQLError

            try:
                logger.info("Inserting data into dds.fct_deliveries.")
                conn.execute(
                    statement=text(
                        """
                        INSERT INTO
                            dds.fct_deliveries(delivery_id, order_id, courier_id, address, rate, order_sum, tip_sum)
                        SELECT
                            d.id AS delivery_id,
                            d.order_id,
                            d.courier_id,
                            tmp.address,
                            tmp.rate,
                            tmp.order_sum,
                            tmp.tip_sum
                        FROM
                            fct_deliveries_tmp tmp
                            LEFT JOIN (
                                SELECT
                                    id,
                                    delivery_id,
                                    timestamp_id,
                                    courier_id,
                                    order_id
                                FROM dds.dm_deliveries
                                ) d ON tmp.delivery_sourse_id = d.delivery_id
                        ON CONFLICT (delivery_id) DO UPDATE
                            SET
                                order_id   = excluded.order_id,
                                courier_id = excluded.courier_id,
                                address    = excluded.address,
                                rate       = excluded.rate,
                                order_sum  = excluded.order_sum,
                                tip_sum    = excluded.tip_sum;
                        """
                    )
                )
                logger.info("Data was inserted successfully.")
            except Exception:
                logger.exception(
                    "Unable to insert data to dds.fct_deliveries table! Updating failed."
                )
                raise SQLError

        logger.info(
            f"dds.fct_deliveries table was succesfully updated with {len(collection)} rows."
        )


if __name__ == "__main__":

    data_loader = DDSDeliverySystemDataLoader()
    data_loader.load_fct_deliveries()

    # res = [*set(some_dates)]
