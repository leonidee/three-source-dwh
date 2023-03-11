from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

from datetime import datetime, timedelta
from pathlib import Path
import os
from dotenv import load_dotenv, find_dotenv

from main import BonussystemDataMover
from utils import get_logger, connect_to_database

logger = get_logger(logger_name=str(Path(Path(__file__).name)))

find_dotenv(raise_error_if_not_found=True)
load_dotenv(verbose=True, override=True)

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


@task
def load_ranks_data(data_mover: BonussystemDataMover) -> None:

    data_mover.load_ranks_data()


@task
def load_users_data(data_mover: BonussystemDataMover) -> None:

    data_mover.load_users_data()


@task
def load_outbox_data(data_mover: BonussystemDataMover) -> None:

    data_mover.load_outbox_data()


@dag(
    schedule_interval="0/15 * * * *",
    start_date=datetime(2023, 3, 11),
    catchup=False,
    is_paused_upon_creation=False,
    dag_id="bonussystem-sync-dag",
    default_args={
        "owner": "leonide",
        "retries": 5,
        "retry_delay": timedelta(minutes=10),
    },
)
def taskflow() -> None:

    start = EmptyOperator(task_id="start")

    load_ranks = load_ranks_data(data_mover=data_mover)

    load_users = load_users_data(data_mover=data_mover)

    load_outbox = load_outbox_data(data_mover=data_mover)

    end = EmptyOperator(task_id="end")

    chain(start, [load_ranks, load_users, load_outbox], end)


dag = taskflow()
