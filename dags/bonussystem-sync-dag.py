from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

from datetime import datetime, timedelta
from pathlib import Path

from main import BonussystemDataMover
from utils import get_logger, DatabaseConnector

DAG_START_DATE = datetime(2023, 3, 12)

logger = get_logger(logger_name=str(Path(Path(__file__).name)))

pg_dwh_engine = DatabaseConnector(db="pg_dwh").connect_to_database()
pg_origin_engine = DatabaseConnector(db="pg_source").connect_to_database()

data_mover = BonussystemDataMover(source_conn=pg_origin_engine, dwh_conn=pg_dwh_engine)


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
    start_date=DAG_START_DATE,
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

    start = EmptyOperator(task_id="starting")

    load_ranks = load_ranks_data(data_mover=data_mover)
    load_users = load_users_data(data_mover=data_mover)
    load_outbox = load_outbox_data(data_mover=data_mover)

    end = EmptyOperator(task_id="ending")

    chain(start, [load_ranks, load_users, load_outbox], end)


dag = taskflow()
