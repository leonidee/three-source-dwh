from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

from datetime import datetime, timedelta
from pathlib import Path

from main import BonussystemDataMover, OrdersystemDataMover
from utils import get_logger

DAG_START_DATE = datetime(2023, 3, 12)

logger = get_logger(logger_name=str(Path(Path(__file__).name)))

bonussystem_data_mover = BonussystemDataMover()
ordersystem_data_mover = OrdersystemDataMover()


@task
def load_bonussystem_ranks(data_mover: BonussystemDataMover) -> None:
    data_mover.load_ranks_data()


@task
def load_bonussystem_users(data_mover: BonussystemDataMover) -> None:
    data_mover.load_users_data()


@task
def load_bonussystem_outbox(data_mover: BonussystemDataMover) -> None:
    data_mover.load_outbox_data()


@task
def load_ordersystem_restaurants(data_mover: OrdersystemDataMover) -> None:
    data_mover.load_restaurants()


@task
def load_ordersystem_users(data_mover: OrdersystemDataMover) -> None:
    data_mover.load_users()


@dag(
    schedule_interval="0/15 * * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    is_paused_upon_creation=False,
    dag_id="sync-dwh-dag",
    default_args={
        "owner": "leonide",
        "retries": 5,
        "retry_delay": timedelta(minutes=10),
    },
)
def taskflow() -> None:

    start = EmptyOperator(task_id="starting")

    bonus_ranks = load_bonussystem_ranks(data_mover=bonussystem_data_mover)
    bonus_users = load_bonussystem_users(data_mover=bonussystem_data_mover)
    bonus_outbox = load_bonussystem_outbox(data_mover=bonussystem_data_mover)

    order_restaurants = load_ordersystem_restaurants(data_mover=ordersystem_data_mover)
    order_users = load_ordersystem_users(data_mover=ordersystem_data_mover)

    end = EmptyOperator(task_id="ending")

    chain(
        start,
        [bonus_ranks, bonus_users, bonus_outbox, order_restaurants, order_users],
        end,
    )


dag = taskflow()
