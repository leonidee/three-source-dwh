from datetime import datetime, timedelta
import sys
from pathlib import Path

# airflow
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

# package
sys.path.append(str(Path(__file__).parent.parent))

from pkg.main import STGBonussystemDataLoader, STGOrdersystemDataLoader

DAG_START_DATE = datetime(2023, 3, 12)

bonussystem_data_loader = STGBonussystemDataLoader()
ordersystem_data_loader = STGOrdersystemDataLoader()


@task
def load_bonussystem_ranks(data_loader: STGBonussystemDataLoader) -> None:
    data_loader.load_ranks_data()


@task
def load_bonussystem_users(data_loader: STGBonussystemDataLoader) -> None:
    data_loader.load_users_data()


@task
def load_bonussystem_outbox(data_loader: STGBonussystemDataLoader) -> None:
    data_loader.load_outbox_data()


@task
def load_ordersystem_restaurants(data_loader: STGOrdersystemDataLoader) -> None:
    data_loader.load_restaurants()


@task
def load_ordersystem_users(data_loader: STGOrdersystemDataLoader) -> None:
    data_loader.load_users()


@task
def load_ordersystem_orders(data_loader: STGOrdersystemDataLoader) -> None:
    data_loader.load_orders()


@dag(
    schedule_interval="0/15 * * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    is_paused_upon_creation=False,
    dag_id="load-stg-dwh-dag",
    default_args={
        "owner": "leonide",
        "retries": 5,
        "retry_delay": timedelta(seconds=30),
    },
)
def taskflow() -> None:

    start = EmptyOperator(task_id="starting")

    bonus_ranks = load_bonussystem_ranks(data_loader=bonussystem_data_loader)
    bonus_users = load_bonussystem_users(data_loader=bonussystem_data_loader)
    bonus_outbox = load_bonussystem_outbox(data_loader=bonussystem_data_loader)

    order_restaurants = load_ordersystem_restaurants(
        data_loader=ordersystem_data_loader
    )
    order_users = load_ordersystem_users(data_loader=ordersystem_data_loader)
    order_orders = load_ordersystem_orders(data_loader=ordersystem_data_loader)

    end = EmptyOperator(task_id="ending")

    chain(
        start,
        [bonus_ranks, bonus_users, bonus_outbox],
        [order_restaurants, order_users, order_orders],
        end,
    )


dag = taskflow()
