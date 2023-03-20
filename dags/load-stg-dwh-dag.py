from datetime import datetime, timedelta
import sys
from pathlib import Path

# airflow
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

# package
sys.path.append(str(Path(__file__).parent.parent))

from pkg.main import (
    STGBonussystemDataLoader,
    STGOrdersystemDataLoader,
    STGDeliverySystemDataLoader,
)

from pkg.tasks import (
    load_bonussystem_ranks,
    load_bonussystem_users,
    load_bonussystem_outbox,
    load_ordersystem_restaurants,
    load_ordersystem_users,
    load_ordersystem_orders,
    load_deliverysystem_restaurants,
    load_deliverysystem_couriers,
    load_deliverysystem_deliveries,
)

DAG_START_DATE = datetime(2023, 3, 12)

bonussystem_data_loader = STGBonussystemDataLoader()
ordersystem_data_loader = STGOrdersystemDataLoader()
deliverysystem_data_loader = STGDeliverySystemDataLoader()


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

    del_restaurants = load_deliverysystem_restaurants(
        data_loader=deliverysystem_data_loader
    )
    del_couriers = load_deliverysystem_couriers(data_loader=deliverysystem_data_loader)
    del_deliveries = load_deliverysystem_deliveries(
        data_loader=deliverysystem_data_loader
    )
    end = EmptyOperator(task_id="ending")

    chain(
        start,
        [bonus_ranks, bonus_users, bonus_outbox],
        [order_restaurants, order_users, order_orders],
        [del_restaurants, del_couriers, del_deliveries],
        end,
    )


dag = taskflow()
