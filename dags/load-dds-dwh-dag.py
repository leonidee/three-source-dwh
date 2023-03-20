from datetime import datetime, timedelta
import sys
from pathlib import Path

# airflow
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

# package
sys.path.append(str(Path(__file__).parent.parent))

from pkg.main import DDSDataLoader

from pkg.tasks import (
    load_users,
    load_restaurants,
    load_timestamps,
    load_products,
    load_orders,
    load_fct_product_sales,
    load_couriers,
    load_dim_deliveries,
    load_fct_deliveries,
)

DAG_START_DATE = datetime(2023, 3, 12)

data_loader = DDSDataLoader()


@dag(
    schedule_interval="0/15 * * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    is_paused_upon_creation=False,
    dag_id="load-dds-dwh-dag",
    default_args={
        "owner": "leonide",
        "retries": 5,
        "retry_delay": timedelta(seconds=30),
    },
)
def taskflow() -> None:

    start = EmptyOperator(task_id="starting")

    users = load_users(data_loader=data_loader)
    restaurants = load_restaurants(data_loader=data_loader)
    timestamps = load_timestamps(data_loader=data_loader)
    products = load_products(data_loader=data_loader)
    orders = load_orders(data_loader=data_loader)
    couriers = load_couriers(data_loader=data_loader)
    dim_deliveries = load_dim_deliveries(data_loader=data_loader)
    fct_deliveries = load_fct_deliveries(data_loader=data_loader)

    fct_sales = load_fct_product_sales(data_loader=data_loader)

    end = EmptyOperator(task_id="ending")

    chain(
        start,
        [users, restaurants, timestamps, couriers],
        products,
        orders,
        dim_deliveries,
        fct_sales,
        fct_deliveries,
        end,
    )


dag = taskflow()
