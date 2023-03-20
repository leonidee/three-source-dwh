from datetime import datetime, timedelta
import sys
from pathlib import Path

# airflow
from airflow.decorators import task

# package
sys.path.append(str(Path(__file__).parent.parent))

from pkg.main import (
    STGBonussystemDataLoader,
    STGOrdersystemDataLoader,
    STGDeliverySystemDataLoader,
    DDSDataLoader,
    CDMDataLoader,
)

# STG tasks


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


@task
def load_deliverysystem_restaurants(data_loader: STGDeliverySystemDataLoader) -> None:
    data_loader.load_restaurants()


@task
def load_deliverysystem_couriers(data_loader: STGDeliverySystemDataLoader) -> None:
    data_loader.load_couriers()


@task
def load_deliverysystem_deliveries(data_loader: STGDeliverySystemDataLoader) -> None:
    data_loader.load_deliveries()


# DDS tasks


@task
def load_users(data_loader: DDSDataLoader) -> None:
    data_loader.load_users()


@task
def load_restaurants(data_loader: DDSDataLoader) -> None:
    data_loader.load_restaurants()


@task
def load_timestamps(data_loader: DDSDataLoader) -> None:
    data_loader.load_timestamps()


@task
def load_products(data_loader: DDSDataLoader) -> None:
    data_loader.load_products()


@task
def load_orders(data_loader: DDSDataLoader) -> None:
    data_loader.load_orders()


@task
def load_fct_product_sales(data_loader: DDSDataLoader) -> None:
    data_loader.load_fct_product_sales()


@task
def load_couriers(data_loader: DDSDataLoader) -> None:
    data_loader.load_couriers()


@task
def load_dim_deliveries(data_loader: DDSDataLoader) -> None:
    data_loader.load_dim_deliveries()


@task
def load_fct_deliveries(data_loader: DDSDataLoader) -> None:
    data_loader.load_fct_deliveries()


# CDM tasks
@task
def load_settlement_report(data_loader: CDMDataLoader) -> None:
    data_loader.load_settlement_report()


@task
def load_dm_courier_ledger(data_loader: CDMDataLoader) -> None:
    data_loader.load_dm_courier_ledger()
