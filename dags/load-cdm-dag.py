from datetime import datetime, timedelta
import sys
from pathlib import Path

# airflow
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

# package
sys.path.append(str(Path(__file__).parent.parent))

from pkg.main import CDMDataLoader
from pkg.tasks import load_settlement_report, load_dm_courier_ledger

DAG_START_DATE = datetime(2023, 3, 12)

data_loader = CDMDataLoader()


@dag(
    schedule_interval="0/15 * * * *",
    start_date=DAG_START_DATE,
    catchup=False,
    is_paused_upon_creation=False,
    dag_id="load-cdm-dag",
    default_args={
        "owner": "leonide",
        "retries": 5,
        "retry_delay": timedelta(seconds=30),
    },
)
def taskflow() -> None:

    start = EmptyOperator(task_id="starting")

    settlement_report = load_settlement_report(data_loader=data_loader)
    courier_report = load_dm_courier_ledger(data_loader=data_loader)

    end = EmptyOperator(task_id="ending")

    chain(start, settlement_report, courier_report, end)


dag = taskflow()
