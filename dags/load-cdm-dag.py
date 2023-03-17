from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain

from datetime import datetime, timedelta

from main import CDMDataLoader

DAG_START_DATE = datetime(2023, 3, 12)

data_loader = CDMDataLoader()


@task
def load_settlement_report(data_loader: CDMDataLoader) -> None:
    data_loader.load_settlement_report()


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

    report = load_settlement_report(data_loader=data_loader)

    end = EmptyOperator(task_id="ending")

    chain(start, report, end)


dag = taskflow()
