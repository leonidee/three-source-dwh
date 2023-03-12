import os
from dotenv import find_dotenv, load_dotenv
from pathlib import Path
from utils import connect_to_database, get_logger
from sqlalchemy.engine import Engine
from datetime import datetime, timedelta

from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


DWH_DDL_SQL = "sql/dwh-init-ddl.sql"
DAG_START_DATE = datetime(2023, 3, 12)

logger = get_logger(logger_name=str(Path(Path(__file__).name)))

find_dotenv(raise_error_if_not_found=True)
load_dotenv(verbose=True, override=True)

pg_dwh_creds = {
    "host": os.getenv("PG_DWH_HOST"),
    "port": os.getenv("PG_DWH_PORT"),
    "user": os.getenv("PG_DWH_USER"),
    "password": os.getenv("PG_DWH_PASSWORD"),
    "database": "dwh",
}

engine = connect_to_database(creds=pg_dwh_creds)


@task
def execute_init_sql(engine: Engine, sql: Path | str) -> None:

    logger.info("Starting initializing process.")

    try:
        logger.info(f"Reading {sql} file.")
        query = Path(Path(__file__).parent, sql).read_text(encoding="UTF-8")
    except Exception:
        logger.exception(f"Unable to read {sql} file!")

    try:
        with engine.begin() as conn:
            conn.execute(statement=(query))
        logger.info(f"{sql} file was successfully executed. DWH layers initialized.")
    except Exception:
        logger.exception(f"Unable to execute {sql} file! Initialize process failed.")


@dag(
    dag_id="dwh-init-dag",
    schedule_interval="@once",
    start_date=DAG_START_DATE,
    default_args={
        "owner": "leonide",
        "retries": 5,
        "retry_delay": timedelta(seconds=20),
    },
    catchup=False,
    is_paused_upon_creation=False,
)
def taskflow() -> None:

    start = EmptyOperator(task_id="starting")

    execute_sql = execute_init_sql(engine=engine, sql=DWH_DDL_SQL)

    trigger = TriggerDagRunOperator(
        task_id="trigger_bonussystem_sync_dag",
        trigger_dag_id="bonussystem-sync-dag",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["skipped", "failed"],
    )

    end = EmptyOperator(task_id="ending")

    chain(start, execute_sql, trigger, end)


dag = taskflow()
