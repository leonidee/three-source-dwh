from pathlib import Path
from sqlalchemy.engine import Engine
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from utils import DatabaseConnector, get_logger
from objs import SQLError, FSError

DWH_DDL_SQL = "sql/init-dwh-ddl.sql"
DAG_START_DATE = datetime(2023, 3, 12)

logger = get_logger(logger_name=str(Path(Path(__file__).name)))

engine = DatabaseConnector(db="pg_dwh").connect_to_database()


@task
def execute_init_sql(engine: Engine, sql: Path | str) -> None:

    logger.info("Starting initializing process.")

    try:
        logger.info(f"Reading {sql} file.")
        query = Path(Path(__file__).parent, sql).read_text(encoding="UTF-8")
    except Exception:
        logger.exception(f"Unable to read {sql} file!")
        raise FSError

    try:
        with engine.begin() as conn:
            conn.execute(statement=(query))
        logger.info(f"{sql} file was successfully executed. DWH layers initialized.")
    except Exception:
        logger.exception(f"Unable to execute {sql} file! Initialize process failed.")
        raise SQLError


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
        trigger_dag_id="load-stg-dwh-dag",
        wait_for_completion=True,
        poke_interval=10,
        allowed_states=["success"],
        failed_states=["skipped", "failed"],
    )

    end = EmptyOperator(task_id="ending")

    chain(start, execute_sql, trigger, end)


dag = taskflow()
