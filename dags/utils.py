import logging
from sqlalchemy.engine import Engine
from sqlalchemy import create_engine, text
from pathlib import Path
from typing import Dict
import sys


class DatabaseConnectionError(Exception):
    pass


class SQLError(Exception):
    pass


def get_logger(logger_name: str) -> logging.Logger:

    logger = logging.getLogger(name=logger_name)

    logger.setLevel(level=logging.INFO)

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger_handler = logging.StreamHandler(stream=sys.stdout)
    logger_handler.setFormatter(formatter)
    logger.addHandler(logger_handler)

    return logger


logger = get_logger(logger_name=str(Path(Path(__file__).name)))


def connect_to_database(creds: Dict) -> Engine:
    logger.info(f"Connecting to `{creds['database']}` database...")

    engine = create_engine(
        f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
    )

    try:
        conn = engine.connect()
        conn.close()
        logger.info(f"Succesfully connected to `{creds['database']}` database.")

    except Exception:
        logger.exception(f"Connection to `{creds['database']}` database failed!")

    return engine
