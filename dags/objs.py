from datetime import datetime
from typing import Dict
from pathlib import Path
from dataclasses import dataclass


@dataclass
class BonussystemUserObj:
    id: int
    order_user_id: str


@dataclass
class BonussystemRankObj:
    id: int
    name: str
    bonus_percent: float
    min_payment_threshold: float


@dataclass
class BonussystemOutboxObj:
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


@dataclass
class EtlObj:
    id: int
    workflow_key: str
    workflow_settings: Dict


@dataclass
class CredentialHolder:
    host: str
    port: str
    user: str
    password: str
    database: str
    ca_path: str | Path = None
    repl_set: str = None


@dataclass
class OrdersystemObj:
    object_id: str
    object_value: Dict
    update_ts: datetime


class DatabaseConnectionError(Exception):
    pass


class SQLError(Exception):
    pass


class DotEnvError(Exception):
    pass


class MongoServiceError(Exception):
    pass


class FSError(Exception):
    pass
