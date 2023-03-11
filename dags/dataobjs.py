from pydantic.dataclasses import dataclass
from datetime import datetime
from typing import Dict


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
