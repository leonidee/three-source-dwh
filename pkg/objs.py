from datetime import datetime
from typing import Dict
from dataclasses import dataclass


@dataclass
class EtlObj:
    id: int
    workflow_key: str
    workflow_settings: Dict


# STG loading objects
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
class OrdersystemObj:
    object_id: str
    object_value: Dict
    update_ts: datetime


@dataclass
class DeliverySystemObj:
    object_id: str
    object_value: dict
    update_ts: datetime


# DDS loading objects
@dataclass
class DDSUser:
    user_id: str
    user_name: str
    user_login: str


@dataclass
class DDSRestaurant:
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime


@dataclass
class DDSTimestamp:
    ts: datetime
    year: int
    month: int
    day: int
    time: int
    date: datetime


@dataclass
class DDSProduct:
    restaurant_id: str
    product_id: int
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime


@dataclass
class DDSOrder:
    order_key: str
    order_status: str
    restaurant_id: str
    date: datetime
    user_id: str


@dataclass
class DDSFactProductSale:
    order_id: str
    product_id: str
    price: float
    quantity: int
    bonus_payment: float
    bonus_grant: float


@dataclass
class DDSCourier:
    courier_id: str
    courier_name: str
    active_from: datetime
    active_to: datetime


@dataclass
class DDSDimDeliveries:
    delivery_id: str
    delivery_ts: datetime
    courier_id: str
    order_id: str


@dataclass
class DDSFctDeliveries:
    delivery_id: str
    address: str
    rate: int
    order_sum: float
    tip_sum: float


# holders


@dataclass
class CredentialHolder:
    host: str
    port: str
    user: str
    password: str
    database: str
    ca_path: str = None
    repl_set: str = None


@dataclass
class HeadersHolder:
    apikey: str
    nickname: str
    cohort: int
