from __future__ import annotations

from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, confloat, conint

EventType = Literal["subscription", "payment", "usage", "cost"]


class EventBase(BaseModel):
    event_id: str = Field(..., description="Unique event id for idempotency")
    event_time: datetime = Field(..., description="When the event happened (UTC)")
    customer_id: str = Field(...)
    region: str = Field(...)


class SubscriptionEvent(EventBase):
    action: Literal["created", "canceled", "upgraded", "downgraded"]
    plan_id: str


class PaymentEvent(EventBase):
    amount: confloat(ge=0)  # type: ignore[valid-type]
    currency: str
    payment_method: Optional[str] = None


class UsageEvent(EventBase):
    metric_name: str
    units: conint(ge=0)  # type: ignore[valid-type]
    plan_id: Optional[str] = None


class CostEvent(EventBase):
    amount: confloat(ge=0)  # type: ignore[valid-type]
    cost_type: str


EventSchemaMap = {
    "subscription": SubscriptionEvent,
    "payment": PaymentEvent,
    "usage": UsageEvent,
    "cost": CostEvent,
}


class IngestionResult(BaseModel):
    status: Literal["accepted", "duplicate", "quarantined"]
    event_id: str
    event_type: EventType
    issues: list[str] = Field(default_factory=list)
    is_late: bool = False
    s3_key: Optional[str] = None


class BatchIngestionResponse(BaseModel):
    accepted: int
    duplicates: int
    quarantined: int
    results: list[IngestionResult]
