"""Shared platform-independent contracts. No platform SDK belongs here."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Ref(Contract):
    channel_id: str = Field(min_length=1)
    message_id: str = Field(min_length=1)

    def key(self) -> tuple[str, str]:
        return self.channel_id, self.message_id


class Message(Ref):
    sender_id: str = Field(min_length=1)
    timestamp: datetime
    text: str = Field(min_length=1)
    reply_to: Ref | None = None
    forwarded_origin: Ref | None = None

    @field_validator("timestamp")
    @classmethod
    def timezone_required(cls, value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must include timezone")
        return value

    def ref(self) -> Ref:
        return Ref(channel_id=self.channel_id, message_id=self.message_id)


class Resource(Contract):
    provider: str
    kind: str
    terms_id: str | None = None


class Quantity(Contract):
    value: float = Field(ge=0, strict=True)
    unit: str | None = None
    kind: Literal["stock", "flow"] = "stock"
    period: str | None = None
    qualifier: Literal["exact", "approx"] = "exact"

    @model_validator(mode="after")
    def consistent_period(self):
        if (self.kind == "flow") != (self.period is not None):
            raise ValueError("flow requires period; stock cannot have period")
        return self


class Price(Contract):
    value: float = Field(ge=0, strict=True)
    currency: str | None = None
    unit: str | None = None
    operator: Literal["eq", "lt", "lte", "gt", "gte"] = "eq"
    is_soft: bool = Field(default=False, strict=True)


Action = Literal[
    "supply", "demand", "set_remaining", "reprice", "retract", "ignore", "unresolved"
]


class Observation(Contract):
    action: Action
    resource: Resource | None = None
    quantity: Quantity | None = None
    price: Price | None = None
    target_message: Ref | None = None
    assertion_mode: Literal["direct", "hearsay"] = "direct"
    source_message_ids: list[Ref] = Field(min_length=1)
    evidence_quote: str = Field(min_length=1)
    unresolved_reason: str | None = None
    reason: str | None = None
    convention_id: str | None = None

    @field_validator("price", "quantity", mode="before")
    @classmethod
    def empty_optional_measurement(cls, value, info):
        # JSON-mode models sometimes spell an absent measurement as an all-null
        # object. Only collapse known fields with no information; never discard
        # a number, qualifier, unknown key, or partially specified measurement.
        fields = (
            Price.model_fields if info.field_name == "price" else Quantity.model_fields
        )
        if (
            isinstance(value, dict)
            and value
            and set(value) <= set(fields)
            and all(v is None for v in value.values())
        ):
            return None
        return value

    @model_validator(mode="after")
    def required_reason(self):
        if self.action == "unresolved" and not self.unresolved_reason:
            raise ValueError("unresolved requires reason")
        return self


class Extraction(Contract):
    observations: list[Observation] = Field(max_length=8)


class Event(Contract):
    event_id: str
    owner: str
    origin_message: Ref
    side: Literal["supply", "demand"]
    resource: Resource
    quantity: Quantity | None = None
    price: Price | None = None
    status: Literal["active", "retracted"] = "active"
    supporting_message_ids: list[Ref]


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if line.strip():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path.name}:{number}: invalid JSON") from exc
    return rows


def write_jsonl(path: Path, rows: list[dict]):
    with path.open("w", encoding="utf-8") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")
