from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Any
from decimal import Decimal

class ParticipantCreate(BaseModel):
    external_id: str
    name: str
    role: Literal["CONSUMER","PROSUMER","OPERATOR"]
    iban: Optional[str] = None

class ParticipantOut(BaseModel):
    id: int
    external_id: str
    name: str
    role: str
    iban: Optional[str]

class PolicyIn(BaseModel):
    version: str
    data: Dict[str, Any]

class EventIn(BaseModel):
    cycle_label: str               # e.g. "2025-08"
    participant_external_id: str
    amount_eur: Decimal            # + credit, - debit
    source: Literal["meter","fee","tax","manual"]
    meta: Dict[str, Any] = Field(default_factory=dict)

class CloseCycleIn(BaseModel):
    cycle_label: str
    policy_version: str

class StatementOut(BaseModel):
    participant_external_id: str
    cycle_label: str
    lines: list[dict]
    total: Decimal
    explanation: str

class SettlementOut(BaseModel):
    run_id: int
    cycle_label: str
    payouts: list[dict]
    totals: dict
