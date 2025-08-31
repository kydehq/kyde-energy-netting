from pydantic import BaseModel, Field
from typing import Optional, Literal, Dict, Any, List
from decimal import Decimal
from datetime import datetime

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
    event_ts: Optional[datetime] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

class CloseDayIn(BaseModel):
    policy_version: str
    fixed_cost_eur: Decimal = Decimal("0.08")        # Kostenmodell für min-cost-flow (Optional)
    variable_cost_rate: Decimal = Decimal("0.0035")  # 0.35%

class CloseCycleIn(BaseModel):
    policy_version: str
    fixed_cost_eur: Decimal = Decimal("0.00")
    variable_cost_rate: Decimal = Decimal("0.00")

class StatementOut(BaseModel):
    participant_external_id: str
    cycle_label: str
    lines: List[dict]
    total: Decimal
    explanation: str

class DayNetOut(BaseModel):
    date: str
    items: List[dict]
    totals: dict

class InternalTransfersOut(BaseModel):
    date: str
    edges: List[dict]

class SettlementOut(BaseModel):
    run_id: int
    cycle_label: str
    payouts: List[dict]
    totals: dict
