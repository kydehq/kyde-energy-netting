from pydantic import BaseModel, Field, ConfigDict
from typing import Optional, Literal, Dict, Any, List
from decimal import Decimal
from datetime import datetime

# --- Participants
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

# --- Policies (raw store)
class PolicyIn(BaseModel):
    version: str
    signature: Optional[str] = None
    data: Dict[str, Any]

class PolicyOut(BaseModel):
    version: str
    hash: str
    signature: Optional[str] = None

# --- Events
class EventIn(BaseModel):
    cycle_label: str               # e.g. "2025-08"
    participant_external_id: str
    amount_eur: Decimal            # + credit, - debit (direkter Ledger-Eintrag)
    source: Literal["meter","fee","tax","manual"]
    event_ts: Optional[datetime] = None
    meta: Dict[str, Any] = Field(default_factory=dict)  # z.B. {"kwh": 2.5, "tags":["energy_import"]}

class EventInWithPolicy(BaseModel):
    """
    Optionaler Shortcut: Event + Policy-Eval in einem Call (für Demos).
    """
    event: EventIn
    policy_version: Optional[str] = None

# --- EoD / Cycle Close
class CloseDayIn(BaseModel):
    policy_version: str
    fixed_cost_eur: Decimal = Decimal("0.08")
    variable_cost_rate: Decimal = Decimal("0.0035")

class CloseCycleIn(BaseModel):
    policy_version: str
    fixed_cost_eur: Decimal = Decimal("0.00")
    variable_cost_rate: Decimal = Decimal("0.00")

# --- Statements
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

# --- Explain DSL
class ExplainEval(BaseModel):
    rule_id: str
    matched: bool = True
    inputs: Dict[str, Any] | None = None
    formula: str | None = None
    result_eur: str | None = None
    beneficiary: str | None = None
    ledger_line_id: int | None = None

class ExplainTraceOut(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    scope: Literal["event","day","cycle"]
    key: str
    evaluations: List[ExplainEval]
    totals: Dict[str, Any]
    trace_hash: str
