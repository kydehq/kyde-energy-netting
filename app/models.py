from typing import Optional
from sqlalchemy import (
    String, Integer, DateTime, Enum, Numeric, ForeignKey, JSON, Index, Text
)
from sqlalchemy.orm import relationship, Mapped, mapped_column
from datetime import datetime
from decimal import Decimal
import enum
from .db import Base


class Role(str, enum.Enum):
    CONSUMER = "CONSUMER"
    PROSUMER = "PROSUMER"
    OPERATOR = "OPERATOR"


class Participant(Base):
    __tablename__ = "participants"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    external_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    name: Mapped[str] = mapped_column(String(200))
    role: Mapped[Role] = mapped_column(Enum(Role), index=True)
    # KORRIGIERT: str | None -> Optional[str]
    iban: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    api_key: Mapped[str] = mapped_column(String(64), index=True)


class Policy(Base):
    __tablename__ = "policies"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    hash_hex: Mapped[str] = mapped_column(String(128), unique=True)
    # KORRIGIERT: str | None -> Optional[str]
    signature: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    data: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class BillingCycle(Base):
    __tablename__ = "billing_cycles"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    label: Mapped[str] = mapped_column(String(32), unique=True, index=True)  # e.g. "2025-08"
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)


class LedgerEntry(Base):
    __tablename__ = "ledger_entries"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cycle_id: Mapped[int] = mapped_column(ForeignKey("billing_cycles.id"), index=True)
    participant_id: Mapped[int] = mapped_column(ForeignKey("participants.id"), index=True)
    amount_eur: Mapped[Decimal] = mapped_column(Numeric(18, 4))  # + credit to participant, - debit
    source: Mapped[str] = mapped_column(String(64))   # meter|fee|tax|manual|<rule_id>
    meta: Mapped[dict] = mapped_column(JSON, default={})
    # HINZUGEFÜGT: Die Spalte, die den ursprünglichen Fehler verursacht hat
    event_ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    
    participant = relationship("Participant")
    cycle = relationship("BillingCycle")

Index("ix_ledger_cycle_participant", LedgerEntry.cycle_id, LedgerEntry.participant_id)


class TradingDay(Base):
    __tablename__ = "trading_days"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cycle_id: Mapped[int] = mapped_column(ForeignKey("billing_cycles.id"), index=True)
    date_str: Mapped[str] = mapped_column(String(10), index=True)  # "YYYY-MM-DD"
    status: Mapped[str] = mapped_column(String(16), default="open", index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    cycle = relationship("BillingCycle")


class DayNet(Base):
    __tablename__ = "day_nets"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    day_id: Mapped[int] = mapped_column(ForeignKey("trading_days.id"), index=True)
    participant_id: Mapped[int] = mapped_column(ForeignKey("participants.id"), index=True)
    net_eur: Mapped[Decimal] = mapped_column(Numeric(18, 4))  # Tages-Netto (+ Auszahlung, - Rechnung)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    day = relationship("TradingDay")
    participant = relationship("Participant")


class SettlementRun(Base):
    __tablename__ = "settlement_runs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cycle_id: Mapped[int] = mapped_column(ForeignKey("billing_cycles.id"), index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    policy_version: Mapped[str] = mapped_column(String(64))
    summary: Mapped[dict] = mapped_column(JSON)  # totals, counts, audit_hash, etc.
    cycle = relationship("BillingCycle")


class PayoutInstruction(Base):
    __tablename__ = "payout_instructions"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("settlement_runs.id"), index=True)
    participant_id: Mapped[int] = mapped_column(ForeignKey("participants.id"), index=True)
    iban: Mapped[str] = mapped_column(String(64))
    amount_eur: Mapped[Decimal] = mapped_column(Numeric(18,4))  # positive = pay this participant
    remittance_info: Mapped[str] = mapped_column(String(140))
    meta: Mapped[dict] = mapped_column(JSON, default={})
    participant = relationship("Participant")


class InternalTransfer(Base):
    __tablename__ = "internal_transfers"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    day_id: Mapped[int] = mapped_column(ForeignKey("trading_days.id"), index=True)
    from_participant_id: Mapped[int] = mapped_column(ForeignKey("participants.id"), index=True)
    to_participant_id: Mapped[int] = mapped_column(ForeignKey("participants.id"), index=True)
    amount_eur: Mapped[Decimal] = mapped_column(Numeric(18,4))
    meta: Mapped[dict] = mapped_column(JSON, default={})
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    day = relationship("TradingDay")


class ExplainTrace(Base):
    """
    Optional persistierter Explain-Trace pro Event (oder Day/Cycle).
    """
    __tablename__ = "explain_traces"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    cycle_id: Mapped[int] = mapped_column(ForeignKey("billing_cycles.id"), index=True)
    participant_id: Mapped[int] = mapped_column(ForeignKey("participants.id"), index=True)
    scope: Mapped[str] = mapped_column(String(16))  # "event" | "day" | "cycle"
    key: Mapped[str] = mapped_column(String(64))    # z.B. event-id oder date_str
    trace_json: Mapped[str] = mapped_column(Text)
    trace_hash: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    cycle = relationship("BillingCycle")
    participant = relationship("Participant")