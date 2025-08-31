from sqlalchemy import Column, String, Integer, DateTime, Enum, Numeric, ForeignKey, JSON, Index
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
    iban: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    api_key: Mapped[str] = mapped_column(String(64), index=True)

class Policy(Base):
    __tablename__ = "policies"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    version: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    hash_hex: Mapped[str] = mapped_column(String(128), unique=True)
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
    source: Mapped[str] = mapped_column(String(32))   # meter|fee|tax|manual
    meta: Mapped[dict] = mapped_column(JSON, default={})
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)

    participant = relationship("Participant")
    cycle = relationship("BillingCycle")

Index("ix_ledger_cycle_participant", LedgerEntry.cycle_id, LedgerEntry.participant_id)

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
