from sqlalchemy.orm import Session
from sqlalchemy import select, func
from decimal import Decimal
from . import models

def get_or_create_cycle(db: Session, label: str) -> models.BillingCycle:
    cycle = db.scalar(select(models.BillingCycle).where(models.BillingCycle.label == label))
    if not cycle:
        cycle = models.BillingCycle(label=label, status="open")
        db.add(cycle); db.commit(); db.refresh(cycle)
    return cycle

def upsert_participant(db: Session, external_id: str, name: str, role: models.Role, iban: str | None, api_key_seed: str) -> models.Participant:
    p = db.scalar(select(models.Participant).where(models.Participant.external_id == external_id))
    if p:
        if name: p.name = name
        if role: p.role = role
        if iban: p.iban = iban
        db.commit(); db.refresh(p)
        return p
    # simple deterministic api_key for MVP (do NOT use in prod)
    import hashlib
    api_key = hashlib.sha256((external_id + api_key_seed).encode()).hexdigest()[:32]
    p = models.Participant(external_id=external_id, name=name, role=role, iban=iban, api_key=api_key)
    db.add(p); db.commit(); db.refresh(p)
    return p

def add_ledger_entry(db: Session, cycle: models.BillingCycle, participant: models.Participant, amount: Decimal, source: str, meta: dict):
    entry = models.LedgerEntry(cycle_id=cycle.id, participant_id=participant.id, amount_eur=amount, source=source, meta=meta)
    db.add(entry); db.commit(); db.refresh(entry)
    return entry

def compute_balances(db: Session, cycle: models.BillingCycle) -> dict[int, Decimal]:
    rows = db.execute(
        select(models.LedgerEntry.participant_id, func.coalesce(func.sum(models.LedgerEntry.amount_eur), 0))
        .where(models.LedgerEntry.cycle_id == cycle.id)
        .group_by(models.LedgerEntry.participant_id)
    ).all()
    return {pid: Decimal(str(total)) for pid, total in rows}

def run_settlement(db: Session, cycle: models.BillingCycle, policy: models.Policy) -> models.SettlementRun:
    # 1) balances
    balances = compute_balances(db, cycle)

    # 2) apply policy splits/fees (MVP: optional simple percent fee to operator)
    operator = db.scalar(select(models.Participant).where(models.Participant.role == models.Role.OPERATOR))
    operator_fee_pct = (policy.data.get("operator_fee_pct", 0.0) or 0.0)
    if operator and operator_fee_pct:
        # Apply fee on positive credits (prosumers)
        for pid, bal in list(balances.items()):
            if pid == operator.id: 
                continue
            if bal > 0:
                fee = (bal * Decimal(operator_fee_pct)).quantize(Decimal("0.01"))
                balances[pid] = bal - fee
                balances[operator.id] = balances.get(operator.id, Decimal("0")) + fee

    # 3) create run
    run = models.SettlementRun(cycle_id=cycle.id, policy_version=policy.version, summary={"participants": len(balances)})
    db.add(run); db.commit(); db.refresh(run)

    # 4) payouts: positive balance → pay to participant; negatives imply their invoices (handled off-rail or via pull)
    from .models import PayoutInstruction, Participant
    for pid, bal in balances.items():
        if bal > 0:
            part: Participant = db.get(Participant, pid)
            db.add(PayoutInstruction(run_id=run.id, participant_id=pid, iban=part.iban or "", amount_eur=bal, 
                                     remittance_info=f"Settlement {cycle.label}", meta={"balance": str(bal)}))
    db.commit()
    return run

def statement_for_participant(db: Session, cycle: models.BillingCycle, participant: models.Participant) -> dict:
    lines = db.execute(
        select(models.LedgerEntry.source, func.sum(models.LedgerEntry.amount_eur))
        .where(models.LedgerEntry.cycle_id == cycle.id, models.LedgerEntry.participant_id == participant.id)
        .group_by(models.LedgerEntry.source)
    ).all()
    total = sum((Decimal(str(v)) for _, v in lines), Decimal("0.00"))
    return {
        "participant_external_id": participant.external_id,
        "cycle_label": cycle.label,
        "lines": [{"source": s, "amount_eur": str(Decimal(str(v)).quantize(Decimal("0.01")))} for s, v in lines],
        "total": str(total.quantize(Decimal("0.01"))),
        "explanation": "Summe aus Messwerten/Fees/Steuern gemäß aktiver Policy; positive Werte = Auszahlung, negative = Rechnung."
    }
