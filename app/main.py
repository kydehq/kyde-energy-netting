from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from decimal import Decimal
import os

from .db import Base, engine, get_db
from . import models, schemas, logic
from .security import require_api_key
from .utils import hash_policy

app = FastAPI(title="KYDE Settlement MVP", version="0.1.0")

# create tables on startup (MVP – später Alembic)
@app.on_event("startup")
def startup():
    Base.metadata.create_all(bind=engine)

@app.get("/healthz")
def health():
    return {"ok": True}

# --- Participants
@app.post("/v1/participants", dependencies=[Depends(require_api_key)], response_model=schemas.ParticipantOut)
def create_participant(body: schemas.ParticipantCreate, db: Session = Depends(get_db)):
    role = models.Role(body.role)
    p = logic.upsert_participant(db, body.external_id, body.name, role, body.iban, api_key_seed=os.getenv("KYDE_API_KEY","seed"))
    return schemas.ParticipantOut(id=p.id, external_id=p.external_id, name=p.name, role=p.role.value, iban=p.iban)

# --- Policy
@app.post("/v1/policy", dependencies=[Depends(require_api_key)])
def set_policy(body: schemas.PolicyIn, db: Session = Depends(get_db)):
    h = hash_policy(body.data)
    exists = db.query(models.Policy).filter(models.Policy.version==body.version).first()
    if exists:
        raise HTTPException(409, "Policy version exists")
    pol = models.Policy(version=body.version, hash_hex=h, data=body.data)
    db.add(pol); db.commit()
    return {"version": pol.version, "hash": pol.hash_hex}

# --- Events -> Ledger
@app.post("/v1/events", dependencies=[Depends(require_api_key)])
def ingest_event(ev: schemas.EventIn, db: Session = Depends(get_db)):
    cycle = logic.get_or_create_cycle(db, ev.cycle_label)
    if cycle.status != "open":
        raise HTTPException(400, "Cycle is closed")
    part = db.query(models.Participant).filter_by(external_id=ev.participant_external_id).first()
    if not part:
        raise HTTPException(404, "Participant not found")
    logic.add_ledger_entry(db, cycle, part, Decimal(ev.amount_eur), ev.source, ev.meta)
    return {"ok": True}

# --- Close Cycle -> Settlement
@app.post("/v1/cycles/{cycle_label}/close", dependencies=[Depends(require_api_key)], response_model=schemas.SettlementOut)
def close_cycle(cycle_label: str, body: schemas.CloseCycleIn, db: Session = Depends(get_db)):
    cycle = logic.get_or_create_cycle(db, cycle_label)
    if cycle.status == "closed":
        raise HTTPException(400, "Already closed")
    pol = db.query(models.Policy).filter_by(version=body.policy_version).first()
    if not pol:
        raise HTTPException(404, "Policy not found")
    run = logic.run_settlement(db, cycle, pol)
    cycle.status = "closed"; db.commit()
    # read payouts
    payouts = db.query(models.PayoutInstruction).filter_by(run_id=run.id).all()
    return {
        "run_id": run.id,
        "cycle_label": cycle.label,
        "payouts": [{"participant_id": p.participant_id, "iban": p.iban, "amount_eur": str(p.amount_eur), "remittance_info": p.remittance_info} for p in payouts],
        "totals": run.summary
    }

# --- Statements (Explainable Bills)
@app.get("/v1/cycles/{cycle_label}/statements/{participant_external_id}", dependencies=[Depends(require_api_key)], response_model=schemas.StatementOut)
def participant_statement(cycle_label: str, participant_external_id: str, db: Session = Depends(get_db)):
    cycle = db.query(models.BillingCycle).filter_by(label=cycle_label).first()
    if not cycle:
        raise HTTPException(404, "Cycle not found")
    part = db.query(models.Participant).filter_by(external_id=participant_external_id).first()
    if not part:
        raise HTTPException(404, "Participant not found")
    return logic.statement_for_participant(db, cycle, part)

@app.post("/v1/quickstart", dependencies=[Depends(require_api_key)])
def quickstart(db: Session = Depends(get_db)):
    from . import models, logic
    op = logic.upsert_participant(db, "op-001", "Colibrie Operator", models.Role.OPERATOR, "AT611904300234573201", os.getenv("KYDE_API_KEY","seed"))
    pros = logic.upsert_participant(db, "pros-001", "PV Dach A", models.Role.PROSUMER, "AT611904300234573202", os.getenv("KYDE_API_KEY","seed"))
    con = logic.upsert_participant(db, "con-001", "Mieter A", models.Role.CONSUMER, "AT611904300234573203", os.getenv("KYDE_API_KEY","seed"))
    # simple policy
    from .models import Policy
    if not db.query(Policy).filter_by(version="v1").first():
        from .utils import hash_policy
        pol = Policy(version="v1", hash_hex=hash_policy({"operator_fee_pct":0.05}), data={"operator_fee_pct":0.05})
        db.add(pol); db.commit()
    # events
    cycle = logic.get_or_create_cycle(db, "2025-08")
    logic.add_ledger_entry(db, cycle, pros, Decimal("40.00"), "meter", {"kwh":100,"type":"export"})
    logic.add_ledger_entry(db, cycle, con,  Decimal("-25.50"), "meter", {"kwh":85,"type":"import"})
    return {"ok": True}
