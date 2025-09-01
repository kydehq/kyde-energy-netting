from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select, func
from decimal import Decimal
from datetime import datetime
import os, json

from .db import Base, engine, get_db
from . import models, schemas, logic
from .security import require_api_key
from .utils import hash_policy
from .policy_dsl import PolicyEngine, canonical_hash

app = FastAPI(title="KYDE EoD Netting + Policy DSL", version="0.4.0")

@app.on_event("startup")
def startup():
    # Migration temporarily disabled for debugging
    pass
    # from alembic.config import Config
    # from alembic import command
    # alembic_cfg = Config("alembic.ini")
    # command.upgrade(alembic_cfg, "head")

@app.get("/healthz")
def health():
    return {"ok": True}

# ---------------- Participants
@app.post("/v1/participants", dependencies=[Depends(require_api_key)], response_model=schemas.ParticipantOut)
def create_participant(body: schemas.ParticipantCreate, db: Session = Depends(get_db)):
    role = models.Role(body.role)
    p = logic.upsert_participant(db, body.external_id, body.name, role, body.iban, api_key_seed=os.getenv("KYDE_API_KEY","seed"))
    return schemas.ParticipantOut(id=p.id, external_id=p.external_id, name=p.name, role=p.role.value, iban=p.iban)

# ---------------- Policy CRUD
@app.post("/v1/policy", dependencies=[Depends(require_api_key)], response_model=schemas.PolicyOut)
def set_policy(body: schemas.PolicyIn, db: Session = Depends(get_db)):
    # Validate + hash canonical
    chash = canonical_hash(body.data)
    exists = db.query(models.Policy).filter(models.Policy.version==body.version).first()
    if exists:
        raise HTTPException(409, "Policy version exists")
    pol = models.Policy(version=body.version, hash_hex=chash, signature=body.signature, data=body.data)
    db.add(pol); db.commit()
    return schemas.PolicyOut(version=pol.version, hash=pol.hash_hex, signature=pol.signature)

@app.get("/v1/policy/{version}", dependencies=[Depends(require_api_key)])
def get_policy(version: str, db: Session = Depends(get_db)):
    pol = db.query(models.Policy).filter_by(version=version).first()
    if not pol: raise HTTPException(404, "Policy not found")
    return {"version": pol.version, "hash": pol.hash_hex, "signature": pol.signature, "data": pol.data}

# ---------------- Events (plain ledger + policy-eval-on-event)
@app.post("/v1/events", dependencies=[Depends(require_api_key)])
def ingest_event(ev: schemas.EventIn, db: Session = Depends(get_db)):
    cycle = logic.get_or_create_cycle(db, ev.cycle_label)
    if cycle.status != "open":
        raise HTTPException(400, "Cycle is closed")

    part = db.query(models.Participant).filter_by(external_id=ev.participant_external_id).first()
    if not part: raise HTTPException(404, "Participant not found")

    # 1) Raw ledger line (as before)
    logic.add_ledger_entry(db, cycle, part, Decimal(ev.amount_eur), ev.source, ev.meta, ev.event_ts)

    return {"ok": True}

@app.post("/v1/events+policy", dependencies=[Depends(require_api_key)])
def ingest_event_and_eval(body: schemas.EventInWithPolicy, db: Session = Depends(get_db)):
    ev = body.event
    cycle = logic.get_or_create_cycle(db, ev.cycle_label)
    if cycle.status != "open":
        raise HTTPException(400, "Cycle is closed")
    part = db.query(models.Participant).filter_by(external_id=ev.participant_external_id).first()
    if not part: raise HTTPException(404, "Participant not found")

    # 1) Raw event line
    base_entry = logic.add_ledger_entry(db, cycle, part, Decimal(ev.amount_eur), ev.source, ev.meta, ev.event_ts)

    # 2) Policy evaluate (if provided, else latest)
    pol = None
    if body.policy_version:
        pol = db.query(models.Policy).filter_by(version=body.policy_version).first()
    else:
        pol = db.query(models.Policy).order_by(models.Policy.id.desc()).first()
    if not pol:
        return {"ok": True, "note": "no policy set, raw event stored"}

    engine = PolicyEngine(pol.data)
    operator = db.scalar(select(models.Participant).where(models.Participant.role == models.Role.OPERATOR))
    operator_id = operator.id if operator else None

    # Build event dict for engine
    ev_dict = {
        "source": ev.source,
        "meta": ev.meta,
        "amount_eur": str(ev.amount_eur),
        "participant_external_id": ev.participant_external_id,
        "event_ts": (ev.event_ts.isoformat() if ev.event_ts else None)
    }
    postings, trace = engine.evaluate_event(ev_dict, part.role.value if part.role else None, operator_id)

    # 3) Persist postings as additional ledger entries (source = rule_id via account mapping in trace)
    # We map accounts to entries by using trace.evaluations order; we also store explain trace row.
    # For simplicity: per-account postings -> participant = primary participant unless beneficiary=OPERATOR rule fired.
    # We'll augment via evaluations to detect beneficiaries.
    # First, index evals by account cumulative impact; then write lines
    per_account = {k: Decimal(v) for k, v in trace["totals"]["per_account"].items()}
    evals = trace["evaluations"]

    created_ids = []
    for e in evals:
        if not e.get("matched"): 
            continue
        amt = Decimal(e.get("result_eur","0") or "0")
        if amt == 0: 
            continue
        beneficiary_pid = None
        if e.get("beneficiary") == "OPERATOR" and operator_id:
            beneficiary_pid = operator_id
        target_pid = beneficiary_pid or part.id
        # rule_id as source, account in meta
        entry = logic.add_ledger_entry(
            db, cycle, db.get(models.Participant, target_pid),
            amt, e["rule_id"], {"account": None, "policy": pol.version, "explain": True},
            ev.event_ts
        )
        e["ledger_line_id"] = entry.id
        created_ids.append(entry.id)

    # 4) Persist ExplainTrace (optional but great)
    trace_blob = {
        "scope": "event",
        "key": f"{part.external_id}@{ev.event_ts.isoformat() if ev.event_ts else 'now'}",
        "evaluations": evals,
        "totals": trace["totals"]
    }
    trace_hash = canonical_hash(trace_blob)
    db.add(models.ExplainTrace(
        cycle_id=cycle.id,
        participant_id=part.id,
        scope="event",
        key=trace_blob["key"],
        trace_json=json.dumps(trace_blob, ensure_ascii=False, separators=(",",":")),
        trace_hash=trace_hash
    ))
    db.commit()

    return {"ok": True, "policy_version": pol.version, "explain_hash": trace_hash, "created_lines": created_ids}

# -------- EoD Close: 24:00 Leveling
@app.post("/v1/days/{date_str}/close", dependencies=[Depends(require_api_key)])
def close_day(date_str: str, body: schemas.CloseDayIn, db: Session = Depends(get_db)):
    if len(date_str) != 10:
        raise HTTPException(400, "date_str must be YYYY-MM-DD")
    cycle_label = date_str[:7]
    cycle = logic.get_or_create_cycle(db, cycle_label)
    if cycle.status != "open":
        raise HTTPException(400, "Cycle is closed")

    pol = db.query(models.Policy).filter_by(version=body.policy_version).first()
    if not pol:
        raise HTTPException(404, "Policy not found")

    day, nets, audit = logic.close_trading_day(
        db, cycle, date_str, pol, body.fixed_cost_eur, body.variable_cost_rate
    )
    return {
        "date": date_str,
        "day_status": day.status,
        "nets": nets,
        "audit_hash": audit
    }

@app.get("/v1/days/{date_str}/nets", dependencies=[Depends(require_api_key)], response_model=schemas.DayNetOut)
def read_day_net(date_str: str, db: Session = Depends(get_db)):
    cycle_label = date_str[:7]
    cycle = db.query(models.BillingCycle).filter_by(label=cycle_label).first()
    if not cycle:
        raise HTTPException(404, "Cycle not found")
    day = db.query(models.TradingDay).filter_by(cycle_id=cycle.id, date_str=date_str).first()
    if not day:
        raise HTTPException(404, "Day not found")

    rows = db.query(models.DayNet).filter_by(day_id=day.id).all()
    items = [{"participant_id": r.participant_id, "net_eur": str(r.net_eur)} for r in rows]
    total = sum(Decimal(r["net_eur"]) for r in items) if items else Decimal("0.00")
    return {"date": date_str, "items": items, "totals": {"sum": str(total)}}

@app.get("/v1/days/{date_str}/internal-transfers", dependencies=[Depends(require_api_key)])
def read_internal_transfers(date_str: str, db: Session = Depends(get_db)):
    cycle_label = date_str[:7]
    cycle = db.query(models.BillingCycle).filter_by(label=cycle_label).first()
    if not cycle:
        raise HTTPException(404, "Cycle not found")
    day = db.query(models.TradingDay).filter_by(cycle_id=cycle.id, date_str=date_str).first()
    if not day:
        raise HTTPException(404, "Day not found")

    rows = db.query(models.InternalTransfer).filter_by(day_id=day.id).all()
    edges = [{"from_id": r.from_participant_id, "to_id": r.to_participant_id, "amount_eur": str(r.amount_eur)} for r in rows]
    return {"date": date_str, "edges": edges}

# -------- Month Close: Payouts
@app.post("/v1/cycles/{cycle_label}/close", dependencies=[Depends(require_api_key)], response_model=schemas.SettlementOut)
def close_cycle(cycle_label: str, body: schemas.CloseCycleIn, db: Session = Depends(get_db)):
    cycle = logic.get_or_create_cycle(db, cycle_label)
    if cycle.status == "closed":
        raise HTTPException(400, "Already closed")

    open_days = db.query(models.TradingDay).filter_by(cycle_id=cycle.id, status="open").count()
    if open_days:
        raise HTTPException(400, f"{open_days} trading day(s) still open in {cycle_label}")

    pol = db.query(models.Policy).filter_by(version=body.policy_version).first()
    if not pol:
        raise HTTPException(404, "Policy not found")

    run = logic.run_monthly_settlement(db, cycle, pol, body.fixed_cost_eur, body.variable_cost_rate)
    cycle.status = "closed"; db.commit()

    payouts = db.query(models.PayoutInstruction).filter_by(run_id=run.id).all()
    return {
        "run_id": run.id,
        "cycle_label": cycle.label,
        "payouts": [
            {"participant_id": p.participant_id, "iban": p.iban, "amount_eur": str(p.amount_eur), "remittance_info": p.remittance_info}
            for p in payouts
        ],
        "totals": run.summary
    }

# -------- Statements
@app.get("/v1/cycles/{cycle_label}/statements/{participant_external_id}", dependencies=[Depends(require_api_key)], response_model=schemas.StatementOut)
def participant_statement(cycle_label: str, participant_external_id: str, db: Session = Depends(get_db)):
    cycle = db.query(models.BillingCycle).filter_by(label=cycle_label).first()
    if not cycle:
        raise HTTPException(404, "Cycle not found")
    part = db.query(models.Participant).filter_by(external_id=participant_external_id).first()
    if not part:
        raise HTTPException(404, "Participant not found")
    return logic.statement_for_participant(db, cycle, part)

# -------- Explain Trace Lookup (optional helper)
@app.get("/v1/explain/{participant_external_id}/{cycle_label}")
def get_explains(participant_external_id: str, cycle_label: str, db: Session = Depends(get_db)):
    cycle = db.query(models.BillingCycle).filter_by(label=cycle_label).first()
    if not cycle: raise HTTPException(404, "Cycle not found")
    part = db.query(models.Participant).filter_by(external_id=participant_external_id).first()
    if not part: raise HTTPException(404, "Participant not found")
    rows = db.query(models.ExplainTrace).filter_by(cycle_id=cycle.id, participant_id=part.id).order_by(models.ExplainTrace.id.desc()).limit(50).all()
    return [{
        "scope": r.scope, "key": r.key, "trace_hash": r.trace_hash, "trace": json.loads(r.trace_json)
    } for r in rows]
