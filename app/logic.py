from sqlalchemy.orm import Session
from sqlalchemy import select, func, and_
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timedelta, timezone
from . import models
import hashlib, json
import networkx as nx

def _to_cents(d: Decimal) -> int:
    return int((d * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def _from_cents(i: int) -> Decimal:
    return (Decimal(i) / Decimal(100)).quantize(Decimal("0.01"))

def get_or_create_cycle(db: Session, label: str) -> models.BillingCycle:
    cycle = db.scalar(select(models.BillingCycle).where(models.BillingCycle.label == label))
    if not cycle:
        cycle = models.BillingCycle(label=label, status="open")
        db.add(cycle); db.commit(); db.refresh(cycle)
    return cycle

def get_or_create_day(db: Session, label: str, date_str: str) -> models.TradingDay:
    cycle = get_or_create_cycle(db, label)
    day = db.scalar(select(models.TradingDay).where(
        models.TradingDay.cycle_id == cycle.id,
        models.TradingDay.date_str == date_str
    ))
    if not day:
        day = models.TradingDay(cycle_id=cycle.id, date_str=date_str, status="open")
        db.add(day); db.commit(); db.refresh(day)
    return day

def upsert_participant(db: Session, external_id: str, name: str, role: models.Role, iban: str | None, api_key_seed: str) -> models.Participant:
    p = db.scalar(select(models.Participant).where(models.Participant.external_id == external_id))
    if p:
        if name: p.name = name
        if role: p.role = role
        if iban: p.iban = iban
        db.commit(); db.refresh(p)
        return p
    api_key = hashlib.sha256((external_id + api_key_seed).encode()).hexdigest()[:32]
    p = models.Participant(external_id=external_id, name=name, role=role, iban=iban, api_key=api_key)
    db.add(p); db.commit(); db.refresh(p)
    return p

def add_ledger_entry(db: Session, cycle: models.BillingCycle, participant: models.Participant, amount: Decimal, source: str, meta: dict, event_ts: datetime | None):
    entry = models.LedgerEntry(
        cycle_id=cycle.id,
        participant_id=participant.id,
        amount_eur=amount,
        source=source,
        meta=meta or {},
        event_ts=event_ts or datetime.utcnow()
    )
    db.add(entry); db.commit(); db.refresh(entry)
    return entry

def _cycle_bounds_for_date(date_str: str) -> tuple[datetime, datetime]:
    # Assumes date_str "YYYY-MM-DD" in UTC-ish; adapt if you want Europe/Berlin logic.
    d = datetime.fromisoformat(date_str).date()
    start = datetime(d.year, d.month, d.day, 0, 0, 0)
    end = start + timedelta(days=1)
    return start, end

def compute_day_balances(db: Session, cycle: models.BillingCycle, date_str: str) -> dict[int, Decimal]:
    start, end = _cycle_bounds_for_date(date_str)
    rows = db.execute(
        select(models.LedgerEntry.participant_id, func.coalesce(func.sum(models.LedgerEntry.amount_eur), 0))
        .where(
            models.LedgerEntry.cycle_id == cycle.id,
            models.LedgerEntry.event_ts >= start,
            models.LedgerEntry.event_ts < end
        )
        .group_by(models.LedgerEntry.participant_id)
    ).all()
    return {pid: Decimal(str(total)).quantize(Decimal("0.01")) for pid, total in rows}

def compute_month_balances_from_daynets(db: Session, cycle: models.BillingCycle) -> dict[int, Decimal]:
    rows = db.execute(
        select(models.DayNet.participant_id, func.coalesce(func.sum(models.DayNet.net_eur), 0))
        .where(models.DayNet.day_id.in_(
            select(models.TradingDay.id).where(models.TradingDay.cycle_id == cycle.id)
        ))
        .group_by(models.DayNet.participant_id)
    ).all()
    return {pid: Decimal(str(total)).quantize(Decimal("0.01")) for pid, total in rows}

def _apply_operator_fee(balances: dict[int, Decimal], operator_id: int | None, pct: float):
    if not operator_id or not pct:
        return balances
    from decimal import Decimal as D
    updated = balances.copy()
    for pid, bal in list(updated.items()):
        if pid == operator_id:
            continue
        if bal > 0:
            fee = (bal * D(pct)).quantize(D("0.01"))
            updated[pid] = bal - fee
            updated[operator_id] = updated.get(operator_id, D("0.00")) + fee
    return updated

def _balances_to_graph(balances: dict[int, Decimal], fixed_cost: Decimal, variable_cost_rate: Decimal):
    G = nx.DiGraph()
    s, t = "SRC", "SINK"
    G.add_node(s, demand=0); G.add_node(t, demand=0)

    debtors = {pid: -bal for pid, bal in balances.items() if bal < 0}
    creditors = {pid: bal for pid, bal in balances.items() if bal > 0}

    total_debt = sum(_to_cents(v) for v in debtors.values())
    total_credit = sum(_to_cents(v) for v in creditors.values())
    if abs(total_debt - total_credit) > 1:
        raise ValueError("Imbalance in totals (rounding)")

    G.nodes[s]['demand'] = -total_debt
    G.nodes[t]['demand'] = total_debt

    for pid, amt in debtors.items():
        dn = f"D_{pid}"
        G.add_node(dn, demand=0)
        G.add_edge(s, dn, capacity=_to_cents(amt), weight=0)

    for pid, amt in creditors.items():
        cn = f"C_{pid}"
        G.add_node(cn, demand=0)
        G.add_edge(cn, t, capacity=_to_cents(amt), weight=0)

    fc = _to_cents(fixed_cost)
    # variable_cost_rate = fee per 1 EUR (e.g. 0.0035). We apply it per cent with scaling.
    vc_scaled = max(1, int(variable_cost_rate * 100000))  # integer weight per cent
    for dpid in debtors.keys():
        dn = f"D_{dpid}"
        for cpid in creditors.keys():
            cn = f"C_{cpid}"
            G.add_edge(dn, f"ACT_{dpid}_{cpid}", capacity=1, weight=fc)
            G.add_edge(f"ACT_{dpid}_{cpid}", cn, capacity=10**12, weight=vc_scaled)
    return G

def optimize_edges(balances: dict[int, Decimal], fixed_cost: Decimal, variable_cost_rate: Decimal):
    if not balances:
        return []
    G = _balances_to_graph(balances, fixed_cost, variable_cost_rate)
    _, flow = nx.network_simplex(G)
    edges = []
    for u, vdict in flow.items():
        if not u.startswith("ACT_"):  # we only care about ACT_* -> C_*
            continue
        _, dpid, cpid = u.split("_")
        dpid = int(dpid); cpid = int(cpid)
        cents = vdict.get(f"C_{cpid}", 0)
        if cents > 0:
            edges.append((dpid, cpid, _from_cents(cents)))
    return edges

def merkleish_hash(items: list[dict]) -> str:
    blob = json.dumps(sorted(items, key=lambda x: (x["from_id"], x["to_id"], x["amount_eur"])), separators=(",",":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def close_trading_day(db: Session, cycle: models.BillingCycle, date_str: str,
                      policy: models.Policy, fixed_cost: Decimal, variable_cost: Decimal) -> tuple[models.TradingDay, list[dict], str]:
    # 1) ensure day
    day = get_or_create_day(db, cycle.label, date_str)
    if day.status == "closed":
        # already closed → just read back
        nets = db.execute(
            select(models.DayNet.participant_id, models.DayNet.net_eur).where(models.DayNet.day_id == day.id)
        ).all()
        items = [{"participant_id": pid, "net_eur": str(Decimal(str(n)).quantize(Decimal("0.01")))} for pid, n in nets]
        edges = db.execute(
            select(models.InternalTransfer.from_participant_id, models.InternalTransfer.to_participant_id, models.InternalTransfer.amount_eur)
            .where(models.InternalTransfer.day_id == day.id)
        ).all()
        edges_out = [{"from_id": a, "to_id": b, "amount_eur": str(Decimal(str(v)).quantize(Decimal("0.01")))} for a,b,v in edges]
        return day, items, merkleish_hash(edges_out)

    # 2) day balances from ledger
    balances = compute_day_balances(db, cycle, date_str)

    # 3) apply operator fee (optional)
    operator = db.scalar(select(models.Participant).where(models.Participant.role == models.Role.OPERATOR))
    op_fee = float(policy.data.get("operator_fee_pct", 0.0) or 0.0)
    balances = _apply_operator_fee(balances, operator.id if operator else None, op_fee)

    # 4) persist DayNet per participant
    nets_out = []
    for pid, bal in balances.items():
        dn = models.DayNet(day_id=day.id, participant_id=pid, net_eur=bal)
        db.add(dn); nets_out.append({"participant_id": pid, "net_eur": str(bal)})
    db.commit()

    # 5) compute internal min-cost edges (for transparency / „gelevelt“ matrix)
    edges = optimize_edges(balances, fixed_cost, variable_cost)
    edge_rows = []
    for deb, cred, amt in edges:
        row = models.InternalTransfer(day_id=day.id, from_participant_id=deb, to_participant_id=cred, amount_eur=amt, meta={})
        db.add(row)
        edge_rows.append({"from_id": deb, "to_id": cred, "amount_eur": str(amt)})
    db.commit()

    # 6) mark day closed
    day.status = "closed"; db.commit()

    return day, nets_out, merkleish_hash(edge_rows)

def run_monthly_settlement(db: Session, cycle: models.BillingCycle, policy: models.Policy,
                           fixed_cost: Decimal, variable_cost: Decimal) -> models.SettlementRun:
    balances = compute_month_balances_from_daynets(db, cycle)
    operator = db.scalar(select(models.Participant).where(models.Participant.role == models.Role.OPERATOR))
    op_fee = float(policy.data.get("operator_fee_pct", 0.0) or 0.0)
    balances = _apply_operator_fee(balances, operator.id if operator else None, op_fee)

    # minimize payout edges for the month (external payments)
    edges = optimize_edges(balances, fixed_cost, variable_cost)

    run = models.SettlementRun(cycle_id=cycle.id, policy_version=policy.version, summary={})
    db.add(run); db.commit(); db.refresh(run)

    payouts = []
    for _, cred, amt in edges:
        cred_p = db.get(models.Participant, cred)
        db.add(models.PayoutInstruction(
            run_id=run.id,
            participant_id=cred,
            iban=cred_p.iban or "",
            amount_eur=amt,
            remittance_info=f"Settlement {cycle.label}",
            meta={}
        ))
        payouts.append({"to_id": cred, "amount_eur": str(amt)})
    db.commit()

    run.summary = {
        "participants": len(balances),
        "payout_count": len(payouts),
        "audit_hash": merkleish_hash(payouts),
        "cost_model": {
            "fixed_cost_eur": str(fixed_cost),
            "variable_cost_rate": str(variable_cost)
        }
    }
    db.commit()
    return run

def statement_for_participant(db: Session, cycle: models.BillingCycle, participant: models.Participant) -> dict:
    # Statement aus DayNets (monatsaggregiert, transparent)
    rows = db.execute(
        select(func.coalesce(func.sum(models.DayNet.net_eur), 0))
        .where(
            models.DayNet.participant_id == participant.id,
            models.DayNet.day_id.in_(select(models.TradingDay.id).where(models.TradingDay.cycle_id == cycle.id))
        )
    ).all()
    total = Decimal(str(rows[0][0] if rows else "0")).quantize(Decimal("0.01"))

    # optionaler Breakdown: pro Quelle (aus Ledger), nur informativ
    lines = db.execute(
        select(models.LedgerEntry.source, func.sum(models.LedgerEntry.amount_eur))
        .where(models.LedgerEntry.cycle_id == cycle.id, models.LedgerEntry.participant_id == participant.id)
        .group_by(models.LedgerEntry.source)
    ).all()

    return {
        "participant_external_id": participant.external_id,
        "cycle_label": cycle.label,
        "lines": [{"source": s, "amount_eur": str(Decimal(str(v)).quantize(Decimal("0.01")))} for s, v in lines],
        "total": total,
        "explanation": "Summe der täglichen Nettos (EoD-Leveling). Positive Werte = Auszahlung am Monatsende, negative = Rechnung."
    }
