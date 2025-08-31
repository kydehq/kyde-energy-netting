from sqlalchemy.orm import Session
from sqlalchemy import select, func
from decimal import Decimal, ROUND_HALF_UP
from . import models
import hashlib, json
import networkx as nx

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
    # Normalize to cents for integer flow
    return {pid: Decimal(str(total)).quantize(Decimal("0.01")) for pid, total in rows}

def _balances_to_graph(balances: dict[int, Decimal], fixed_cost: Decimal, variable_cost: Decimal):
    """
    Build a min-cost flow graph:
    - Create source -> debtors (supply = abs(negative))
    - Creditors -> sink (demand = abs(positive))
    - Fully connected edges debtors -> creditors with cost = fixed_cost + variable_cost*amount.
      We model fixed cost by splitting into:
        - a small 'activate edge' arc with capacity 1 and cost = fixed_cost_in_cents
        - the main amount arc with cost = variable_cost per cent
    """
    G = nx.DiGraph()

    # Work in cents to keep integer linear program exact
    def eur_to_cents(d: Decimal) -> int:
        return int((d * 100).quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    s, t = "SRC", "SINK"
    G.add_node(s, demand=0)
    G.add_node(t, demand=0)

    debtors = {pid: -bal for pid, bal in balances.items() if bal < 0}
    creditors = {pid: bal for pid, bal in balances.items() if bal > 0}

    total_debt = sum(eur_to_cents(v) for v in debtors.values())
    total_credit = sum(eur_to_cents(v) for v in creditors.values())
    # Sanity: allow tiny rounding mismatch
    if abs(total_debt - total_credit) > 1:
        raise ValueError("Imbalance in totals (check rounding)")

    # Demands: network_simplex uses node 'demand': negative = supply, positive = demand
    # Set SRC supply = total_debt, SINK demand = total_debt
    G.nodes[s]['demand'] = -total_debt
    G.nodes[t]['demand'] = total_debt

    # Add intermediate nodes for participants
    for pid, amt in debtors.items():
        cents = eur_to_cents(amt)
        dn = f"D_{pid}"
        G.add_node(dn, demand=0)
        # source -> debtor
        G.add_edge(s, dn, capacity=cents, weight=0)

    for pid, amt in creditors.items():
        cents = eur_to_cents(amt)
        cn = f"C_{pid}"
        G.add_node(cn, demand=0)
        # creditor -> sink
        G.add_edge(cn, t, capacity=cents, weight=0)

    # Costs in integer per cent
    fc = eur_to_cents(fixed_cost)          # cost to activate an edge (once)
    vc_per_cent = int(variable_cost * 1)   # cost per cent moved; variable_cost is in "cost per 1 EUR" → convert:
    # If variable_cost is a rate (e.g. 0.0035 of amount), then per-cent cost ~ variable_cost/100 of a euro.
    # To avoid floats, we approximate: cost per cent (1¢) = round(variable_cost) in "cost units".
    # For small rates this may be zero; use a scaled integer:
    if vc_per_cent == 0:
        vc_scale = 100000  # scale up
        vc_per_cent = int(variable_cost * vc_scale)
        cost_scale = vc_scale
    else:
        cost_scale = 1

    # Build bipartite arcs
    for dpid in debtors.keys():
        dn = f"D_{dpid}"
        for cpid in creditors.keys():
            cn = f"C_{cpid}"
            # Activation arc: 1 cent capacity with fixed cost (to pay once if we use this edge)
            G.add_edge(dn, f"ACT_{dpid}_{cpid}", capacity=1, weight=fc)
            # Main flow arc to creditor with per-cent variable cost
            G.add_edge(f"ACT_{dpid}_{cpid}", cn, capacity=10**12, weight=vc_per_cent)

    return G, cost_scale

def optimize_settlement(balances: dict[int, Decimal], fixed_cost: Decimal, variable_cost_rate: Decimal):
    """
    Returns list of (debtor_id, creditor_id, amount_eur) minimizing total cost.
    - fixed_cost: EUR per transaction (booked payout)
    - variable_cost_rate: fraction per EUR (e.g. Decimal('0.0035') for 0.35%)
    """
    if not balances:
        return []

    # Build graph & run min-cost flow
    G, scale = _balances_to_graph(balances, fixed_cost, variable_cost_rate)
    flow_cost, flow_dict = nx.network_simplex(G)

    # Extract flows D_* -> ACT_* -> C_* to reconstruct payer->payee edges
    payouts_cents = {}
    for u, vdict in flow_dict.items():
        if not u.startswith("ACT_"):
            continue
        for v, f in vdict.items():
            if f <= 0 or not v.startswith("C_"):
                continue
            # Edge ACT_d_c -> C_c has flow "f" (= cents moved after 1c activation)
            _, dpid, cpid = u.split("_")  # "ACT_{dpid}_{cpid}"
            dpid = int(dpid); cpid = int(cpid)
            payouts_cents[(dpid, cpid)] = payouts_cents.get((dpid, cpid), 0) + f

    # Convert to EUR (round to 0.01)
    result = []
    for (deb, cred), cents in payouts_cents.items():
        if cents <= 0:
            continue
        eur = (Decimal(cents) / Decimal(100)).quantize(Decimal("0.01"))
        if eur > 0:
            result.append((deb, cred, eur))
    return result

def merkleish_hash(items: list[dict]) -> str:
    """Deterministischer Audit-Hash über die Payouts."""
    blob = json.dumps(sorted(items, key=lambda x: (x["from_id"], x["to_id"], x["amount_eur"])), separators=(",",":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def run_settlement_optimized(db: Session, cycle: models.BillingCycle, policy: models.Policy,
                             fixed_cost_eur: Decimal, variable_cost_rate: Decimal) -> models.SettlementRun:
    balances = compute_balances(db, cycle)

    # Optional: Operator-Fee anwenden (wie vorher), bevor wir optimieren
    operator = db.scalar(select(models.Participant).where(models.Participant.role == models.Role.OPERATOR))
    operator_fee_pct = (policy.data.get("operator_fee_pct", 0.0) or 0.0)
    if operator and operator_fee_pct:
        for pid, bal in list(balances.items()):
            if pid == operator.id:
                continue
            if bal > 0:
                fee = (bal * Decimal(operator_fee_pct)).quantize(Decimal("0.01"))
                balances[pid] = bal - fee
                balances[operator.id] = balances.get(operator.id, Decimal("0.00")) + fee

    # Optimize payouts (debtor -> creditor)
    edges = optimize_settlement(balances, fixed_cost_eur, variable_cost_rate)

    # Create run
    run = models.SettlementRun(cycle_id=cycle.id, policy_version=policy.version, summary={})
    db.add(run); db.commit(); db.refresh(run)

    # Persist payouts (positive flow to creditors)
    payouts = []
    for debtor_id, creditor_id, amount in edges:
        cred = db.get(models.Participant, creditor_id)
        db.add(models.PayoutInstruction(
            run_id=run.id,
            participant_id=creditor_id,
            iban=cred.iban or "",
            amount_eur=amount,
            remittance_info=f"Settlement {cycle.label}",
            meta={"from_id": debtor_id}
        ))
        payouts.append({"from_id": debtor_id, "to_id": creditor_id, "amount_eur": str(amount)})

    db.commit()

    # Add summary with audit hash
    run.summary = {
        "participants": len(balances),
        "payout_count": len(payouts),
        "audit_hash": merkleish_hash(payouts),
        "cost_model": {
            "fixed_cost_eur": str(fixed_cost_eur),
            "variable_cost_rate": str(variable_cost_rate)
        }
    }
    db.commit()
    return run

def statement_for_participant(db: Session, cycle: models.BillingCycle, participant: models.Participant) -> dict:
    rows = db.execute(
        select(models.LedgerEntry.source, func.sum(models.LedgerEntry.amount_eur))
        .where(models.LedgerEntry.cycle_id == cycle.id, models.LedgerEntry.participant_id == participant.id)
        .group_by(models.LedgerEntry.source)
    ).all()
    total = sum((Decimal(str(v)) for _, v in rows), Decimal("0.00"))
    return {
        "participant_external_id": participant.external_id,
        "cycle_label": cycle.label,
        "lines": [{"source": s, "amount_eur": str(Decimal(str(v)).quantize(Decimal("0.01")))} for s, v in rows],
        "total": str(total.quantize(Decimal("0.01"))),
        "explanation": "Summe aus Messwerten/Fees/Steuern gemäß aktiver Policy; positive Werte = Auszahlung, negative = Rechnung."
    }
