from __future__ import annotations
import ast, json, hashlib
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple

# --- Safe Expression Evaluator (whitelist AST)
_ALLOWED_NAMES = set([
    "kwh","price_ct_per_kwh","feedin_ct_per_kwh","amount_eur_prev",
    "base_sum","percent","value","qty"
])
_ALLOWED_FUNCS = set(["min","max","round"])

class SafeEvaluator(ast.NodeVisitor):
    def __init__(self, variables: Dict[str, Decimal]):
        self.vars = variables

    def visit_Expression(self, node): return self.visit(node.body)

    def visit_BinOp(self, node):
        left = self.visit(node.left); right = self.visit(node.right)
        if isinstance(node.op, ast.Add): return left + right
        if isinstance(node.op, ast.Sub): return left - right
        if isinstance(node.op, ast.Mult): return left * right
        if isinstance(node.op, ast.Div): return (left / right) if right != 0 else Decimal("0")
        raise ValueError("Operator not allowed")

    def visit_UnaryOp(self, node):
        val = self.visit(node.operand)
        if isinstance(node.op, ast.USub): return -val
        if isinstance(node.op, ast.UAdd): return val
        raise ValueError("Unary op not allowed")

    def visit_Name(self, node):
        if node.id not in _ALLOWED_NAMES:
            raise ValueError(f"name '{node.id}' not allowed")
        return Decimal(str(self.vars.get(node.id, 0)))

    def visit_Call(self, node):
        if not isinstance(node.func, ast.Name) or node.func.id not in _ALLOWED_FUNCS:
            raise ValueError("function not allowed")
        args = [self.visit(a) for a in node.args]
        if node.func.id == "min": return min(args)
        if node.func.id == "max": return max(args)
        if node.func.id == "round":
            if len(args) == 1: return args[0].quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            if len(args) == 2:
                q = Decimal("1").scaleb(-int(args[1]))
                return args[0].quantize(q, rounding=ROUND_HALF_UP)
        raise ValueError("bad round args")

    def visit_Constant(self, node):
        if isinstance(node.value, (int,float,str)):
            try:
                return Decimal(str(node.value))
            except Exception:
                raise ValueError("constant not numeric")
        raise ValueError("constant type not allowed")

    def generic_visit(self, node):
        raise ValueError("expression not allowed")

def safe_eval(expr: str, variables: Dict[str, Any]) -> Decimal:
    tree = ast.parse(expr, mode="eval")
    ev = SafeEvaluator(variables)
    res = ev.visit(tree)
    if not isinstance(res, Decimal):
        res = Decimal(str(res))
    return res

# --- Policy Rule Dataclasses
@dataclass
class RuleOut:
    account: str
    sign: str  # '+' or '-'

@dataclass
class Rule:
    id: str
    kind: str
    applies_to: Dict[str, Any] | None
    depends_on: List[str]
    out: RuleOut
    params: Dict[str, Any] | None = None
    rate_expr: Optional[str] = None
    base_account: Optional[str] = None
    accounts: Optional[List[str]] = None
    beneficiary: Optional[Dict[str, str]] = None
    tiers: Optional[List[Dict[str, Any]]] = None

# --- Canonical hash
def canonical_hash(obj: dict) -> str:
    raw = json.dumps(obj, sort_keys=True, separators=(",",":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

# --- Engine
class PolicyEngine:
    """
    Event-Scope Evaluator.
    """
    def __init__(self, policy_data: dict):
        self.policy = policy_data
        self.rules: List[Rule] = []
        self._parse_policy()

    def _parse_policy(self):
        rules = self.policy.get("rules", [])
        for r in rules:
            out = r.get("out", {})
            self.rules.append(
                Rule(
                    id=r["id"],
                    kind=r["kind"],
                    applies_to=r.get("applies_to"),
                    depends_on=r.get("depends_on", []),
                    out=RuleOut(account=out["account"], sign=out.get("sign","-")),
                    params=r.get("params"),
                    rate_expr=r.get("rate_expr"),
                    base_account=r.get("base_account"),
                    accounts=r.get("accounts"),
                    beneficiary=r.get("beneficiary"),
                    tiers=r.get("tiers"),
                )
            )
        # Optional: topo sort by depends_on (simple stable pass)
        ordered = []
        added = set()
        def add_rule(rule: Rule):
            for dep in rule.depends_on:
                if dep not in added:
                    # add dep first
                    for rr in self.rules:
                        if rr.id == dep:
                            add_rule(rr)
            if rule.id not in added:
                ordered.append(rule); added.add(rule.id)
        for rr in self.rules:
            add_rule(rr)
        self.rules = ordered

    def applies(self, rule: Rule, event: dict, participant_role: Optional[str]) -> bool:
        if not rule.applies_to:
            return True
        src_ok = True; tags_ok = True; role_ok = True
        if "source" in rule.applies_to:
            src_ok = event.get("source") in rule.applies_to["source"]
        if "tags" in rule.applies_to:
            evtags = set(event.get("meta",{}).get("tags", []))
            tags_ok = bool(evtags.intersection(set(rule.applies_to["tags"])))
        if "role" in rule.applies_to:
            role_ok = (participant_role == rule.applies_to["role"])
        return src_ok and tags_ok and role_ok

    def _sum_accounts(self, accounts_state: Dict[str, Decimal], names: List[str]) -> Decimal:
        return sum((accounts_state.get(n, Decimal("0.00")) for n in names), Decimal("0.00"))

    def _eval_tiers(self, tiers: List[Dict[str, Any]], kwh: Decimal) -> Decimal:
        # Simple increasing block tariff
        remaining = Decimal(str(kwh))
        total_ct = Decimal("0.0")
        prev_cap = Decimal("0.0")
        for t in tiers:
            price = Decimal(str(t["price_ct_per_kwh"]))
            if t.get("above"):
                qty = remaining
            else:
                upto = Decimal(str(t["upto_kwh"]))
                block = max(Decimal("0"), min(remaining, upto - prev_cap))
                qty = block
                prev_cap = upto
            if qty <= 0: 
                continue
            total_ct += qty * price
            remaining -= qty
            if remaining <= 0:
                break
        # convert ct to EUR
        return (total_ct / Decimal("100")).quantize(Decimal("0.0001"))

    def evaluate_event(self, event: dict, participant_role: Optional[str], operator_participant_id: Optional[int]) -> Tuple[List[dict], dict]:
        """
        Returns (ledger_lines, explain_trace)
        Each ledger_line: { "rule_id", "account", "sign", "amount_eur", "beneficiary_participant_id" (optional) }
        """
        accounts: Dict[str, Decimal] = {}
        evals: List[dict] = []

        for rule in self.rules:
            if not self.applies(rule, event, participant_role):
                evals.append({"rule_id": rule.id, "matched": False})
                continue

            amount = Decimal("0.00")
            inputs = {}
            beneficiary_pid: Optional[int] = None

            if rule.kind == "rate":
                # e.g. kwh * price_ct_per_kwh / 100
                kwh = Decimal(str(event.get("meta",{}).get("kwh", 0)))
                inputs = {"kwh": kwh}
                if rule.params:
                    for k, v in rule.params.items():
                        inputs[k] = Decimal(str(v))
                amount = safe_eval(rule.rate_expr or "0", inputs)

            elif rule.kind == "tiered_cap":
                kwh = Decimal(str(event.get("meta",{}).get("kwh", 0)))
                amount = self._eval_tiers(rule.tiers or [], kwh)
                inputs = {"kwh": kwh}

            elif rule.kind == "percent_of_account":
                base = accounts.get(rule.base_account or "", Decimal("0.00"))
                pct = Decimal(str(rule.params["percent"] if rule.params and "percent" in rule.params else rule.__dict__.get("percent", 0) or 0))
                amount = (abs(base) * (pct / Decimal("100"))).quantize(Decimal("0.0001"))
                inputs = {"base_sum": base, "percent": pct}

            elif rule.kind == "percent_over_sum_accounts":
                base_sum = self._sum_accounts(accounts, rule.accounts or [])
                pct = Decimal(str(rule.params["percent"] if rule.params and "percent" in rule.params else rule.__dict__.get("percent", 0) or 0))
                amount = (abs(base_sum) * (pct / Decimal("100"))).quantize(Decimal("0.0001"))
                inputs = {"base_sum": base_sum, "percent": pct}

            else:
                # future kinds
                evals.append({"rule_id": rule.id, "matched": True, "result_eur": "0.00"})
                continue

            # set beneficiary (e.g., OPERATOR)
            beneficiary = None
            if rule.beneficiary and rule.beneficiary.get("role") == "OPERATOR":
                if operator_participant_id:
                    beneficiary_pid = operator_participant_id
                    beneficiary = "OPERATOR"

            # sign handling
            signed_amount = amount if rule.out.sign == "+" else -amount
            accounts[rule.out.account] = accounts.get(rule.out.account, Decimal("0.00")) + signed_amount

            evals.append({
                "rule_id": rule.id,
                "matched": True,
                "inputs": {k: (str(v) if isinstance(v, Decimal) else v) for k,v in inputs.items()},
                "formula": rule.rate_expr if rule.rate_expr else (rule.kind),
                "result_eur": str(signed_amount.quantize(Decimal("0.01"))),
                "beneficiary": beneficiary,
                "ledger_line_id": None
            })

        totals = {
            "per_account": {k: str(v.quantize(Decimal("0.01"))) for k,v in accounts.items()},
            "sum_event_eur": str(sum(accounts.values(), Decimal("0.00")).quantize(Decimal("0.01")))
        }
        trace = {"evaluations": evals, "totals": totals}
        return self._accounts_to_ledger(accounts, event, operator_participant_id), trace

    def _accounts_to_ledger(self, accounts: Dict[str, Decimal], event: dict, operator_pid: Optional[int]) -> List[dict]:
        """
        Map per-account deltas to concrete ledger postings.
        Positive value => credit to participant (or beneficiary if defined per-rule in eval step)
        We return postings grouped by account; beneficiary mapping is resolved by the calling site per eval item.
        """
        # Here we only return the raw account impacts; main will map to participants.
        return [{"account": k, "amount_eur": v} for k, v in accounts.items()]
