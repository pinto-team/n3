# Folder: noema/n3_core/block_10_adaptation
# File:   b10f1_policy_delta_planner.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b10f1_plan_policy_delta"]

RULES_VERSION = "1.0"


# ------------------------- utils -------------------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _as_float(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _as_int(x: Any, default: int = 0) -> int:
    return int(x) if isinstance(x, (int, float)) else default


def _clip(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


# ------------------------- collectors -------------------------

def _collect_metrics(inp: Dict[str, Any]) -> Dict[str, float]:
    mets = _get(inp, ["observability", "telemetry", "metrics"], []) or []
    # pick last occurrence per name
    last: Dict[str, float] = {}
    for m in mets:
        if not isinstance(m, dict):
            continue
        n = m.get("name")
        if not isinstance(n, str):
            continue
        last[n] = _as_float(m.get("value"), last.get(n, 0.0))
    # fallbacks from executor aggregate
    agg = _get(inp, ["executor", "results", "aggregate"], {}) or {}
    last.setdefault("exec_total_cost", _as_float(agg.get("total_cost"), 0.0))
    last.setdefault("exec_avg_latency_ms", _as_float(agg.get("avg_latency_ms"), 0.0))
    last.setdefault("exec_items", _as_int(agg.get("count"), 0))
    return last


def _collect_slo(inp: Dict[str, Any]) -> Tuple[Optional[float], List[Dict[str, Any]]]:
    slo = _get(inp, ["observability", "slo"], {}) or {}
    score = slo.get("score")
    checks = slo.get("checks") if isinstance(slo.get("checks"), list) else []
    return (float(score) if isinstance(score, (int, float)) else None), [c for c in checks if isinstance(c, dict)]


def _collect_wm(inp: Dict[str, Any]) -> Dict[str, Any]:
    u = _as_float(_get(inp, ["world_model", "uncertainty", "score"], 0.0))
    rec = _get(inp, ["world_model", "uncertainty", "recommendation"], "")
    return {"uncertainty": u, "recommendation": rec or ""}


# ------------------------- delta logic -------------------------

def _mk_change(path: str, new_value: Any, change_type: str, rationale: str, confidence: float,
               bounds: Optional[Tuple[float, float]] = None) -> Dict[str, Any]:
    ch = {
        "path": path,  # dotted path (policy/config)
        "new_value": new_value,  # absolute target value
        "change_type": change_type,  # tighten | relax | retune | set
        "rationale": rationale,
        "confidence": round(_clip(confidence, 0.0, 1.0), 3),
    }
    if bounds:
        ch["bounds"] = {"min": bounds[0], "max": bounds[1]}
    return ch


def _suggest_from_checks(checks: List[Dict[str, Any]], mets: Dict[str, float], wm: Dict[str, Any]) -> List[
    Dict[str, Any]]:
    out: List[Dict[str, Any]] = []

    # Defaults/budgets used elsewhere in Noema (proposed targets)
    # These are not applied here; only proposed as deltas.
    BUDGETS = {
        "dialog.max_len": 800,  # aligns with B6F2 MAX_LEN
        "safety.max_out_len": 1200,  # aligns with B6F3 MAX_OUT_LEN
        "exec.avg_latency_ms": 1500,  # SLO default
        "exec.total_cost_usd": 0.01,  # per turn
        "guardrails.must_confirm_u_thresh": 0.4,  # used by B5F2/B5F3 heuristics
        "executor.request_timeout_ms": 30000,  # aligns with B7F1
        "execution.retries.max": 2,
    }

    # Map failing checks to deltas
    for c in checks:
        name = c.get("name", "")
        ok = bool(c.get("ok", False))
        val = _as_float(c.get("value"), 0.0)
        thr = _as_float(c.get("threshold"), 0.0)
        weight = _as_float(c.get("weight"), 0.0)
        score = _as_float(_get(c, ["details", "score"], 0.0), 0.0)

        if ok:
            continue

        if name == "answer.length":
            # tighten dialog.max_len and safety.max_out_len by a small factor
            factor = 0.9 if val > thr else 1.0
            new_dialog = max(400, int(BUDGETS["dialog.max_len"] * factor))
            new_safety = max(600, int(BUDGETS["safety.max_out_len"] * factor))
            conf = 0.55 + 0.2 * (1.0 - score)
            out.append(
                _mk_change("dialog.surface.max_len", new_dialog, "tighten", "Answer length exceeded budget.", conf,
                           (400, 2000)))
            out.append(_mk_change("safety_filter.max_out_len", new_safety, "tighten",
                                  "Safety cap should align with dialog max.", conf, (600, 4000)))

        elif name == "execution.latency_ms":
            # reduce timeouts a bit and allow 1 extra retry only if errors are low
            cur_timeout = BUDGETS["executor.request_timeout_ms"]
            new_timeout = max(8000, int(cur_timeout * 0.9))
            conf = 0.6 + 0.25 * (1.0 - score)
            out.append(_mk_change("executor.timeout_ms", new_timeout, "tighten",
                                  "High average latency; reduce timeout to fail fast.", conf, (8000, 60000)))

        elif name == "execution.error_rate":
            # increase retries if latency is within budget; else keep retries same
            latency = mets.get("exec_avg_latency_ms", 0.0)
            base_retries = BUDGETS["execution.retries.max"]
            if latency <= 1.05 * BUDGETS["exec.avg_latency_ms"]:
                new_retries = min(4, base_retries + 1)
                out.append(_mk_change("executor.retries.max", new_retries, "relax",
                                      "Error rate high with acceptable latency; allow one more retry.", 0.58, (0, 6)))
            else:
                # tighten: reduce parallelism knob (advisory)
                out.append(_mk_change("executor.parallelism.max_inflight", 2, "tighten",
                                      "Error rate and latency both high; limit inflight ops.", 0.52, (1, 8)))

        elif name == "execution.cost_usd":
            # lower cost budget target and prefer cheaper skills/models (advisory knobs)
            new_budget = max(0.002, round(BUDGETS["exec.total_cost_usd"] * 0.85, 4))
            out.append(_mk_change("budget.exec_total_cost_max", new_budget, "tighten",
                                  "Total execution cost over budget; lower per-turn budget.", 0.62, (0.002, 0.05)))
            out.append(_mk_change("planner.skill_selection.cost_bias", 0.15, "retune",
                                  "Favor cheaper skills/models under cost pressure.", 0.55, (0.0, 0.5)))

        elif name == "storage.wal_ops":
            # increase batching in persistence layer
            out.append(_mk_change("persistence.batch.max_ops", 50, "retune", "High WAL ops; increase batch size.", 0.57,
                                  (20, 200)))
            out.append(_mk_change("persistence.batch.max_interval_ms", 120, "retune",
                                  "Batch more aggressively to reduce WAL count.", 0.55, (50, 500)))

        elif name == "index.queue_items":
            # throttle enqueue rate or boost indexer workers
            out.append(
                _mk_change("index.enqueue.rate_limit_per_s", 30, "tighten", "Large index queue; rate-limit enqueue.",
                           0.54, (10, 200)))
            out.append(
                _mk_change("index.workers.min_parallel", 2, "relax", "Increase indexers to drain queue faster.", 0.56,
                           (1, 32)))

        elif name == "guardrails.must_confirm_adhered":
            # raise must-confirm sensitivity by lowering uncertainty threshold
            u = wm.get("uncertainty", 0.0)
            # propose lower threshold if uncertainty tends to be high
            new_thresh = 0.35 if u >= 0.45 else 0.4
            out.append(_mk_change("guardrails.must_confirm.u_threshold", new_thresh, "tighten",
                                  "Must-confirm was not adhered; be more conservative.", 0.64, (0.25, 0.7)))

        else:
            # generic advisory
            out.append(
                _mk_change(f"advice.{name}", True, "set", "Generic failed SLO check; manual review advised.", 0.4))

    # If SLO score is very good, propose gentle relaxations (bounded)
    # Note: this is optional and conservative.
    return out


def _cap_changes(changes: List[Dict[str, Any]], max_per_turn: int = 8) -> List[Dict[str, Any]]:
    # Keep most important first: tighten > retune > relax > set
    order = {"tighten": 0, "retune": 1, "relax": 2, "set": 3}
    changes.sort(key=lambda c: (order.get(c.get("change_type", "retune"), 1), -c.get("confidence", 0.0)))
    return changes[:max_per_turn]


# ------------------------- main -------------------------

def b10f1_plan_policy_delta(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B10F1 — Adaptation.PolicyDeltaPlanner (Noema)

    Input:
      {
        "observability": {
          "telemetry": { "metrics": [ {name, value, labels{}, ts}, ... ] }?,
          "slo": { "score": float, "checks": [ {name, ok, value, threshold, weight, details{score}}, ... ] }?
        },
        "executor": { "results": { "aggregate": {avg_latency_ms, total_cost, count, ok, errors} } }?,
        "world_model": { "uncertainty": { "score": float, "recommendation": str } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "policy": {
          "delta": {
            "changes": [ {path, new_value, change_type, rationale, confidence, bounds?}, ... ],
            "guards": {
              "max_changes": int,
              "ttl": { "seconds": int },           # validity of these suggestions
              "applies_safely": True
            },
            "meta": { "source": "B10F1", "rules_version": "1.0", "created_at": str }
          }
        },
        "diag": { "reason": "ok|no_signal", "counts": { "changes": int } }
      }
    """
    mets = _collect_metrics(input_json)
    slo_score, checks = _collect_slo(input_json)
    wm = _collect_wm(input_json)

    if not mets and not checks and slo_score is None:
        return {
            "status": "SKIP",
            "policy": {
                "delta": {"changes": [], "guards": {"max_changes": 0, "ttl": {"seconds": 0}, "applies_safely": True},
                          "meta": {"source": "B10F1", "rules_version": RULES_VERSION, "created_at": _now_z()}}},
            "diag": {"reason": "no_signal", "counts": {"changes": 0}},
        }

    changes = _suggest_from_checks(checks or [], mets, wm)
    changes = _cap_changes(changes, max_per_turn=8)

    delta = {
        "changes": changes,
        "guards": {
            "max_changes": len(changes),
            "ttl": {"seconds": 1800},  # suggestions are valid for 30 minutes unless refreshed
            "applies_safely": True  # only proposes changes; does not mutate live config
        },
        "meta": {"source": "B10F1", "rules_version": RULES_VERSION, "created_at": _now_z()},
    }

    return {
        "status": "OK",
        "policy": {"delta": delta},
        "diag": {"reason": "ok", "counts": {"changes": len(changes)}},
    }


if __name__ == "__main__":
    # Minimal demo
    sample = {
        "observability": {
            "telemetry": {
                "metrics": [
                    {"name": "exec_avg_latency_ms", "value": 1800},
                    {"name": "exec_total_cost", "value": 0.013},
                    {"name": "dialog_out_length", "value": 1250}
                ]
            },
            "slo": {
                "score": 0.61,
                "checks": [
                    {"name": "execution.latency_ms", "ok": False, "value": 1800, "threshold": 1500, "weight": 0.22,
                     "details": {"score": 0.3}},
                    {"name": "execution.cost_usd", "ok": False, "value": 0.013, "threshold": 0.01, "weight": 0.18,
                     "details": {"score": 0.4}},
                    {"name": "answer.length", "ok": False, "value": 1250, "threshold": 900, "weight": 0.12,
                     "details": {"score": 0.5}}
                ]
            }
        },
        "world_model": {"uncertainty": {"score": 0.48, "recommendation": "answer_or_probe"}}
    }
    out = b10f1_plan_policy_delta(sample)
    print(out["diag"])
    for ch in out["policy"]["delta"]["changes"]:
        print(ch["change_type"], ch["path"], ch["new_value"], ch["confidence"])
