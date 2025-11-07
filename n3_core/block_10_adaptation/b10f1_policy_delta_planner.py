# Folder: noema/n3_core/block_10_adaptation
# File:   b10f1_policy_delta_planner.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b10f1_plan_policy_delta"]

RULES_VERSION = "1.1"
LEARNING_LABELS = [
    "direct_answer",
    "execute_action",
    "ask_clarification",
    "acknowledge_only",
    "small_talk",
    "closing",
    "refuse_or_safecheck",
    "other",
]
TRACE_CONSIDER = 12
LEARNING_RATE_BASE = 0.18
CONFIDENCE_DECAY = 0.4


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


def _sha1(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _deepcopy(obj: Any) -> Any:
    return json.loads(json.dumps(obj, ensure_ascii=False))


# ------------------------- learning collectors -------------------------

def _collect_learning(inp: Dict[str, Any]) -> Dict[str, Any]:
    pol = inp.get("policy", {}) if isinstance(inp.get("policy"), dict) else {}
    learning = pol.get("learning", {}) if isinstance(pol.get("learning"), dict) else {}
    weights = learning.get("weights") if isinstance(learning.get("weights"), dict) else {}
    version = learning.get("version") if isinstance(learning.get("version"), dict) else {}
    summary = learning.get("summary") if isinstance(learning.get("summary"), dict) else {}
    rollback = learning.get("rollback") if isinstance(learning.get("rollback"), dict) else {}

    base_weights = {label: float(weights.get(label, 0.5)) for label in LEARNING_LABELS}
    return {
        "weights": base_weights,
        "version": {
            "id": version.get("id"),
            "parent_id": version.get("parent_id"),
            "updated_at": version.get("updated_at"),
        },
        "summary": {
            "avg_reward": float(summary.get("avg_reward", 0.0)),
            "updates": int(summary.get("updates", 0)),
            "confidence": float(summary.get("confidence", 0.5)),
        },
        "rollback": {
            "version": rollback.get("version"),
            "weights": {label: float(rollback.get("weights", {}).get(label, 0.5)) for label in LEARNING_LABELS},
        },
    }


def _collect_trace(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    wm = inp.get("world_model", {}) if isinstance(inp.get("world_model"), dict) else {}
    trace = wm.get("trace", {}) if isinstance(wm.get("trace"), dict) else {}
    history = trace.get("error_history") if isinstance(trace.get("error_history"), list) else []
    out: List[Dict[str, Any]] = []
    for item in history[-TRACE_CONSIDER:]:
        if not isinstance(item, dict):
            continue
        reward = item.get("reward")
        if not isinstance(reward, (int, float)):
            continue
        entry = {
            "reward": float(reward),
            "target": item.get("target"),
            "actual": item.get("actual"),
            "top_pred": item.get("top_pred"),
            "l1": float(item.get("l1", 0.0)) if isinstance(item.get("l1"), (int, float)) else 0.0,
            "kl": float(item.get("kl", 0.0)) if isinstance(item.get("kl"), (int, float)) else 0.0,
            "speech_act": item.get("speech_act"),
        }
        out.append(entry)
    return out


def _collect_uncertainty(inp: Dict[str, Any]) -> float:
    return _as_float(_get(inp, ["world_model", "uncertainty", "score"], 0.0), 0.0)


# ------------------------- reinforcement learner -------------------------

def _reinforce(weights: Dict[str, float], trace: List[Dict[str, Any]], uncertainty: float) -> Tuple[Dict[str, float], Dict[str, Any]]:
    if not trace:
        return weights, {
            "avg_reward": 0.0,
            "updates": 0,
            "confidence": max(0.05, 1.0 - uncertainty),
            "delta_norm": 0.0,
        }

    lr = LEARNING_RATE_BASE * (1.0 - 0.5 * uncertainty)
    updated = _deepcopy(weights)
    total_reward = 0.0
    delta_norm = 0.0

    for item in trace:
        reward = float(item.get("reward", 0.0))
        total_reward += reward
        target = item.get("target") if isinstance(item.get("target"), str) else None
        actual = item.get("actual") if isinstance(item.get("actual"), str) else None
        top_pred = item.get("top_pred") if isinstance(item.get("top_pred"), str) else None

        if target in updated:
            delta = lr * reward
            updated[target] = _clip(updated[target] + delta, 0.0, 1.5)
            delta_norm += abs(delta)
        if actual in updated:
            delta = lr * (reward - 0.5)
            updated[actual] = _clip(updated[actual] + delta, 0.0, 1.5)
            delta_norm += abs(delta)
        if top_pred and top_pred in updated and target and top_pred != target:
            delta = -lr * (0.6 - reward)
            updated[top_pred] = _clip(updated[top_pred] + delta, 0.0, 1.5)
            delta_norm += abs(delta)

    avg_reward = total_reward / max(1, len(trace))
    conf = _clip(0.5 * (1.0 - uncertainty) + 0.5 * avg_reward, 0.05, 0.98)
    summary = {
        "avg_reward": round(avg_reward, 4),
        "updates": len(trace),
        "confidence": round(conf, 4),
        "delta_norm": round(delta_norm, 4),
    }
    return updated, summary


def _plan_learning_update(inp: Dict[str, Any]) -> Dict[str, Any]:
    learning = _collect_learning(inp)
    trace = _collect_trace(inp)
    uncertainty = _collect_uncertainty(inp)
    new_weights, summary = _reinforce(learning["weights"], trace, uncertainty)

    if not trace and learning["version"].get("id"):
        # no change
        out = {
            "version": learning["version"],
            "weights": learning["weights"],
            "rollback": learning["rollback"],
            "summary": summary,
            "delta": {},
        }
        return out

    now = _now_z()
    version_payload = {
        "parent_id": learning["version"].get("id"),
        "weights": new_weights,
        "summary": summary,
        "ts": now,
    }
    ver_id = _sha1(version_payload)
    delta = {
        label: round(new_weights[label] - learning["weights"][label], 6)
        for label in LEARNING_LABELS
        if abs(new_weights[label] - learning["weights"][label]) >= 1e-6
    }
    out = {
        "version": {"id": ver_id, "parent_id": learning["version"].get("id"), "updated_at": now},
        "weights": {label: round(new_weights[label], 6) for label in LEARNING_LABELS},
        "rollback": {
            "version": learning["version"].get("id"),
            "weights": {label: round(learning["weights"][label], 6) for label in LEARNING_LABELS},
        },
        "summary": summary,
        "delta": delta,
        "trace_used": len(trace),
    }
    return out


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
    trace = _get(inp, ["world_model", "trace", "error_history"], []) or []
    rewards: List[float] = []
    for item in trace[-12:]:
        if isinstance(item, dict) and isinstance(item.get("reward"), (int, float)):
            rewards.append(float(item["reward"]))
    reward_avg = sum(rewards) / len(rewards) if rewards else 0.0
    reward_trend = rewards[-3:] if rewards else []
    return {"uncertainty": u, "recommendation": rec or "", "reward_avg": reward_avg, "reward_recent": reward_trend}


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


def _suggest_from_world_model(wm: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    reward_avg = wm.get("reward_avg", 0.0)
    recent = wm.get("reward_recent", []) or []
    uncertainty = wm.get("uncertainty", 0.0)

    if reward_avg < 0.35:
        out.append(_mk_change("planner.learning.reward_bias", 0.15, "retune",
                              "Prediction reward low; encourage clarification bias.",
                              0.55))
        out.append(_mk_change("world_model.model.prototype_mix", 0.65, "retune",
                              "Blend prototypes more when reward is low.", 0.52, (0.3, 0.9)))
    elif reward_avg > 0.75 and uncertainty < 0.4:
        out.append(_mk_change("planner.learning.reward_bias", 0.05, "relax",
                              "High reward and low uncertainty; allow more direct answers.",
                              0.5, (0.0, 0.3)))

    if recent:
        trend = sum(1 if r >= 0.6 else -1 for r in recent)
        if trend < 0:
            out.append(_mk_change("dialog.surface.hedging", True, "set",
                                  "Recent reward dropping; enable hedging language.", 0.48))

    if uncertainty >= 0.7:
        out.append(_mk_change("guardrails.must_confirm.u_threshold", 0.38, "tighten",
                              "High uncertainty sustained; lower confirmation threshold.",
                              0.6, (0.3, 0.6)))

    return out


def _blend_learning(changes: List[Dict[str, Any]], learning_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not changes:
        return changes
    conf_scale = _clip(float(learning_summary.get("confidence", 0.5)), 0.05, 1.0)
    avg_reward = float(learning_summary.get("avg_reward", 0.0))
    for ch in changes:
        base_conf = float(ch.get("confidence", 0.5))
        adjusted = base_conf * (conf_scale ** CONFIDENCE_DECAY)
        if avg_reward < 0.3 and ch.get("change_type") == "relax":
            adjusted *= 0.7
        ch["confidence"] = round(_clip(adjusted, 0.05, 0.99), 3)
    return changes


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

    learning_update = _plan_learning_update(input_json)

    changes = _suggest_from_checks(checks or [], mets, wm)
    changes.extend(_suggest_from_world_model(wm))
    changes = _blend_learning(changes, learning_update.get("summary", {}))
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

    adaptation_summary = {
        "updates": learning_update.get("summary", {}).get("updates", 0),
        "avg_reward": learning_update.get("summary", {}).get("avg_reward", 0.0),
        "confidence": learning_update.get("summary", {}).get("confidence", 0.0),
        "delta_norm": learning_update.get("summary", {}).get("delta_norm", 0.0),
        "learning_version": learning_update.get("version", {}).get("id"),
    }

    return {
        "status": "OK",
        "policy": {"delta": delta, "learning": learning_update},
        "adaptation": {"policy": adaptation_summary},
        "diag": {
            "reason": "ok",
            "counts": {
                "changes": len(changes),
                "learning_updates": adaptation_summary["updates"],
                "learning_delta_keys": len(learning_update.get("delta", {})),
            },
        },
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
