# Folder: noema/n3_core/block_12_orchestration
# File:   b12f1_orchestrator_tick.py

from __future__ import annotations

from typing import Any, Dict, List, Tuple, Optional

import unicodedata

__all__ = ["b12f1_orchestrate"]

RULES_VERSION = "1.0"
MAX_EMIT_LEN = 1200
MAX_REQS = 24
MAX_APPLY_OPS = 5000
MAX_INDEX_ITEMS = 2000


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


def _clip_text(s: Optional[str], n: int = MAX_EMIT_LEN) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _first_route(schedule: Dict[str, Any], rtype: str) -> Optional[Dict[str, Any]]:
    routes = schedule.get("routes") if isinstance(schedule.get("routes"), list) else []
    for r in routes:
        if isinstance(r, dict) and r.get("type") == rtype:
            return r
    return None


# ------------------------- core -------------------------

def _compose_actions(inp: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[str]]:
    gates = _get(inp, ["runtime", "gates"], {}) or {}
    schedule = _get(inp, ["runtime", "schedule"], {}) or {}
    reasons = list(_get(inp, ["runtime", "reasons"], [])) or []
    actions: List[Dict[str, Any]] = []

    # Delay/throttle (if any)
    delay_ms = int(schedule.get("delay_ms", 0)) if isinstance(schedule.get("delay_ms"), (int, float)) else 0
    if delay_ms > 0:
        actions.append({"type": "delay", "ms": delay_ms})

    action = schedule.get("action") if isinstance(schedule.get("action"), str) else ""

    # 1) Emit (answer/confirm)
    if action in {"answer", "confirm"}:
        r = _first_route(schedule, action)
        text = _clip_text(_get(r or {}, ["text"], "") or _get(inp, ["dialog", "final", "text"], ""))
        move = action
        if text:
            actions.append({"type": "emit", "move": move, "text": text})
        else:
            reasons.append("emit_without_text")

    # 2) Execute batch (if scheduled)
    if action == "execute":
        r = _first_route(schedule, "execute") or {}
        run = [x for x in (r.get("run") or []) if isinstance(x, dict)]
        defer = [str(x) for x in (r.get("defer") or [])]
        limits = r.get("limits") if isinstance(r.get("limits"), dict) else {
            "timeout_ms": _get(gates, ["limits", "timeout_ms"], 30000),
            "max_inflight": _get(gates, ["limits", "max_inflight"], 4)}
        if run:
            actions.append({
                "type": "execute",
                "requests": run[:MAX_REQS],
                "limits": {"timeout_ms": int(limits.get("timeout_ms", 30000)),
                           "max_inflight": int(limits.get("max_inflight", 4))},
                "defer": defer
            })
        else:
            reasons.append("execute_without_run")

    # 3) Persist (if optimized apply/queue exist)
    apply_ops = _get(inp, ["storage", "apply_optimized", "ops"], []) or _get(inp, ["storage", "apply", "ops"], []) or []
    index_items = _get(inp, ["index", "queue_optimized", "items"], []) or _get(inp, ["index", "queue", "items"],
                                                                               []) or []
    if apply_ops or index_items:
        actions.append({
            "type": "persist",
            "apply_ops": [op for op in apply_ops if isinstance(op, dict)][:MAX_APPLY_OPS],
            "index_items": [it for it in index_items if isinstance(it, dict)][:MAX_INDEX_ITEMS],
        })

    # 4) If nothing selected, noop
    if not actions:
        actions.append({"type": "noop"})

    return actions, reasons


# ------------------------- main -------------------------

def b12f1_orchestrate(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B12F1 — Orchestration.Tick (Noema)

    Input (best-effort):
      {
        "runtime": { "gates": {...}, "schedule": {action, delay_ms, routes[]}, "reasons": [str]? },
        "dialog":  { "final": { "move": "answer|confirm|...", "text": str } }?,
        "executor":{ "requests": [ ... ] }?,
        "storage": { "apply_optimized": { "ops": [...] } | "apply": { "ops": [...] } }?,
        "index":   { "queue_optimized": { "items": [...] } | "queue": { "items": [...] } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "engine": {
          "actions": [
            { "type":"delay",   "ms": int } |
            { "type":"emit",    "move":"answer|confirm", "text": str } |
            { "type":"execute", "requests":[...], "limits": {"timeout_ms":int,"max_inflight":int}, "defer":[req_id...] } |
            { "type":"persist", "apply_ops":[...], "index_items":[...] } |
            { "type":"noop" }
          ],
          "stop": bool,
          "meta": { "source":"B12F1", "rules_version":"1.0" }
        },
        "diag": { "reason": "ok|no_schedule", "counts": { "actions": int } }
      }
    """
    sched = _get(input_json, ["runtime", "schedule"], {})
    if not isinstance(sched, dict) or not sched:
        return {"status": "SKIP",
                "engine": {"actions": [], "stop": False, "meta": {"source": "B12F1", "rules_version": RULES_VERSION}},
                "diag": {"reason": "no_schedule", "counts": {"actions": 0}}}

    actions, reasons = _compose_actions(input_json)

    # Heuristic: if we emitted or scheduled exec (not just delay/persist), we can stop this tick.
    stop = any(a.get("type") in {"emit", "execute"} for a in actions)

    return {
        "status": "OK",
        "engine": {
            "actions": actions,
            "stop": bool(stop),
            "meta": {"source": "B12F1", "rules_version": RULES_VERSION}
        },
        "diag": {"reason": "ok", "counts": {"actions": len(actions)}, "reasons": reasons},
    }


if __name__ == "__main__":
    # Demo A: execute + persist + delay
    sample_exec = {
        "runtime": {"schedule": {
            "action": "execute",
            "delay_ms": 180,
            "routes": [{
                "type": "execute",
                "run": [{"req_id": "r1", "skill_id": "skill.web_summarize"},
                        {"req_id": "r2", "skill_id": "skill.write_file"}],
                "defer": ["r3"],
                "limits": {"timeout_ms": 28000, "max_inflight": 2}
            }]
        }},
        "storage": {"apply_optimized": {"ops": [{"op": "put", "key": "k/a", "value": {"x": 1}}]}},
        "index": {"queue_optimized": {"items": [{"type": "packz", "id": "u1", "ns": "store/noema/t-1"}]}}
    }
    out = b12f1_orchestrate(sample_exec)
    print(out["engine"]["actions"])
    print(out["engine"]["stop"])

    # Demo B: answer only
    sample_answer = {
        "runtime": {"schedule": {"action": "answer", "routes": [{"type": "answer", "text": "Done."}]}},
        "dialog": {"final": {"move": "answer", "text": "Done."}}
    }
    print(b12f1_orchestrate(sample_answer)["engine"]["actions"])

    # Demo C: noop
    sample_noop = {"runtime": {"schedule": {"action": "noop", "routes": []}}}
    print(b12f1_orchestrate(sample_noop)["engine"]["actions"])
