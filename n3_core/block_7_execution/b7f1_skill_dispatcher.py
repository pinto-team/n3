# Folder: noema/n3_core/block_7_execution
# File:   b7f1_skill_dispatcher.py

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple

import unicodedata

__all__ = ["b7f1_dispatch"]

RULES_VERSION = "1.0"


# -------------- utils --------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _make_key(skill_id: str, params: Dict[str, Any], plan_id: str = "") -> str:
    payload = json.dumps({"skill_id": skill_id, "params": params, "plan": plan_id}, ensure_ascii=False, sort_keys=True,
                         separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _collect_execute_ops(inp: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], str, str, bool, str]:
    """
    Returns (ops, plan_id, skill_name_hint, must_confirm, block_reason)
    Tries dialog.turn.ops first; falls back to planner.plan.steps.
    """
    # Safety & gating
    final_move = _get(inp, ["dialog", "final", "move"], "")
    reason = _get(inp, ["dialog", "final", "reason"], "") or ""
    if final_move and _cf(final_move) != "execute":
        return [], "", "", False, "not_execute_move"

    # Prefer explicit ops from dialog.turn
    turn_ops = _get(inp, ["dialog", "turn", "ops"], [])
    if isinstance(turn_ops, list) and turn_ops:
        ex_ops = [op for op in turn_ops if isinstance(op, dict) and op.get("op") == "execute_skill"]
    else:
        # Fallback to planner.plan.steps
        steps = _get(inp, ["planner", "plan", "steps"], [])
        ex_ops = [st for st in steps if isinstance(st, dict) and st.get("op") == "execute_skill"]

    plan_id = _get(inp, ["planner", "plan", "id"], "") or _get(inp, ["dialog", "turn", "meta", "plan_id"], "")
    skill_name_hint = ""
    if ex_ops:
        nm = ex_ops[0].get("skill_name")
        if isinstance(nm, str):
            skill_name_hint = nm

    # Guard: if SafetyFilter asked to confirm, do not dispatch
    must_confirm = bool(_get(inp, ["planner", "plan", "guardrails", "must_confirm"], False))
    if reason in {"must_confirm", "secret_detected"}:
        return [], plan_id, skill_name_hint, True, reason

    return ex_ops, plan_id, skill_name_hint, must_confirm, ""


# -------------- main --------------

def b7f1_dispatch(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B7F1 — Execution.SkillDispatcher (Noema)

    Input:
      {
        "dialog": {
          "final": { "move": "execute"|..., "reason": "must_confirm|secret_detected"?, ... },
          "turn":  { "ops": [ {"op":"execute_skill","skill_id":str,"skill_name":str?,"params":{...}} ]? }
        },
        "planner": { "plan": { "id": str, "steps": [...], "guardrails": {"must_confirm": bool} } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "executor": {
          "requests": [
            { "req_id": str, "skill_id": str, "params": {...},
              "timeout_ms": int, "retries": {"max": int, "policy": "exponential", "backoff_ms": int},
              "idempotency_key": str, "meta": {"plan_id": str, "skill_name": str?} }
          ],
          "meta": { "source": "B7F1", "rules_version": "1.0" }
        },
        "diag": { "reason": "ok|not_execute_move|must_confirm|no_ops", "counts": {"ops": int, "requests": int} }
      }
    """
    ops, plan_id, skill_name_hint, must_confirm, block_reason = _collect_execute_ops(input_json)
    if block_reason == "not_execute_move":
        return {
            "status": "SKIP",
            "executor": {"requests": [], "meta": {"source": "B7F1", "rules_version": RULES_VERSION}},
            "diag": {"reason": "not_execute_move", "counts": {"ops": 0, "requests": 0}},
        }
    if must_confirm:
        return {
            "status": "SKIP",
            "executor": {"requests": [], "meta": {"source": "B7F1", "rules_version": RULES_VERSION}},
            "diag": {"reason": block_reason or "must_confirm", "counts": {"ops": 0, "requests": 0}},
        }
    if not ops:
        return {
            "status": "SKIP",
            "executor": {"requests": [], "meta": {"source": "B7F1", "rules_version": RULES_VERSION}},
            "diag": {"reason": "no_ops", "counts": {"ops": 0, "requests": 0}},
        }

    requests: List[Dict[str, Any]] = []
    for op in ops:
        sid = op.get("skill_id") if isinstance(op.get("skill_id"), str) else ""
        params = op.get("params") if isinstance(op.get("params"), dict) else {}
        if not sid:
            continue
        key = _make_key(sid, params, plan_id)
        req = {
            "req_id": key,  # stable hash id
            "skill_id": sid,
            "params": params,
            "timeout_ms": 30000,
            "retries": {"max": 2, "policy": "exponential", "backoff_ms": 1200},
            "idempotency_key": key,
            "meta": {"plan_id": plan_id, "skill_name": op.get("skill_name", skill_name_hint) or ""},
        }
        requests.append(req)

    return {
        "status": "OK",
        "executor": {
            "requests": requests,
            "meta": {"source": "B7F1", "rules_version": RULES_VERSION},
        },
        "diag": {"reason": "ok", "counts": {"ops": len(ops), "requests": len(requests)}},
    }


if __name__ == "__main__":
    # Demo A: normal execute
    sample_execute = {
        "dialog": {"final": {"move": "execute"}, "turn": {"ops": [
            {"op": "execute_skill", "skill_id": "skill.web_summarize", "skill_name": "Web Summarizer",
             "params": {"action": "summarize", "url": "https://example.com/a.pdf"}}
        ]}},
        "planner": {"plan": {"id": "plan-123", "guardrails": {"must_confirm": False}}}
    }
    out = b7f1_dispatch(sample_execute)
    print(out["executor"]["requests"][0]["skill_id"], out["diag"])

    # Demo B: must_confirm → SKIP
    sample_blocked = {
        "dialog": {"final": {"move": "execute", "reason": "must_confirm"}},
        "planner": {"plan": {"id": "plan-xyz", "guardrails": {"must_confirm": True},
                             "steps": [{"op": "execute_skill", "skill_id": "skill.write_file",
                                        "params": {"path": "/tmp/a.txt", "text": "hi"}}]}}
    }
    print(b7f1_dispatch(sample_blocked)["diag"])
