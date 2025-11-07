# Folder: noema/n3_core/block_9_observability
# File:   b9f2_trace_builder.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b9f2_build_trace"]

RULES_VERSION = "1.0"


# ------------------------- utils -------------------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _znow() -> datetime:
    return datetime.now(timezone.utc)


def _to_z(dt: Optional[datetime]) -> str:
    dt = dt or _znow()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_z(s: Optional[str]) -> Optional[datetime]:
    if not isinstance(s, str) or not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    except Exception:
        return None


def _hash(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


# ------------------------- collectors -------------------------

def _collect_turn_times(inp: Dict[str, Any]) -> Tuple[Optional[datetime], Optional[datetime]]:
    """
    Returns (t_user, t_assistant) best-effort from WAL turns or dialog artifacts.
    """
    t_user = None
    t_assistant = None

    # Prefer WAL turns (have stable commit times)
    wal = _get(inp, ["memory", "wal", "ops"], []) or []
    for op in wal:
        if not isinstance(op, dict) or op.get("op") != "append_turn":
            continue
        turn = op.get("turn", {})
        if not isinstance(turn, dict):
            continue
        role = turn.get("role")
        t = turn.get("time") or _get(turn, ["packz", "meta", "commit_time"])
        dt = _parse_z(t)
        if role == "user":
            if dt and (t_user is None or dt < t_user):
                t_user = dt
        elif role == "assistant":
            if dt and (t_assistant is None or dt < t_assistant):
                t_assistant = dt

    # Fallback from dialog.final/surface if WAL missing
    if t_assistant is None:
        t = _get(inp, ["dialog", "final", "time"], None)
        t_assistant = _parse_z(t) or t_assistant
    return t_user, t_assistant


def _collect_exec(inp: Dict[str, Any]) -> Tuple[Optional[int], Optional[str]]:
    best = _get(inp, ["executor", "results", "best"], {}) or {}
    if not isinstance(best, dict) or not best:
        return None, None
    dur = best.get("duration_ms")
    rid = best.get("req_id")
    return (int(dur) if isinstance(dur, (int, float)) else None), (rid if isinstance(rid, str) else None)


def _collect_plan(inp: Dict[str, Any]) -> Dict[str, Any]:
    plan = _get(inp, ["planner", "plan"], {}) or {}
    return {
        "id": plan.get("id"),
        "skill_id": plan.get("skill_id"),
        "skill_name": plan.get("skill_name"),
        "next_move": plan.get("next_move"),
        "must_confirm": bool(_get(plan, ["guardrails", "must_confirm"], False)),
    }


def _collect_apply(inp: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    apply_ops = _get(inp, ["storage", "apply_optimized", "ops"], []) \
                or _get(inp, ["storage", "apply", "ops"], []) or []
    index_items = _get(inp, ["index", "queue_optimized", "items"], []) \
                  or _get(inp, ["index", "queue", "items"], []) or []
    return [op for op in apply_ops if isinstance(op, dict)], [it for it in index_items if isinstance(it, dict)]


# ------------------------- span maker -------------------------

def _mk_span(name: str, start: datetime, end: datetime, attrs: Dict[str, Any], parent_id: Optional[str] = None) -> Dict[
    str, Any]:
    sid = _hash({"n": name, "s": _to_z(start), "e": _to_z(end), "a": attrs, "p": parent_id})
    return {
        "id": sid,
        "name": name,
        "ts_start": _to_z(start),
        "ts_end": _to_z(end),
        "duration_ms": int(max(0, (end - start).total_seconds() * 1000)),
        "attrs": attrs,
        "parent": parent_id,
    }


# ------------------------- main -------------------------

def b9f2_build_trace(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B9F2 — Observability.TraceBuilder (Noema)
    Produces a best-effort span trace from plan/exec/dialog/persist artifacts (pure; no I/O).

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "observability": {
          "trace": {
            "spans": [ {id,name,ts_start,ts_end,duration_ms,attrs{},parent}, ... ],
            "timeline": [ {ts,name,ref:id,hint}, ... ],
            "meta": { "source": "B9F2", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|insufficient", "counts": { "spans": int } }
      }
    """
    t_user, t_assistant = _collect_turn_times(input_json)
    exec_dur_ms, req_id = _collect_exec(input_json)
    plan = _collect_plan(input_json)
    apply_ops, index_items = _collect_apply(input_json)

    # Anchor times (best-effort)
    if t_user is None and t_assistant is None and exec_dur_ms is None and not apply_ops:
        return {"status": "SKIP", "observability": {
            "trace": {"spans": [], "timeline": [], "meta": {"source": "B9F2", "rules_version": RULES_VERSION}}},
                "diag": {"reason": "insufficient", "counts": {"spans": 0}}}

    now = _znow()
    t_anchor = t_user or (t_assistant - timedelta(milliseconds=exec_dur_ms) if (
                t_assistant and exec_dur_ms) else None) or now - timedelta(milliseconds=200)

    # Infer plan window
    plan_start = t_anchor
    plan_end = (t_assistant - timedelta(milliseconds=max(50, exec_dur_ms or 50))) if t_assistant else (
                plan_start + timedelta(milliseconds=80))

    # Infer execution window
    if exec_dur_ms is not None:
        exec_end = t_assistant or (plan_end + timedelta(milliseconds=exec_dur_ms))
        exec_start = exec_end - timedelta(milliseconds=exec_dur_ms)
    else:
        exec_start = plan_end + timedelta(milliseconds=20)
        exec_end = exec_start + timedelta(milliseconds=60)

    # Persistence window (after assistant turn, small tail)
    persist_start = (t_assistant or exec_end) + timedelta(milliseconds=20)
    persist_end = persist_start + timedelta(milliseconds=40 + 5 * (len(apply_ops) + len(index_items)))

    spans: List[Dict[str, Any]] = []

    # Root span for the thread/plan
    root_attrs = {"plan_id": plan.get("id"), "skill_id": plan.get("skill_id"), "skill_name": plan.get("skill_name")}
    root = _mk_span("noema.turn", plan_start, persist_end, root_attrs, None)
    spans.append(root)

    # User turn
    if t_user:
        spans.append(_mk_span("user.turn", t_user, t_user + timedelta(milliseconds=10), {"role": "user"}, root["id"]))

    # Planning
    spans.append(_mk_span("planner.plan", plan_start, plan_end,
                          {"next_move": plan.get("next_move"), "must_confirm": plan.get("must_confirm")}, root["id"]))

    # Execution
    exec_attrs = {"req_id": req_id,
                  "count_items": int(_get(input_json, ["executor", "results", "aggregate", "count"], 0))}
    spans.append(_mk_span("executor.run", exec_start, exec_end, exec_attrs, root["id"]))

    # Assistant rendering (surface + safety)
    if t_assistant:
        spans.append(_mk_span("dialog.surface", exec_end, t_assistant - timedelta(milliseconds=10),
                              {"move": _get(input_json, ["dialog", "turn", "move"], "")}, root["id"]))
        spans.append(_mk_span("dialog.final", t_assistant - timedelta(milliseconds=10), t_assistant,
                              {"move": _get(input_json, ["dialog", "final", "move"], "")}, root["id"]))
    else:
        spans.append(_mk_span("dialog.surface", exec_end, exec_end + timedelta(milliseconds=30),
                              {"move": _get(input_json, ["dialog", "turn", "move"], "")}, root["id"]))

    # Persistence (apply/index)
    if apply_ops:
        spans.append(
            _mk_span("storage.apply", persist_start, persist_start + timedelta(milliseconds=20 + 2 * len(apply_ops)),
                     {"ops": len(apply_ops)}, root["id"]))
    if index_items:
        spans.append(_mk_span("index.queue", persist_start + timedelta(milliseconds=10), persist_end,
                              {"items": len(index_items)}, root["id"]))

    # Timeline view
    tl: List[Dict[str, Any]] = []
    for sp in spans:
        tl.append({"ts": sp["ts_start"], "name": sp["name"] + ":start", "ref": sp["id"], "hint": sp["attrs"]})
        tl.append({"ts": sp["ts_end"], "name": sp["name"] + ":end", "ref": sp["id"], "hint": sp["attrs"]})
    tl.sort(key=lambda x: x["ts"])

    return {
        "status": "OK",
        "observability": {
            "trace": {
                "spans": spans,
                "timeline": tl,
                "meta": {"source": "B9F2", "rules_version": RULES_VERSION}
            }
        },
        "diag": {"reason": "ok", "counts": {"spans": len(spans)}},
    }


if __name__ == "__main__":
    # Minimal demo with best-effort inference
    sample = {
        "memory": {"wal": {"ops": [
            {"op": "append_turn", "turn": {"role": "user", "time": "2025-11-07T08:59:58Z"}},
            {"op": "append_turn", "turn": {"role": "assistant", "time": "2025-11-07T09:00:03Z"}}
        ]}},
        "planner": {"plan": {"id": "p1", "skill_id": "skill.web_summarize", "skill_name": "Web Summarizer",
                             "next_move": "execute", "guardrails": {"must_confirm": True}}},
        "executor": {"results": {"best": {"req_id": "r1", "duration_ms": 1200}, "aggregate": {"count": 2}}},
        "storage": {"apply_optimized": {"ops": [{"op": "put"}, {"op": "inc"}]}},
        "index": {"queue_optimized": {"items": [{"type": "packz", "id": "u1", "ns": "store/noema/t-1"}]}}
    }
    out = b9f2_build_trace(sample)
    print(out["diag"], len(out["observability"]["trace"]["spans"]))
    for s in out["observability"]["trace"]["spans"]:
        print(s["name"], s["ts_start"], s["ts_end"], s["duration_ms"])
