# Folder: noema/n3_core/block_9_observability
# File:   b9f1_telemetry_aggregator.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b9f1_aggregate_telemetry"]

RULES_VERSION = "1.0"
MAX_AUDIT_ITEMS = 50
MAX_LABEL_KV = 12


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


def _clip(s: Optional[str], n: int) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else (s[: n - 1] + "…")


def _hash(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _metric(name: str, value: float, labels: Dict[str, Any]) -> Dict[str, Any]:
    clean_labels = {}
    for k, v in list(labels.items())[:MAX_LABEL_KV]:
        if v is None:
            continue
        if isinstance(v, (int, float, bool)):
            clean_labels[k] = v
        else:
            clean_labels[k] = _clip(str(v), 120)
    return {"name": name, "value": float(value), "ts": _now_z(), "labels": clean_labels}


def _audit(kind: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": _hash({"k": kind, "p": payload, "t": _now_z()}),
        "kind": kind,
        "payload": payload,
        "ts": _now_z(),
    }


# ------------------------- collectors -------------------------

def _collect_dialog(inp: Dict[str, Any]) -> Tuple[str, str, int]:
    move = _get(inp, ["dialog", "final", "move"], "") or _get(inp, ["dialog", "turn", "move"], "") or ""
    text = _get(inp, ["dialog", "final", "text"], "") or _get(inp, ["dialog", "surface", "text"], "") or _get(inp,
                                                                                                              ["dialog",
                                                                                                               "turn",
                                                                                                               "content"],
                                                                                                              "") or ""
    return str(move), str(text), len(text)


def _collect_plan(inp: Dict[str, Any]) -> Dict[str, Any]:
    plan = _get(inp, ["planner", "plan"], {}) or {}
    return {
        "plan_id": plan.get("id"),
        "skill_id": plan.get("skill_id"),
        "skill_name": plan.get("skill_name"),
        "next_move": plan.get("next_move"),
        "must_confirm": bool(_get(plan, ["guardrails", "must_confirm"], False)),
    }


def _collect_exec(inp: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    agg = _get(inp, ["executor", "results", "aggregate"], {}) or {}
    best = _get(inp, ["executor", "results", "best"], {}) or {}
    return agg, best


def _collect_persist(inp: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    wal = _get(inp, ["memory", "wal", "ops"], []) or []
    apply_ops = _get(inp, ["storage", "apply_optimized", "ops"], []) \
                or _get(inp, ["storage", "apply", "ops"], []) or []
    index_items = _get(inp, ["index", "queue_optimized", "items"], []) \
                  or _get(inp, ["index", "queue", "items"], []) or []
    return (
        [op for op in wal if isinstance(op, dict)],
        [op for op in apply_ops if isinstance(op, dict)],
        [it for it in index_items if isinstance(it, dict)],
    )


def _collect_world(inp: Dict[str, Any]) -> Dict[str, Any]:
    top = _get(inp, ["world_model", "prediction", "top"], "")
    sa = _get(inp, ["world_model", "context", "features", "speech_act"], "")
    u = _get(inp, ["world_model", "uncertainty", "score"], 0.0)
    return {"reply_top": top, "speech_act": sa, "uncertainty": float(u)}


def _session(inp: Dict[str, Any]) -> Dict[str, Any]:
    tid = _get(inp, ["session", "thread_id"], "") or "default"
    return {"thread_id": tid, "namespace": f"store/noema/{tid}"}


# ------------------------- main -------------------------

def b9f1_aggregate_telemetry(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B9F1 — Observability.TelemetryAggregator (Noema)
    Produces metrics & audit events from planning/execution/dialog/persistence artifacts (pure; no I/O).
    """
    sess = _session(input_json)
    plan = _collect_plan(input_json)
    move, out_text, out_len = _collect_dialog(input_json)
    agg, best = _collect_exec(input_json)
    wal, apply_ops, index_items = _collect_persist(input_json)
    world = _collect_world(input_json)

    metrics: List[Dict[str, Any]] = []
    audit: List[Dict[str, Any]] = []

    # Metrics: dialog
    metrics.append(_metric("dialog_out_length", out_len, {**sess, "move": move or "unknown"}))

    # Metrics: plan
    metrics.append(_metric("plan_must_confirm", 1.0 if plan["must_confirm"] else 0.0,
                           {**sess, "next_move": plan.get("next_move") or "unknown"}))

    # Metrics: execution aggregate
    if agg:
        metrics.append(_metric("exec_total_cost", float(agg.get("total_cost", 0.0)), {**sess}))
        metrics.append(_metric("exec_avg_latency_ms", float(agg.get("avg_latency_ms", 0.0)), {**sess}))
        metrics.append(_metric("exec_items", int(agg.get("count", 0)),
                               {**sess, "ok": int(agg.get("ok", 0)), "errors": int(agg.get("errors", 0))}))

    # Metrics: persistence
    if wal:
        metrics.append(_metric("wal_ops", len(wal), {**sess}))
        # breakdown
        k = {"append_turn": 0, "append_result": 0, "bump_counters": 0}
        for op in wal:
            if op.get("op") in k:
                k[op["op"]] += 1
        for name, cnt in k.items():
            metrics.append(_metric(f"wal_{name}", cnt, {**sess}))
    if apply_ops:
        puts = sum(1 for x in apply_ops if x.get("op") == "put")
        incs = sum(1 for x in apply_ops if x.get("op") == "inc")
        links = sum(1 for x in apply_ops if x.get("op") == "link")
        metrics.append(_metric("apply_ops", len(apply_ops), {**sess}))
        metrics.append(_metric("apply_puts", puts, {**sess}))
        metrics.append(_metric("apply_incs", incs, {**sess}))
        metrics.append(_metric("apply_links", links, {**sess}))
    if index_items:
        metrics.append(_metric("index_queue_items", len(index_items), {**sess}))

    # Metrics: world model signals
    if world:
        metrics.append(_metric("wm_uncertainty", world.get("uncertainty", 0.0),
                               {**sess, "reply_top": world.get("reply_top") or "",
                                "speech_act": world.get("speech_act") or ""}))

    # Audit events (capped)
    if plan.get("skill_id") or plan.get("skill_name"):
        audit.append(_audit("plan_selected", {
            "plan_id": plan.get("plan_id"),
            "skill_id": plan.get("skill_id"),
            "skill_name": plan.get("skill_name"),
            "must_confirm": plan["must_confirm"],
            "next_move": plan.get("next_move"),
            "session": sess,
        }))
    if best:
        audit.append(_audit("exec_best", {
            "req_id": best.get("req_id"),
            "kind": best.get("kind"),
            "ok": bool(best.get("ok", True)),
            "score": best.get("score", 0.0),
            "duration_ms": best.get("duration_ms", 0),
            "usage": best.get("usage", {}),
            "session": sess,
        }))
    if move:
        audit.append(_audit("dialog_emit", {
            "move": move,
            "preview": _clip(out_text, 240),
            "len": out_len,
            "session": sess,
        }))
    if wal:
        audit.append(_audit("wal_commit", {"ops": len(wal), "session": sess}))
    if apply_ops or index_items:
        audit.append(_audit("storage_apply", {
            "apply_ops": len(apply_ops),
            "index_items": len(index_items),
            "checksum_apply": _hash(apply_ops) if apply_ops else None,
            "checksum_index": _hash(index_items) if index_items else None,
            "session": sess,
        }))

    audit = audit[:MAX_AUDIT_ITEMS]

    return {
        "status": "OK",
        "observability": {
            "telemetry": {
                "metrics": metrics,
                "audit": audit,
                "meta": {"source": "B9F1", "rules_version": RULES_VERSION}
            }
        },
        "diag": {"reason": "ok", "counts": {"metrics": len(metrics), "audit": len(audit)}},
    }


if __name__ == "__main__":
    sample = {
        "session": {"thread_id": "t-007"},
        "world_model": {"prediction": {"top": "execute_action"}, "context": {"features": {"speech_act": "request"}},
                        "uncertainty": {"score": 0.42}},
        "planner": {"plan": {"id": "p1", "skill_id": "skill.web_summarize", "skill_name": "Web Summarizer",
                             "next_move": "execute", "guardrails": {"must_confirm": True}}},
        "dialog": {
            "final": {"move": "confirm", "text": "Confirm to run 'Web Summarizer' with action=summarize, url=..."}},
        "executor": {
            "results": {"aggregate": {"count": 2, "ok": 1, "errors": 1, "total_cost": 0.0023, "avg_latency_ms": 640.5},
                        "best": {"req_id": "r1", "ok": True, "kind": "json", "score": 0.72, "duration_ms": 540,
                                 "usage": {"input_tokens": 100, "output_tokens": 80}}}},
        "memory": {"wal": {"ops": [{"op": "append_turn"}, {"op": "append_turn"}, {"op": "bump_counters"}]}},
        "storage": {"apply_optimized": {"ops": [{"op": "put"}, {"op": "put"}, {"op": "inc"}]}},
        "index": {"queue_optimized": {"items": [{"type": "packz", "id": "u1", "ns": "store/noema/t-007"}]}}
    }
    out = b9f1_aggregate_telemetry(sample)
    print(out["diag"])
    print(out["observability"]["telemetry"]["metrics"][0])
