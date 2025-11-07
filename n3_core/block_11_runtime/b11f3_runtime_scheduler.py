# Folder: noema/n3_core/block_11_runtime
# File:   b11f3_runtime_scheduler.py

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import unicodedata

__all__ = ["b11f3_schedule_runtime"]

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


def _clip(v: Any, n: int = 2000) -> Any:
    if isinstance(v, str) and len(v) > n:
        return v[: n - 1] + "…"
    return v


# ------------------------- core -------------------------

def _batch_requests(reqs: List[Dict[str, Any]], max_inflight: int) -> Tuple[List[Dict[str, Any]], List[str]]:
    if not isinstance(reqs, list) or not reqs:
        return [], []
    if max_inflight <= 0:
        return [], [str(r.get("req_id", "")) for r in reqs]
    run = reqs[:max_inflight]
    defer = reqs[max_inflight:]
    return run, [str(r.get("req_id", "")) for r in defer]


def _decide_action(gates: Dict[str, Any], has_exec: bool, has_answer: bool) -> Tuple[str, List[str]]:
    reasons = list(_get(gates, ["reasons"], [])) if isinstance(_get(gates, ["reasons"], []), list) else []
    if gates.get("require_confirm", False) and (has_exec or has_answer):
        return "confirm", reasons + ["require_confirm"]
    if has_exec and not gates.get("allow_execute", True):
        return "sleep", reasons + ["execute_blocked"]
    if has_answer and not gates.get("allow_answer", True):
        return "sleep", reasons + ["answer_blocked"]
    if has_exec:
        return "execute", reasons
    if has_answer:
        return "answer", reasons
    return "noop", reasons + ["nothing_to_do"]


# ------------------------- main -------------------------

def b11f3_schedule_runtime(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B11F3 — Runtime.Scheduler (Noema)

    Input:
      {
        "runtime": {
          "gates": {
            "allow_execute": bool, "allow_answer": bool, "require_confirm": bool,
            "throttle_ms": int, "limits": {"timeout_ms": int, "max_inflight": int},
            "features": {...}
          },
          "reasons": [str]?
        },
        "executor": { "requests": [ {req_id, skill_id, params{}, ...}, ... ] }?,
        "dialog":   { "final": {"move": "answer|confirm|ack|refuse", "text": str}? }?,
        "planner":  { "plan": {"next_move": str}? }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "runtime": {
          "schedule": {
            "action": "execute|answer|confirm|sleep|noop|block",
            "delay_ms": int,                           # combined throttle/backoff
            "routes": [
              { "type":"execute", "run":[req...], "defer":[req_id...], "limits":{"timeout_ms":int,"max_inflight":int} } |
              { "type":"answer",  "text": str? } |
              { "type":"confirm", "reason": str }
            ],
            "features": {...},
            "meta": { "source":"B11F3", "rules_version":"1.0" }
          },
          "reasons": [str]
        },
        "diag": { "reason": "ok|no_gates", "counts": { "requests_total": int, "run": int, "defer": int } }
      }
    """
    gates = _get(input_json, ["runtime", "gates"], {})
    if not isinstance(gates, dict) or not gates:
        return {"status": "SKIP", "runtime": {"schedule": {}},
                "diag": {"reason": "no_gates", "counts": {"requests_total": 0, "run": 0, "defer": 0}}}

    throttle_ms = int(_get(gates, ["throttle_ms"], 0) or 0)
    limits = _get(gates, ["limits"], {}) or {}
    timeout_ms = int(limits.get("timeout_ms", 30000))
    max_inflight = int(limits.get("max_inflight", 4))
    features = _get(gates, ["features"], {}) or {}

    reqs = _get(input_json, ["executor", "requests"], []) or []
    has_exec = isinstance(reqs, list) and len(reqs) > 0

    final = _get(input_json, ["dialog", "final"], {}) or {}
    has_answer = isinstance(final.get("move"), str) and final.get("move") in {"answer", "ack", "refuse"}
    answer_text = final.get("text") if isinstance(final.get("text"), str) else None

    action, reasons = _decide_action(gates, has_exec, has_answer)

    routes: List[Dict[str, Any]] = []
    run_n = defer_n = 0

    if action == "confirm":
        routes.append({"type": "confirm", "reason": "require_confirm"})
    elif action == "execute":
        run, defer_ids = _batch_requests(reqs, max_inflight=max_inflight)
        run_n, defer_n = len(run), len(defer_ids)
        # attach limits to execution route
        routes.append({"type": "execute", "run": run, "defer": defer_ids,
                       "limits": {"timeout_ms": timeout_ms, "max_inflight": max_inflight}})
    elif action == "answer":
        routes.append({"type": "answer", "text": _clip(answer_text, 1200) if answer_text else None})
    elif action == "sleep":
        # keep empty routes; only delay
        pass
    elif action == "noop":
        pass

    schedule = {
        "action": action if action in {"execute", "answer", "confirm", "sleep", "noop"} else "noop",
        "delay_ms": max(0, throttle_ms),
        "routes": routes,
        "features": features,
        "meta": {"source": "B11F3", "rules_version": RULES_VERSION}
    }

    return {
        "status": "OK",
        "runtime": {"schedule": schedule, "reasons": reasons},
        "diag": {"reason": "ok", "counts": {"requests_total": len(reqs) if isinstance(reqs, list) else 0, "run": run_n,
                                            "defer": defer_n}},
    }


if __name__ == "__main__":
    # Demo A: execute with batching and throttle
    sample_exec = {
        "runtime": {"gates": {
            "allow_execute": True, "allow_answer": True, "require_confirm": False,
            "throttle_ms": 250, "limits": {"timeout_ms": 28000, "max_inflight": 2},
            "features": {"fast_nlg": True}
        }},
        "executor": {"requests": [
            {"req_id": "r1", "skill_id": "skill.web_summarize", "params": {"url": "https://ex/a"}},
            {"req_id": "r2", "skill_id": "skill.web_summarize", "params": {"url": "https://ex/b"}},
            {"req_id": "r3", "skill_id": "skill.web_summarize", "params": {"url": "https://ex/c"}}
        ]}
    }
    out = b11f3_schedule_runtime(sample_exec)
    print(out["runtime"]["schedule"]["action"], out["diag"], len(out["runtime"]["schedule"]["routes"][0]["run"]))

    # Demo B: require confirm
    sample_confirm = {
        "runtime": {"gates": {"allow_execute": True, "allow_answer": True, "require_confirm": True, "throttle_ms": 0,
                              "limits": {"timeout_ms": 30000, "max_inflight": 3}}},
        "executor": {"requests": [
            {"req_id": "r1", "skill_id": "skill.write_file", "params": {"path": "/tmp/a.txt", "text": "hi"}}]}
    }
    print(b11f3_schedule_runtime(sample_confirm)["runtime"]["schedule"]["routes"][0])

    # Demo C: answer only
    sample_answer = {
        "runtime": {"gates": {"allow_execute": False, "allow_answer": True, "require_confirm": False, "throttle_ms": 0,
                              "limits": {"timeout_ms": 30000, "max_inflight": 3}}},
        "dialog": {"final": {"move": "answer", "text": "Done."}}
    }
    print(b11f3_schedule_runtime(sample_answer)["runtime"]["schedule"]["routes"])
