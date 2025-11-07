# Folder: noema/n3_core/block_13_drivers
# File:   b13f1_driver_protocol_builder.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

import unicodedata

__all__ = ["b13f1_build_protocol"]

RULES_VERSION = "1.0"
MAX_EMIT = 8
MAX_REQS = 32
MAX_APPLY = 6000
MAX_INDEX = 3000
MAX_TEXT = 1200


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


def _clip_text(s: Optional[str], n: int = MAX_TEXT) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _cap_list(lst: Any, n: int) -> List[Dict[str, Any]]:
    return [x for x in (lst or []) if isinstance(x, dict)][:n]


# ------------------------- frame builders -------------------------

def _frame_transport(job: Dict[str, Any], endpoints: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if job.get("type") != "transport.emit":
        return None
    outbound = _cap_list(job.get("items"), MAX_EMIT)
    if not outbound:
        return None
    channel = _get(endpoints, ["transport", "channel"], "default")
    # sanitize text
    for m in outbound:
        if "text" in m:
            m["text"] = _clip_text(m["text"])
    return {
        "type": "transport",
        "channel": channel,
        "messages": outbound,
        "deadline_ms": int(job.get("deadline_ms", 8000)),
        "idempotency_key": job.get("idempotency_key"),
        "meta": {"job_id": job.get("job_id")}
    }


def _frame_skills(job: Dict[str, Any], endpoints: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if job.get("type") != "skills.execute":
        return None
    batch = _cap_list(job.get("batch"), MAX_REQS)
    if not batch:
        return None

    calls: List[Dict[str, Any]] = []
    for r in batch:
        sid = r.get("skill_id") if isinstance(r.get("skill_id"), str) else ""
        params = r.get("params") if isinstance(r.get("params"), dict) else {}
        endpoint = _get(endpoints, ["skills", sid, "endpoint"],
                        _get(endpoints, ["skills", "default", "endpoint"], "skills://default"))
        calls.append({
            "req_id": r.get("req_id"),
            "skill_id": sid,
            "endpoint": endpoint,
            "params": params,
            "timeout_ms": int(_get(job, ["limits", "timeout_ms"], 30000)),
            "idempotency_key": r.get("idempotency_key"),
        })

    return {
        "type": "skills",
        "calls": calls,
        "limits": {
            "timeout_ms": int(_get(job, ["limits", "timeout_ms"], 30000)),
            "max_inflight": int(_get(job, ["limits", "max_inflight"], 4))
        },
        "defer": [str(x) for x in (job.get("defer") or [])],
        "deadline_ms": int(job.get("deadline_ms", 35000)),
        "idempotency_key": job.get("idempotency_key"),
        "meta": {"job_id": job.get("job_id")}
    }


def _frame_storage(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if job.get("type") != "storage.apply_index":
        return None
    apply_ops = _cap_list(job.get("apply_ops"), MAX_APPLY)
    index_q = _cap_list(job.get("index_queue"), MAX_INDEX)
    if not apply_ops and not index_q:
        return None
    return {
        "type": "storage",
        "namespace": job.get("namespace") or "store/noema/default",
        "apply": apply_ops,
        "index": index_q,
        "deadline_ms": int(job.get("deadline_ms", 12000)),
        "idempotency_key": job.get("idempotency_key"),
        "meta": {"job_id": job.get("job_id")}
    }


def _frame_timer(job: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if job.get("type") != "timer.sleep":
        return None
    ms = int(job.get("ms", 0)) if isinstance(job.get("ms"), (int, float)) else 0
    if ms <= 0:
        return None
    return {
        "type": "timer",
        "sleep_ms": ms,
        "deadline_ms": int(job.get("deadline_ms", ms + 2000)),
        "idempotency_key": job.get("idempotency_key"),
        "meta": {"job_id": job.get("job_id")}
    }


# ------------------------- main -------------------------

def b13f1_build_protocol(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B13F1 — Drivers.ProtocolBuilder (Noema)
    Turns driver.jobs into IO-neutral protocol frames for concrete drivers (pure; no I/O).

    Input:
      {
        "driver": { "jobs": [ {type, ...}, ... ] },
        "endpoints": {
          "transport": {"channel": "default"}?,
          "skills": { "<skill_id>": {"endpoint": str}, "default": {"endpoint": str}? }?
        }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "driver": {
          "protocol": {
            "frames": [
              { "type":"transport","channel":str,"messages":[...],"deadline_ms":int,"idempotency_key":str }? ,
              { "type":"skills","calls":[...],"limits":{...},"defer":[...],"deadline_ms":int,"idempotency_key":str }? ,
              { "type":"storage","namespace":str,"apply":[...],"index":[...],"deadline_ms":int,"idempotency_key":str }? ,
              { "type":"timer","sleep_ms":int,"deadline_ms":int,"idempotency_key":str }?
            ],
            "meta": { "source": "B13F1", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_jobs", "counts": { "frames": int } }
      }
    """
    jobs = _get(input_json, ["driver", "jobs"], [])
    if not isinstance(jobs, list) or not jobs:
        return {
            "status": "SKIP",
            "driver": {"protocol": {"frames": [], "meta": {"source": "B13F1", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_jobs", "counts": {"frames": 0}},
        }

    endpoints = _get(input_json, ["endpoints"], {}) or {}

    frames: List[Dict[str, Any]] = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        # Build frames explicitly with correct signatures
        fr = _frame_transport(j, endpoints)
        if fr:
            frames.append(fr)
        fr = _frame_skills(j, endpoints)
        if fr:
            frames.append(fr)
        fr = _frame_storage(j)
        if fr:
            frames.append(fr)
        fr = _frame_timer(j)
        if fr:
            frames.append(fr)

    out = {
        "status": "OK",
        "driver": {"protocol": {"frames": frames, "meta": {"source": "B13F1", "rules_version": RULES_VERSION}}},
        "diag": {"reason": "ok", "counts": {"frames": len(frames)}},
    }
    return out


if __name__ == "__main__":
    # Demo
    sample = {
        "endpoints": {
            "transport": {"channel": "default"},
            "skills": {"default": {"endpoint": "skills://local"},
                       "skill.web_summarize": {"endpoint": "skills://web_summarize"}}
        },
        "driver": {
            "jobs": [
                {"type": "transport.emit",
                 "items": [{"role": "assistant", "move": "answer", "text": "Done.", "id": "m1"}],
                 "idempotency_key": "em1", "deadline_ms": 7000, "job_id": "J1"},
                {"type": "skills.execute",
                 "batch": [{"req_id": "r1", "skill_id": "skill.web_summarize", "params": {"url": "https://ex/a"}}],
                 "limits": {"timeout_ms": 28000, "max_inflight": 2}, "defer": ["r2"], "idempotency_key": "sk1",
                 "deadline_ms": 32000, "job_id": "J2"},
                {"type": "storage.apply_index", "namespace": "store/noema/t-99",
                 "apply_ops": [{"op": "put", "key": "k/a", "value": {"x": 1}}],
                 "index_queue": [{"type": "packz", "id": "u1", "ns": "store/noema/t-99"}], "idempotency_key": "st1",
                 "deadline_ms": 10000, "job_id": "J3"},
                {"type": "timer.sleep", "ms": 180, "idempotency_key": "tm1", "deadline_ms": 2000, "job_id": "J4"}
            ]
        }
    }
    out = b13f1_build_protocol(sample)
    print(out["diag"])
    for f in out["driver"]["protocol"]["frames"]:
        print(f["type"], f.get("channel") or f.get("namespace") or f.get("sleep_ms"))
