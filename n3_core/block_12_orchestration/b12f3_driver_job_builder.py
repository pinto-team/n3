# Folder: noema/n3_core/block_12_orchestration
# File:   b12f3_driver_job_builder.py

from __future__ import annotations

import hashlib
import json
import unicodedata
from typing import Any, Dict, List, Optional

__all__ = ["b12f3_build_jobs"]

RULES_VERSION = "1.0"
MAX_EMIT = 8
MAX_REQS = 32
MAX_APPLY = 6000
MAX_INDEX = 3000
DEFAULT_TIMEOUT = 30000
DEFAULT_DEADLINE = 35000


# ---------- utils ----------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _hash(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _cap_list(lst: Any, n: int) -> List[Any]:
    return [x for x in (lst or []) if isinstance(x, dict)][:n]


def _ns(inp: Dict[str, Any]) -> str:
    tid = _get(inp, ["session", "thread_id"], "") or "default"
    return f"noema/{tid}"


def _deadline_ms(limits: Dict[str, Any], pad: int = 2000) -> int:
    t = limits.get("timeout_ms") if isinstance(limits, dict) else None
    base = int(t) if isinstance(t, (int, float)) else DEFAULT_TIMEOUT
    return int(min(120000, max(2000, base + pad)))


# ---------- builders ----------

def _job_transport(plan: Dict[str, Any], ns: str) -> Optional[Dict[str, Any]]:
    out = _get(plan, ["transport", "outbound"], [])
    out = _cap_list(out, MAX_EMIT)
    if not out:
        return None
    job = {
        "type": "transport.emit",
        "items": out,
        "idempotency_key": _hash({"ns": ns, "type": "emit", "items": out}),
        "deadline_ms": _deadline_ms({"timeout_ms": 8000}, pad=1000),
    }
    job["job_id"] = _hash({"k": job["idempotency_key"], "t": "emit"})
    return job


def _job_skills(plan: Dict[str, Any], ns: str) -> Optional[Dict[str, Any]]:
    sk = plan.get("skills") if isinstance(plan.get("skills"), dict) else {}
    batch = _cap_list(sk.get("batch"), MAX_REQS)
    if not batch:
        return None
    limits = sk.get("limits") if isinstance(sk.get("limits"), dict) else {}
    defer = [str(x) for x in (sk.get("defer") or [])]
    job = {
        "type": "skills.execute",
        "batch": batch,
        "limits": {"timeout_ms": int(limits.get("timeout_ms", DEFAULT_TIMEOUT)),
                   "max_inflight": int(limits.get("max_inflight", 4))},
        "defer": defer,
        "idempotency_key": _hash({"ns": ns, "type": "skills", "batch": batch, "limits": job_limits_signature(limits)}),
        "deadline_ms": _deadline_ms({"timeout_ms": limits.get("timeout_ms", DEFAULT_TIMEOUT)}, pad=3000),
    }
    job["job_id"] = _hash({"k": job["idempotency_key"], "t": "skills"})
    return job


def job_limits_signature(lim: Dict[str, Any]) -> Dict[str, Any]:
    return {"timeout_ms": int(lim.get("timeout_ms", DEFAULT_TIMEOUT)), "max_inflight": int(lim.get("max_inflight", 4))}


def _job_storage(plan: Dict[str, Any], ns: str) -> Optional[Dict[str, Any]]:
    st = plan.get("storage") if isinstance(plan.get("storage"), dict) else {}
    apply_ns = _get(st, ["apply", "namespace"], None) or f"store/{ns}"
    apply_ops = _cap_list(_get(st, ["apply", "ops"], []), MAX_APPLY)
    index_q = _cap_list(_get(st, ["index", "queue"], []), MAX_INDEX)
    if not apply_ops and not index_q:
        return None
    job = {
        "type": "storage.apply_index",
        "namespace": apply_ns,
        "apply_ops": apply_ops,
        "index_queue": index_q,
        "idempotency_key": _hash({"ns": apply_ns, "apply_ops": apply_ops, "idx": index_q}),
        "deadline_ms": _deadline_ms({"timeout_ms": 10000}, pad=2000),
    }
    job["job_id"] = _hash({"k": job["idempotency_key"], "t": "storage"})
    return job


def _job_timers(plan: Dict[str, Any], ns: str) -> Optional[Dict[str, Any]]:
    timers = _get(plan, ["timers"], [])
    timers = [t for t in timers if isinstance(t, dict) and int(t.get("ms", 0)) > 0]
    if not timers:
        return None
    # Merge timers into a single minimal sleep (best-effort)
    ms = min(int(t.get("ms", 0)) for t in timers)
    job = {
        "type": "timer.sleep",
        "ms": int(ms),
        "idempotency_key": _hash({"ns": ns, "sleep_ms": ms}),
        "deadline_ms": int(min(60000, ms + 2000)),
    }
    job["job_id"] = _hash({"k": job["idempotency_key"], "t": "timer"})
    return job


# ---------- main ----------

def b12f3_build_jobs(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B12F3 — Orchestration.DriverJobBuilder (Noema)
    Compiles driver.plan into concrete driver jobs with ids, idempotency keys, and deadlines (pure; no I/O).

    Input:
      {
        "driver": { "plan": {
          "transport": {"outbound":[...] }?,
          "skills": {"batch":[...], "limits":{"timeout_ms":int,"max_inflight":int}, "defer":[...]}?,
          "storage": {"apply":{"namespace":str,"ops":[...]}, "index":{"queue":[...]}}?,
          "timers": [ {"ms": int, "reason": str}? ]?
        }},
        "session": {"thread_id": str}?    # optional, for namespace
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "driver": {
          "jobs": [
            { "type":"transport.emit", "job_id":str, "idempotency_key":str, "items":[...], "deadline_ms":int }?,
            { "type":"skills.execute", "job_id":str, "idempotency_key":str, "batch":[...], "limits":{...}, "defer":[...], "deadline_ms":int }?,
            { "type":"storage.apply_index", "job_id":str, "idempotency_key":str, "namespace":str, "apply_ops":[...], "index_queue":[...], "deadline_ms":int }?,
            { "type":"timer.sleep", "job_id":str, "idempotency_key":str, "ms":int, "deadline_ms":int }?
          ],
          "meta": { "source": "B12F3", "rules_version": "1.0" }
        },
        "diag": { "reason": "ok|no_plan", "counts": { "jobs": int } }
      }
    """
    plan = _get(input_json, ["driver", "plan"], {})
    if not isinstance(plan, dict) or not plan:
        return {"status": "SKIP", "driver": {"jobs": [], "meta": {"source": "B12F3", "rules_version": RULES_VERSION}},
                "diag": {"reason": "no_plan", "counts": {"jobs": 0}}}

    ns = _ns(input_json)

    jobs: List[Dict[str, Any]] = []
    for builder in (_job_transport, _job_skills, _job_storage, _job_timers):
        j = builder(plan, ns)
        if j:
            jobs.append(j)

    return {
        "status": "OK",
        "driver": {"jobs": jobs, "meta": {"source": "B12F3", "rules_version": RULES_VERSION}},
        "diag": {"reason": "ok", "counts": {"jobs": len(jobs)}},
    }


if __name__ == "__main__":
    # Demo
    sample = {
        "session": {"thread_id": "t-99"},
        "driver": {"plan": {
            "transport": {"outbound": [{"role": "assistant", "move": "answer", "text": "Done."}]},
            "skills": {
                "batch": [{"req_id": "r1", "skill_id": "skill.web_summarize", "params": {"url": "https://ex/a"}}],
                "limits": {"timeout_ms": 28000, "max_inflight": 2}, "defer": ["r2"]},
            "storage": {
                "apply": {"namespace": "store/noema/t-99", "ops": [{"op": "put", "key": "k/a", "value": {"x": 1}}]},
                "index": {"queue": [{"type": "packz", "id": "u1", "ns": "store/noema/t-99"}]}},
            "timers": [{"ms": 180, "reason": "throttle"}]
        }}
    }
    out = b12f3_build_jobs(sample)
    print(out["diag"])
    for j in out["driver"]["jobs"]:
        print(j["type"], j["job_id"][:8], j["deadline_ms"])
