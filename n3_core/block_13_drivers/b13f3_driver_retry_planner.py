# Folder: noema/n3_core/block_13_drivers
# File:   b13f3_driver_retry_planner.py

from __future__ import annotations

import hashlib
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

__all__ = ["b13f3_plan_retry"]

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


def _hash_to_int(s: str) -> int:
    return int(hashlib.sha1(s.encode("utf-8")).hexdigest()[:8], 16)


def _cap_list(lst: Any, n: int) -> List[Dict[str, Any]]:
    return [x for x in (lst or []) if isinstance(x, dict)][:n]


def _as_int(x: Any, default: int = 0) -> int:
    return int(x) if isinstance(x, (int, float)) else default


def _as_bool(x: Any, default: bool = False) -> bool:
    return bool(x) if isinstance(x, bool) else default


# ------------------------- defaults -------------------------

DEFAULTS = {
    "skills": {"max_attempts": 3, "backoff_ms": 400, "factor": 1.7, "jitter_ms": 120},
    "transport": {"max_attempts": 2, "backoff_ms": 200, "factor": 1.5, "jitter_ms": 80},
    "storage": {"max_attempts": 2, "backoff_ms": 300, "factor": 1.6, "jitter_ms": 100},
    "timer": {"max_attempts": 0, "backoff_ms": 0, "factor": 1.0, "jitter_ms": 0},
}

MAX_REQS = 32
MAX_APPLY = 6000
MAX_INDEX = 3000
MAX_EMIT = 8


# ------------------------- core helpers -------------------------

def _policy_for(kind: str, policy: Dict[str, Any]) -> Dict[str, Any]:
    p = _get(policy, ["retry", kind], {}) or {}
    base = dict(DEFAULTS.get(kind, {}))
    for k, v in p.items():
        base[k] = v
    return base


def _next_backoff_ms(base: int, factor: float, jitter: int, attempts_done: int, salt: str) -> int:
    # E.g., attempts_done=0 -> base, 1 -> base*factor, ...
    raw = base * (factor ** max(0, attempts_done))
    jitter_val = 0 if jitter <= 0 else (_hash_to_int(salt) % max(1, jitter))
    return int(min(120000, max(0, raw + jitter_val)))


def _attempts_map(inp: Dict[str, Any]) -> Dict[str, int]:
    hist = _get(inp, ["driver", "history", "attempts"], {}) or {}
    out: Dict[str, int] = {}
    for k, v in hist.items():
        if isinstance(k, str) and isinstance(v, (int, float)):
            out[k] = int(v)
    return out


def _job_index(jobs: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for j in jobs:
        if not isinstance(j, dict):
            continue
        jid = j.get("job_id") or j.get("id")
        if isinstance(jid, str):
            idx[jid] = j
    return idx


# ------------------------- failure collectors -------------------------

def _failed_skill_req_ids(normalized: Dict[str, Any]) -> List[str]:
    items = _get(normalized, ["executor", "results", "items"], []) or []
    out: List[str] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if not _as_bool(it.get("ok"), True):
            rid = it.get("req_id")
            if isinstance(rid, str):
                out.append(rid)
    return out


def _transport_failed(normalized: Dict[str, Any]) -> bool:
    return not _as_bool(_get(normalized, ["transport", "outbound", "ok"], True), True)


def _storage_failed(normalized: Dict[str, Any]) -> Tuple[bool, bool]:
    ap_ok = _as_bool(_get(normalized, ["storage", "apply_result", "ok"], True), True)
    ix_ok = _as_bool(_get(normalized, ["storage", "index_result", "ok"], True), True)
    return (not ap_ok), (not ix_ok)


# ------------------------- planners per subsystem -------------------------

def _plan_skills_retry(jobs: List[Dict[str, Any]], failed_req_ids: List[str], attempts: Dict[str, int],
                       policy: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], int]:
    # Find last sent skills job
    sk_jobs = [j for j in jobs if j.get("type") == "skills.execute"]
    if not sk_jobs or not failed_req_ids:
        return None, 0
    last = sk_jobs[-1]
    batch = _cap_list(last.get("batch"), MAX_REQS)
    # Filter only failed reqs (preserve params/limits)
    to_retry = [r for r in batch if isinstance(r.get("req_id"), str) and r["req_id"] in set(failed_req_ids)]
    if not to_retry:
        return None, 0

    jid = last.get("job_id", "skills")
    done = attempts.get(jid, 0)
    if done >= int(policy["max_attempts"]):
        return None, 0

    backoff = _next_backoff_ms(int(policy["backoff_ms"]), float(policy["factor"]), int(policy["jitter_ms"]), done, jid)
    retry_job = {
        "type": "skills.execute",
        "batch": to_retry,
        "limits": {
            "timeout_ms": int(_get(last, ["limits", "timeout_ms"], 30000)),
            "max_inflight": int(_get(last, ["limits", "max_inflight"], 4))
        },
        "defer": list(_get(last, ["defer"], []) or []),
        "idempotency_key": last.get("idempotency_key"),
        "deadline_ms": int(last.get("deadline_ms", 35000))
    }
    return retry_job, backoff


def _plan_transport_retry(jobs: List[Dict[str, Any]], attempts: Dict[str, int], policy: Dict[str, Any], failed: bool) -> \
Tuple[Optional[Dict[str, Any]], int]:
    if not failed:
        return None, 0
    tr_jobs = [j for j in jobs if j.get("type") == "transport.emit"]
    if not tr_jobs:
        return None, 0
    last = tr_jobs[-1]
    items = _cap_list(last.get("items"), MAX_EMIT)
    if not items:
        return None, 0
    jid = last.get("job_id", "transport")
    done = attempts.get(jid, 0)
    if done >= int(policy["max_attempts"]):
        return None, 0
    backoff = _next_backoff_ms(int(policy["backoff_ms"]), float(policy["factor"]), int(policy["jitter_ms"]), done, jid)
    retry_job = {
        "type": "transport.emit",
        "items": items,
        "idempotency_key": last.get("idempotency_key"),
        "deadline_ms": int(last.get("deadline_ms", 8000))
    }
    return retry_job, backoff


def _plan_storage_retry(jobs: List[Dict[str, Any]], attempts: Dict[str, int], policy: Dict[str, Any],
                        apply_failed: bool, index_failed: bool) -> Tuple[Optional[Dict[str, Any]], int]:
    if not (apply_failed or index_failed):
        return None, 0
    st_jobs = [j for j in jobs if j.get("type") == "storage.apply_index"]
    if not st_jobs:
        return None, 0
    last = st_jobs[-1]
    apply_ops = _cap_list(last.get("apply_ops"), MAX_APPLY) if apply_failed else []
    index_q = _cap_list(last.get("index_queue"), MAX_INDEX) if index_failed else []
    if not apply_ops and not index_q:
        return None, 0
    jid = last.get("job_id", "storage")
    done = attempts.get(jid, 0)
    if done >= int(policy["max_attempts"]):
        return None, 0
    backoff = _next_backoff_ms(int(policy["backoff_ms"]), float(policy["factor"]), int(policy["jitter_ms"]), done, jid)
    retry_job = {
        "type": "storage.apply_index",
        "namespace": last.get("namespace") or "store/noema/default",
        "apply_ops": apply_ops,
        "index_queue": index_q,
        "idempotency_key": last.get("idempotency_key"),
        "deadline_ms": int(last.get("deadline_ms", 12000))
    }
    return retry_job, backoff


# ------------------------- main -------------------------

def b13f3_plan_retry(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B13F3 — Drivers.RetryPlanner (Noema)
    Builds a deterministic retry plan (pure; no I/O) based on normalized replies and previously sent jobs.

    Input:
      {
        "driver": {
          "jobs": [ {type, job_id, idempotency_key, ...}, ... ],          # previously sent jobs
          "history": { "attempts": { "<job_id>": int, ... } }?            # attempts per job_id
        },
        "executor": { "results": {"items":[{ok, req_id, ...}], ...} }?,   # from B13F2
        "transport": { "outbound": {"ok": bool} }?,                       # from B13F2
        "storage": { "apply_result": {"ok": bool}, "index_result": {"ok": bool} }?,  # from B13F2
        "policy": {
          "retry": {
            "skills":    {"max_attempts": int, "backoff_ms": int, "factor": float, "jitter_ms": int}?,
            "transport": {"max_attempts": int, "backoff_ms": int, "factor": float, "jitter_ms": int}?,
            "storage":   {"max_attempts": int, "backoff_ms": int, "factor": float, "jitter_ms": int}?
          }
        }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "driver": {
          "retry": {
            "jobs": [ {type,...}, ... ],          # same shape as B12F3 jobs (without job_id; will be assigned later)
            "backoff_ms": int,                    # max backoff among planned jobs
            "attempts_next": { "<job_id>": int }, # attempts after applying this plan (for bookkeeping)
            "meta": { "source": "B13F3", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|nothing_to_retry", "counts": { "jobs": int } }
      }
    """
    jobs = _get(input_json, ["driver", "jobs"], []) or []
    if not isinstance(jobs, list) or not jobs:
        return {"status": "SKIP", "driver": {"retry": {"jobs": [], "backoff_ms": 0, "attempts_next": {},
                                                       "meta": {"source": "B13F3", "rules_version": RULES_VERSION}}},
                "diag": {"reason": "nothing_to_retry", "counts": {"jobs": 0}}}

    attempts = _attempts_map(input_json)
    pol_all = _get(input_json, ["policy"], {}) or {}

    # Failures
    failed_req_ids = _failed_skill_req_ids(input_json)
    tr_failed = _transport_failed(input_json)
    ap_failed, ix_failed = _storage_failed(input_json)

    retry_jobs: List[Dict[str, Any]] = []
    backoffs: List[int] = []
    attempts_next = dict(attempts)

    # Skills
    sk_pol = _policy_for("skills", pol_all)
    sk_job, sk_backoff = _plan_skills_retry(jobs, failed_req_ids, attempts, sk_pol)
    if sk_job:
        retry_jobs.append(sk_job);
        backoffs.append(sk_backoff)
        # bump attempts for last skills job_id
        last_sk = [j for j in jobs if j.get("type") == "skills.execute"]
        if last_sk:
            jid = last_sk[-1].get("job_id", "skills")
            attempts_next[jid] = attempts.get(jid, 0) + 1

    # Transport
    tr_pol = _policy_for("transport", pol_all)
    tr_job, tr_backoff = _plan_transport_retry(jobs, attempts, tr_pol, tr_failed)
    if tr_job:
        retry_jobs.append(tr_job);
        backoffs.append(tr_backoff)
        last_tr = [j for j in jobs if j.get("type") == "transport.emit"]
        if last_tr:
            jid = last_tr[-1].get("job_id", "transport")
            attempts_next[jid] = attempts.get(jid, 0) + 1

    # Storage
    st_pol = _policy_for("storage", pol_all)
    st_job, st_backoff = _plan_storage_retry(jobs, attempts, st_pol, ap_failed, ix_failed)
    if st_job:
        retry_jobs.append(st_job);
        backoffs.append(st_backoff)
        last_st = [j for j in jobs if j.get("type") == "storage.apply_index"]
        if last_st:
            jid = last_st[-1].get("job_id", "storage")
            attempts_next[jid] = attempts.get(jid, 0) + 1

    if not retry_jobs:
        return {"status": "SKIP", "driver": {"retry": {"jobs": [], "backoff_ms": 0, "attempts_next": attempts_next,
                                                       "meta": {"source": "B13F3", "rules_version": RULES_VERSION}}},
                "diag": {"reason": "nothing_to_retry", "counts": {"jobs": 0}}}

    backoff_ms = max(backoffs) if backoffs else 0

    return {
        "status": "OK",
        "driver": {
            "retry": {
                "jobs": retry_jobs,
                "backoff_ms": int(backoff_ms),
                "attempts_next": attempts_next,
                "meta": {"source": "B13F3", "rules_version": RULES_VERSION}
            }
        },
        "diag": {"reason": "ok", "counts": {"jobs": len(retry_jobs)}},
    }


if __name__ == "__main__":
    # Minimal demo (pure)
    sample = {
        "driver": {
            "jobs": [
                {"type": "transport.emit", "items": [{"role": "assistant", "move": "answer", "text": "Done."}],
                 "idempotency_key": "em1", "deadline_ms": 7000, "job_id": "J1"},
                {"type": "skills.execute", "batch": [
                    {"req_id": "r1", "skill_id": "skill.web_summarize", "params": {"url": "https://ex/a"}},
                    {"req_id": "r2", "skill_id": "skill.web_summarize", "params": {"url": "https://ex/b"}}
                ], "limits": {"timeout_ms": 28000, "max_inflight": 2}, "defer": [], "idempotency_key": "sk1",
                 "deadline_ms": 32000, "job_id": "J2"},
                {"type": "storage.apply_index", "namespace": "store/noema/t-99",
                 "apply_ops": [{"op": "put", "key": "k/a", "value": {"x": 1}}],
                 "index_queue": [{"type": "packz", "id": "u1", "ns": "store/noema/t-99"}], "idempotency_key": "st1",
                 "deadline_ms": 10000, "job_id": "J3"}
            ],
            "history": {"attempts": {"J1": 1, "J2": 0, "J3": 1}}
        },
        "transport": {"outbound": {"ok": False}},
        "executor": {"results": {"items": [
            {"ok": False, "req_id": "r2", "text": "timeout"},
            {"ok": True, "req_id": "r1", "text": "ok"}
        ]}},
        "storage": {"apply_result": {"ok": True}, "index_result": {"ok": False}},
        "policy": {
            "retry": {"skills": {"max_attempts": 3}, "transport": {"max_attempts": 2}, "storage": {"max_attempts": 2}}}
    }
    out = b13f3_plan_retry(sample)
    print(out["diag"])
    print("backoff_ms:", out["driver"]["retry"]["backoff_ms"])
    for j in out["driver"]["retry"]["jobs"]:
        print(j["type"], list(j.keys()))
