# Folder: noema/n3_drivers/skills
# File:   local_runner.py

from typing import Any, Dict, List, Callable
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor
import json

__all__ = ["execute", "register_skill"]

_SKILLS: Dict[str, Callable[[Dict[str, Any]], Any]] = {}

def register_skill(skill_id: str, fn: Callable[[Dict[str, Any]], Any]) -> None:
    _SKILLS[skill_id] = fn

def _run_call(call: Dict[str, Any], timeout_ms: int) -> Dict[str, Any]:
    sid = str(call.get("skill_id") or "")
    params = call.get("params") if isinstance(call.get("params"), dict) else {}
    req_id = call.get("req_id")

    start = perf_counter()
    ok, kind, text, data, usage = True, "json", "", None, {"cost": 0.0}
    try:
        fn = _SKILLS.get(sid)
        if fn is None:
            raise RuntimeError(f"unknown skill: {sid}")
        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(fn, params)
            res = fut.result(timeout=max(0.001, timeout_ms / 1000))
        if isinstance(res, (dict, list)):
            data = res
            kind = "json"
        else:
            text = str(res)
            kind = "text"
    except Exception as e:
        ok = False
        text = f"error: {e.__class__.__name__}: {e}"
        kind = "text"
    dur_ms = int((perf_counter() - start) * 1000)
    return {"ok": ok, "req_id": req_id, "kind": kind, "text": text, "data": data, "usage": usage, "latency_ms": dur_ms, "score": 0.0, "attachments": []}

def execute(frame: Dict[str, Any]) -> Dict[str, Any]:
    """
    Consume a B13F1 skills frame and return a reply for B13F2.
    Input frame shape:
      {"type":"skills","calls":[{req_id,skill_id,params,...}], "limits":{"timeout_ms":int,"max_inflight":int}}
    Output reply shape (for B13F2):
      {"type":"skills","ok":bool,"calls":[...]}
    """
    calls = [c for c in (frame.get("calls") or []) if isinstance(c, dict)]
    tmo = int(frame.get("limits", {}).get("timeout_ms", 30000))
    results: List[Dict[str, Any]] = []
    for c in calls:
        results.append(_run_call(c, timeout_ms=tmo))
    ok = all(r.get("ok", False) for r in results) if results else True
    return {"type": "skills", "ok": ok, "calls": results}

# Register a sample dev skill
def _dev_echo(params: Dict[str, Any]) -> Any:
    return {"echo": params}

register_skill("skill.dev.echo", _dev_echo)
