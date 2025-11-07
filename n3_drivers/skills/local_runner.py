# noema/n3_drivers/skills/local_runner.py
from typing import Any, Dict, List, Callable
from time import perf_counter
from concurrent.futures import ThreadPoolExecutor

__all__ = ["execute", "register_skill"]

_SKILLS: Dict[str, Callable[[Dict[str, Any]], Any]] = {}

_INDEX: List[Dict[str, str]] = []  # [{"id": str, "text": str}]

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
    return {
        "ok": ok,
        "req_id": req_id,
        "kind": kind,
        "text": text,
        "data": data,
        "usage": usage,
        "latency_ms": dur_ms,
        "score": 0.0,
        "attachments": [],
    }

def execute(frame: Dict[str, Any]) -> Dict[str, Any]:

    calls = [c for c in (frame.get("calls") or []) if isinstance(c, dict)]
    tmo = int(frame.get("limits", {}).get("timeout_ms", 30000))
    results: List[Dict[str, Any]] = []
    for c in calls:
        results.append(_run_call(c, timeout_ms=tmo))
    ok = all(r.get("ok", False) for r in results) if results else True
    return {"type": "skills", "ok": ok, "calls": results}


def _dev_echo(params: Dict[str, Any]) -> Any:
    return {"echo": params}

def _dev_ingest(params: Dict[str, Any]) -> Any:
    doc_id = str(params.get("id") or f"doc:{len(_INDEX)+1}")
    text = str(params.get("text") or "")
    if not text.strip():
        raise ValueError("empty text")
    _INDEX.append({"id": doc_id, "text": text})
    return {"ok": True, "id": doc_id, "count": len(_INDEX)}

def _score(q: str, t: str) -> float:
    qw = {w for w in q.lower().split() if len(w) > 1}
    tw = {w for w in t.lower().split() if len(w) > 1}
    if not qw or not tw:
        return 0.0
    inter = len(qw & tw)
    return inter / (len(qw) ** 0.5 * len(tw) ** 0.5)

def _snippet(t: str, q: str, max_len: int = 200) -> str:
    if len(t) <= max_len:
        return t
    return t[:max_len] + "…"

def _dev_search(params: Dict[str, Any]) -> Any:
    q = str(params.get("q") or "")
    k = int(params.get("k") or 5)
    scored = []
    for d in _INDEX:
        s = _score(q, d["text"])
        if s > 0:
            scored.append((s, d))
    scored.sort(key=lambda x: x[0], reverse=True)
    hits = [
        {"id": d["id"], "score": float(s), "snippet": _snippet(d["text"], q)}
        for s, d in scored[:k]
    ]
    return {"hits": hits}

def _dev_reward(params: Dict[str, Any]) -> Any:
    score = float(params.get("score", 0.0))
    reason = str(params.get("reason", ""))
    return {"reward": {"score": score, "reason": reason}}

register_skill("skill.dev.echo", _dev_echo)
register_skill("skill.dev.ingest", _dev_ingest)
register_skill("skill.dev.search", _dev_search)
register_skill("skill.dev.reward", _dev_reward)
