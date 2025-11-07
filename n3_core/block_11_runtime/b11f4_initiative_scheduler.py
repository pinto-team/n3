# Folder: n3_core/block_11_runtime
# File:   b11f4_initiative_scheduler.py

from __future__ import annotations
from typing import Any, Dict, List

__all__ = ["b11f4_initiative_scheduler"]

def _now_ms(state: Dict[str, Any]) -> int:
    return int(((state.get("clock") or {}).get("now_ms") or 0))

def b11f4_initiative_scheduler(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Picks due initiative items and turns them into dialog.final (say) or executor.requests (run_skill).
    Pure; expects time via state.clock.now_ms.
    """
    q = ((state.get("initiative") or {}).get("queue") or [])
    if not isinstance(q, list):
        return {"status": "SKIP", "diag": {"reason": "no_queue"}}

    now = _now_ms(state)
    if now <= 0:
        return {"status": "SKIP", "diag": {"reason": "no_clock"}}

    taken = 0
    new_q: List[Dict[str, Any]] = []
    dialog_busy = bool(((state.get("dialog") or {}).get("final") or {}))

    # ensure containers
    state.setdefault("executor", {}).setdefault("requests", [])

    for it in q:
        if not isinstance(it, dict):
            continue
        when_ms = int(it.get("when_ms") or 0)
        typ = str(it.get("type") or "")
        once = bool(it.get("once", True))
        cooldown = int(it.get("cooldown_ms") or 0)
        payload = it.get("payload") or {}

        due = when_ms > 0 and when_ms <= now
        if not due:
            new_q.append(it)
            continue

        if typ == "say" and not dialog_busy:
            text = str(payload.get("text") or "")
            if text:
                state.setdefault("dialog", {})["final"] = {"move": "answer", "text": text, "origin": "initiative"}
                dialog_busy = True
                taken += 1
                if not once and cooldown > 0:
                    it["when_ms"] = now + cooldown
                    new_q.append(it)
                continue

        elif typ == "run_skill":
            req = payload.get("req")
            if isinstance(req, dict):
                state["executor"]["requests"].append(req)
                taken += 1
                if not once and cooldown > 0:
                    it["when_ms"] = now + cooldown
                    new_q.append(it)
                continue

        # if we got here, either invalid payload or blocked; keep the item for later
        new_q.append(it)

    out = {
        "status": "OK",
        "initiative": {"queue": new_q, "stats": {"taken": taken, "remain": len(new_q)}},
    }
    return out
