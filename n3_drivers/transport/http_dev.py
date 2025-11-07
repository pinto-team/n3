# noema/n3_drivers/transport/http_dev.py
from typing import Any, Dict, List
import asyncio

__all__ = ["emit", "outbox", "subscribe", "unsubscribe"]

_OUTBOX: List[Dict[str, Any]] = []
_SUBS: List[asyncio.Queue] = []

def subscribe() -> asyncio.Queue:
    q: asyncio.Queue = asyncio.Queue()
    _SUBS.append(q)
    return q

def unsubscribe(q: asyncio.Queue) -> None:
    if q in _SUBS:
        _SUBS.remove(q)

async def _publish(msgs: List[Dict[str, Any]]) -> None:
    for q in list(_SUBS):
        for m in msgs:
            await q.put(m)

def emit(frame: Dict[str, Any]) -> Dict[str, Any]:

    msgs = frame.get("messages")
    if not msgs and "text" in frame:
        msgs = [{"text": frame["text"]}]
    msgs = msgs or []
    ch = frame.get("channel", "default")

    for m in msgs:
        _OUTBOX.append({"channel": ch, **m})

    try:
        loop = asyncio.get_running_loop()
        loop.create_task(_publish(msgs))
    except RuntimeError:
        pass

    return {"type": "transport", "ok": True, "channel": ch, "messages": msgs}

def outbox() -> List[Dict[str, Any]]:
    return list(_OUTBOX)
