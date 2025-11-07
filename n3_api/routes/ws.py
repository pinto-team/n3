# noema/n3_api/routes/ws.py
import asyncio
import json
from contextlib import asynccontextmanager

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from n3_api.utils.drivers import build_drivers_safe
from n3_api.utils.state import ensure_state, update_state
from n3_runtime.loop.io_tick import run_tick_io

def _build_drivers_safe():
    """Build drivers and FORCE the required shape for run_tick_io and WS:
       - drivers["skills"]["execute"] is callable
       - drivers["transport"]["emit"] and ["outbox"] are callable
       - also provide flat aliases: transport_emit / transport_outbox
    """
    # 1) try external builder
    drivers = {}
    try:
        from examples.minimal_chat.drivers_dev import build_drivers
        d = build_drivers() or {}
        if isinstance(d, dict):
            drivers.update(d)
    except Exception:
        pass

    # 2) normalize skills
    skills = drivers.get("skills")
    if isinstance(skills, dict) and callable(skills.get("execute")):
        pass
    elif callable(skills):
        drivers["skills"] = {"execute": skills}
    else:
        from n3_drivers.skills import local_runner
        drivers["skills"] = {"execute": local_runner.execute}

    # 3) normalize transport
    t_emit = t_out = None
    t = drivers.get("transport")
    if isinstance(t, dict):
        t_emit = t.get("emit")
        t_out  = t.get("outbox")

    # try http_dev if missing/incomplete
    if not (callable(t_emit) and callable(t_out)):
        try:
            from n3_drivers.transport import http_dev
            t_emit = getattr(http_dev, "emit", t_emit)
            t_out  = getattr(http_dev, "outbox", t_out)
        except Exception:
            pass

    # last-resort in-memory transport
    if not callable(t_emit) or not callable(t_out):
        _BUF = []
        def _emit(item):
            _BUF.append(item); return True
        def _outbox():
            return list(_BUF)
        t_emit, t_out = _emit, _outbox

    drivers["transport"] = {"emit": t_emit, "outbox": t_out}
    drivers["transport_emit"] = t_emit         # flat alias
    drivers["transport_outbox"] = t_out        # flat alias
    return drivers

_DRIVERS = _build_drivers_safe()

router = APIRouter(prefix="/ws", tags=["WS"])

@asynccontextmanager
async def lifespan(app):
    yield

# PUSH: poll outbox every 200ms (no need to wait for client messages)
@router.websocket("/{thread_id}")
async def ws_push(ws: WebSocket, thread_id: str):
    await ws.accept()
    try:
        sent = 0
        while True:
            ob = _DRIVERS["transport_outbox"]()
            for item in ob[sent:]:
                txt = item.get("text")
                await ws.send_text(txt if isinstance(txt, str) else str(item))
            sent = len(ob)
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        return


_DRIVERS = build_drivers_safe()

router = APIRouter(prefix="/ws", tags=["WS"])

@asynccontextmanager
async def lifespan(app):
    yield

# PUSH: poll outbox every 200ms
@router.websocket("/{thread_id}")
async def ws_push(ws: WebSocket, thread_id: str):
    await ws.accept()
    try:
        sent = 0
        while True:
            ob = _DRIVERS["transport_outbox"]()
            for item in ob[sent:]:
                txt = item.get("text")
                await ws.send_text(txt if isinstance(txt, str) else str(item))
            sent = len(ob)
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        return

# CHAT: دو مرحله‌ای (search → answer → emit)
@router.websocket("/chat/{thread_id}")
async def ws_chat(ws: WebSocket, thread_id: str):
    await ws.accept()
    try:
        while True:
            text = await ws.receive_text()
            state = ensure_state(thread_id)

            # Phase 1: search
            state.setdefault("executor", {}).setdefault("requests", []).append(
                {"req_id": "r-chat", "skill_id": "skill.dev.search", "params": {"q": text, "k": 5}}
            )
            state = run_tick_io(state, _DRIVERS)

            items = (((state.get("executor") or {}).get("results") or {}).get("items") or [])
            answer = ""
            for item in items:
                if item.get("req_id") != "r-chat":
                    continue
                data = item.get("data") if isinstance(item.get("data"), dict) else {}
                hits = data.get("hits") if isinstance(data.get("hits"), list) else []
                if not hits:
                    continue
                top = hits[0] if isinstance(hits[0], dict) else {}
                answer = top.get("snippet") or top.get("text") or ""
                if not answer:
                    answer = json.dumps(top, ensure_ascii=False)
                break

            if not answer:
                answer = json.dumps({"echo": text}, ensure_ascii=False)

            # Phase 2: finalize and emit
            state.setdefault("executor", {})["requests"] = []
            state.setdefault("dialog", {})["final"] = {"move": "answer", "text": answer}
            state = run_tick_io(state, _DRIVERS)
            update_state(thread_id, state)

            await ws.send_json({"text": answer})
    except WebSocketDisconnect:
        return
