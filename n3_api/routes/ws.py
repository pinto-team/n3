# ============================
# File: noema/n3_api/routes/ws.py
# ============================

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, FastAPI
from contextlib import asynccontextmanager
from n3_api.utils.state import ensure_state, update_state, get_sessions
from n3_runtime.loop.io_tick import run_tick_io
from n3_drivers.transport import http_dev
from examples.minimal_chat.drivers_dev import build_drivers
import asyncio

router = APIRouter(tags=["WebSocket"])
_DRIVERS = build_drivers()


@router.websocket("/ws/{thread_id}")
async def ws_thread(websocket: WebSocket, thread_id: str):
    """Push channel for real-time transport output."""
    await websocket.accept()
    q = http_dev.subscribe()
    try:
        while True:
            msg = await q.get()
            await websocket.send_json(msg)
    except WebSocketDisconnect:
        http_dev.unsubscribe(q)


@router.websocket("/ws/chat/{thread_id}")
async def ws_chat(websocket: WebSocket, thread_id: str):
    """Interactive chat WebSocket."""
    await websocket.accept()
    ensure_state(thread_id)
    try:
        while True:
            payload = await websocket.receive_text()
            state = ensure_state(thread_id)
            state.setdefault("executor", {}).setdefault("requests", []).append({
                "req_id": "r-chat",
                "skill_id": "skill.dev.search",
                "params": {"q": payload, "k": 5},
            })
            state = run_tick_io(state, _DRIVERS)
            items = (((state.get("executor") or {}).get("results") or {}).get("items") or [])
            hits = items[0]["data"].get("hits", []) if items and isinstance(items[0].get("data"), dict) else []
            answer = hits[0].get("snippet") if hits else f'{{"echo":"{payload}"}}'
            state.setdefault("executor", {})["requests"] = []
            state.setdefault("dialog", {})["final"] = {"move": "answer", "text": answer}
            state = run_tick_io(state, _DRIVERS)
            update_state(thread_id, state)
            await websocket.send_json({"role": "assistant", "text": answer})
    except WebSocketDisconnect:
        return


# =========================================================
# Lifespan handler (replaces deprecated @router.on_event)
# =========================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context replacing on_event('startup')."""
    async def daemon():
        while True:
            try:
                for tid, state in list(get_sessions().items()):
                    new_state = run_tick_io(state, _DRIVERS)
                    update_state(tid, new_state)
            except Exception:
                pass
            await asyncio.sleep(0.25)

    task = asyncio.create_task(daemon())
    try:
        yield  # app runs while this context is active
    finally:
        task.cancel()
