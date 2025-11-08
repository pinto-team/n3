# Folder: n3_api/routes
# File:   ws.py

from __future__ import annotations

import asyncio
import json
import re
import uuid
import time
import hashlib
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from n3_api.utils.drivers import build_drivers_safe
from n3_api.utils.state import ensure_state, update_state

from n3_runtime.loop.io_tick import run_tick_io
from n3_runtime.adapters.registry import build_registry
from n3_core.kernel.b0f1_noema_kernel_step import b0f1_noema_kernel_step as kernel_step  # alias for clarity

from n3_drivers.storage import sqlite_driver
from n3_drivers.index import bm25_indexer

router = APIRouter(prefix="/ws", tags=["WS"])
_DRIVERS = build_drivers_safe()

# ---------- DB / FTS bootstrap ----------
_CONN = sqlite_driver.get_connection()
bm25_indexer.ensure_schema(_CONN)

@asynccontextmanager
async def lifespan(app):
    yield

# ---------- Utils ----------
def _say(thread_id: str, text: str) -> None:
    """Emit a message to the transport outbox (push channel will forward it)."""
    try:
        emit = _DRIVERS.get("transport_emit")
        if callable(emit) and isinstance(text, str) and text.strip():
            emit({"id": f"m-{uuid.uuid4().hex}", "thread_id": thread_id, "text": text})
    except Exception:
        pass

def _peek(state: Dict[str, Any], path: str) -> Any:
    cur: Any = state
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return None
        cur = cur[k]
    return cur

def _available_ops() -> List[str]:
    reg = build_registry()
    try:
        keys = list(reg.keys())  # type: ignore[attr-defined]
    except Exception:
        keys = []
        for cand in ("ops", "_ops"):
            try:
                keys = list(getattr(reg, cand).keys())  # type: ignore
                break
            except Exception:
                continue
    return sorted(k for k in keys if isinstance(k, str))

def _pipeline_order_dynamic() -> List[str]:
    reg_keys = set(_available_ops())
    wanted = [
        # B1
        "b1f1_collector","b1f2_normalizer","b1f3_sentence_splitter","b1f4_tokenizer",
        "b1f5_script_tagger","b1f6_addressing","b1f7_speech_act","b1f8_confidence",
        "b1f9_novelty","b1f10_packz",
        # B2
        "b2f1_context_builder","b2f2_predictor","b2f3_error_computer","b2f4_uncertainty",
        # B3
        "b3f1_wal_writer","b3f2_indexer","b3f3_retriever","b3f4_context_cache",
        # B4
        "b4f1_mine_patterns","b4f2_manage_nodes","b4f3_score_edges","b4f4_extract_rules",
        # B5
        "b5f1_intent_router","b5f2_slot_collector","b5f3_build_plan",
        # B6
        "b6f1_turn_realizer","b6f2_surface_nlg","b6f3_safety_filter",
        # B7
        "b7f1_skill_dispatcher","b7f2_result_normalizer","b7f3_result_presenter",
        # B8
        "b8f1_memory_commit","b8f2_wal_apply_planner","b8f3_apply_optimizer",
        # B9
        "b9f1_aggregate_telemetry","b9f2_trace_builder","b9f3_slo_evaluator",
        # B11
        "b11f1_activate_config","b11f2_runtime_gatekeeper","b11f3_runtime_scheduler","b11f4_initiative_scheduler",
        # B12
        "b12f1_orchestrator_tick","b12f2_action_enveloper","b12f3_driver_job_builder",
        # B13
        "b13f1_driver_protocol_builder","b13f2_driver_reply_normalizer","b13f3_driver_retry_planner",
    ]
    return [name for name in wanted if name in reg_keys]

# Normalizers / matchers
_Q_CLEAN_PUNCT = re.compile(r"[؟?!.،,:;]+", flags=re.UNICODE)
_WS_SPLIT = re.compile(r"\s+", flags=re.UNICODE)
_DEF_PAT = re.compile(r"^\s*(?P<k>.+?)\s+(?:یعنی|برابر|معنی(?:\s*اش)?|معنیش|is|means|=)\s+(?P<v>.+?)\s*$", re.IGNORECASE | re.UNICODE)
_Q_STOPWORDS = re.compile(
    r"\b(چیه|چیست|چی|یعنی\s*چی|یعنی|تعریف|معنی|what\s+is|who\s+is|meaning|define)\b",
    re.IGNORECASE | re.UNICODE,
)

def _simplify(s: str) -> str:
    if not isinstance(s, str):
        return ""
    t = _Q_CLEAN_PUNCT.sub(" ", s)
    t = _WS_SPLIT.sub(" ", t).strip().casefold()
    return t

def _extract_query_key(text: str) -> str:
    """From 'نوما چیه؟' -> 'نوما'."""
    if not isinstance(text, str):
        return ""
    t = _Q_CLEAN_PUNCT.sub(" ", text)
    t = _Q_STOPWORDS.sub(" ", t)
    t = _WS_SPLIT.sub(" ", t).strip()
    m = re.search(r"[\"'«](.+?)[\"'»]", t)
    if m and m.group(1).strip():
        return m.group(1).strip()
    return t

def _is_question(text: str) -> bool:
    if not isinstance(text, str):
        return False
    if "?" in text or "؟" in text:
        return True
    return bool(re.search(r"\b(چیه|چیست|یعنی|تعریف|meaning|what\s+is|who\s+is)\b", text, re.IGNORECASE))

def _split_defs(line: str) -> List[Tuple[str, str]]:
    """Split 'سیب یعنی میوه؛ گلابی یعنی میوه' to pairs."""
    out: List[Tuple[str, str]] = []
    if not isinstance(line, str):
        return out
    parts = re.split(r"[;\n]+|؛+", line)
    for p in parts:
        m = _DEF_PAT.match(p.strip())
        if m:
            k = m.group("k").strip(" :،؛")
            v = m.group("v").strip(" :،؛")
            if k and v and k != v:
                out.append((k, v))
    return out

def _answer_override_from_facts(thread_id: str, text: str) -> Optional[str]:
    """
    If it's a 'what is X?' question and we have X in facts, return a canonical answer.
    """
    try:
        key = _extract_query_key(text)
        if not key:
            return None

        got = sqlite_driver.fact_get(_CONN, thread_id, key)
        if got:
            k, v = got
            return f"[{k}] = {v}"

        # fallback: scan list
        rows = sqlite_driver.fact_list(_CONN, thread_id, limit=200)
        key_s = _simplify(key)
        for k, v, _ts in rows:
            if _simplify(k) == key_s or key_s in _simplify(k):
                return f"[{k}] = {v}"
        for k, v, _ts in rows:
            if key_s in _simplify(v):
                return f"[{k}] = {v}"
    except Exception:
        pass
    return None

def _index_fact_doc(thread_id: str, k: str, v: str) -> None:
    try:
        pid = f"fact:{thread_id}:{hashlib.sha1((k+'='+v).encode('utf-8')).hexdigest()}"
        bm25_indexer.index_doc(_CONN, pid, f"[{k}] = {v}")
    except Exception:
        pass

def _feed_perception_inputs(state: Dict[str, Any], user_text: str) -> None:
    state["text"] = user_text
    per = state.setdefault("perception", {})
    per.setdefault("input", {})["text"] = user_text
    per.setdefault("hints", {})["dir"] = "rtl"

def _set_mc_config(state: Dict[str, Any], u_threshold: float, rec_requires_confirm: bool) -> None:
    rt = state.setdefault("runtime", {}).setdefault("config", {})
    rt.setdefault("guardrails", {})["must_confirm"] = {
        "u_threshold": float(u_threshold),
        "rec_requires_confirm": bool(rec_requires_confirm),
    }

# ---------- Push channel (filtered) ----------
@router.websocket("/{thread_id}")
async def ws_push(ws: WebSocket, thread_id: str):
    await ws.accept()
    try:
        sent = 0
        while True:
            ob = _DRIVERS["transport_outbox"]()
            for item in ob[sent:]:
                if not isinstance(item, dict):
                    continue
                if isinstance(item.get("thread_id"), str) and item["thread_id"] != thread_id:
                    continue
                txt = item.get("text")
                if isinstance(txt, str) and txt.strip():
                    await ws.send_text(txt)
            sent = len(ob)
            await asyncio.sleep(0.2)
    except WebSocketDisconnect:
        return

# ---------- Chat channel ----------
@router.websocket("/chat/{thread_id}")
async def ws_chat(ws: WebSocket, thread_id: str):
    await ws.accept()
    try:
        while True:
            raw = await ws.receive_text()
            t = (raw or "").strip()

            # ---- service & diagnostics ----
            if t == "/wsver":
                _say(thread_id, "wsver=mc-guard-2")
                continue

            if t == "/registry":
                _say(thread_id, "REG " + json.dumps(_available_ops(), ensure_ascii=False))
                continue

            if t == "/db":
                try:
                    cur = _CONN.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view');")
                    tables = [(r[0], r[1]) for r in cur.fetchall() if isinstance(r[0], str)]
                    files = []
                    try:
                        for row in _CONN.execute("PRAGMA database_list;").fetchall():
                            files.append({"seq": row[0], "name": row[1], "file": row[2]})
                    except Exception:
                        pass
                    counts = {}
                    for name, _typ in tables:
                        if name.startswith("sqlite_"):
                            continue
                        try:
                            c = _CONN.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
                            counts[name] = int(c[0]) if c and isinstance(c[0], (int, float)) else 0
                        except Exception:
                            counts[name] = -1
                    _say(thread_id, "DB: " + json.dumps({"files": files, "tables": tables, "counts": counts}, ensure_ascii=False))
                except Exception as e:
                    _say(thread_id, f"DB error: {e}")
                continue

            if t.startswith("/peek "):
                path = t[len("/peek "):].strip()
                state = ensure_state(thread_id)
                val = _peek(state, path)
                try:
                    _say(thread_id, f"PEEK {path} " + json.dumps(val, ensure_ascii=False)[:1800])
                except Exception:
                    _say(thread_id, f"PEEK {path} {str(val)[:1000]}")
                continue

            if t == "/diag":
                state = ensure_state(thread_id)
                parts = {
                    "has.perception": bool(_peek(state, "perception")),
                    "has.world_model": bool(_peek(state, "world_model")),
                    "plan.next": (_peek(state, "planner.plan") or {}).get("next_move") if isinstance(_peek(state, "planner.plan"), dict) else None,
                    "dialog.surface": (_peek(state, "dialog.surface") or {}).get("text"),
                    "executor.aggregate": ((_peek(state, "executor.results") or {}).get("aggregate")),
                    "storage.apply": (_peek(state, "storage.apply_result") or {}),
                    "concept.version": (_peek(state, "concept_graph.version") or {}),
                }
                _say(thread_id, "DIAG " + json.dumps(parts, ensure_ascii=False))
                continue

            if t == "/reset":
                try:
                    _CONN.execute("DELETE FROM facts;")
                    _CONN.execute("DELETE FROM kv;")
                    for tbl in ("fts", "fts_data", "fts_idx", "fts_content", "fts_docsize", "fts_config"):
                        try:
                            _CONN.execute(f"DROP TABLE IF EXISTS {tbl};")
                        except Exception:
                            pass
                    _CONN.commit()
                    bm25_indexer.ensure_schema(_CONN)
                    # پاکسازی state هم خوبه:
                    update_state(thread_id, {})
                    _say(thread_id, "RESET ok")
                except Exception as e:
                    _say(thread_id, f"RESET error: {e}")
                continue

            # ---- management of must-confirm guard ----
            if t.startswith("/mc "):
                # example: /mc u=0.95 rec=off
                try:
                    parts = t[len("/mc "):].strip().split()
                    u = 0.8
                    rec = False
                    for p in parts:
                        if p.startswith("u="):
                            u = float(p.split("=", 1)[1])
                        elif p.startswith("rec="):
                            v = p.split("=", 1)[1].lower()
                            rec = v in {"1", "on", "true", "yes"}
                    state = ensure_state(thread_id)
                    _set_mc_config(state, u, rec)
                    update_state(thread_id, state)
                    _say(thread_id, f"MC " + json.dumps({"u_threshold": u, "rec_requires_confirm": rec}, ensure_ascii=False))
                except Exception as e:
                    _say(thread_id, f"MC error: {e}")
                continue

            if t == "/mc-guard-2":
                state = ensure_state(thread_id)
                _set_mc_config(state, 0.8, False)
                update_state(thread_id, state)
                _say(thread_id, "MC " + json.dumps({"u_threshold": 0.8, "rec_requires_confirm": False}))
                continue

            # ---- facts & search shortcuts ----
            if t == "/facts":
                rows = sqlite_driver.fact_list(_CONN, thread_id, limit=200)
                _say(thread_id, "FACTS " + json.dumps([{"k": k, "v": v, "ts": ts} for k, v, ts in rows], ensure_ascii=False))
                continue

            if t.startswith("/forget "):
                key = t[len("/forget "):].strip()
                n = sqlite_driver.fact_delete(_CONN, thread_id, key)
                _say(thread_id, f"FORGET removed={n}")
                continue

            if t.startswith("/search "):
                q = t[len("/search "):].strip()
                try:
                    try:
                        hits = bm25_indexer.search(_CONN, q, 5)  # prefer positional top=5
                    except TypeError:
                        # fallback to older signature
                        hits = bm25_indexer.search(_CONN, q)
                    top = []
                    for h in (hits or [])[:5]:
                        txt = (h.get("snippet") or h.get("text") or h.get("content") or "").strip()
                        if txt:
                            top.append(txt)
                    _say(thread_id, "SEARCH " + json.dumps({"q": q, "top": top}, ensure_ascii=False))
                except Exception as e:
                    _say(thread_id, f"SEARCH error: {e}")
                continue

            # ---- learning & policy commands ----
            if t.startswith("/search "):
                q = t[len("/search "):].strip()
                try:
                    try:
                        hits = bm25_indexer.search(_CONN, q, 5)
                    except TypeError:
                        hits = bm25_indexer.search(_CONN, q)
                    top = []
                    for h in (hits or [])[:5]:
                        raw = (h.get("text") or h.get("content") or h.get("snippet") or "").strip()
                        raw = re.sub(r"\[\[([^\[\]]+)\]\]", r"[\1]", raw)
                        if raw:
                            top.append(raw)
                    _say(thread_id, "SEARCH " + json.dumps({"q": q, "top": top}, ensure_ascii=False))
                except Exception as e:
                    _say(thread_id, f"SEARCH error: {e}")
                continue

            if t.startswith("/apply"):
                try:
                    reg = build_registry()
                    order = ["b10f2_plan_policy_apply","b10f3_stage_policy_apply","b11f1_activate_config"]
                    state = ensure_state(thread_id)
                    out = kernel_step(state, reg, order=order)
                    state = out.get("state", state)
                    update_state(thread_id, state)
                    _say(thread_id, "APPLY ok")
                except Exception as e:
                    _say(thread_id, f"APPLY error: {e}")
                continue

            if t.startswith("/reward"):
                try:
                    val = 1 if "+1" in t or "up" in t else -1
                    state = ensure_state(thread_id)
                    trace = state.setdefault("world_model", {}).setdefault("trace", {})
                    hist = trace.setdefault("error_history", [])
                    hist.append({"reward": val, "time": time.time()})
                    update_state(thread_id, state)
                    _say(thread_id, f"REWARD stored ({val:+d})")
                except Exception as e:
                    _say(thread_id, f"REWARD error: {e}")
                continue

            # ---- FACT teach-in (X یعنی Y; ... ) ----
            defs = _split_defs(t)
            if defs:
                for k, v in defs:
                    sqlite_driver.fact_upsert(_CONN, thread_id, k, v)
                    _index_fact_doc(thread_id, k, v)
                if len(defs) == 1:
                    _say(thread_id, f"[{defs[0][0]}] = [{defs[0][1]}]")
                else:
                    parts = "; ".join(f"[{k}] یعنی [{v}]" for k, v in defs)
                    _say(thread_id, parts)
                continue

            # ---- pipeline chat ----
            state = ensure_state(thread_id)

            # تضمین ورودی برای B1
            _feed_perception_inputs(state, t)

            # اجرای هسته (پاس اول: ساخت پیش‌بینی/پلن/سطح‌پردازی)
            order = _pipeline_order_dynamic()
            out = kernel_step(state, build_registry(), order=order if order else None)
            state = out.get("state", state)

            # **Override از فکت‌ها**: قبل از IO/Envelope نهایی، اگر سوال است و فکتی داریم، خروجی را تزریق کن
            if _is_question(t):
                ov = _answer_override_from_facts(thread_id, t)
                if ov:
                    dlg = state.setdefault("dialog", {})
                    dlg["final"] = {"move": "answer", "text": ov}
                    # یک Surface ساده هم می‌گذاریم تا بقیه بلاک‌ها سردرگم نشوند
                    dlg["surface"] = {"text": ov, "language": "fa", "move": "answer",
                                      "meta": {"source": "WS-OVERRIDE", "rules_version": "1.0"}}
                    # اگر پلان قبلی وجود داشت، next_move را هم هماهنگ کنیم
                    pl = state.setdefault("planner", {}).setdefault("plan", {})
                    pl["next_move"] = "answer"

            # حلقه IO (Envelope → Driver frames → Emit)
            state = run_tick_io(state, _DRIVERS)
            state = run_tick_io(state, _DRIVERS)
            update_state(thread_id, state)

            # نکته مهم: اینجا دیگر _say(answer) نمی‌زنیم تا پیام دوبار ارسال نشود.
            # ارسال خروجی از طریق فریم‌های ترنسپورت انجام شد.

    except WebSocketDisconnect:
        return
