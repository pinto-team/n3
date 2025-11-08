# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from n3_api.utils.drivers import build_drivers_safe
from n3_api.utils.state import ensure_state, update_state
from n3_runtime.loop.io_tick import run_tick_io

from n3_runtime.adapters.registry import build_registry
from n3_core.kernel.b0f1_noema_kernel_step import b0f1_noema_kernel_step as kernel_step  # if name differs, fix import

from n3_drivers.storage import sqlite_driver
from n3_drivers.index import bm25_indexer

router = APIRouter(prefix="/ws", tags=["WS"])
_DRIVERS = build_drivers_safe()

# DB/FTS bootstrap (single shared conn)
_CONN = sqlite_driver.get_connection()
bm25_indexer.ensure_schema(_CONN)

@asynccontextmanager
async def lifespan(app):
    yield

# ---------------- utils ----------------

_WSVER = "mc-guard-2"

_Q_STOPWORDS = re.compile(
    r"\b(چیه|چیست|چی|یعنی\s*چی|یعنی|تعریف|معنی|what\s+is|who\s+is|meaning|define|\?|:)\b",
    re.IGNORECASE | re.UNICODE,
)

def _extract_query_key(text: str) -> str:
    """
    از سوالی مثل «نوما چیه؟» یا «تهران چیست» کلید «نوما» / «تهران» را در می‌آورد.
    """
    if not isinstance(text, str):
        return ""
    t = _Q_CLEAN_PUNCT.sub(" ", text)          #
    t = _Q_STOPWORDS.sub(" ", t)               #
    t = _WS_SPLIT.sub(" ", t).strip()          #
    m = re.search(r"[\"'«](.+?)[\"'»]", t)
    if m and m.group(1).strip():
        return m.group(1).strip()
    return t


# تشخیص «X یعنی Y»
_DEF_PATTERNS = [
    re.compile(r"^\s*(?P<k>.+?)\s+(?:یعنی|برابر|معنی(?:\s*اش)?|معنیش)\s+(?P<v>.+?)\s*$"),
    re.compile(r"^\s*(?P<k>.+?)\s+(?:is|means|=)\s+(?P<v>.+?)\s*$", re.IGNORECASE),
]
_Q_CLEAN_PUNCT = re.compile(r"[؟?!.،,:;]+")
_WS_SPLIT = re.compile(r"\s+")
def _simplify(text: str) -> str:
    t = _Q_CLEAN_PUNCT.sub(" ", text or "")
    t = _WS_SPLIT.sub(" ", t).strip()
    return t

def _is_question(text: str) -> bool:
    if not text:
        return False
    t = text.strip()
    return t.endswith("؟") or t.endswith("?") or "چیه" in t or "چیست" in t

def _say(thread_id: str, text: str) -> None:
    try:
        emit = _DRIVERS.get("transport_emit")
        if callable(emit):
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
        return sorted([k for k in reg.keys() if isinstance(k, str)])  # type: ignore
    except Exception:
        return []

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
    return [w for w in wanted if w in reg_keys]

def _feed_perception_inputs(state: Dict[str, Any], user_text: str) -> None:
    state["text"] = user_text
    per = state.setdefault("perception", {})
    per.setdefault("input", {})["text"] = user_text
    per.setdefault("hints", {})["dir"] = "rtl"

def _store_fact_and_index(thread_id: str, k: str, v: str) -> None:
    # 1) persist
    sqlite_driver.fact_upsert(_CONN, thread_id, k, v)
    # 2) index in FTS (immediate, outside kernel)
    try:
        _DRIVERS["storage"]["apply_index"]({
            "type": "storage",
            "namespace": "store/noema/default",
            "apply": [],
            "index": [{"type": "fact", "k": k, "v": v}]
        })
    except Exception:
        # last resort: direct index
        try:
            bm25_indexer.index_doc(_CONN, uuid.uuid4().hex, f"{k} = {v}")
        except Exception:
            pass

def _search_fts_and_fallback(thread_id: str, q: str, topk: int = 5) -> Tuple[List[str], str]:
    q = (q or "").strip()
    texts: List[str] = []
    err = ""
    # try bm25 with multiple signatures
    try:
        hits = bm25_indexer.search(_CONN, q, topk)  # signature: (conn, q, k)
        if isinstance(hits, list):
            texts = [h.get("text") or h.get("snippet") or "" for h in hits if isinstance(h, dict)]
    except TypeError:
        try:
            hits = bm25_indexer.search(_CONN, q)  # signature: (conn, q)
            if isinstance(hits, list):
                texts = [h.get("text") or h.get("snippet") or "" for h in hits if isinstance(h, dict)]
        except Exception as e:
            err = f"{e}"
    except Exception as e:
        err = f"{e}"

    # fallback: scan facts (very fast for small sets)
    if not texts:
        rows = sqlite_driver.fact_list(_CONN, thread_id, limit=200)
        qn = _simplify(q)
        for k, v, _ts in rows:
            if _simplify(k) == qn or qn in _simplify(k):
                texts.append(f"[{k}] = {v}")
        # if still empty, try substring on v
        if not texts:
            for k, v, _ts in rows:
                if qn and (qn in _simplify(v)):
                    texts.append(f"[{k}] = {v}")

    return [t for t in texts if t], err

def _answer_override_from_facts(thread_id: str, text: str) -> Optional[str]:
    """
    اگر سوال بود، از استور فکت جواب بساز: [X] = Y
    """
    try:
        # کلید تمیزشده از سوال (مثلاً «نوما» از «نوما چیه؟»)
        key = _extract_query_key(text)
        if not key:
            return None

        # 1) تلاش با کلید مستقیم
        got = sqlite_driver.fact_get(_CONN, thread_id, key)
        if got:
            k, v = got
            return f"[{k}] = {v}"

        # 2) اگر مستقیم نبود: یک آزمون سادۀ substring روی لیست فکت‌ها
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


# ---------------- push channel ----------------

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

# ---------------- chat channel ----------------

@router.websocket("/chat/{thread_id}")
async def ws_chat(ws: WebSocket, thread_id: str):
    await ws.accept()
    try:
        while True:
            text = await ws.receive_text()
            t = (text or "").strip()

            # ------- commands -------
            if t == "/wsver":
                _say(thread_id, f"wsver={_WSVER}")
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
                    for tbl in ("fts","fts_data","fts_idx","fts_content","fts_docsize","fts_config"):
                        try: _CONN.execute(f"DROP TABLE IF EXISTS {tbl};")
                        except Exception: pass
                    _CONN.commit()
                    bm25_indexer.ensure_schema(_CONN)
                    _say(thread_id, "RESET ok")
                except Exception as e:
                    _say(thread_id, f"RESET error: {e}")
                continue

            if t.startswith("/mc"):
                # /mc u=0.95 rec=off|on
                try:
                    u_th = 0.8
                    rec_requires_confirm = False
                    m1 = re.search(r"u\s*=\s*([0-9.]+)", t)
                    if m1: u_th = float(m1.group(1))
                    m2 = re.search(r"rec\s*=\s*(on|off)", t, re.IGNORECASE)
                    if m2: rec_requires_confirm = (m2.group(1).lower() == "on")
                    state = ensure_state(thread_id)
                    state.setdefault("runtime", {}).setdefault("config", {}).setdefault("guardrails", {})["must_confirm"] = {
                        "u_threshold": u_th, "rec_requires_confirm": rec_requires_confirm
                    }
                    update_state(thread_id, state)
                    _say(thread_id, "MC " + json.dumps({"u_threshold": u_th, "rec_requires_confirm": rec_requires_confirm}, ensure_ascii=False))
                except Exception as e:
                    _say(thread_id, f"MC error: {e}")
                continue

            if t.startswith("/facts"):
                rows = sqlite_driver.fact_list(_CONN, thread_id, limit=100)
                out = [{"k": k, "v": v, "ts": ts} for k, v, ts in rows]
                _say(thread_id, "FACTS " + json.dumps(out, ensure_ascii=False))
                continue

            if t.startswith("/forget"):
                # /forget <key>
                key = t.replace("/forget", "", 1).strip()
                n = sqlite_driver.fact_delete(_CONN, thread_id, key) if key else 0
                _say(thread_id, f"FORGET removed={n}")
                continue

            if t.startswith("/search"):
                # /search <query>
                q = t.replace("/search", "", 1).strip()
                top, err = _search_fts_and_fallback(thread_id, q, topk=5)
                if err:
                    _say(thread_id, f"SEARCH error: {err}")
                else:
                    _say(thread_id, "SEARCH " + json.dumps({"q": q, "top": top[:5]}, ensure_ascii=False))
                continue

            if t.startswith("/train"):
                try:
                    reg = build_registry()
                    order = [
                        "b4f1_mine_patterns","b4f2_manage_nodes","b4f3_score_edges","b4f4_extract_rules",
                        "b10f1_plan_policy_delta","b9f1_aggregate_telemetry"
                    ]
                    state = ensure_state(thread_id)
                    out = kernel_step(state, reg, order=order)
                    state = out.get("state", state)
                    update_state(thread_id, state)
                    version = _peek(state, "concept_graph.version")
                    conf = _peek(state, "policy.confidence")
                    _say(thread_id, f"TRAIN ok; concept.version={version}, confidence={conf}")
                except Exception as e:
                    _say(thread_id, f"TRAIN error: {e}")
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
                    trace.setdefault("error_history", []).append({"reward": val, "time": time.time()})
                    update_state(thread_id, state)
                    _say(thread_id, f"REWARD stored ({val:+d})")
                except Exception as e:
                    _say(thread_id, f"REWARD error: {e}")
                continue

            # ------- normal text -------
            # 0) تعریف؟ → persist + index + پاسخ کوتاه
            m_def = None
            for p in _DEF_PATTERNS:
                m_def = p.match(t)
                if m_def: break
            if m_def:
                k = (m_def.group("k") or "").strip(" :،؛")
                v = (m_def.group("v") or "").strip(" :،؛")
                if k and v and k != v:
                    _store_fact_and_index(thread_id, k, v)
                    _say(thread_id, f"[{k}] = [{v}]")
                    continue

            # 1) pipeline
            state = ensure_state(thread_id)
            _feed_perception_inputs(state, t)
            order = _pipeline_order_dynamic()
            out = kernel_step(state, build_registry(), order=order if order else None)
            state = out.get("state", state)

            # 2) I/O passes
            state = run_tick_io(state, _DRIVERS)
            state = run_tick_io(state, _DRIVERS)
            update_state(thread_id, state)

            # 3) answer extraction
            dlg = (state.get("dialog") or {})
            fin = (dlg.get("final") or {})
            surf = (dlg.get("surface") or {})
            answer = fin.get("text") or surf.get("text") or ""

            # 4) Answer override از فکت‌ها (برای سوال‌ها)
            if _is_question(t):
                ov = _answer_override_from_facts(thread_id, t)  # <== متن خام سوال را بده
                if ov:
                    answer = ov

            if not answer:
                answer = "No output from pipeline."

            _say(thread_id, answer)

    except WebSocketDisconnect:
        return
