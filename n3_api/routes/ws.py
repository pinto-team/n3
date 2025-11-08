import asyncio
import json
import re
import uuid
import time
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional, Tuple

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from n3_api.utils.drivers import build_drivers_safe
from n3_api.utils.state import ensure_state, update_state
from n3_runtime.loop.io_tick import run_tick_io

from n3_drivers.storage import sqlite_driver
from n3_drivers.index import bm25_indexer

router = APIRouter(prefix="/ws", tags=["WS"])
_DRIVERS = build_drivers_safe()

# single shared DB connection + ensure schema for BM25
_CONN = sqlite_driver.get_connection()
bm25_indexer.ensure_schema(_CONN)

@asynccontextmanager
async def lifespan(app):
    yield

# ---------------- text utilities ----------------

_DEF_PATTERNS = [
    re.compile(r"^\s*(?P<k>.+?)\s+(?:یعنی|برابر|معنی(?:\s*اش)?|معنیش)\s+(?P<v>.+?)\s*$"),
    re.compile(r"^\s*(?P<k>.+?)\s+(?:is|means|=)\s+(?P<v>.+?)\s*$", re.IGNORECASE),
]
_FIND_ALL_PATTERNS = [
    re.compile(r"(?P<k>[^=\n\r;]+?)\s+(?:یعنی|برابر|معنی(?:\s*اش)?|معنیش)\s+(?P<v>[^=\n\r;]+?)(?=;|\n|$)"),
    re.compile(r"(?P<k>[^=\n\r;]+?)\s+(?:is|means|=)\s+(?P<v>[^=\n\r;]+?)(?=;|\n|$)", re.IGNORECASE),
]
_Q_CLEAN_PUNCT = re.compile(r"[؟?!.،,:;]+")
_Q_STOPWORDS = re.compile(
    r"\b(چیه|چیست|چی|یعنی\s*چی|یعنی|تعریف|بگو|درباره|در\s*باره|راجع|راجب|what\s+is|meaning|define|who\s+is|what)\b",
    re.IGNORECASE,
)

def _simplify_query(text: str) -> str:
    t = _Q_CLEAN_PUNCT.sub(" ", text)
    t = _Q_STOPWORDS.sub(" ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return t

def _extract_all_definitions(text: str) -> List[Dict[str, str]]:
    defs: List[Dict[str, str]] = []
    for pat in _FIND_ALL_PATTERNS:
        for m in pat.finditer(text):
            k = str(m.group("k")).strip(" :،؛")
            v = str(m.group("v")).strip(" :،؛")
            if k and v and k != v:
                defs.append({"k": k, "v": v})
    seen = set()
    out: List[Dict[str, str]] = []
    for d in defs:
        key = (d["k"], d["v"])
        if key in seen:
            continue
        seen.add(key)
        out.append(d)
    return out

def _hit_text(h: Dict[str, Any]) -> str:
    if not isinstance(h, dict):
        return ""
    for key in ("snippet", "text", "content", "doc", "value"):
        v = h.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return json.dumps(h, ensure_ascii=False)[:160]

def _say(thread_id: str, text: str) -> None:
    try:
        emit = _DRIVERS.get("transport_emit")
        if callable(emit):
            emit({"id": f"m-{uuid.uuid4().hex}", "thread_id": thread_id, "text": text})
    except Exception:
        pass

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
                it_tid = item.get("thread_id")
                if isinstance(it_tid, str) and it_tid != thread_id:
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
            t = text.strip()

            # ---- commands (push-first) ----
            if t == "/wsver":
                _say(thread_id, "wsver=robust-db-5")
                continue

            if t == "/db":
                try:
                    cur = _CONN.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table','view');")
                    tables = [(r[0], r[1]) for r in cur.fetchall() if isinstance(r[0], str)]
                    dbfiles = []
                    try:
                        for row in _CONN.execute("PRAGMA database_list;").fetchall():
                            dbfiles.append({"seq": row[0], "name": row[1], "file": row[2]})
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
                    _say(thread_id, "DB: " + json.dumps({"files": dbfiles, "tables": tables, "counts": counts}, ensure_ascii=False))
                except Exception as e:
                    _say(thread_id, f"DB error: {e}")
                continue

            if t.startswith("/search "):
                q_raw = t[len("/search "):].strip()
                q = _simplify_query(q_raw) or q_raw
                bm25_indexer.ensure_schema(_CONN)
                try:
                    hits = bm25_indexer.search(_CONN, q, limit=10) or []
                    preview = [_hit_text(h) for h in hits[:3]]
                    _say(thread_id, "SEARCH " + json.dumps({"q": q, "top": preview}, ensure_ascii=False))
                except Exception as e:
                    _say(thread_id, f"SEARCH error: {e}")
                continue

            if t == "/facts":
                facts = sqlite_driver.fact_list(_CONN, thread_id, limit=200)
                out = [{"k": k, "v": v} for (k, v, _ts) in facts]
                _say(thread_id, "FACTS " + json.dumps(out, ensure_ascii=False))
                continue

            if t.startswith("/forget "):
                key = t[len("/forget "):].strip()
                n = sqlite_driver.fact_delete(_CONN, thread_id, key)
                _say(thread_id, f"FORGET removed={n}")
                continue

            if t.startswith("/reward"):
                arg = (t.split(" ", 1)[1].strip() if " " in t else "").lower()
                val = 1.0 if arg in {"+1", "good", "ok", "true", "yes"} else (-1.0 if arg in {"-1", "bad", "no", "false"} else 0.0)
                state = ensure_state(thread_id)
                trace = state.setdefault("world_model", {}).setdefault("trace", {}).setdefault("error_history", [])
                trace.append({
                    "ts": time.time(),
                    "reward": float(val),
                    "target": "direct_answer",
                    "actual": "direct_answer",
                    "top_pred": "direct_answer"
                })
                update_state(thread_id, state)
                _say(thread_id, f"REWARD {val:+.0f}")
                continue

            if t == "/train":
                try:
                    from n3_core.kernel.b0f1_noema_kernel_step import b0f1_kernel_step
                    from n3_runtime.adapters.registry import build_registry
                    state = ensure_state(thread_id)
                    order = [
                        "b4f1_mine_patterns",
                        "b4f2_manage_nodes",
                        "b4f3_score_edges",
                        "b4f4_extract_rules",
                        "b10f1_plan_policy_delta",
                        "b9f1_aggregate_telemetry",
                    ]
                    out = b0f1_kernel_step(state, build_registry(), order=order)
                    state = out.get("state", state)
                    update_state(thread_id, state)
                    adaptation = (state.get("adaptation") or {}).get("policy", {}) or {}
                    concept = (state.get("concept_graph") or {})
                    _say(
                        thread_id,
                        "TRAIN " + json.dumps(
                            {
                                "updates": adaptation.get("updates", 0),
                                "avg_reward": adaptation.get("avg_reward", 0.0),
                                "confidence": adaptation.get("confidence", 0.0),
                                "version": adaptation.get("learning_version"),
                                "concept_version": (concept.get("version") or {}).get("id"),
                            },
                            ensure_ascii=False,
                        ),
                    )
                except Exception as e:
                    _say(thread_id, f"TRAIN error: {e}")
                continue

            if t == "/apply":
                try:
                    from n3_core.kernel.b0f1_noema_kernel_step import b0f1_kernel_step
                    from n3_runtime.adapters.registry import build_registry
                    state = ensure_state(thread_id)
                    out = b0f1_kernel_step(
                        state, build_registry(),
                        order=["b10f2_plan_policy_apply", "b10f3_stage_policy_apply", "b11f1_activate_config"]
                    )
                    state = out.get("state", state)
                    update_state(thread_id, state)
                    runtime = (state.get("runtime") or {})
                    version = (runtime.get("version") or {})
                    _say(thread_id, "APPLY " + json.dumps({"activated_version": version}, ensure_ascii=False))
                except Exception as e:
                    _say(thread_id, f"APPLY error: {e}")
                continue

            if t == "/diag":
                state = ensure_state(thread_id)
                exec_agg = (((state.get("executor") or {}).get("results") or {}).get("aggregate") or {})
                storage = (state.get("storage") or {})
                adaptation = (state.get("adaptation") or {}).get("policy", {}) or {}
                concept = (state.get("concept_graph") or {})
                _say(
                    thread_id,
                    "DIAG " + json.dumps(
                        {
                            "executor": exec_agg,
                            "storage": {
                                "apply_ops": (((storage.get("apply_result") or {}).get("ops")) or
                                              ((storage.get("apply") or {}).get("ops")
                                               if isinstance(storage.get("apply"), dict) else None))
                            },
                            "policy": {
                                "updates": adaptation.get("updates", 0),
                                "confidence": adaptation.get("confidence", 0.0),
                            },
                            "concept_version": (concept.get("version") or {}).get("id"),
                        },
                        ensure_ascii=False,
                    ),
                )
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
                    _say(thread_id, "RESET ok")
                except Exception as e:
                    _say(thread_id, f"RESET error: {e}")
                continue

            # ---- default path: index + teach + answer ----

            # Phase 0: index raw message into BM25
            bm25_indexer.ensure_schema(_CONN)
            msg_id = f"{thread_id}:{uuid.uuid4().hex}"
            bm25_indexer.index_doc(_CONN, msg_id, text)

            # Extract teaching facts and upsert into fact store; also index fact lines for BM25
            facts = _extract_all_definitions(text)
            for f in facts:
                sqlite_driver.fact_upsert(_CONN, thread_id, f["k"], f["v"])
                fact_id = f"{thread_id}:fact:{uuid.uuid4().hex}"
                bm25_indexer.index_doc(_CONN, fact_id, f"{f['k']} = {f['v']}")

            _CONN.commit()

            # Phase 1: exact fact lookup first (deterministic)
            q = _simplify_query(text)
            fact_hit: Optional[Tuple[str, str]] = sqlite_driver.fact_get(_CONN, thread_id, q)

            if fact_hit:
                k_raw, v_raw = fact_hit
                answer = f"[{k_raw}] = {v_raw}"
            else:
                # fallback: BM25 retrieval
                hits = bm25_indexer.search(_CONN, q, limit=5) or []
                if hits:
                    top = _hit_text(hits[0])
                    if top and top != text.strip():
                        answer = top
                    elif len(hits) > 1:
                        answer = _hit_text(hits[1]) or "Saved, but no meaningful match yet."
                    else:
                        answer = "Saved, but no meaningful match yet."
                    if answer.strip() == text.strip():
                        answer = "Saved, try asking about it to see what I recall."
                else:
                    if facts:
                        if len(facts) == 1:
                            f0 = facts[0]
                            answer = f"[{f0['k']}] = [{f0['v']}]"
                        else:
                            preview = "; ".join([f"{f['k']}={f['v']}" for f in facts[:3]])
                            more = "" if len(facts) <= 3 else f" (+{len(facts)-3} more)"
                            answer = f"Learned {len(facts)} items: {preview}{more}."
                    else:
                        answer = "I stored your message; nothing relevant found yet."

            # Phase 2: push the answer
            _say(thread_id, answer)

            # Keep kernel state in sync (optional)
            state = ensure_state(thread_id)
            state.setdefault("executor", {})["requests"] = []
            state.setdefault("dialog", {})["final"] = {"move": "answer", "text": answer}
            state = run_tick_io(state, _DRIVERS)
            update_state(thread_id, state)

    except WebSocketDisconnect:
        return
