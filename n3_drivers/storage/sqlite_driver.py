import uuid
from typing import Any, Dict, List, Tuple, Optional
import sqlite3
import json
import os
import re
import time

from n3_drivers.index import bm25_indexer

__all__ = [
    "apply_index",
    "connect",
    "get_connection",
    "fact_upsert",
    "fact_get",
    "fact_delete",
    "fact_list",
]

# ---------------- basics ----------------

def connect(db_path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS facts (
            thread_id TEXT NOT NULL,
            k_raw     TEXT NOT NULL,
            v_raw     TEXT NOT NULL,
            k_norm    TEXT NOT NULL,
            created_at REAL NOT NULL DEFAULT (strftime('%s','now')),
            PRIMARY KEY(thread_id, k_norm)
        );
        """
    )
    return conn

_CONN: Optional[sqlite3.Connection] = None

def _ensure_conn(namespace: str) -> sqlite3.Connection:
    global _CONN
    if _CONN is None:
        dbf = os.environ.get("NOEMA_DB", ":memory:")
        _CONN = connect(dbf)
    return _CONN

def get_connection() -> sqlite3.Connection:
    return _ensure_conn("store/noema/default")

# ---------------- helpers: normalize ----------------

_RE_PUNCT = re.compile(r"[؟?!.،,:;]+", flags=re.UNICODE)
_RE_WS = re.compile(r"\s+", flags=re.UNICODE)

def _norm_key(s: str) -> str:
    s = s or ""
    s = _RE_PUNCT.sub(" ", s)
    s = _RE_WS.sub(" ", s).strip().casefold()
    return s

# ---------------- fact store ----------------

def fact_upsert(conn: sqlite3.Connection, thread_id: str, k_raw: str, v_raw: str) -> None:
    k_norm = _norm_key(k_raw)
    if not k_norm:
        return
    conn.execute(
        "INSERT INTO facts(thread_id, k_raw, v_raw, k_norm, created_at) VALUES(?,?,?,?,?) "
        "ON CONFLICT(thread_id, k_norm) DO UPDATE SET k_raw=excluded.k_raw, v_raw=excluded.v_raw, created_at=excluded.created_at;",
        (thread_id, k_raw, v_raw, k_norm, time.time()),
    )

def fact_get(conn: sqlite3.Connection, thread_id: str, query_text: str) -> Optional[Tuple[str, str]]:
    k_norm = _norm_key(query_text)
    if not k_norm:
        return None
    row = conn.execute(
        "SELECT k_raw, v_raw FROM facts WHERE thread_id=? AND k_norm=? LIMIT 1;",
        (thread_id, k_norm),
    ).fetchone()
    if not row:
        return None
    return str(row[0]), str(row[1])

def fact_delete(conn: sqlite3.Connection, thread_id: str, key_text: str) -> int:
    k_norm = _norm_key(key_text)
    if not k_norm:
        return 0
    cur = conn.execute("DELETE FROM facts WHERE thread_id=? AND k_norm=?;", (thread_id, k_norm))
    return cur.rowcount or 0

def fact_list(conn: sqlite3.Connection, thread_id: str, limit: int = 50) -> List[Tuple[str, str, float]]:
    cur = conn.execute(
        "SELECT k_raw, v_raw, created_at FROM facts WHERE thread_id=? ORDER BY created_at DESC LIMIT ?;",
        (thread_id, int(limit)),
    )
    out: List[Tuple[str, str, float]] = []
    for r in cur.fetchall():
        try:
            out.append((str(r[0]), str(r[1]), float(r[2])))
        except Exception:
            continue
    return out

# ---------------- kv + bm25 index (unchanged public API) ----------------

def _apply_ops(conn: sqlite3.Connection, ops: List[Dict[str, Any]]) -> int:
    n = 0
    for op in ops:
        if not isinstance(op, dict):
            continue
        if op.get("op") == "put":
            key = str(op.get("key"))
            val = json.dumps(op.get("value"), ensure_ascii=False)
            conn.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v;", (key, val))
            n += 1
        elif op.get("op") == "inc":
            key = str(op.get("key"))
            cur = conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
            x = int(json.loads(cur[0])) if cur else 0
            x += int(op.get("value", 1))
            conn.execute("INSERT INTO kv(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=?;", (key, json.dumps(x)))
            n += 1
    return n

# add at top of file:
# import uuid

# at top
import hashlib, uuid

def _index_items(conn: sqlite3.Connection, items: List[Dict[str, Any]]) -> int:
    if not items:
        return 0
    bm25_indexer.ensure_schema(conn)
    n = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        typ = it.get("type")
        if typ == "doc":
            if isinstance(it.get("id"), str) and isinstance(it.get("text"), str):
                bm25_indexer.index_doc(conn, it["id"], it["text"])
                n += 1
        elif typ == "packz":
            # accept either flat fields or nested packz
            pk = it
            if "packz" in it and isinstance(it["packz"], dict):
                pk = it["packz"]
            pid = pk.get("id") or uuid.uuid4().hex
            txt = pk.get("text")
            if isinstance(pid, str) and isinstance(txt, str) and txt.strip():
                bm25_indexer.index_doc(conn, pid, txt)
                n += 1
        elif typ == "fact":
            # optional: keep facts searchable too
            k = it.get("k"); v = it.get("v")
            if isinstance(k, str) and isinstance(v, str):
                pid = it.get("id") or hashlib.sha1(f"{k}={v}".encode("utf-8")).hexdigest()
                bm25_indexer.index_doc(conn, pid, f"{k} = {v}")
                n += 1
    return n



def apply_index(frame: Dict[str, Any]) -> Dict[str, Any]:
    ns = str(frame.get("namespace") or "store/noema/default")
    conn = _ensure_conn(ns)

    # قبول هر دو فرمت (list یا dict)
    apply_field = frame.get("apply") or {}
    index_field = frame.get("index") or {}
    if isinstance(apply_field, dict):
        apply_ops = apply_field.get("ops", [])
    else:
        apply_ops = [op for op in apply_field if isinstance(op, dict)]
    if isinstance(index_field, dict):
        index_queue = index_field.get("queue", [])
    else:
        index_queue = [it for it in index_field if isinstance(it, dict)]

    ok = True
    n_ops = n_idx = 0
    try:
        with conn:
            n_ops = _apply_ops(conn, apply_ops)
            n_idx = _index_items(conn, index_queue)
    except Exception as e:
        ok = False
        print("apply_index error:", e)

    return {
        "type": "storage",
        "ok": ok,
        "apply": {"ops": apply_ops[:n_ops]},
        "index": {"queue": index_queue[:n_idx]},
    }
