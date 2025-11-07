# Folder: noema/n3_drivers/storage
# File:   sqlite_driver.py

from typing import Any, Dict, List, Tuple
import sqlite3
import json
import os

from n3_drivers.index import bm25_indexer  # if your package root is "noema"
# If your root is different, adjust to: from n3_drivers.index import bm25_indexer

__all__ = ["apply_index", "connect", "get_connection"]

def connect(db_path: str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("CREATE TABLE IF NOT EXISTS kv (k TEXT PRIMARY KEY, v TEXT NOT NULL);")
    return conn

_CONN = None

def _ensure_conn(namespace: str) -> sqlite3.Connection:
    global _CONN
    if _CONN is None:
        dbf = os.environ.get("NOEMA_DB", ":memory:")
        _CONN = connect(dbf)
    return _CONN

def get_connection() -> sqlite3.Connection:
    return _ensure_conn("store/noema/default")

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

def _index_items(conn: sqlite3.Connection, items: List[Dict[str, Any]]) -> int:
    if not items:
        return 0
    bm25_indexer.ensure_schema(conn)
    n = 0
    for it in items:
        if not isinstance(it, dict):
            continue
        if it.get("type") == "doc" and isinstance(it.get("id"), str) and isinstance(it.get("text"), str):
            bm25_indexer.index_doc(conn, it["id"], it["text"])
            n += 1
    return n

def apply_index(frame: Dict[str, Any]) -> Dict[str, Any]:
    """
    Input frame:
      {"type":"storage","namespace":str,"apply":[...],"index":[...]}
    Output reply:
      {"type":"storage","ok":bool,"apply":{"ops":[...]}, "index":{"queue":[...]}}
    """
    ns = str(frame.get("namespace") or "store/noema/default")
    apply_ops = [op for op in (frame.get("apply") or []) if isinstance(op, dict)]
    index_queue = [it for it in (frame.get("index") or []) if isinstance(it, dict)]

    conn = _ensure_conn(ns)
    ok = True
    try:
        with conn:
            n = _apply_ops(conn, apply_ops)
        # index outside the transaction to keep it simple; you can wrap in TX if needed
        idx_n = _index_items(conn, index_queue)
    except Exception:
        ok = False
        n = 0
        idx_n = 0

    return {
        "type": "storage",
        "ok": ok,
        "apply": {"ops": apply_ops[:n] if not ok else apply_ops},
        "index": {"queue": index_queue[:idx_n] if not ok else index_queue},
    }
