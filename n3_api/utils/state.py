# noema/n3_api/utils/state.py

import os, sqlite3, json, threading
from datetime import datetime, timezone

DB_PATH = os.getenv("NOEMA_DB", "noema_state.db")
_CONN_LOCK = threading.Lock()
_STATE_CACHE: dict[str, dict] = {}

def now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# ★ NEW: now_ms برای initiative.py
def now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def _conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("""
    CREATE TABLE IF NOT EXISTS session_state(
        thread_id   TEXT PRIMARY KEY,
        state_json  TEXT NOT NULL,
        updated_at  TEXT NOT NULL
    )
    """)
    return conn

def _load(thread_id: str) -> dict | None:
    with _CONN_LOCK:
        with _conn() as c:
            row = c.execute(
                "SELECT state_json FROM session_state WHERE thread_id=?",
                (thread_id,)
            ).fetchone()
    return json.loads(row[0]) if row else None

def _save(thread_id: str, state: dict) -> None:
    blob = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
    ts = now_iso()
    with _CONN_LOCK:
        with _conn() as c:
            c.execute("""
            INSERT INTO session_state(thread_id, state_json, updated_at)
            VALUES (?,?,?)
            ON CONFLICT(thread_id) DO UPDATE SET
              state_json = excluded.state_json,
              updated_at = excluded.updated_at
            """, (thread_id, blob, ts))

def ensure_state(thread_id: str) -> dict:
    s = _STATE_CACHE.get(thread_id)
    if s is None:
        s = _load(thread_id)
        if s is None:
            s = {"session": {"thread_id": thread_id}}
        _STATE_CACHE[thread_id] = s
    return s

def update_state(thread_id: str, new_state: dict) -> None:
    _STATE_CACHE[thread_id] = new_state
    _save(thread_id, new_state)

def list_threads(limit: int = 100) -> list[dict]:
    with _CONN_LOCK:
        with _conn() as c:
            rows = c.execute(
                "SELECT thread_id, updated_at FROM session_state ORDER BY updated_at DESC LIMIT ?",
                (limit,)
            ).fetchall()
    return [{"thread_id": tid, "updated_at": ts} for (tid, ts) in rows]

# ★ NEW: get_sessions برای /health
def get_sessions() -> dict[str, dict]:
    sessions: dict[str, dict] = {}
    # از DB بخوان
    with _CONN_LOCK:
        with _conn() as c:
            rows = c.execute("SELECT thread_id, state_json FROM session_state").fetchall()
    for tid, blob in rows:
        try:
            sessions[tid] = json.loads(blob)
        except Exception:
            pass
    # کش درون‌حافظه را هم لحاظ کن (آخرین تغییرات هنوز flush نشده؟)
    sessions.update(_STATE_CACHE)
    return sessions
