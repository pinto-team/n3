# Folder: noema/n3_drivers/index
# File:   bm25_indexer.py

from __future__ import annotations
from typing import Any, Dict, List, Tuple
import sqlite3

__all__ = ["ensure_schema", "index_doc", "search"]

def ensure_schema(conn: sqlite3.Connection) -> None:
    # Contentless FTS5 table with UNINDEXED doc_id
    conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(doc_id UNINDEXED, text)")
    conn.commit()

def index_doc(conn: sqlite3.Connection, doc_id: str, text: str) -> None:
    ensure_schema(conn)
    # Replace by delete + insert (FTS5 doesn't support OR REPLACE)
    conn.execute("DELETE FROM fts WHERE doc_id = ?", (doc_id,))
    conn.execute("INSERT INTO fts(doc_id, text) VALUES(?, ?)", (doc_id, text))
    conn.commit()

def search(conn: sqlite3.Connection, query: str, limit: int = 5) -> List[Dict[str, Any]]:
    ensure_schema(conn)
    try:
        sql = """
        SELECT doc_id, snippet(fts, 1, '[', ']', ' … ', 10) AS snip, bm25(fts) AS score
        FROM fts
        WHERE fts MATCH ?
        ORDER BY score
        LIMIT ?
        """
        rows = conn.execute(sql, (query, int(limit))).fetchall()
    except sqlite3.OperationalError:
        # Fallback if bm25 not available on platform
        sql = """
        SELECT doc_id, snippet(fts, 1, '[', ']', ' … ', 10) AS snip, rank
        FROM fts
        WHERE fts MATCH ?
        ORDER BY rank
        LIMIT ?
        """
        rows = conn.execute(sql, (query, int(limit))).fetchall()

    out: List[Dict[str, Any]] = []
    for r in rows:
        doc_id, snip, score = r
        out.append({"doc_id": doc_id, "snippet": snip, "score": float(score) if score is not None else 0.0})
    return out
