from typing import Dict, Any
from n3_drivers.transport import http_dev
from n3_drivers.skills import local_runner
from n3_drivers.storage import sqlite_driver
from n3_drivers.index import bm25_indexer
from n3_drivers.timer import asyncio_timer

__all__ = ["build_drivers"]


def build_drivers() -> Dict[str, Any]:
    if "skill.dev.echo" not in local_runner._SKILLS:
        def _dev_echo(params: Dict[str, Any]):
            return {"echo": params}
        local_runner.register_skill("skill.dev.echo", _dev_echo)

    def _dev_search(params: Dict[str, Any]):
        q = str(params.get("q", "")).strip()
        k = int(params.get("k", 5))
        conn = sqlite_driver.get_connection()
        bm25_indexer.ensure_schema(conn)
        if not q:
            return {"hits": []}
        hits = bm25_indexer.search(conn, q, limit=k)
        return {"hits": hits}

    local_runner.register_skill("skill.dev.search", _dev_search)

    def _dev_db_inspect(params: Dict[str, Any]):
        out = {"tables": [], "counts": {}}
        try:
            conn = sqlite_driver.get_connection()
            cur = conn.execute(
                "SELECT name, type FROM sqlite_master WHERE type IN ('table','view');"
            )
            tables = [(r[0], r[1]) for r in cur.fetchall() if isinstance(r[0], str)]
            out["tables"] = tables
            for name, _ in tables:
                if name.startswith("sqlite_"):
                    continue
                try:
                    c = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()
                    out["counts"][name] = int(c[0]) if c and isinstance(c[0], (int, float)) else 0
                except Exception:
                    out["counts"][name] = -1
        except Exception as e:
            out["error"] = str(e)
        return out

    local_runner.register_skill("skill.dev.db_inspect", _dev_db_inspect)

    return {
        "transport": {"emit": http_dev.emit, "outbox": http_dev.outbox},
        "skills": {"execute": local_runner.execute},
        "storage": {"apply_index": sqlite_driver.apply_index},
        "timer": {"sleep": asyncio_timer.sleep_ms},
    }
