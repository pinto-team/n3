# Folder: noema/examples/minimal_chat
# File:   drivers_dev.py

from typing import Dict, Any
from n3_drivers.transport import http_dev
from n3_drivers.skills import local_runner
from n3_drivers.storage import sqlite_driver
from n3_drivers.index import bm25_indexer
from n3_drivers.timer import asyncio_timer  # <-- add this import

__all__ = ["build_drivers"]

def build_drivers() -> Dict[str, Any]:
    # Ensure the sample echo skill exists
    if "skill.dev.echo" not in local_runner._SKILLS:
        def _dev_echo(params: Dict[str, Any]):
            return {"echo": params}
        local_runner.register_skill("skill.dev.echo", _dev_echo)

    # Register a simple BM25 search skill
    def _dev_search(params: Dict[str, Any]):
        q = str(params.get("q", "")).strip()
        k = int(params.get("k", 5))
        if not q:
            return {"hits": []}
        conn = sqlite_driver.get_connection()
        hits = bm25_indexer.search(conn, q, limit=k)
        return {"hits": hits}

    local_runner.register_skill("skill.dev.search", _dev_search)

    return {
        "transport": {"emit": http_dev.emit, "outbox": http_dev.outbox},
        "skills":    {"execute": local_runner.execute},
        "storage":   {"apply_index": sqlite_driver.apply_index},
        "timer":     {"sleep": asyncio_timer.sleep_ms},
    }
