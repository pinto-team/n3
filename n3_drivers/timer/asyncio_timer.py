# Folder: noema/n3_drivers/timer
# File:   asyncio_timer.py

from typing import Any, Dict
import time

__all__ = ["sleep_ms"]

def sleep_ms(frame: Dict[str, Any]) -> Dict[str, Any]:
    """
    Consume a B13F1 timer frame and return a reply for B13F2.
    Input frame shape:  {"type":"timer","sleep_ms":int}
    Output reply:       {"type":"timer","ok":bool,"sleep_ms":int}
    """
    ms = int(frame.get("sleep_ms", 0))
    if ms > 0:
        time.sleep(ms / 1000.0)
    return {"type": "timer", "ok": True, "sleep_ms": ms}
