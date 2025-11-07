# Folder: noema/n3_core/block_3_memory
# File:   b3f1_wal_writer.py

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import unicodedata

__all__ = ["b3f1_wal_write"]

RULES_VERSION = "1.0"
DEFAULT_STREAM = "wal/noema/perception"


def _get_packz(inp: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    p = inp.get("perception")
    if isinstance(p, dict) and isinstance(p.get("packz"), dict):
        return p["packz"]
    if isinstance(inp.get("packz"), dict):
        return inp["packz"]
    return None


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _stable_id(text: str, commit_time: Optional[str]) -> str:
    h = hashlib.sha1()
    h.update(unicodedata.normalize("NFC", text).encode("utf-8"))
    if isinstance(commit_time, str):
        h.update(commit_time.encode("utf-8"))
    return h.hexdigest()


def _sig(record: Dict[str, Any]) -> str:
    payload = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _pack_record(pk: Dict[str, Any]) -> Tuple[Dict[str, Any], str, str]:
    text = pk.get("text") if isinstance(pk.get("text"), str) else ""
    if not text.strip():
        return {}, "", ""
    meta = pk.get("meta", {}) if isinstance(pk.get("meta"), dict) else {}
    commit_time = meta.get("commit_time") if isinstance(meta.get("commit_time"), str) else None
    pid = pk.get("id") if isinstance(pk.get("id"), str) and pk.get("id") else _stable_id(text, commit_time)

    record = {
        "id": pid,
        "text": text,
        "counts": pk.get("counts", {}),
        "signals": pk.get("signals", {}),
        # Keep spans minimal but present for downstream indexers
        "spans": {
            "sentences": pk.get("spans", {}).get("sentences", []) if isinstance(pk.get("spans"), dict) else [],
            "tokens": [],  # leave heavy tokens out of WAL by default
            "script_tags": [],  # can be rebuilt or fetched on demand
        },
        "meta": {
            "commit_time": commit_time or _iso_now(),
            "truncated_spans": bool(meta.get("truncated_spans", False)),
            "source": "B3F1",
            "rules_version": RULES_VERSION,
        },
    }
    ts = record["meta"]["commit_time"]
    return record, pid, ts


def b3f1_wal_write(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B3F1 — Memory.WAL.Writer (Noema)
    Input:
      { "perception": { "packz": {...} } }
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "memory": {
          "wal": {
            "stream": "wal/noema/perception",
            "op": "append",
            "key": str,             # idempotency key (packz.id)
            "ts": "ISO8601Z",
            "record": {...},        # compact, deterministic
            "sig": str              # SHA1 over record JSON (sorted)
          },
          "idempotency_key": str
        },
        "diag": { "reason": "ok|no_packz|invalid_packz", "size_json_bytes": int? }
      }
    """
    pk = _get_packz(input_json)
    if pk is None:
        return {
            "status": "SKIP",
            "memory": {"wal": {}, "idempotency_key": ""},
            "diag": {"reason": "no_packz"},
        }
    if not isinstance(pk, dict):
        return {"status": "FAIL", "diag": {"reason": "invalid_packz"}}

    record, key, ts = _pack_record(pk)
    if not record:
        return {
            "status": "SKIP",
            "memory": {"wal": {}, "idempotency_key": ""},
            "diag": {"reason": "no_packz"},
        }

    signature = _sig(record)
    wal = {
        "stream": DEFAULT_STREAM,
        "op": "append",
        "key": key,
        "ts": ts,
        "record": record,
        "sig": signature,
    }

    payload = json.dumps(wal, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return {
        "status": "OK",
        "memory": {"wal": wal, "idempotency_key": key},
        "diag": {"reason": "ok", "size_json_bytes": len(payload.encode("utf-8"))},
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "packz": {
                "id": "abc123",
                "text": "سلام نوما، لطفاً این متن را در حافظه ثبت کن.",
                "counts": {"chars": 35, "words": 7, "tokens": 0, "sentences": 1},
                "signals": {"direction": "rtl", "addressed_to_noema": True, "speech_act": "request", "confidence": 0.88,
                            "novelty": 0.66},
                "meta": {"commit_time": "2025-11-07T09:30:00Z", "truncated_spans": False},
                "spans": {"sentences": [
                    {"text": "سلام نوما، لطفاً این متن را در حافظه ثبت کن.", "span": {"start": 0, "end": 45}}]}
            }
        }
    }
    out = b3f1_wal_write(sample)
    print(out["memory"]["wal"]["key"], out["memory"]["wal"]["sig"], out["diag"])
