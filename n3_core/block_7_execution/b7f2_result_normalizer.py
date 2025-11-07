# Folder: noema/n3_core/block_7_execution
# File:   b7f2_result_normalizer.py

from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional

import unicodedata

__all__ = ["b7f2_normalize_results"]

RULES_VERSION = "1.0"

MAX_TEXT = 8000
MAX_JSON_CHARS = 20000
MAX_ATTACH = 12


# ------------------------- utils -------------------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _trim_text(s: Optional[str], limit: int = MAX_TEXT) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _safe_json_dump(x: Any, limit: int = MAX_JSON_CHARS) -> str:
    try:
        s = json.dumps(x, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    except Exception:
        try:
            s = json.dumps(str(x), ensure_ascii=False)
        except Exception:
            return ""
    return s if len(s) <= limit else s[: limit - 1] + "…"


def _approx_tokens(s: str) -> int:
    if not isinstance(s, str) or not s:
        return 0
    # very rough: ~4 chars/token
    return max(1, math.ceil(len(s) / 4))


def _is_table_like(obj: Any) -> bool:
    if isinstance(obj, list) and obj and all(isinstance(r, dict) for r in obj):
        keys0 = set(obj[0].keys())
        if keys0 and all(set(r.keys()) == keys0 for r in obj[: min(10, len(obj))]):
            return True
    return False


def _infer_kind(content: Any, mime: Optional[str]) -> str:
    mt = _cf(mime or "")
    if mt.startswith("text/markdown") or mt == "text/md":
        return "markdown"
    if mt.startswith("text/"):
        return "text"
    if mt.endswith("/json") or mt == "application/json":
        return "json"
    if mt.startswith("image/"):
        return "image"
    if mt.startswith("audio/"):
        return "audio"
    if mt.startswith("video/"):
        return "video"
    if isinstance(content, (dict, list)):
        return "json" if not _is_table_like(content) else "table"
    if isinstance(content, str):
        # naive URL check
        if content.strip().lower().startswith(("http://", "https://")):
            return "url"
        return "text"
    if isinstance(content, (bytes, bytearray)):
        return "binary"
    return "unknown"


def _normalize_one(resp: Dict[str, Any]) -> Dict[str, Any]:
    """
    Expected raw shapes (best-effort):
      {
        "req_id": str, "ok": bool?, "content": Any, "mime": "text/markdown|application/json|...",
        "text": str?, "data": Any?, "attachments": [ {type:"file|image|url", "ref": str, ...}, ... ]?,
        "usage": {"input_tokens":int?, "output_tokens":int?, "cost": float?}?,
        "duration_ms": int?, "error": { "type":str, "message":str }?
      }
    """
    req = resp.get("req_id") if isinstance(resp.get("req_id"), str) else ""
    ok = bool(resp.get("ok", True))
    mime = resp.get("mime") if isinstance(resp.get("mime"), str) else None
    content = resp.get("content") if "content" in resp else (
        resp.get("text") if "text" in resp else (resp.get("data") if "data" in resp else None))

    kind = _infer_kind(content, mime)
    text = ""
    data: Any = None
    attachments: List[Dict[str, Any]] = []

    if kind in {"text", "markdown", "url"}:
        text = _trim_text(str(content))
    elif kind in {"json", "table"}:
        data = content
        text = _trim_text(_safe_json_dump(content))
    elif kind in {"image", "audio", "video", "binary"}:
        # leave content out; only register attachment if present
        if isinstance(resp.get("attachments"), list):
            attachments = [a for a in resp["attachments"] if isinstance(a, dict)]
        # fallback: if content is a URL string
        if isinstance(content, str) and content.strip().lower().startswith(("http://", "https://")):
            attachments.append({"type": kind, "ref": content})
        text = resp.get("text") if isinstance(resp.get("text"), str) else ""
        text = _trim_text(text)
    else:
        # unknown → try to stringify
        text = _trim_text(str(content)) if content is not None else ""

    if not attachments and isinstance(resp.get("attachments"), list):
        attachments = [a for a in resp["attachments"] if isinstance(a, dict)]
    if len(attachments) > MAX_ATTACH:
        attachments = attachments[:MAX_ATTACH]

    # usage & duration
    usage = resp.get("usage") if isinstance(resp.get("usage"), dict) else {}
    in_tok = int(usage.get("input_tokens", 0)) if isinstance(usage.get("input_tokens"), (int, float)) else 0
    out_tok = int(usage.get("output_tokens", 0)) if isinstance(usage.get("output_tokens"), (int, float)) else (
        _approx_tokens(text) if text else 0)
    cost = float(usage.get("cost", 0.0)) if isinstance(usage.get("cost"), (int, float)) else 0.0
    duration_ms = int(resp.get("duration_ms", 0)) if isinstance(resp.get("duration_ms"), (int, float)) else 0

    # error
    err = resp.get("error") if isinstance(resp.get("error"), dict) else {}
    if err:
        ok = False
        kind = "error"
        if not text:
            msg = err.get("message") if isinstance(err.get("message"), str) else str(err)
            text = _trim_text(msg)

    # scoring: prefer ok, richer content, longer informative text, and tables/json
    richness = 0.0
    if kind in {"table", "json"} and data:
        richness = 0.25
        if _is_table_like(data):
            richness += 0.1
    elif kind in {"markdown"}:
        richness = 0.15
    elif kind in {"text"}:
        richness = 0.1
    if attachments:
        richness += min(0.2, 0.05 * len(attachments))

    length_bonus = min(0.2, max(0.0, (len(text) - 80) / 500.0)) if text else 0.0
    ok_bonus = 0.5 if ok else 0.0
    score = round(ok_bonus + richness + length_bonus, 6)

    return {
        "req_id": req,
        "ok": ok,
        "kind": kind,
        "text": text,
        "data": data,
        "attachments": attachments,
        "usage": {"input_tokens": in_tok, "output_tokens": out_tok, "cost": round(cost, 6)},
        "duration_ms": duration_ms,
        "score": score,
        "meta": {"source": "B7F2", "rules_version": RULES_VERSION, "mime": mime or None},
    }


# ------------------------- main -------------------------

def b7f2_normalize_results(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B7F2 — Execution.ResultNormalizer (Noema)

    Input (best-effort):
      {
        "executor": {
          "responses": [ raw_response, ... ]                  # preferred
          # or
          "raw":       [ raw_response, ... ]
        }
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "executor": {
          "results": {
            "items": [
              { "req_id": str, "ok": bool, "kind": "text|markdown|json|table|image|url|binary|error|unknown",
                "text": str, "data": Any?, "attachments": [ {type, ref, ...}, ... ],
                "usage": {"input_tokens": int, "output_tokens": int, "cost": float},
                "duration_ms": int, "score": float, "meta": {...} }
            ],
            "best": { ... }?,     # one of the items
            "aggregate": {
              "count": int, "ok": int, "errors": int,
              "total_cost": float, "total_input_tokens": int, "total_output_tokens": int,
              "avg_latency_ms": float
            },
            "meta": { "source": "B7F2", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_responses" }
      }
    """
    ex = input_json.get("executor", {})
    raw = []
    if isinstance(ex, dict):
        if isinstance(ex.get("responses"), list) and ex["responses"]:
            raw = [r for r in ex["responses"] if isinstance(r, dict)]
        elif isinstance(ex.get("raw"), list) and ex["raw"]:
            raw = [r for r in ex["raw"] if isinstance(r, dict)]

    if not raw:
        return {
            "status": "SKIP",
            "executor": {"results": {"items": [], "best": None,
                                     "aggregate": {"count": 0, "ok": 0, "errors": 0, "total_cost": 0.0,
                                                   "total_input_tokens": 0, "total_output_tokens": 0,
                                                   "avg_latency_ms": 0.0},
                                     "meta": {"source": "B7F2", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_responses"},
        }

    items: List[Dict[str, Any]] = []
    total_cost = 0.0
    total_in = 0
    total_out = 0
    lat_sum = 0
    ok_n = 0
    err_n = 0

    for r in raw:
        it = _normalize_one(r)
        items.append(it)
        total_cost += float(it["usage"]["cost"])
        total_in += int(it["usage"]["input_tokens"])
        total_out += int(it["usage"]["output_tokens"])
        lat_sum += int(it["duration_ms"])
        if it["ok"]:
            ok_n += 1
        else:
            err_n += 1

    items.sort(key=lambda x: (x["ok"], x["score"], -x["duration_ms"]), reverse=True)
    best = items[0] if items else None

    results = {
        "items": items,
        "best": best,
        "aggregate": {
            "count": len(items),
            "ok": ok_n,
            "errors": err_n,
            "total_cost": round(total_cost, 6),
            "total_input_tokens": total_in,
            "total_output_tokens": total_out,
            "avg_latency_ms": round(lat_sum / max(1, len(items)), 2),
        },
        "meta": {"source": "B7F2", "rules_version": RULES_VERSION},
    }

    return {"status": "OK", "executor": {"results": results}, "diag": {"reason": "ok"}}


if __name__ == "__main__":
    sample = {
        "executor": {
            "responses": [
                {
                    "req_id": "r1", "ok": True, "mime": "application/json",
                    "content": [{"title": "A", "value": 1}, {"title": "B", "value": 2}],
                    "usage": {"input_tokens": 120, "output_tokens": 80, "cost": 0.0021},
                    "duration_ms": 740
                },
                {
                    "req_id": "r2", "ok": True, "mime": "text/markdown",
                    "content": "# Summary\nThis is a result.",
                    "duration_ms": 540
                },
                {
                    "req_id": "r3", "ok": False, "error": {"type": "Timeout", "message": "skill timed out after 30s"},
                    "duration_ms": 30000
                }
            ]
        }
    }
    out = b7f2_normalize_results(sample)
    print(out["executor"]["results"]["aggregate"])
    print(out["executor"]["results"]["best"]["kind"], out["executor"]["results"]["best"]["score"])
