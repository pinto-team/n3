# Folder: noema/n3_core/block_8_persistence
# File:   b8f3_apply_optimizer.py

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple, Optional

import unicodedata

__all__ = ["b8f3_optimize_apply"]

RULES_VERSION = "1.0"

MAX_APPLY_OPS = 5000
MAX_INDEX_ITEMS = 2000
MAX_TEXT_LEN = 4000


# ------------------------- utils -------------------------

def _cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold() if isinstance(s, str) else ""


def _get(o: Dict[str, Any], path: List[str], default=None):
    cur = o
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def _clip_text(s: Optional[str], n: int = MAX_TEXT_LEN) -> str:
    s = (s or "").strip()
    return s if len(s) <= n else (s[: n - 1] + "…")


def _hash(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _seq_key(op: Dict[str, Any], fallback_idx: int) -> Tuple[int, int]:
    # Sort by explicit seq (None -> +inf) and by arrival index to stabilize
    seq = op.get("seq")
    return (seq if isinstance(seq, int) else 10 ** 12, fallback_idx)


# ------------------------- core optimizers -------------------------

def _opt_storage_ops(ops: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """
    - put: last-wins per key (highest seq, else latest appearance)
    - inc: aggregate sum per key
    - link: dedupe by (key,value-hash), keep last (highest seq)
    Order: puts -> links -> incs; each group stable by seq asc then arrival.
    """
    seen_put: Dict[str, Tuple[Dict[str, Any], int]] = {}
    inc_sum: Dict[str, int] = {}
    seen_link: Dict[Tuple[str, str], Tuple[Dict[str, Any], int]] = {}

    arrival = 0
    for op in ops:
        if not isinstance(op, dict) or "op" not in op:
            continue
        kind = op.get("op")
        if kind == "put":
            k = op.get("key")
            if not isinstance(k, str):
                continue
            # last-wins by seq, else arrival
            prev = seen_put.get(k)
            if not prev:
                seen_put[k] = (op, arrival)
            else:
                prev_op, prev_arr = prev
                s_old = prev_op.get("seq") if isinstance(prev_op.get("seq"), int) else None
                s_new = op.get("seq") if isinstance(op.get("seq"), int) else None
                if (isinstance(s_new, int) and (s_old is None or s_new >= s_old)) or s_old is None and s_new is None:
                    seen_put[k] = (op, arrival)
        elif kind == "inc":
            k = op.get("key")
            d = op.get("delta")
            if isinstance(k, str) and isinstance(d, (int, float)):
                inc_sum[k] = inc_sum.get(k, 0) + int(d)
        elif kind == "link":
            k = op.get("key")
            v = op.get("value")
            if not isinstance(k, str) or v is None:
                continue
            sig = (k, _hash(v))
            prev = seen_link.get(sig)
            if not prev:
                seen_link[sig] = (op, arrival)
            else:
                prev_op, _ = prev
                s_old = prev_op.get("seq") if isinstance(prev_op.get("seq"), int) else None
                s_new = op.get("seq") if isinstance(op.get("seq"), int) else None
                if (isinstance(s_new, int) and (s_old is None or s_new >= s_old)) or s_old is None and s_new is None:
                    seen_link[sig] = (op, arrival)
        arrival += 1

    puts = sorted((op for op, idx in seen_put.values()), key=lambda x: _seq_key(x, 0))
    links = sorted((op for op, idx in seen_link.values()), key=lambda x: _seq_key(x, 0))
    incs = [{"op": "inc", "key": k, "delta": v} for k, v in inc_sum.items() if v != 0]

    out = puts + links + incs
    if len(out) > MAX_APPLY_OPS:
        out = out[:MAX_APPLY_OPS]

    counts = {"puts": len(puts), "links": len(links), "incs": len(incs)}
    return out, counts


def _opt_index_items(items: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    """
    Dedupe by (type,id,ns) last-wins; clip text.
    """
    seen: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    order: List[Tuple[str, str, str]] = []

    for it in items:
        if not isinstance(it, dict):
            continue
        typ = it.get("type") if isinstance(it.get("type"), str) else ""
        iid = it.get("id") if isinstance(it.get("id"), str) else ""
        ns = it.get("ns") if isinstance(it.get("ns"), str) else ""
        if not (typ and iid and ns):
            continue
        it = dict(it)
        if isinstance(it.get("text"), str):
            it["text"] = _clip_text(it["text"])
        key = (typ, iid, ns)
        if key not in seen:
            seen[key] = it
            order.append(key)
        else:
            # last-wins
            seen[key] = it

    out = [seen[k] for k in order]
    if len(out) > MAX_INDEX_ITEMS:
        out = out[:MAX_INDEX_ITEMS]
    return out, len(out)


# ------------------------- main -------------------------

def b8f3_optimize_apply(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B8F3 — Persistence.ApplyOptimizer (Noema)

    Input:
      {
        "storage": { "apply": { "namespace": str, "ops": [ {"op":"put|inc|link", ...}, ... ] } }?,
        "index": { "queue": { "items": [ {...}, ... ] } }?
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "storage": {
          "apply_optimized": {
            "namespace": str,
            "ops": [ ... ],
            "checksum": str,
            "meta": { "source": "B8F3", "rules_version": "1.0" }
          }
        },
        "index": {
          "queue_optimized": {
            "items": [ ... ],
            "checksum": str,
            "meta": { "source": "B8F3", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_ops", "counts": { "apply_in": int, "apply_out": int, "puts": int, "links": int, "incs": int, "index_in": int, "index_out": int } }
      }
    """
    apply_in = _get(input_json, ["storage", "apply", "ops"], []) or []
    ns = _get(input_json, ["storage", "apply", "namespace"], "") or "store/noema/default"
    index_in = _get(input_json, ["index", "queue", "items"], []) or []

    if not isinstance(apply_in, list) and not isinstance(index_in, list):
        return {"status": "SKIP", "storage": {"apply_optimized": {}}, "index": {"queue_optimized": {}},
                "diag": {"reason": "no_ops",
                         "counts": {"apply_in": 0, "apply_out": 0, "puts": 0, "links": 0, "incs": 0, "index_in": 0,
                                    "index_out": 0}}}

    # Optimize storage ops
    apply_in_list = [op for op in apply_in if isinstance(op, dict)]
    apply_out, counts = _opt_storage_ops(apply_in_list)
    apply_checksum = _hash({"ns": ns, "ops": apply_out})

    # Optimize index items
    index_in_list = [x for x in index_in if isinstance(x, dict)]
    idx_out, idx_n = _opt_index_items(index_in_list)
    idx_checksum = _hash({"items": idx_out})

    out = {
        "status": "OK",
        "storage": {
            "apply_optimized": {
                "namespace": ns,
                "ops": apply_out,
                "checksum": apply_checksum,
                "meta": {"source": "B8F3", "rules_version": RULES_VERSION},
            }
        },
        "index": {
            "queue_optimized": {
                "items": idx_out,
                "checksum": idx_checksum,
                "meta": {"source": "B8F3", "rules_version": RULES_VERSION},
            }
        },
        "diag": {
            "reason": "ok",
            "counts": {
                "apply_in": len(apply_in_list),
                "apply_out": len(apply_out),
                "puts": counts.get("puts", 0),
                "links": counts.get("links", 0),
                "incs": counts.get("incs", 0),
                "index_in": len(index_in_list),
                "index_out": idx_n,
            }
        },
    }
    return out


if __name__ == "__main__":
    sample = {
        "storage": {
            "apply": {
                "namespace": "store/noema/t-42",
                "ops": [
                    {"op": "put", "key": "k/a", "value": {"x": 1}, "seq": 5},
                    {"op": "put", "key": "k/a", "value": {"x": 2}, "seq": 7},  # last-wins
                    {"op": "inc", "key": "c/turns", "delta": 1},
                    {"op": "inc", "key": "c/turns", "delta": 2},  # aggregate→ +3
                    {"op": "link", "key": "link/t2r", "value": {"a": "1", "r": "x"}, "seq": 10},
                    {"op": "link", "key": "link/t2r", "value": {"a": "1", "r": "x"}, "seq": 12},  # dedupe, keep seq=12
                ]
            }
        },
        "index": {
            "queue": {
                "items": [
                    {"type": "packz", "id": "u1", "text": "Hello Noema", "ns": "store/noema/t-42"},
                    {"type": "packz", "id": "u1", "text": "Hello Noema!!!", "ns": "store/noema/t-42"}  # last-wins
                ]
            }
        }
    }
    out = b8f3_optimize_apply(sample)
    print(out["diag"])
    print(out["storage"]["apply_optimized"]["ops"])
    print(out["index"]["queue_optimized"]["items"])
