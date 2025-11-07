# Folder: noema/n3_core/block_3_memory
# File:   b3f3_retriever.py

from __future__ import annotations

import math
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import unicodedata

__all__ = ["b3f3_retrieve"]

RULES_VERSION = "1.0"
DEFAULT_TOPK = 5
GRAM_N = 3

RE_WS = re.compile(r"\s+", re.UNICODE)


# ------------------------- utils -------------------------

def _nfc_casefold(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold()


def _collapse_ws(s: str) -> str:
    return RE_WS.sub(" ", s).strip()


def _char_ngrams(s: str, n: int = GRAM_N) -> List[str]:
    s = _collapse_ws(_nfc_casefold(s))
    if len(s) == 0:
        return []
    if len(s) < n:
        return [s]
    return [s[i:i + n] for i in range(len(s) - n + 1)]


def _ws_tokens(s: str) -> List[str]:
    s = _collapse_ws(_nfc_casefold(s))
    if not s:
        return []
    return s.split()


def _jaccard(a: List[str], b: List[str]) -> float:
    if not a and not b:
        return 1.0
    A, B = set(a), set(b)
    inter = len(A & B)
    union = len(A | B) or 1
    return inter / union


def _safe_iso_date(ts: Optional[str]) -> Optional[datetime]:
    if not isinstance(ts, str) or not ts:
        return None
    t = ts.replace("Z", "+00:00") if ts.endswith("Z") else ts
    try:
        return datetime.fromisoformat(t)
    except Exception:
        return None


def _best_snippet(text: str, max_len: int = 120) -> str:
    t = text.strip()
    if len(t) <= max_len:
        return t
    return t[: max_len - 1] + "…"


# ------------------------- extractors -------------------------

def _get_query_text(inp: Dict[str, Any]) -> Optional[str]:
    # Prefer perception.packz.text, fallback to world_model.context.current.text
    per = inp.get("perception", {})
    if isinstance(per, dict):
        pk = per.get("packz", {})
        if isinstance(pk, dict) and isinstance(pk.get("text"), str):
            return pk["text"]
        if isinstance(per.get("normalized_text"), str):
            return per["normalized_text"]
    wm = inp.get("world_model", {})
    if isinstance(wm, dict):
        ctx = wm.get("context", {})
        if isinstance(ctx, dict):
            cur = ctx.get("current", {})
            if isinstance(cur, dict) and isinstance(cur.get("text"), str):
                return cur["text"]
    # Fallbacks
    if isinstance(inp.get("text"), str):
        return inp["text"]
    if isinstance(inp.get("raw_text"), str):
        return inp["raw_text"]
    return None


def _get_query_facets(inp: Dict[str, Any]) -> Dict[str, Any]:
    # Optional hints to bias retrieval
    wm = inp.get("world_model", {})
    feats = {}
    if isinstance(wm, dict):
        ctx = wm.get("context", {})
        if isinstance(ctx, dict) and isinstance(ctx.get("features"), dict):
            feats = ctx["features"]
    per = inp.get("perception", {})
    if not feats and isinstance(per, dict):
        pk = per.get("packz", {})
        sig = pk.get("signals", {}) if isinstance(pk, dict) else {}
        feats = {
            "dir": sig.get("direction"),
            "speech_act": sig.get("speech_act"),
        }
    return feats if isinstance(feats, dict) else {}


def _get_candidates(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Accepts candidates from:
      - memory.corpus: [packz-like or {id,text,meta:{commit_time},signals:{...}}]
      - context.recent_packz: as packz-like
      - memory.retrieved_packz: as packz-like
    """
    out: List[Dict[str, Any]] = []

    mem = inp.get("memory", {})
    if isinstance(mem, dict) and isinstance(mem.get("corpus"), list):
        out.extend([x for x in mem["corpus"] if isinstance(x, dict)])

    ctx = inp.get("context", {})
    if isinstance(ctx, dict) and isinstance(ctx.get("recent_packz"), list):
        for it in ctx["recent_packz"]:
            if isinstance(it, dict):
                out.append(it.get("packz", it))

    if isinstance(mem, dict) and isinstance(mem.get("retrieved_packz"), list):
        out.extend([x for x in mem["retrieved_packz"] if isinstance(x, dict)])

    return out


def _packz_like_to_text_id_meta(x: Dict[str, Any]) -> Tuple[str, str, Dict[str, Any]]:
    if "packz" in x and isinstance(x["packz"], dict):
        x = x["packz"]
    text = x.get("text") if isinstance(x.get("text"), str) else ""
    pid = x.get("id") if isinstance(x.get("id"), str) else ""
    meta = x.get("meta", {}) if isinstance(x.get("meta"), dict) else {}
    return text, pid, meta


def _facet_from_packz_like(x: Dict[str, Any]) -> Dict[str, Any]:
    if "packz" in x and isinstance(x["packz"], dict):
        x = x["packz"]
    sig = x.get("signals", {}) if isinstance(x.get("signals"), dict) else {}
    meta = x.get("meta", {}) if isinstance(x.get("meta"), dict) else {}
    return {
        "dir": sig.get("direction"),
        "sa": sig.get("speech_act"),
        "commit_time": meta.get("commit_time"),
    }


# ------------------------- scoring -------------------------

def _facet_bonus(query_feats: Dict[str, Any], cand_facets: Dict[str, Any]) -> float:
    bonus = 0.0
    if query_feats.get("dir") and cand_facets.get("dir") and query_feats["dir"] == cand_facets["dir"]:
        bonus += 0.03
    if query_feats.get("speech_act") and cand_facets.get("sa") and _nfc_casefold(
            query_feats["speech_act"]) == _nfc_casefold(cand_facets["sa"]):
        bonus += 0.02
    return bonus


def _recency_bonus(cand_commit: Optional[str], now: Optional[datetime] = None) -> float:
    if not cand_commit:
        return 0.0
    dt = _safe_iso_date(cand_commit)
    if not dt:
        return 0.0
    if not now:
        now = datetime.utcnow()
    delta_days = max(0.0, (now - dt).days)
    # Exponential decay with ~30-day half-life, scaled to 0..0.1
    return 0.1 * math.exp(-delta_days / 30.0)


def _score_example(q_text: str, c_text: str, q_feats: Dict[str, Any], c_facets: Dict[str, Any]) -> Tuple[
    float, Dict[str, float]]:
    gq = _char_ngrams(q_text, GRAM_N)
    gc = _char_ngrams(c_text, GRAM_N)
    gram_j = _jaccard(gq, gc)

    tq = _ws_tokens(q_text)
    tc = _ws_tokens(c_text)
    tok_j = _jaccard(tq, tc)

    f_bonus = _facet_bonus(q_feats, c_facets)
    r_bonus = _recency_bonus(c_facets.get("commit_time"))

    # Weighted blend
    score = 0.6 * gram_j + 0.3 * tok_j + f_bonus + r_bonus
    score = round(min(1.0, max(0.0, score)), 6)

    comp = {
        "gram_jaccard": round(gram_j, 6),
        "token_jaccard": round(tok_j, 6),
        "facet_bonus": round(f_bonus, 6),
        "recency_bonus": round(r_bonus, 6),
    }
    return score, comp


# ------------------------- main -------------------------

def b3f3_retrieve(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B3F3 — Memory.Retriever (Noema)
    Pure, in-memory retrieval over provided candidates.

    Input (best-effort):
      {
        "perception": { "packz": { "text": str, "signals": {...}, "meta": {...} } },
        "world_model": { "context": { "current": {"text": str}, "features": {...} } },
        "memory": {
          "corpus": [ packz_like, ... ]              # required for retrieval
        },
        "retrieval": { "top_k": int? }               # optional
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "memory": {
          "retrieval": {
            "query": { "text": str, "features": {...} },
            "results": [
              { "id": str, "score": float, "components": {...}, "snippet": str, "facets": {"dir": str?, "sa": str?, "commit_time": str?} }
            ],
            "top_k": int,
            "meta": { "source": "B3F3", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_query|no_candidates", "counts": {"candidates": int, "returned": int} }
      }
    """
    q_text = _get_query_text(input_json)
    if not isinstance(q_text, str) or not q_text.strip():
        return {
            "status": "SKIP",
            "memory": {"retrieval": {"query": {}, "results": [], "top_k": 0,
                                     "meta": {"source": "B3F3", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_query", "counts": {"candidates": 0, "returned": 0}},
        }

    candidates = _get_candidates(input_json)
    if not candidates:
        return {
            "status": "SKIP",
            "memory": {"retrieval": {"query": {"text": q_text}, "results": [], "top_k": 0,
                                     "meta": {"source": "B3F3", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_candidates", "counts": {"candidates": 0, "returned": 0}},
        }

    top_k = DEFAULT_TOPK
    if isinstance(input_json.get("retrieval"), dict) and isinstance(input_json["retrieval"].get("top_k"), int):
        top_k = max(1, int(input_json["retrieval"]["top_k"]))

    q_feats = _get_query_facets(input_json)

    scored: List[Dict[str, Any]] = []
    for cand in candidates:
        c_text, c_id, c_meta = _packz_like_to_text_id_meta(cand)
        if not c_text or not c_id:
            # skip malformed entries
            continue
        facets = _facet_from_packz_like(cand)
        score, comp = _score_example(q_text, c_text, q_feats, facets)
        scored.append({
            "id": c_id,
            "score": score,
            "components": comp,
            "snippet": _best_snippet(c_text),
            "facets": facets,
        })

    scored.sort(key=lambda x: x["score"], reverse=True)
    results = scored[:top_k]

    return {
        "status": "OK",
        "memory": {
            "retrieval": {
                "query": {"text": q_text, "features": q_feats},
                "results": results,
                "top_k": top_k,
                "meta": {"source": "B3F3", "rules_version": RULES_VERSION},
            }
        },
        "diag": {"reason": "ok", "counts": {"candidates": len(candidates), "returned": len(results)}},
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "packz": {
                "text": "لطفاً خلاصه معماری Noema را بده.",
                "signals": {"direction": "rtl", "speech_act": "request"},
                "meta": {"commit_time": "2025-11-07T09:50:00Z"}
            }
        },
        "memory": {
            "corpus": [
                {
                    "id": "d1",
                    "text": "دیروز درباره ساختار پوشه‌ها و بلوک‌ها در Noema حرف زدیم.",
                    "signals": {"direction": "rtl", "speech_act": "statement"},
                    "meta": {"commit_time": "2025-11-06T18:00:00Z"}
                },
                {
                    "id": "d2",
                    "text": "خلاصه معماری Noema: ده بلوک ادراک، مدل جهان، حافظه و ...",
                    "signals": {"direction": "rtl", "speech_act": "statement"},
                    "meta": {"commit_time": "2025-11-07T08:00:00Z"}
                },
                {
                    "id": "d3",
                    "text": "Please send the summary of architecture.",
                    "signals": {"direction": "ltr", "speech_act": "request"},
                    "meta": {"commit_time": "2025-10-30T10:00:00Z"}
                }
            ]
        },
        "retrieval": {"top_k": 2}
    }
    out = b3f3_retrieve(sample)
    for r in out["memory"]["retrieval"]["results"]:
        print(r["id"], r["score"], r["components"])
