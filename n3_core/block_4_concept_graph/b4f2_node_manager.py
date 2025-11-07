# Folder: noema/n3_core/block_4_concept_graph
# File:   b4f2_node_manager.py

from __future__ import annotations

import hashlib
import math
import re
from typing import Any, Dict, List, Tuple

import unicodedata

__all__ = ["b4f2_manage_nodes"]

RULES_VERSION = "1.0"

MAX_NODES_OUT = 600
MAX_SURFACES_PER_NODE = 5
RE_WS = re.compile(r"\s+", re.UNICODE)
RE_PUNCT = re.compile(r"[^\w\s\u200c\-’']", re.UNICODE)  # keep ZWNJ and hyphen/quotes


# ------------------------- helpers -------------------------

def _nfc_cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold()


def _collapse_ws(s: str) -> str:
    return RE_WS.sub(" ", s).strip()


def _lang_hint(txt: str) -> str:
    for ch in txt:
        cp = ord(ch)
        if 0x0600 <= cp <= 0x06FF or 0x0750 <= cp <= 0x08FF or 0xFB50 <= cp <= 0xFEFF:
            return "fa"  # Arabic script; bias to fa for Noema
    for ch in txt:
        if ("A" <= ch <= "Z") or ("a" <= ch <= "z"):
            return "en"
    return "und"


def _canon_key(term: str) -> Tuple[str, int, str]:
    """
    Returns normalized (key, ngram_n, lang).
    """
    s = _nfc_cf(term)
    s = RE_PUNCT.sub(" ", s)
    s = _collapse_ws(s)
    if not s:
        return "", 0, "und"
    n = len(s.split())
    lang = _lang_hint(s)
    return s, n, lang


def _node_id(key: str, n: int, lang: str) -> str:
    h = hashlib.sha1()
    h.update(key.encode("utf-8"))
    h.update(f"|n={n}|{lang}".encode("utf-8"))
    return h.hexdigest()


def _idf(doc_count: int, df: int) -> float:
    # Smooth IDF with +1 to avoid division by zero; shifted by +1 for positive range
    return 1.0 + math.log((1.0 + max(1, doc_count)) / (1.0 + max(1, df)))


def _score(tf: int, df: int, doc_count: int, n: int) -> float:
    # Length-aware TF-IDF: higher n-grams get a slight bonus
    base = tf * _idf(doc_count, df)
    if n == 2:
        base *= 1.08
    elif n >= 3:
        base *= 1.15
    return round(base, 6)


def _merge_surfaces(dst: List[str], src: List[str], cap: int = MAX_SURFACES_PER_NODE) -> List[str]:
    out = list(dst)
    for s in src:
        if s and s not in out:
            out.append(s)
        if len(out) >= cap:
            break
    return out[:cap]


# ------------------------- collectors -------------------------

def _get_patterns(inp: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], int]:
    cg = inp.get("concept_graph", {})
    if isinstance(cg, dict):
        pat = cg.get("patterns", {})
        if isinstance(pat, dict) and isinstance(pat.get("terms"), list):
            terms = pat["terms"]
            # Extract doc count from diag if present
            diag = inp.get("concept_graph", {}).get("patterns", {})
            # Fallback doc count (unknown → 10 as mild proxy)
            doc_count = 10
            # Try to detect from caller's diag if supplied
            if isinstance(inp.get("diag"), dict):
                dc = inp["diag"].get("counts", {}).get("docs")
                if isinstance(dc, int) and dc > 0:
                    doc_count = dc
            # Or if patterns stored meta with docs
            if isinstance(pat.get("meta"), dict):
                maybe_docs = pat["meta"].get("docs")
                if isinstance(maybe_docs, int) and maybe_docs > 0:
                    doc_count = maybe_docs
            return terms, doc_count
    return [], 10


def _get_existing_nodes(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    cg = inp.get("concept_graph", {})
    if isinstance(cg, dict) and isinstance(cg.get("nodes"), list):
        return [n for n in cg["nodes"] if isinstance(n, dict)]
    mem = inp.get("memory", {})
    if isinstance(mem, dict) and isinstance(mem.get("nodes"), list):
        return [n for n in mem["nodes"] if isinstance(n, dict)]
    return []


# ------------------------- main -------------------------

def b4f2_manage_nodes(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B4F2 — ConceptGraph.NodeManager (Noema)
    Converts mined pattern terms into canonical concept nodes and merges them with optional existing nodes.

    Input:
      {
        "concept_graph": {
          "patterns": { "terms": [ {"key": str, "tf": int, "df": int, "surfaces": [str,...]} ] },
          "nodes": [ {existing nodes ...} ]?    # optional, will be merged
        },
        "memory": { "nodes": [ ... ] }?          # optional alternative source
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "concept_graph": {
          "nodes": {
            "nodes": [
              { "id": str, "key": str, "n": 1|2|3, "lang": "fa|en|und",
                "tf": int, "df": int, "score": float,
                "surfaces": [str,...], "aliases": [str,...] }
            ],
            "term_to_node": { original_term_key: node_id, ... },
            "meta": { "source": "B4F2", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_terms", "counts": { "input_terms": int, "output_nodes": int, "merged_from_existing": int } }
      }
    """
    terms, doc_count = _get_patterns(input_json)
    existing = _get_existing_nodes(input_json)

    if not terms and not existing:
        return {
            "status": "SKIP",
            "concept_graph": {
                "nodes": {"nodes": [], "term_to_node": {}, "meta": {"source": "B4F2", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_terms", "counts": {"input_terms": 0, "output_nodes": 0, "merged_from_existing": 0}},
        }

    # Seed map from existing nodes (by canonical key)
    by_key: Dict[Tuple[str, int, str], Dict[str, Any]] = {}
    merged_from_existing = 0
    for en in existing:
        key = en.get("key")
        n = int(en.get("n", 0))
        lang = en.get("lang") if isinstance(en.get("lang"), str) else _lang_hint(key or "")
        if isinstance(key, str) and key:
            can = (key, n, lang)
            node = {
                "id": en.get("id") if isinstance(en.get("id"), str) else _node_id(key, n, lang),
                "key": key,
                "n": n,
                "lang": lang,
                "tf": int(en.get("tf", 0)),
                "df": int(en.get("df", 1)),
                "score": float(en.get("score", 0.0)),
                "surfaces": list(en.get("surfaces", []))[:MAX_SURFACES_PER_NODE],
                "aliases": list(en.get("aliases", []))[:MAX_SURFACES_PER_NODE],
            }
            by_key[can] = node
            merged_from_existing += 1

    term_to_node: Dict[str, str] = {}

    # Add/merge terms from patterns
    for t in terms:
        if not isinstance(t, dict):
            continue
        raw = t.get("key")
        tf = int(t.get("tf", 0))
        df = int(t.get("df", 1))
        surfaces = [s for s in t.get("surfaces", []) if isinstance(s, str)]
        if not isinstance(raw, str) or not raw:
            continue

        can_key, n, lang = _canon_key(raw)
        if not can_key:
            continue
        nid = _node_id(can_key, n, lang)

        # Merge into bucket
        bucket = by_key.get((can_key, n, lang))
        if not bucket:
            bucket = {
                "id": nid,
                "key": can_key,
                "n": n,
                "lang": lang,
                "tf": 0,
                "df": 0,
                "score": 0.0,
                "surfaces": [],
                "aliases": [],
            }
            by_key[(can_key, n, lang)] = bucket

        bucket["tf"] += max(0, tf)
        bucket["df"] = max(bucket["df"], max(1, df))
        bucket["surfaces"] = _merge_surfaces(bucket["surfaces"], surfaces)
        # Keep the original surface term as alias if it differs from canonical
        if _collapse_ws(_nfc_cf(raw)) != can_key and raw not in bucket["aliases"]:
            if len(bucket["aliases"]) < MAX_SURFACES_PER_NODE:
                bucket["aliases"].append(raw)

        # Map the original term key to node id
        term_to_node[raw] = bucket["id"]

    # Compute scores
    nodes: List[Dict[str, Any]] = []
    for (_, _, _), node in by_key.items():
        node["score"] = _score(node["tf"], node["df"], doc_count, node["n"])
        nodes.append(node)

    # Rank and clip
    nodes.sort(key=lambda x: (x["score"], x["tf"], -x["n"]), reverse=True)
    if len(nodes) > MAX_NODES_OUT:
        nodes = nodes[:MAX_NODES_OUT]

    return {
        "status": "OK",
        "concept_graph": {
            "nodes": {
                "nodes": nodes,
                "term_to_node": term_to_node,
                "meta": {"source": "B4F2", "rules_version": RULES_VERSION},
            }
        },
        "diag": {
            "reason": "ok",
            "counts": {"input_terms": len(terms), "output_nodes": len(nodes),
                       "merged_from_existing": merged_from_existing},
        },
    }


if __name__ == "__main__":
    sample = {
        "concept_graph": {
            "patterns": {
                "terms": [
                    {"key": "ساختار پوشه", "tf": 3, "df": 2, "surfaces": ["ساختار پوشه پروژه ..."]},
                    {"key": "folder structure", "tf": 2, "df": 2, "surfaces": ["folder structure of noema"]},
                    {"key": "گراف مفهومی", "tf": 4, "df": 3, "surfaces": ["گراف مفهومی را بساز"]},
                    {"key": "concept graph", "tf": 3, "df": 3, "surfaces": ["concept graph builds ..."]},
                ],
                "meta": {"docs": 5}
            },
            "nodes": [
                {"id": "oldx", "key": "noema", "n": 1, "lang": "en", "tf": 5, "df": 4, "score": 2.1,
                 "surfaces": ["Noema"], "aliases": []}
            ]
        }
    }
    out = b4f2_manage_nodes(sample)
    print(out["diag"])
    for n in out["concept_graph"]["nodes"]["nodes"][:3]:
        print(n["id"], n["key"], n["n"], n["lang"], n["score"])
