# Folder: noema/n3_core/block_4_concept_graph
# File:   b4f3_edge_scorer.py

from __future__ import annotations

import math
import re
from typing import Any, Dict, List, Tuple, Optional

import unicodedata

__all__ = ["b4f3_score_edges"]

RULES_VERSION = "1.0"
MAX_EDGES_OUT = 1200
MAX_LABELS_PER_EDGE = 3

RE_WS = re.compile(r"\s+", re.UNICODE)
RE_PUNCT = re.compile(r"[^\w\s\u200c\-’']", re.UNICODE)  # keep ZWNJ and hyphen/quotes


# ------------------------- utils -------------------------

def _nfc_cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold()


def _collapse_ws(s: str) -> str:
    return RE_WS.sub(" ", s).strip()


def _canon_key(term: str) -> str:
    s = _nfc_cf(term)
    s = RE_PUNCT.sub(" ", s)
    return _collapse_ws(s)


def _edge_key(u: str, v: str) -> Tuple[str, str]:
    return (u, v) if u < v else (v, u)


def _squash_pos(x: float) -> float:
    # Smooth 0..1 squash for non-negative inputs
    x = max(0.0, float(x))
    return 1.0 - math.exp(-x)


def _safe_float(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _safe_int(x: Any, default: int = 0) -> int:
    return int(x) if isinstance(x, (int, float)) else default


# ------------------------- collectors -------------------------

def _get_patterns_edges(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    cg = inp.get("concept_graph", {})
    if isinstance(cg, dict):
        pat = cg.get("patterns", {})
        if isinstance(pat, dict) and isinstance(pat.get("edges"), list):
            return [e for e in pat["edges"] if isinstance(e, dict)]
    return []


def _get_nodes_and_map(inp: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, str]]:
    cg = inp.get("concept_graph", {})
    nodes = []
    t2n = {}
    if isinstance(cg, dict):
        nd = cg.get("nodes", {})
        if isinstance(nd, dict):
            if isinstance(nd.get("nodes"), list):
                nodes = [n for n in nd["nodes"] if isinstance(n, dict)]
            if isinstance(nd.get("term_to_node"), dict):
                t2n = {str(k): str(v) for k, v in nd["term_to_node"].items()}
    return nodes, t2n


# ------------------------- mapping terms -> node ids -------------------------

def _build_canonical_index(nodes: List[Dict[str, Any]]) -> Dict[str, str]:
    idx: Dict[str, str] = {}
    for n in nodes:
        key = n.get("key")
        if isinstance(key, str) and key:
            idx[_canon_key(key)] = n.get("id", "")
        # also index aliases/surfaces if present
        for al in n.get("aliases", []) or []:
            if isinstance(al, str):
                idx[_canon_key(al)] = n.get("id", "")
        for sv in n.get("surfaces", []) or []:
            if isinstance(sv, str):
                idx[_canon_key(sv)] = n.get("id", "")
    return {k: v for k, v in idx.items() if v}


def _map_term_to_node_id(term: str, t2n: Dict[str, str], canon_idx: Dict[str, str]) -> Optional[str]:
    # 1) direct map from B4F2
    if term in t2n:
        return t2n[term]
    # 2) canonical fallback
    can = _canon_key(term)
    return canon_idx.get(can)


# ------------------------- scoring -------------------------

def _node_score(nodes_by_id: Dict[str, Dict[str, Any]], nid: str) -> float:
    n = nodes_by_id.get(nid, {})
    return _safe_float(n.get("score"), 0.0)


def _edge_weight(pmi: float, cooc: int, ns: float) -> Tuple[float, Dict[str, float]]:
    # Squash PMI to 0..1, cooc to ~0..1, and normalize node strength
    pmi_n = _squash_pos(pmi)  # fast monotone
    cooc_n = _squash_pos(cooc / 5.0)  # ≈5 cooc → ~0.63
    ns_n = _squash_pos(ns / 3.0)  # node score ~3 → ~0.63

    # Blend (sum ~ 1.0)
    W_PMI, W_COO, W_NS = 0.5, 0.3, 0.2
    w = W_PMI * pmi_n + W_COO * cooc_n + W_NS * ns_n
    return (round(min(1.0, max(0.0, w)), 6), {
        "pmi_n": round(pmi_n, 6),
        "cooc_n": round(cooc_n, 6),
        "node_strength_n": round(ns_n, 6),
    })


# ------------------------- main -------------------------

def b4f3_score_edges(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B4F3 — ConceptGraph.EdgeScorer (Noema)

    Input:
      {
        "concept_graph": {
          "patterns": { "edges": [ {"a": str, "b": str, "cooc": int, "pmi": float}, ... ] },
          "nodes": { "nodes": [ ... ], "term_to_node": { term: node_id } }
        }
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "concept_graph": {
          "edges": {
            "edges": [
              { "u": str, "v": str, "w": float, "pmi": float, "cooc": int, "support": int,
                "labels": [str,...], "undirected": True, "components": {"pmi_n":..., "cooc_n":..., "node_strength_n":...} }
            ],
            "meta": { "source": "B4F3", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_edges|no_nodes|no_mapping", "counts": { "input_pairs": int, "linked_pairs": int, "output_edges": int } }
      }
    """
    pat_edges = _get_patterns_edges(input_json)
    nodes, t2n = _get_nodes_and_map(input_json)

    if not pat_edges:
        return {
            "status": "SKIP",
            "concept_graph": {"edges": {"edges": [], "meta": {"source": "B4F3", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_edges", "counts": {"input_pairs": 0, "linked_pairs": 0, "output_edges": 0}},
        }
    if not nodes:
        return {
            "status": "SKIP",
            "concept_graph": {"edges": {"edges": [], "meta": {"source": "B4F3", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_nodes",
                     "counts": {"input_pairs": len(pat_edges), "linked_pairs": 0, "output_edges": 0}},
        }

    canon_idx = _build_canonical_index(nodes)
    nodes_by_id = {n.get("id"): n for n in nodes if isinstance(n.get("id"), str)}

    # Aggregate edges by node-id pair
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    linked_pairs = 0

    for e in pat_edges:
        a = e.get("a");
        b = e.get("b")
        if not isinstance(a, str) or not isinstance(b, str):
            continue
        na = _map_term_to_node_id(a, t2n, canon_idx)
        nb = _map_term_to_node_id(b, t2n, canon_idx)
        if not na or not nb or na == nb:
            continue

        u, v = _edge_key(na, nb)
        pmi = _safe_float(e.get("pmi"), 0.0)
        cooc = _safe_int(e.get("cooc"), 0)

        # Merge
        bucket = agg.get((u, v))
        if not bucket:
            bucket = {"u": u, "v": v, "pmi": 0.0, "cooc": 0, "labels": set()}
            agg[(u, v)] = bucket

        bucket["pmi"] = max(bucket["pmi"], pmi)  # keep strongest PMI seen
        bucket["cooc"] += max(0, cooc)  # sum support
        if len(bucket["labels"]) < MAX_LABELS_PER_EDGE:
            bucket["labels"].add(f"{a} ~ {b}")
        linked_pairs += 1

    if not agg:
        return {
            "status": "SKIP",
            "concept_graph": {"edges": {"edges": [], "meta": {"source": "B4F3", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_mapping",
                     "counts": {"input_pairs": len(pat_edges), "linked_pairs": 0, "output_edges": 0}},
        }

    # Score and finalize
    out_edges: List[Dict[str, Any]] = []
    for (u, v), data in agg.items():
        ns = min(_node_score(nodes_by_id, u), _node_score(nodes_by_id, v))
        w, comps = _edge_weight(data["pmi"], data["cooc"], ns)
        out_edges.append({
            "u": u, "v": v,
            "w": w,
            "pmi": round(float(data["pmi"]), 6),
            "cooc": int(data["cooc"]),
            "support": int(data["cooc"]),
            "labels": list(data["labels"])[:MAX_LABELS_PER_EDGE],
            "undirected": True,
            "components": comps,
        })

    # Rank & clip
    out_edges.sort(key=lambda x: (x["w"], x["cooc"], x["pmi"]), reverse=True)
    if len(out_edges) > MAX_EDGES_OUT:
        out_edges = out_edges[:MAX_EDGES_OUT]

    return {
        "status": "OK",
        "concept_graph": {
            "edges": {
                "edges": out_edges,
                "meta": {"source": "B4F3", "rules_version": RULES_VERSION},
            }
        },
        "diag": {
            "reason": "ok",
            "counts": {"input_pairs": len(pat_edges), "linked_pairs": linked_pairs, "output_edges": len(out_edges)},
        },
    }


if __name__ == "__main__":
    sample = {
        "concept_graph": {
            "patterns": {
                "edges": [
                    {"a": "گراف مفهومی", "b": "الگو", "cooc": 7, "pmi": 2.1},
                    {"a": "concept graph", "b": "patterns", "cooc": 5, "pmi": 1.6},
                    {"a": "folder structure", "b": "noema", "cooc": 3, "pmi": 1.2},
                ]
            },
            "nodes": {
                "nodes": [
                    {"id": "n1", "key": "گراف مفهومی", "n": 2, "lang": "fa", "score": 3.4, "surfaces": [],
                     "aliases": []},
                    {"id": "n2", "key": "الگو", "n": 1, "lang": "fa", "score": 2.2},
                    {"id": "n3", "key": "concept graph", "n": 2, "lang": "en", "score": 3.1},
                    {"id": "n4", "key": "patterns", "n": 1, "lang": "en", "score": 1.8},
                    {"id": "n5", "key": "noema", "n": 1, "lang": "en", "score": 2.5},
                    {"id": "n6", "key": "folder structure", "n": 2, "lang": "en", "score": 1.9},
                ],
                "term_to_node": {
                    "گراف مفهومی": "n1",
                    "الگو": "n2",
                    "concept graph": "n3",
                    "patterns": "n4",
                    "noema": "n5",
                    "folder structure": "n6"
                }
            }
        }
    }
    res = b4f3_score_edges(sample)
    print(res["diag"])
    for e in res["concept_graph"]["edges"]["edges"]:
        print(e["u"], e["v"], e["w"], e["components"])
