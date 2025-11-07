# Folder: noema/n3_core/block_4_concept_graph
# File:   b4f4_rule_extractor.py

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

import hashlib
import json
from datetime import datetime, timezone
import unicodedata

__all__ = ["b4f4_extract_rules"]

RULES_VERSION = "1.1"

# Limits & thresholds
MAX_RULES_OUT = 1200
MAX_LABELS_PER_RULE = 3
THRESH_ASSOC_W = 0.45
THRESH_SYNONYM_JACC = 0.9
THRESH_SYNONYM_W_MIN = 0.30
THRESH_SUBSUME_W = 0.35

RE_WS = re.compile(r"\s+", re.UNICODE)
RE_PUNCT = re.compile(r"[^\w\s\u200c\-’']", re.UNICODE)  # keep ZWNJ and hyphen/quotes

EN_STOPS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at", "by", "from",
    "is", "are", "was", "were", "be", "been", "being", "as", "that", "this", "it", "its", "if",
    "but", "into", "about", "over", "after", "before", "then", "so", "than", "not"
}
FA_STOPS = {
    "و", "یا", "از", "به", "در", "برای", "با", "بی", "بدون", "این", "آن", "که", "را", "تا", "اما", "اگر",
    "بر", "پس", "نه", "هم", "چه", "چرا", "چطور", "چگونه"
}


# ------------------------- utils -------------------------

def _nfc_cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold()


def _collapse_ws(s: str) -> str:
    return RE_WS.sub(" ", s).strip()


def _canon_text(s: str) -> str:
    s = _nfc_cf(s)
    s = RE_PUNCT.sub(" ", s)
    return _collapse_ws(s)


def _tokens(s: str) -> List[str]:
    s = _canon_text(s)
    if not s:
        return []
    toks = s.split()
    out: List[str] = []
    for t in toks:
        if t in EN_STOPS or t in FA_STOPS:
            continue
        out.append(t)
    return out


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a and not b:
        return 1.0
    u = a | b
    if not u:
        return 0.0
    return len(a & b) / len(u)


def _safe_float(x: Any, default: float = 0.0) -> float:
    return float(x) if isinstance(x, (int, float)) else default


def _now_z() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _hash(obj: Any) -> str:
    payload = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


# ------------------------- collectors -------------------------

def _get_nodes(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    cg = inp.get("concept_graph", {})
    if isinstance(cg, dict):
        nd = cg.get("nodes", {})
        if isinstance(nd, dict) and isinstance(nd.get("nodes"), list):
            return [n for n in nd["nodes"] if isinstance(n, dict)]
    return []


def _get_edges(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    cg = inp.get("concept_graph", {})
    if isinstance(cg, dict):
        ed = cg.get("edges", {})
        if isinstance(ed, dict) and isinstance(ed.get("edges"), list):
            return [e for e in ed["edges"] if isinstance(e, dict)]
    return []


# ------------------------- rule builders -------------------------

def _rule_assoc(u: Dict[str, Any], v: Dict[str, Any], e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    w = _safe_float(e.get("w"), 0.0)
    if w < THRESH_ASSOC_W:
        return None
    labels = e.get("labels") or []
    reward = _safe_float(e.get("reward_avg"), 0.0)
    conf = min(1.0, 0.5 * w + 0.2 * _safe_float(e.get("components", {}).get("pmi_n"), 0.0) + 0.3 * reward)
    return {
        "type": "assoc",
        "u": u.get("id"),
        "v": v.get("id"),
        "confidence": round(conf, 6),
        "evidence": {
            "edge_w": round(w, 6),
            "pmi": _safe_float(e.get("pmi"), 0.0),
            "cooc": int(e.get("cooc", 0)),
            "labels": list(labels)[:MAX_LABELS_PER_RULE],
            "reward_avg": round(reward, 6),
        },
        "meta": {"source": "B4F4", "rules_version": RULES_VERSION}
    }


def _rule_synonym(u: Dict[str, Any], v: Dict[str, Any], e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Token Jaccard on keys; allow different langs but prefer same-n
    uk, vk = str(u.get("key", "")), str(v.get("key", ""))
    ut, vt = set(_tokens(uk)), set(_tokens(vk))
    j = _jaccard(ut, vt)
    w = _safe_float(e.get("w"), 0.0)
    if j >= THRESH_SYNONYM_JACC and w >= THRESH_SYNONYM_W_MIN and int(u.get("n", 0)) == int(v.get("n", 0)):
        conf = min(1.0, 0.6 * j + 0.4 * w)
        return {
            "type": "synonym",
            "a": u.get("id"),
            "b": v.get("id"),
            "confidence": round(conf, 6),
            "evidence": {
                "token_jaccard": round(j, 6),
                "edge_w": round(w, 6),
                "keys": [uk, vk],
            },
            "meta": {"source": "B4F4", "rules_version": RULES_VERSION}
        }
    return None


def _rule_subsumes(u: Dict[str, Any], v: Dict[str, Any], e: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # If tokens(shorter) ⊂ tokens(longer) and longer.n > shorter.n and edge is reasonably strong
    uk, vk = str(u.get("key", "")), str(v.get("key", ""))
    ut, vt = set(_tokens(uk)), set(_tokens(vk))
    if not ut or not vt:
        return None
    w = _safe_float(e.get("w"), 0.0)
    if w < THRESH_SUBSUME_W:
        return None

    un, vn = int(u.get("n", 0)), int(v.get("n", 0))
    # Determine parent/child
    parent = None
    child = None
    if ut.issubset(vt) and vn > un:
        parent, child = u, v
        overlap = len(ut) / max(1, len(vt))
    elif vt.issubset(ut) and un > vn:
        parent, child = v, u
        overlap = len(vt) / max(1, len(ut))
    else:
        return None

    conf = min(1.0, 0.5 * w + 0.5 * overlap)
    return {
        "type": "subsumes",
        "parent": parent.get("id"),
        "child": child.get("id"),
        "confidence": round(conf, 6),
        "evidence": {
            "edge_w": round(w, 6),
            "overlap": round(overlap, 6),
            "parent_key": parent.get("key"),
            "child_key": child.get("key"),
        },
        "meta": {"source": "B4F4", "rules_version": RULES_VERSION}
    }


# ------------------------- main -------------------------

def b4f4_extract_rules(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B4F4 — ConceptGraph.RuleExtractor (Noema)

    Input:
      {
        "concept_graph": {
          "nodes": { "nodes": [ ... ] },
          "edges": { "edges": [ {"u": id, "v": id, "w": float, "pmi": float, "cooc": int, "labels": [..], "components": {...}}, ... ] }
        }
      }

    Output:
      {
        "status": "OK|SKIP|FAIL",
        "concept_graph": {
          "rules": {
            "rules": [
              # assoc
              { "type": "assoc", "u": str, "v": str, "confidence": float, "evidence": {...}, "meta": {...} },
              # synonym
              { "type": "synonym", "a": str, "b": str, "confidence": float, "evidence": {...}, "meta": {...} },
              # subsumes
              { "type": "subsumes", "parent": str, "child": str, "confidence": float, "evidence": {...}, "meta": {...} },
            ],
            "meta": { "source": "B4F4", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_nodes|no_edges", "counts": { "assoc": int, "synonym": int, "subsumes": int, "total": int } }
      }
    """
    nodes = _get_nodes(input_json)
    edges = _get_edges(input_json)

    if not nodes:
        return {
            "status": "SKIP",
            "concept_graph": {"rules": {"rules": [], "meta": {"source": "B4F4", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_nodes", "counts": {"assoc": 0, "synonym": 0, "subsumes": 0, "total": 0}},
        }
    if not edges:
        return {
            "status": "SKIP",
            "concept_graph": {"rules": {"rules": [], "meta": {"source": "B4F4", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_edges", "counts": {"assoc": 0, "synonym": 0, "subsumes": 0, "total": 0}},
        }

    nodes_by_id = {n.get("id"): n for n in nodes if isinstance(n.get("id"), str)}
    out_rules: List[Dict[str, Any]] = []
    c_assoc = c_syn = c_sub = 0

    for e in edges:
        uid = e.get("u");
        vid = e.get("v")
        if not isinstance(uid, str) or not isinstance(vid, str):
            continue
        u = nodes_by_id.get(uid);
        v = nodes_by_id.get(vid)
        if not u or not v:
            continue

        r1 = _rule_assoc(u, v, e)
        if r1:
            out_rules.append(r1);
            c_assoc += 1

        r2 = _rule_synonym(u, v, e)
        if r2:
            out_rules.append(r2);
            c_syn += 1

        r3 = _rule_subsumes(u, v, e)
        if r3:
            out_rules.append(r3);
            c_sub += 1

        if len(out_rules) >= MAX_RULES_OUT:
            break

    cg_prev = input_json.get("concept_graph", {}) if isinstance(input_json.get("concept_graph"), dict) else {}
    prev_version = None
    if isinstance(cg_prev.get("version"), dict):
        prev_version = cg_prev["version"].get("id")

    version_payload = {
        "parent": prev_version,
        "rules": out_rules[:MAX_RULES_OUT],
        "node_ids": sorted([n.get("id") for n in nodes if isinstance(n.get("id"), str)])[:120],
        "edge_pairs": sorted([(e.get("u"), e.get("v")) for e in edges if isinstance(e.get("u"), str) and isinstance(e.get("v"), str)])[:200],
    }
    version_doc = {
        "id": _hash(version_payload),
        "parent_id": prev_version,
        "updated_at": _now_z(),
        "counts": {"rules": len(out_rules[:MAX_RULES_OUT]), "edges": len(edges), "nodes": len(nodes)},
        "meta": {"source": "B4F4", "rules_version": RULES_VERSION},
    }

    updates = {
        "new_rules": len(out_rules[:MAX_RULES_OUT]),
        "assoc": c_assoc,
        "synonym": c_syn,
        "subsumes": c_sub,
    }

    return {
        "status": "OK",
        "concept_graph": {
            "rules": {
                "rules": out_rules[:MAX_RULES_OUT],
                "meta": {"source": "B4F4", "rules_version": RULES_VERSION},
            },
            "version": version_doc,
            "updates": updates,
        },
        "diag": {
            "reason": "ok",
            "counts": {"assoc": c_assoc, "synonym": c_syn, "subsumes": c_sub, "total": len(out_rules[:MAX_RULES_OUT])},
        },
    }


if __name__ == "__main__":
    sample = {
        "concept_graph": {
            "nodes": {
                "nodes": [
                    {"id": "n1", "key": "concept graph", "n": 2, "lang": "en", "score": 3.2},
                    {"id": "n2", "key": "graph", "n": 1, "lang": "en", "score": 2.1},
                    {"id": "n3", "key": "گراف مفهومی", "n": 2, "lang": "fa", "score": 3.4},
                    {"id": "n4", "key": "الگو", "n": 1, "lang": "fa", "score": 2.0},
                ]
            },
            "edges": {
                "edges": [
                    {"u": "n1", "v": "n2", "w": 0.62, "pmi": 1.8, "cooc": 7, "labels": ["concept graph ~ graph"]},
                    {"u": "n1", "v": "n3", "w": 0.52, "pmi": 1.4, "cooc": 5, "labels": ["concept graph ~ گراف مفهومی"]},
                    {"u": "n3", "v": "n4", "w": 0.47, "pmi": 1.2, "cooc": 4, "labels": ["گراف مفهومی ~ الگو"]},
                ]
            }
        }
    }
    res = b4f4_extract_rules(sample)
    print(res["diag"])
    for r in res["concept_graph"]["rules"]["rules"]:
        print(r["type"], r["confidence"], r["evidence"])
