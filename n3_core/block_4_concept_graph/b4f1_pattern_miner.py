# Folder: noema/n3_core/block_4_concept_graph
# File:   b4f1_pattern_miner.py

from __future__ import annotations

import math
import re
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Iterable, Optional

import unicodedata

__all__ = ["b4f1_mine_patterns"]

RULES_VERSION = "1.1"

# ----------------------------- limits -----------------------------
MAX_DOCS = 12
MAX_TERMS_OUT = 500
MAX_EDGES_OUT = 1000
MAX_SURFACES = 3
NGRAM_MAX_N = 3
WINDOW_SIZE = 6
INTENT_PREFIX = "intent::"
TRACE_EDGE_WEIGHT = 0.8

# ----------------------------- regex & stops -----------------------------
RE_WS = re.compile(r"\s+", re.UNICODE)

EN_STOPS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with", "at", "by", "from",
    "is", "are", "was", "were", "be", "been", "being", "as", "that", "this", "it", "its", "if",
    "but", "into", "about", "over", "after", "before", "then", "so", "than", "not"
}
FA_STOPS = {
    "و", "یا", "از", "به", "در", "برای", "با", "بی", "بدون", "این", "آن", "که", "را", "تا", "اما", "اگر",
    "بر", "پس", "نه", "هم", "یا", "چه", "چرا", "چطور", "چگونه"
}


def _nfc_cf(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold()


def _collapse_ws(s: str) -> str:
    return RE_WS.sub(" ", s).strip()


def _is_word_like(tok: str) -> bool:
    if not tok:
        return False
    # Accept alphabetic words (any script) and mixed with internal hyphen or apostrophe
    if tok.replace("-", "").replace("’", "").replace("'", "").isalpha():
        return True
    return False


def _is_stop(tok_cf: str) -> bool:
    return tok_cf in EN_STOPS or tok_cf in FA_STOPS


# ----------------------------- trace helpers -----------------------------

def _intent_key(label: Optional[str]) -> str:
    label = (label or "other").strip()
    if not label:
        label = "other"
    return f"{INTENT_PREFIX}{label}"


def _intent_surface(label: str) -> str:
    return f"Policy intent — {label.replace('_', ' ')}"


def _collect_trace(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    wm = inp.get("world_model", {}) if isinstance(inp.get("world_model"), dict) else {}
    trace = wm.get("trace", {}) if isinstance(wm.get("trace"), dict) else {}
    hist = trace.get("error_history") if isinstance(trace.get("error_history"), list) else []
    out: List[Dict[str, Any]] = []
    for item in hist:
        if not isinstance(item, dict):
            continue
        reward = item.get("reward")
        if not isinstance(reward, (int, float)):
            continue
        target = item.get("target") if isinstance(item.get("target"), str) else None
        actual = item.get("actual") if isinstance(item.get("actual"), str) else None
        top_pred = item.get("top_pred") if isinstance(item.get("top_pred"), str) else None
        if not target and not actual and not top_pred:
            continue
        out.append({
            "reward": float(reward),
            "target": target or top_pred or "other",
            "actual": actual or top_pred or target or "other",
            "top_pred": top_pred or target or actual or "other",
        })
    return out


def _trace_terms(trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = {}
    for item in trace:
        for label in {item.get("target"), item.get("actual"), item.get("top_pred")}:
            if isinstance(label, str) and label:
                key = _intent_key(label)
                counts[key] = counts.get(key, 0) + 1
    terms: List[Dict[str, Any]] = []
    for key, tf in counts.items():
        label = key.split("::", 1)[-1]
        terms.append({
            "key": key,
            "tf": tf,
            "df": max(1, tf // 2 + 1),
            "surfaces": [_intent_surface(label)],
        })
    return terms


def _trace_edges(trace: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for item in trace:
        reward = float(item.get("reward", 0.0))
        target = _intent_key(item.get("target"))
        actual = _intent_key(item.get("actual"))
        top_pred = _intent_key(item.get("top_pred"))

        for a, b, weight in ((target, actual, 1.0), (target, top_pred, TRACE_EDGE_WEIGHT)):
            if a == b:
                continue
            key = tuple(sorted((a, b)))
            bucket = agg.get(key)
            if not bucket:
                bucket = {"a": key[0], "b": key[1], "cooc": 0, "reward_sum": 0.0, "reward_sq": 0.0}
                agg[key] = bucket
            bucket["cooc"] += 1
            bucket["reward_sum"] += reward * weight
            bucket["reward_sq"] += (reward * weight) ** 2

    edges: List[Dict[str, Any]] = []
    for data in agg.values():
        cooc = max(1, int(data["cooc"]))
        reward_avg = data["reward_sum"] / cooc
        reward_var = max(0.0, data["reward_sq"] / cooc - reward_avg ** 2)
        pmi_like = reward_avg * math.log1p(cooc)
        edges.append({
            "a": data["a"],
            "b": data["b"],
            "cooc": cooc,
            "pmi": round(pmi_like, 6),
            "reward_avg": round(reward_avg, 6),
            "reward_var": round(reward_var, 6),
            "labels": [f"{data['a']} ↔ {data['b']}"]
        })
    return edges


# ----------------------------- input collectors -----------------------------
def _as_packz_list(inp: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    # 1) perception.packz (current)
    per = inp.get("perception", {})
    if isinstance(per, dict) and isinstance(per.get("packz"), dict):
        out.append(per["packz"])
    # 2) memory.corpus (optional)
    mem = inp.get("memory", {})
    if isinstance(mem, dict) and isinstance(mem.get("corpus"), list):
        for it in mem["corpus"]:
            if isinstance(it, dict):
                out.append(it.get("packz", it))
    # 3) context.recent_packz (optional)
    ctx = inp.get("context", {})
    if isinstance(ctx, dict) and isinstance(ctx.get("recent_packz"), list):
        for it in ctx["recent_packz"]:
            if isinstance(it, dict):
                out.append(it.get("packz", it))
    # Clip
    uniq: Dict[str, Dict[str, Any]] = {}
    for pk in out:
        if not isinstance(pk, dict):
            continue
        pid = pk.get("id") if isinstance(pk.get("id"), str) else None
        text = pk.get("text") if isinstance(pk.get("text"), str) else None
        if not pid or not text:
            continue
        uniq[pid] = pk
    return list(uniq.values())[:MAX_DOCS]


def _extract_tokens(pk: Dict[str, Any]) -> Tuple[List[str], List[Tuple[int, int]]]:
    """
    Returns (tokens, sentence_spans_in_token_index).
    If spans.sentences exists, split by those slices; else infer a single sentence.
    Tokens are taken from spans.tokens if available; else whitespace split over text.
    """
    spans = pk.get("spans", {}) if isinstance(pk.get("spans"), dict) else {}
    toks = spans.get("tokens") if isinstance(spans.get("tokens"), list) else None
    sents = spans.get("sentences") if isinstance(spans.get("sentences"), list) else None
    text = pk.get("text") if isinstance(pk.get("text"), str) else ""

    # Build token list
    tokens: List[str] = []
    if isinstance(toks, list) and toks:
        for t in toks:
            if isinstance(t, dict) and isinstance(t.get("text"), str):
                tokens.append(t["text"])
    else:
        tokens = [t for t in RE_WS.split(text) if t]

    # Build sentence spans in token index space
    if isinstance(sents, list) and sents and isinstance(toks, list) and toks:
        # Map char offsets to token indices (approx by nearest start)
        idx_by_start: List[int] = []
        for i, t in enumerate(toks):
            sp = t.get("span", {})
            if isinstance(sp, dict) and isinstance(sp.get("start"), int):
                idx_by_start.append((int(sp["start"]), i))
        idx_by_start.sort()
        sent_spans_tok: List[Tuple[int, int]] = []
        for s in sents:
            sp = s.get("span", {})
            if not isinstance(sp, dict):
                continue
            s0 = int(sp.get("start", 0));
            s1 = int(sp.get("end", 0))
            # find first token index >= s0 and last token index <= s1
            start_i = 0
            end_i = len(tokens) - 1
            for cs, i in idx_by_start:
                if cs >= s0:
                    start_i = i
                    break
            for cs, i in reversed(idx_by_start):
                if cs <= s1:
                    end_i = i
                    break
            if 0 <= start_i <= end_i < len(tokens):
                sent_spans_tok.append((start_i, end_i))
        if not sent_spans_tok:
            sent_spans_tok = [(0, len(tokens) - 1)]
        return tokens, sent_spans_tok
    else:
        return tokens, [(0, len(tokens) - 1)] if tokens else []


# ----------------------------- mining -----------------------------
def _candidate_terms(tokens: List[str]) -> List[str]:
    out: List[str] = []
    for t in tokens:
        cf = _nfc_cf(t)
        if not _is_word_like(cf):
            continue
        if _is_stop(cf):
            continue
        out.append(cf)
    return out


def _generate_ngrams(seq: List[str], nmax: int = NGRAM_MAX_N) -> Iterable[str]:
    L = len(seq)
    for n in range(1, nmax + 1):
        if L < n:
            break
        for i in range(L - n + 1):
            yield " ".join(seq[i:i + n])


def _mine_single_doc(pk: Dict[str, Any]) -> Tuple[
    Dict[str, int], Dict[str, int], Dict[Tuple[str, str], int], Dict[str, List[str]]]:
    tokens, sent_spans = _extract_tokens(pk)
    if not tokens:
        return {}, {}, {}, {}

    cand = _candidate_terms(tokens)
    if not cand:
        return {}, {}, {}, {}

    # Term & n-gram counts
    term_tf: Dict[str, int] = defaultdict(int)
    ngram_tf: Dict[str, int] = defaultdict(int)
    surfaces: Dict[str, List[str]] = defaultdict(list)

    # Build by sentences to preserve local order
    for s0, s1 in sent_spans:
        slice_terms = [w for w in _candidate_terms(tokens[s0:s1 + 1])]
        for ng in _generate_ngrams(slice_terms, NGRAM_MAX_N):
            ngram_tf[ng] += 1
            if len(surfaces[ng]) < MAX_SURFACES:
                surfaces[ng].append(" ".join(tokens[s0:s1 + 1])[:120])

        for w in slice_terms:
            term_tf[w] += 1
            if len(surfaces[w]) < MAX_SURFACES:
                surfaces[w].append(tokens[s0:s1 + 1 and s0])

    # Co-occurrence within sliding window over candidate terms (sentence-wise)
    cooc: Dict[Tuple[str, str], int] = defaultdict(int)
    for s0, s1 in sent_spans:
        seq = [w for w in _candidate_terms(tokens[s0:s1 + 1])]
        for i in range(len(seq)):
            for j in range(i + 1, min(i + 1 + WINDOW_SIZE, len(seq))):
                a, b = seq[i], seq[j]
                if a == b:
                    continue
                if a > b:
                    a, b = b, a
                cooc[(a, b)] += 1

    return dict(term_tf), dict(ngram_tf), dict(cooc), dict(surfaces)


def _merge_counts(dst: Dict[str, int], src: Dict[str, int]) -> None:
    for k, v in src.items():
        dst[k] = dst.get(k, 0) + v


def _merge_pairs(dst: Dict[Tuple[str, str], int], src: Dict[Tuple[str, str], int]) -> None:
    for k, v in src.items():
        dst[k] = dst.get(k, 0) + v


def _doc_freq(ng_tf_per_doc: List[Dict[str, int]]) -> Dict[str, int]:
    df: Dict[str, int] = defaultdict(int)
    for d in ng_tf_per_doc:
        for k in d.keys():
            df[k] += 1
    return dict(df)


def _pmi_scores(term_tf: Dict[str, int], pair_tf: Dict[Tuple[str, str], int]) -> Dict[Tuple[str, str], float]:
    total_terms = max(1, sum(term_tf.values()))
    total_pairs = max(1, sum(pair_tf.values()))
    pmi: Dict[Tuple[str, str], float] = {}
    for (a, b), c in pair_tf.items():
        pa = term_tf.get(a, 1) / total_terms
        pb = term_tf.get(b, 1) / total_terms
        pab = c / total_pairs
        val = pab / max(1e-12, pa * pb)
        pmi[(a, b)] = max(0.0, math.log(val, 2))
    return pmi


# ----------------------------- main -----------------------------
def b4f1_mine_patterns(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B4F1 — ConceptGraph.PatternMiner (Noema)
    Input (best-effort):
      - perception.packz (current message)
      - memory.corpus / context.recent_packz (optional mini-corpus)
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "concept_graph": {
          "patterns": {
            "terms": [ { "key": str, "tf": int, "df": int, "surfaces": [str,...] } ],
            "edges": [ { "a": str, "b": str, "cooc": int, "pmi": float } ],
            "meta": { "source": "B4F1", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_docs", "counts": { "docs": int, "terms": int, "pairs": int } }
      }
    """
    docs = _as_packz_list(input_json)
    trace = _collect_trace(input_json)
    if not docs and not trace:
        return {
            "status": "SKIP",
            "concept_graph": {
                "patterns": {"terms": [], "edges": [],
                               "meta": {"source": "B4F1", "rules_version": RULES_VERSION, "docs": 0, "trace_samples": 0}}},
            "diag": {"reason": "no_docs", "counts": {"docs": 0, "terms": 0, "pairs": 0, "trace_samples": 0}},
        }

    corpus_term_tf: Dict[str, int] = {}
    corpus_ng_tf: Dict[str, int] = {}
    corpus_pairs: Dict[Tuple[str, str], int] = {}
    surfaces_all: Dict[str, List[str]] = defaultdict(list)
    ng_tf_docs: List[Dict[str, int]] = []

    for pk in docs:
        t_tf, ng_tf, pairs, sur = _mine_single_doc(pk)
        _merge_counts(corpus_term_tf, t_tf)
        _merge_counts(corpus_ng_tf, ng_tf)
        _merge_pairs(corpus_pairs, pairs)
        ng_tf_docs.append(ng_tf)
        for k, arr in sur.items():
            if k not in surfaces_all:
                surfaces_all[k] = []
            # keep up to MAX_SURFACES examples total
            if len(surfaces_all[k]) < MAX_SURFACES:
                surfaces_all[k].extend(arr[:MAX_SURFACES - len(surfaces_all[k])])

    df = _doc_freq(ng_tf_docs)
    pmi = _pmi_scores(corpus_term_tf, corpus_pairs)

    # Rank and clip outputs
    terms_sorted = sorted(corpus_ng_tf.items(), key=lambda kv: (kv[1], df.get(kv[0], 0)), reverse=True)
    edges_sorted = sorted(corpus_pairs.items(), key=lambda kv: (pmi.get(kv[0], 0.0), kv[1]), reverse=True)

    terms_out = [
        {"key": k, "tf": int(tf), "df": int(df.get(k, 1)), "surfaces": surfaces_all.get(k, [])}
        for k, tf in terms_sorted[:MAX_TERMS_OUT]
    ]
    edges_out = [
        {"a": a, "b": b, "cooc": int(c), "pmi": round(float(pmi.get((a, b), 0.0)), 6)}
        for (a, b), c in edges_sorted[:MAX_EDGES_OUT]
    ]

    if trace:
        terms_out.extend(_trace_terms(trace))
        edges_out.extend(_trace_edges(trace))

    terms_out = sorted(terms_out, key=lambda t: (-int(t.get("tf", 0)), -int(t.get("df", 0))))[:MAX_TERMS_OUT]

    edges_out = sorted(edges_out, key=lambda e: (-e.get("cooc", 0), -e.get("reward_avg", 0.0), -e.get("pmi", 0.0)))[:MAX_EDGES_OUT]

    return {
        "status": "OK",
        "concept_graph": {
            "patterns": {
                "terms": terms_out,
                "edges": edges_out,
                "meta": {
                    "source": "B4F1",
                    "rules_version": RULES_VERSION,
                    "docs": len(docs),
                    "trace_samples": len(trace),
                },
            }
        },
        "diag": {
            "reason": "ok",
            "counts": {
                "docs": len(docs),
                "terms": len(terms_out),
                "pairs": len(edges_out),
                "trace_samples": len(trace),
            },
        },
    }


if __name__ == "__main__":
    sample = {
        "perception": {
            "packz": {
                "id": "cur123",
                "text": "Noema mines concept patterns from recent messages. الگوهای مفهومی را استخراج کن.",
                "spans": {
                    "sentences": [
                        {"text": "Noema mines concept patterns from recent messages.", "span": {"start": 0, "end": 60}},
                        {"text": " الگوهای مفهومی را استخراج کن.", "span": {"start": 61, "end": 93}},
                    ]
                }
            }
        },
        "memory": {
            "corpus": [
                {"id": "d1", "text": "Concept graph builds nodes and edges from patterns."},
                {"id": "d2", "text": "استخراج الگو و هم‌وقوعی برای ساخت گراف مفهومی."}
            ]
        }
    }
    out = b4f1_mine_patterns(sample)
    print(out["diag"], out["concept_graph"]["patterns"]["terms"][:3], out["concept_graph"]["patterns"]["edges"][:3])
