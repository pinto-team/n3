# Folder: noema/n3_core/block_1_perception
# File:   b1f7_speech_act.py

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import unicodedata

__all__ = ["b1f7_speech_act"]

RULES_VERSION = "1.0"

QUESTION_PUNCT = {"?", "؟"}
EXCLAM_PUNCT = {"!"}

# Triggers (lowercased, NFC)
EN_GREET = {"hi", "hello", "hey", "dear"}
FA_GREET = {"سلام", "درود"}
EN_THANKS = {"thanks", "thank", "thankyou", "thx", "ty"}
FA_THANKS = {"ممنون", "مرسی", "سپاس", "تشکر"}
EN_APOLOGY = {"sorry", "apologies", "apologize"}
FA_APOLOGY = {"ببخشید", "متاسفم", "متأسفم", "عذرخواهی"}
EN_AFFIRM = {"yes", "yeah", "yep", "ok", "okay", "sure", "roger", "yup"}
FA_AFFIRM = {"بله", "آره", "اره", "اوکی", "باشه", "حتما", "حتماً", "درسته", "قبول"}
EN_NEG = {"no", "nope", "nah", "never"}
FA_NEG = {"نه", "نخیر", "هرگز"}

EN_REQUEST = {"please", "pls", "plz", "could", "would", "can", "couldyou", "wouldyou", "canyou", "help"}
FA_REQUEST = {"لطفا", "لطفاً", "ممکنه", "می‌شه", "می شه", "میشه", "می‌تونی", "میتونی", "خواهشمندم", "بی‌زحمت",
              "بی زحمت"}

FA_IMPERATIVE_VERBS = {
    "بفرست", "بده", "بگو", "باز کن", "بنویس", "اجرا کن", "چک کن", "اضافه کن", "حذف کن", "نشان بده", "نمایش بده"
}
EN_IMPERATIVE_VERBS = {"send", "give", "say", "open", "write", "run", "check", "add", "delete", "show"}

EN_WH = {"what", "why", "how", "when", "where", "which", "who", "whom", "whose"}
EN_AUX_Q = {"is", "are", "am", "do", "does", "did", "can", "could", "would", "will", "should"}
FA_WH = {"چه", "چرا", "چطور", "چگونه", "کی", "کجا", "کدام", "آیا"}

LABELS = [
    "request", "command", "question", "greeting", "thanks",
    "apology", "affirmation", "negation", "exclamation", "statement"
]


def _nfc_casefold(s: str) -> str:
    return unicodedata.normalize("NFC", s).casefold()


def _get_text_tokens_sentences(inp: Dict[str, Any]) -> Tuple[
    str, List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    p = inp.get("perception", {}) if isinstance(inp.get("perception"), dict) else {}
    text = ""
    if isinstance(p.get("normalized_text"), str):
        text = p["normalized_text"]
    elif isinstance(inp.get("text"), str):
        text = inp["text"]
    elif isinstance(inp.get("raw_text"), str):
        text = inp["raw_text"]

    tokens = p.get("tokens") if isinstance(p.get("tokens"), list) else []
    sentences = p.get("sentences") if isinstance(p.get("sentences"), list) else []
    addressing = p.get("addressing") if isinstance(p.get("addressing"), dict) else {}
    return text, tokens, sentences, addressing


def _first_tokens(tokens: List[Dict[str, Any]], k: int = 5) -> List[str]:
    out: List[str] = []
    for t in tokens:
        if len(out) >= k:
            break
        val = t.get("text")
        if isinstance(val, str) and val.strip():
            out.append(val)
    return out


def _contains_any(surface_tokens: List[str], vocab: set) -> bool:
    for t in surface_tokens:
        if _nfc_casefold(t) in vocab:
            return True
    return False


def _has_question_mark(text: str) -> bool:
    return any(ch in QUESTION_PUNCT for ch in text)


def _has_exclamation(text: str) -> bool:
    return any(ch in EXCLAM_PUNCT for ch in text)


def _starts_with_any(surface_tokens: List[str], vocab: set) -> bool:
    if not surface_tokens:
        return False
    return _nfc_casefold(surface_tokens[0]) in vocab


def _starts_with_aux_or_wh(surface_tokens: List[str]) -> bool:
    if not surface_tokens:
        return False
    fst = _nfc_casefold(surface_tokens[0])
    if fst in EN_WH or fst in EN_AUX_Q or fst in FA_WH:
        return True
    return False


def _starts_with_imperative(surface_tokens: List[str]) -> bool:
    if not surface_tokens:
        return False
    fst = surface_tokens[0]
    fst_cf = _nfc_casefold(fst)
    if fst_cf in EN_IMPERATIVE_VERBS:
        return True
    # simple Persian imperative heuristics
    if fst in FA_IMPERATIVE_VERBS:
        return True
    # crude fallback: token starts with "ب"
    return fst.startswith("ب") and len(fst) > 1


def _request_modal_present(tokens5: List[str], text: str) -> bool:
    cf = _nfc_casefold(" ".join(tokens5))
    if any(w in cf for w in ["could you", "would you", "can you"]):
        return True
    if any(w in cf for w in ["می شه", "می‌شه", "میشه", "می‌تونی", "میتونی", "ممکنه"]):
        return True
    if _contains_any(tokens5, {_nfc_casefold(x) for x in EN_REQUEST} | {_nfc_casefold(x) for x in FA_REQUEST}):
        return True
    if "please" in _nfc_casefold(text) or "لطفا" in _nfc_casefold(text) or "لطفاً" in _nfc_casefold(text):
        return True
    return False


def _score_sentence(text: str, tokens: List[str], addressing_to_noema: bool) -> Dict[str, float]:
    scores = {k: 0.0 for k in LABELS}
    has_q = _has_question_mark(text)
    has_exc = _has_exclamation(text)
    tokens5 = tokens[:5]

    # Greeting / Thanks / Apology / Affirmation / Negation
    if _starts_with_any(tokens5, {_nfc_casefold(x) for x in EN_GREET} | {_nfc_casefold(x) for x in FA_GREET}):
        scores["greeting"] += 0.8
    if _contains_any(tokens, {_nfc_casefold(x) for x in EN_THANKS} | {_nfc_casefold(x) for x in FA_THANKS}):
        scores["thanks"] += 0.9
    if _contains_any(tokens, {_nfc_casefold(x) for x in EN_APOLOGY} | {_nfc_casefold(x) for x in FA_APOLOGY}):
        scores["apology"] += 0.9
    if len(tokens) <= 3 and _contains_any(tokens, {_nfc_casefold(x) for x in EN_AFFIRM} | {_nfc_casefold(x) for x in
                                                                                           FA_AFFIRM}):
        scores["affirmation"] += 0.9
    if len(tokens) <= 3 and _contains_any(tokens,
                                          {_nfc_casefold(x) for x in EN_NEG} | {_nfc_casefold(x) for x in FA_NEG}):
        scores["negation"] += 0.9

    # Question / Request
    if has_q or _starts_with_aux_or_wh(tokens5):
        scores["question"] += 0.7
    if _request_modal_present(tokens5, text):
        scores["request"] += 0.8
        if addressing_to_noema:
            scores["request"] += 0.1  # small boost if addressed

    # Command (imperative at start, no strong question cues)
    if _starts_with_imperative(tokens) and not has_q:
        scores["command"] += 0.7
        if addressing_to_noema:
            scores["command"] += 0.1

    # Exclamation
    if has_exc and max(scores["greeting"], scores["thanks"], scores["apology"], scores["affirmation"],
                       scores["negation"]) < 0.6:
        scores["exclamation"] += 0.6

    # Statement default if nothing else strong
    if all(v < 0.5 for k, v in scores.items() if k != "statement"):
        scores["statement"] = 0.6

    # Normalize to 0..1 by clipping at 1.0 (weights are already <=1.0)
    for k in scores:
        if scores[k] > 1.0:
            scores[k] = 1.0
    return scores


def _sentence_text_and_tokens(full_text: str, sent: Dict[str, Any]) -> Tuple[str, List[str], Tuple[int, int]]:
    if not isinstance(sent, dict) or "span" not in sent:
        return full_text, full_text.split(), (0, max(0, len(full_text) - 1))
    sp = sent["span"]
    if not isinstance(sp, dict) or "start" not in sp or "end" not in sp:
        return full_text, full_text.split(), (0, max(0, len(full_text) - 1))
    s, e = int(sp["start"]), int(sp["end"])
    chunk = full_text[s:e + 1]
    toks = chunk.split()
    return chunk, toks, (s, e)


def b1f7_speech_act(input_json: Dict[str, Any]) -> Dict[str, Any]:
    """
    B1F7 — Perception.Text.SpeechAct (Noema)
    Input:
      {
        "perception": {
          "normalized_text": "...",
          "sentences": [ {"text": "...", "span": {"start": int, "end": int}} ]?,
          "tokens": [...],
          "addressing": {"is_to_noema": bool}?
        }
      }
    Output:
      {
        "status": "OK|SKIP|FAIL",
        "perception": {
          "speech_act": {
            "top": str,
            "scores": {label: float},
            "per_sentence": [ { "span": {"start": int, "end": int}, "act": str, "confidence": float } ],
            "cues": [str],
            "meta": { "source": "B1F7", "rules_version": "1.0" }
          }
        },
        "diag": { "reason": "ok|no_text|invalid_text_type" }
      }
    """
    text, tokens_full, sentences, addressing = _get_text_tokens_sentences(input_json)
    if not isinstance(text, str) or text.strip() == "":
        return {
            "status": "SKIP",
            "perception": {"speech_act": {"top": "statement", "scores": {}, "per_sentence": [], "cues": [],
                                          "meta": {"source": "B1F7", "rules_version": RULES_VERSION}}},
            "diag": {"reason": "no_text"},
        }
    if not isinstance(text, str):
        return {"status": "FAIL", "diag": {"reason": "invalid_text_type"}}

    to_noema = bool(addressing.get("is_to_noema")) if isinstance(addressing, dict) else False

    per_sentence: List[Dict[str, Any]] = []
    global_scores = {k: 0.0 for k in LABELS}
    cues: List[str] = []

    if not sentences:
        chunk, toks, span = text, text.split(), (0, max(0, len(text) - 1))
        s_scores = _score_sentence(chunk, toks, to_noema)
        top = max(s_scores.items(), key=lambda kv: kv[1])[0]
        per_sentence.append(
            {"span": {"start": span[0], "end": span[1]}, "act": top, "confidence": round(s_scores[top], 3)})
        for k in LABELS:
            global_scores[k] += s_scores[k]
    else:
        for s in sentences:
            chunk, toks, span = _sentence_text_and_tokens(text, s)
            s_scores = _score_sentence(chunk, toks, to_noema)
            top = max(s_scores.items(), key=lambda kv: kv[1])[0]
            per_sentence.append(
                {"span": {"start": span[0], "end": span[1]}, "act": top, "confidence": round(s_scores[top], 3)})
            for k in LABELS:
                global_scores[k] += s_scores[k]

    # Average over sentences
    if per_sentence:
        for k in LABELS:
            global_scores[k] = round(min(1.0, global_scores[k] / len(per_sentence)), 3)

    top_label = max(global_scores.items(), key=lambda kv: kv[1])[0]

    # Collect simple cues for transparency
    if _has_question_mark(text):
        cues.append("question_mark")
    if _has_exclamation(text):
        cues.append("exclamation_mark")
    toks5 = _first_tokens(tokens_full, 5)
    if _request_modal_present(toks5, text):
        cues.append("request_modal")
    if _starts_with_imperative(text.split()):
        cues.append("imperative_start")
    if to_noema:
        cues.append("addressed_to_noema")

    out = {
        "status": "OK",
        "perception": {
            "speech_act": {
                "top": top_label,
                "scores": global_scores,
                "per_sentence": per_sentence,
                "cues": cues,
                "meta": {"source": "B1F7", "rules_version": RULES_VERSION},
            }
        },
        "diag": {"reason": "ok"},
    }
    return out


if __name__ == "__main__":
    sample = {
        "perception": {
            "normalized_text": "سلام نوما، میشه این فایل رو بررسی کنی؟ ممنون!",
            "sentences": [
                {"text": "سلام نوما، میشه این فایل رو بررسی کنی؟", "span": {"start": 0, "end": 35}},
                {"text": " ممنون!", "span": {"start": 36, "end": 43}},
            ],
            "tokens": [
                {"text": "سلام", "span": {"start": 0, "end": 3}, "type": "word"},
                {"text": "نوما", "span": {"start": 5, "end": 8}, "type": "word"},
                {"text": "میشه", "span": {"start": 11, "end": 14}, "type": "word"},
            ],
            "addressing": {"is_to_noema": True}
        }
    }
    res = b1f7_speech_act(sample)
    print(res["perception"]["speech_act"]["top"], res["perception"]["speech_act"]["scores"])
