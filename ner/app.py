"""
WHG place-name extraction microservice (Map-your-Data NER / geoparser).

Returns candidate place names from free text. Built on the host via the compose `build:` directive
(same pattern as ./hocuspocus) so the heavy model dependencies never touch the main pulled web image.
Django (workbench.ner_extract) proxies to it over the compose network; it is not exposed publicly and
holds no state.

TWO ENGINES:

  spacy  en_core_web_sm, in-process. Fast (~3 ms/record) but trained on modern news, so on historical
         registers its LABELS are unusable — measured on TNA REQ 2 pleadings it tags Barking and
         Chelmsford ORG, Little Burstead and Plymouth PERSON, and the common noun "cloth" PERSON
         (place#211). Kept as the default and as the fallback when the LLM is unavailable.

  llm    qwen3:0.6b on the shared ./ollama container. ~700x slower, but on ten real TNA REQ 2
         descriptions it recalls 27 of 33 hand-labelled place names at 2.2 s/record, and it splits
         the "<Person> of <Place>" residence formula that spaCy swallows whole. It also reads
         registers spaCy cannot: 8/9 toponyms in a travel narrative against spaCy's 2/9. Its extra
         false positives are almost all personal names, which are cheap because the caller
         reconciles inside a container polygon and no place in the county is called John Fever.

Contract:
  GET  /health          → {"status":"ok","model":"...","engines":{...}}
  POST /extract {text, engine?, labels?}
                        → {"entities":[{"name","label","count","context","role"?,"verbatim"?}],
                           "engine", "text_chars", "truncated"}
"""
import json
import os
import re
import threading
from collections import OrderedDict

import requests
import spacy
from fastapi import FastAPI
from pydantic import BaseModel

# GPE = countries / cities / states; LOC = non-GPE locations (water bodies, mountains, regions);
# FAC = facilities (buildings, bridges, airports) — frequently place-like in historical sources.
ALLOWED_LABELS = {"GPE", "LOC", "FAC"}
MODEL = os.environ.get("NER_MODEL", "en_core_web_sm")
MAX_CHARS = int(os.environ.get("NER_MAX_CHARS", "200000"))
CONTEXT_CHARS = 140

# Keep tok2vec + parser (sentence boundaries for context) + ner; drop what NER doesn't need.
nlp = spacy.load(MODEL, disable=["lemmatizer", "attribute_ruler", "tagger"])
nlp.max_length = MAX_CHARS + 1000

# ── LLM engine (Ollama) ───────────────────────────────────────────────────────
# One instance per host, in the production compose stack, shared with dev over the `whg-llm` bridge —
# so `ollama` resolves identically from both. Empty OLLAMA_URL simply disables the engine.
OLLAMA_URL = (os.environ.get("OLLAMA_URL") or "").rstrip("/")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen3:0.6b")
OLLAMA_TIMEOUT = float(os.environ.get("OLLAMA_TIMEOUT", "120"))
OLLAMA_NUM_THREAD = int(os.environ.get("OLLAMA_NUM_THREAD", "4"))
DEFAULT_ENGINE = os.environ.get("NER_ENGINE", "spacy")
# qwen3 is a reasoning model and will spend its whole token budget thinking about a list of place
# names if allowed to. Ollama accepts `think` on models that cannot think anyway, so this is sent
# unconditionally; set OLLAMA_THINK=true only for a model whose reasoning you actually want.
OLLAMA_THINK = (os.environ.get("OLLAMA_THINK", "false") or "false").strip().lower() == "true"

# The 0.5B model degrades sharply on long inputs, and each chunk costs ~1 s of eight-thread CPU, so
# bound both the chunk size and the number of chunks. Per-row extraction (place#211) sends a few
# hundred characters and never chunks; a pasted article does.
LLM_CHUNK_CHARS = int(os.environ.get("NER_LLM_CHUNK_CHARS", "3000"))
LLM_MAX_CHUNKS = int(os.environ.get("NER_LLM_MAX_CHUNKS", "40"))

# Ollama is configured OLLAMA_NUM_PARALLEL=1, so requests serialise there anyway; this just keeps the
# ner service's own worker threads from piling up behind it.
_llm_gate = threading.Semaphore(int(os.environ.get("NER_LLM_CONCURRENCY", "2")))

LLM_SYSTEM = "You extract place names. Reply with JSON only."

# Benchmarked on ten REAL TNA REQ 2 descriptions (33 hand-labelled place names) plus three
# off-register texts, at 2.2 s/record: 27/33 on REQ 2, 8/9 on a travel narrative thick with Ottoman
# toponyms, no malformed replies. Two rules hold whatever model is configured:
#
#   * A worked example gets COPIED, not followed. An early version ended with
#     {"places": [{"name": "Great Easton", "role": "residence"}]} and the model returned Great Easton
#     — or, on other records, Norwich and Kent — instead of reading the text. Hence "..." as the only
#     example value.
#   * Length is dangerous. Too much instruction sends a small model into a repetition loop in which
#     the reply degenerates into one name over and over until num_predict truncates the JSON.
#     "list EVERY place, in the order they appear" costs recall on every model tested.
#
# The noun list including rivers, mountains and seas is EXACTLY what the model has to be big enough
# to carry, and it is why this service runs qwen3:0.6b rather than the smaller qwen2.5:0.5b that
# scores identically on REQ 2. On qwen2.5:0.5b these three extra nouns trigger the loop above and
# halve recall to 12/33; on qwen3:0.6b they cost nothing on REQ 2 and lift the travel narrative from
# 6/9 to 8/9. Upsizing within the older family does NOT substitute: qwen2.5:1.5b takes the longer
# prompt no better (23/33) at 2.5x the cost, and still misses the sea and the mountain pass.
#
# Re-benchmark before changing a single word, and re-benchmark the prompt whenever the model changes.
LLM_PROMPT = """%s

Which place names appear in the text above? Include towns, villages, parishes, manors, counties,
rivers, mountains and seas. Exclude people's names. Answer as JSON: {"places": ["..."]}
"""

app = FastAPI(title="WHG NER", version="1.1")


class ExtractIn(BaseModel):
    text: str = ""
    labels: list[str] | None = None
    engine: str | None = None          # "spacy" | "llm"; defaults to NER_ENGINE


def _clean(surface: str) -> str:
    s = re.sub(r"\s+", " ", surface).strip()
    s = re.sub(r"^(the|The)\s+", "", s)            # "the Thames" → "Thames"
    return s.strip(" ,.;:’'\"-")


def _context(ent) -> str:
    try:
        sent = ent.sent.text
    except Exception:                              # no sentence boundaries available
        start = max(0, ent.start_char - CONTEXT_CHARS // 2)
        sent = ent.doc.text[start:ent.end_char + CONTEXT_CHARS // 2]
    sent = re.sub(r"\s+", " ", sent).strip()
    if len(sent) > CONTEXT_CHARS:
        sent = sent[:CONTEXT_CHARS].rstrip() + "…"
    return sent


# Quoted string values from a truncated JSON array; excludes the object keys we ask for.
_SALVAGE_RE = re.compile(r'"([^"\\]{2,80})"')


def _find_all(text: str, name: str):
    """Case-insensitive occurrences of `name` as a whole word. Returns a list of start offsets."""
    if not name:
        return []
    try:
        pat = re.compile(r"(?<!\w)" + re.escape(name) + r"(?!\w)", re.IGNORECASE)
    except re.error:
        return []
    return [m.start() for m in pat.finditer(text)]


def _snippet(text: str, start: int, length: int) -> str:
    a = max(0, start - CONTEXT_CHARS // 2)
    b = min(len(text), start + length + CONTEXT_CHARS // 2)
    snip = re.sub(r"\s+", " ", text[a:b]).strip()
    return ("…" if a > 0 else "") + snip + ("…" if b < len(text) else "")


def _split_compound(name: str, text: str):
    """Split "Kingsbridge, Devon" into its parts.

    qwen3 sometimes returns a place together with its container as a single span, which reconciles
    as neither — on one REQ 2 record it cost all three of the record's place names. Split only when
    every part occurs in the source text in its own right, so a name that merely contains a comma
    survives intact.
    """
    if "," not in name:
        return [name]
    parts = [p.strip(" .;:") for p in name.split(",")]
    parts = [p for p in parts if len(p) >= 2]
    if len(parts) < 2 or not all(_find_all(text, p) for p in parts):
        return [name]
    return parts


def _llm_chunks(text: str):
    """Split on blank lines / sentence ends so a place name is never cut in half."""
    if len(text) <= LLM_CHUNK_CHARS:
        return [text] if text.strip() else []
    chunks, buf = [], ""
    for part in re.split(r"(?<=[.;\n])\s+", text):
        if buf and len(buf) + len(part) + 1 > LLM_CHUNK_CHARS:
            chunks.append(buf)
            buf = part
        else:
            buf = f"{buf} {part}".strip() if buf else part
        if len(chunks) >= LLM_MAX_CHUNKS:
            break
    if buf and len(chunks) < LLM_MAX_CHUNKS:
        chunks.append(buf)
    return chunks


def _llm_call(chunk: str):
    """One Ollama generation → list of {"name","role"}. Raises on transport/HTTP failure."""
    r = requests.post(
        OLLAMA_URL + "/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "system": LLM_SYSTEM,
            "prompt": LLM_PROMPT % chunk,
            "stream": False,
            "think": OLLAMA_THINK,
            "format": "json",          # constrained decoding — a model this size will not emit valid JSON otherwise
            "options": {
                "temperature": 0,      # extraction, not composition: never sample
                "num_predict": 512,
                "num_thread": OLLAMA_NUM_THREAD,
            },
        },
        timeout=OLLAMA_TIMEOUT,
    )
    r.raise_for_status()
    raw = (r.json() or {}).get("response") or ""
    try:
        parsed = json.loads(raw)
    except ValueError:
        # A small model that falls into a repetition loop runs out of num_predict mid-array, leaving
        # unterminated JSON. The names before the loop started are usually fine, so recover the
        # quoted strings rather than throwing the whole chunk away. Grounding against the source text
        # (see _extract_llm) discards whatever the loop invented.
        return [{"name": _clean(v), "role": ""} for v in _SALVAGE_RE.findall(raw) if _clean(v)]
    # format=json guarantees an object, but the model chooses the shape inside it. Accept the asked-for
    # {"places":[…]}, a bare list, and the common near-misses rather than losing a whole chunk.
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict):
        items = None
        for key in ("places", "place_names", "placeNames", "names", "results"):
            if isinstance(parsed.get(key), list):
                items = parsed[key]
                break
        if items is None:
            items = []
    else:
        items = []

    out = []
    for it in items:
        if isinstance(it, str):
            out.append({"name": _clean(it), "role": ""})
        elif isinstance(it, dict):
            name = _clean(str(it.get("name") or it.get("place") or ""))
            role = str(it.get("role") or "").strip().lower()
            if name:
                out.append({"name": name, "role": role})
    return out


def _extract_llm(text: str):
    """Aggregate the model's output over chunks, then ground every name back in the source text.

    Grounding matters: a 0.5B model paraphrases and occasionally invents. Names found verbatim get a
    real occurrence count and context snippet; names it produced but that are not in the text are kept
    (a normalised spelling can still reconcile) but marked `verbatim: false` with count 0, so the
    caller can rank or drop them.
    """
    agg = OrderedDict()
    chunks = _llm_chunks(text)
    for chunk in chunks:
        for item in _llm_call(chunk):
            for name in _split_compound(item["name"], text):
                if not name or len(name) > 120:
                    continue
                key = name.lower()
                rec = agg.get(key)
                if rec is None:
                    agg[key] = {"name": name, "role": item.get("role") or ""}
                elif not rec.get("role"):
                    rec["role"] = item.get("role") or ""

    entities = []
    for rec in agg.values():
        hits = _find_all(text, rec["name"])
        entities.append({
            "name": text[hits[0]:hits[0] + len(rec["name"])] if hits else rec["name"],
            "label": "LLM",
            "count": len(hits),
            "context": _snippet(text, hits[0], len(rec["name"])) if hits else "",
            "role": rec["role"],
            "verbatim": bool(hits),
        })
    entities.sort(key=lambda e: (not e["verbatim"], -e["count"], e["name"].lower()))
    return entities, len(chunks)


def _extract_spacy(text: str, labels=None):
    wanted = ALLOWED_LABELS & set(labels) if labels else ALLOWED_LABELS
    doc = nlp(text)
    # Aggregate by case-insensitive name; keep the most frequent surface form + first context.
    agg = OrderedDict()
    for ent in doc.ents:
        if ent.label_ not in wanted:
            continue
        name = _clean(ent.text)
        if not name or len(name) > 120:
            continue
        key = name.lower()
        rec = agg.get(key)
        if rec is None:
            agg[key] = {"name": name, "label": ent.label_, "count": 1,
                        "context": _context(ent), "_forms": {name: 1}}
        else:
            rec["count"] += 1
            rec["_forms"][name] = rec["_forms"].get(name, 0) + 1

    entities = []
    for rec in agg.values():
        rec["name"] = max(rec["_forms"].items(), key=lambda kv: kv[1])[0]  # most common casing
        rec.pop("_forms", None)
        entities.append(rec)
    entities.sort(key=lambda e: (-e["count"], e["name"].lower()))
    return entities


def _llm_status():
    """Cheap reachability probe: is the server up, and is the configured model actually pulled?"""
    if not OLLAMA_URL:
        return {"configured": False, "reachable": False, "model": OLLAMA_MODEL,
                "model_present": False, "detail": "OLLAMA_URL not set"}
    try:
        r = requests.get(OLLAMA_URL + "/api/tags", timeout=5)
        r.raise_for_status()
        tags = [m.get("name", "") for m in (r.json() or {}).get("models") or []]
    except Exception as e:                                   # noqa: BLE001 — report, never raise
        return {"configured": True, "reachable": False, "model": OLLAMA_MODEL,
                "model_present": False, "detail": str(e)[:200]}
    # Ollama reports "qwen2.5:0.5b"; tolerate a caller configuring the bare name.
    present = any(t == OLLAMA_MODEL or t.split(":")[0] == OLLAMA_MODEL.split(":")[0] for t in tags)
    return {"configured": True, "reachable": True, "model": OLLAMA_MODEL,
            "model_present": present, "models": tags}


@app.get("/health")
def health():
    llm = _llm_status()
    return {
        "status": "ok",
        "model": MODEL,
        "default_engine": DEFAULT_ENGINE,
        "engines": {"spacy": {"available": True, "model": MODEL}, "llm": llm},
    }


@app.post("/extract")
def extract(body: ExtractIn):
    text = (body.text or "")[:MAX_CHARS]
    truncated = len(body.text or "") > MAX_CHARS
    engine = (body.engine or DEFAULT_ENGINE or "spacy").lower()

    chunks = 0
    fallback = None
    if engine == "llm":
        if not OLLAMA_URL:
            engine, fallback = "spacy", "llm not configured"
        else:
            acquired = _llm_gate.acquire(timeout=OLLAMA_TIMEOUT)
            if not acquired:
                # Ollama's own queue is bounded too; both paths degrade to spaCy rather than to an
                # error, because extraction with the weaker engine beats no extraction.
                engine, fallback = "spacy", "llm busy"
            else:
                try:
                    entities, chunks = _extract_llm(text)
                except Exception as e:                       # noqa: BLE001
                    engine, fallback = "spacy", f"llm error: {str(e)[:160]}"
                finally:
                    _llm_gate.release()

    if engine != "llm":
        entities = _extract_spacy(text, body.labels)

    out = {"entities": entities, "engine": engine, "text_chars": len(text), "truncated": truncated}
    if engine == "llm":
        out["llm_model"] = OLLAMA_MODEL
        out["llm_chunks"] = chunks
    if fallback:
        out["fallback"] = fallback
    return out
