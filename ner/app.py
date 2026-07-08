"""
WHG place-name extraction microservice (Map-your-Data NER / geoparser).

Runs spaCy NER server-side and returns candidate place names from free text. Built on the host via
the compose `build:` directive (same pattern as ./hocuspocus) so the heavy spaCy + model dependency
never touches the main pulled web image. Django (workbench.ner_extract) proxies to it over the compose
network; it is not exposed publicly and holds no state.

Contract:
  GET  /health          → {"status":"ok","model": "..."}
  POST /extract {text}   → {"entities":[{"name","label","count","context"}], "text_chars", "truncated"}
"""
import os
import re
from collections import OrderedDict

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

app = FastAPI(title="WHG NER", version="1.0")


class ExtractIn(BaseModel):
    text: str = ""
    labels: list[str] | None = None


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


@app.get("/health")
def health():
    return {"status": "ok", "model": MODEL}


@app.post("/extract")
def extract(body: ExtractIn):
    text = (body.text or "")[:MAX_CHARS]
    truncated = len(body.text or "") > MAX_CHARS
    wanted = ALLOWED_LABELS & set(body.labels) if body.labels else ALLOWED_LABELS

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
    return {"entities": entities, "text_chars": len(text), "truncated": truncated}
