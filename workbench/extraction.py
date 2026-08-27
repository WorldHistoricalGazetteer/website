"""Place-name extraction for Map-your-Data (place#211).

Sends text to the on-host Ollama container and returns the place names it finds, each grounded back
in the source text with an occurrence count and a context snippet. This is the one Map-your-Data step
that leaves the browser; the UI says so, and nothing is stored.

This replaced a spaCy microservice (``en_core_web_sm`` behind FastAPI). spaCy's labels are unusable
on the registers WHG's contributors actually bring: on TNA REQ 2 pleadings it tags Barking and
Chelmsford ORG, Little Burstead and Plymouth PERSON, and the common noun "cloth" PERSON — so the
GPE/LOC/FAC filter discarded real places before anything downstream could rescue them. Measured on
ten of those records (33 hand-labelled names) the local model scores 30/33 against spaCy's 20/33, and
on a travel narrative thick with Ottoman toponyms 7/9 against 2/9.

The cost is real: ~2.8 s a record against spaCy's 3 ms. Ollama is configured for one request at a
time behind a bounded queue, so concurrent callers wait rather than trampling production's CPU.
"""
import json
import logging
import os
import re

import requests
from django.conf import settings

logger = logging.getLogger(__name__)

CONTEXT_CHARS = 140

# The model degrades sharply on long inputs and each chunk costs seconds of CPU, so bound both the
# chunk size and the number of chunks. Per-row extraction sends a few hundred characters and never
# chunks; a pasted article does.
CHUNK_CHARS = 3000
MAX_CHUNKS = 40

SYSTEM = "You extract place names. Reply with JSON only."

# Benchmarked on ten REAL TNA REQ 2 descriptions (33 hand-labelled place names) plus three
# off-register texts, at ~2.2 s/record: 27/33 on REQ 2 from the prompt alone, 8/9 on a travel
# narrative thick with Ottoman toponyms, no malformed replies. Two rules hold whatever model is
# configured:
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
# to carry, and it is why this runs qwen3:0.6b rather than the smaller qwen2.5:0.5b that scores
# identically on REQ 2. On qwen2.5:0.5b these three extra nouns trigger the loop above and halve
# recall to 12/33; on qwen3:0.6b they cost nothing on REQ 2 and lift the travel narrative from 6/9 to
# 8/9. Upsizing within the older family does NOT substitute: qwen2.5:1.5b takes the longer prompt no
# better (23/33) at 2.5x the cost, and still misses the sea and the mountain pass.
#
# Re-benchmark before changing a single word, and re-benchmark the prompt whenever the model changes.
PROMPT = """%s

Which place names appear in the text above? Include towns, villages, parishes, manors, counties,
rivers, mountains and seas. Exclude people's names. Answer as JSON: {"places": ["..."]}
"""

# Quoted string values recovered from a truncated JSON array.
_SALVAGE_RE = re.compile(r'"([^"\\]{2,80})"')


class ExtractionUnavailable(Exception):
    """The model host is unconfigured or unreachable — the caller should return a friendly 503."""


def _conf(name, default):
    return getattr(settings, name, None) or default


def _clean(surface):
    s = re.sub(r'\s+', ' ', surface or '').strip()
    s = re.sub(r'^(the|The)\s+', '', s)               # "the Thames" → "Thames"
    return s.strip(' ,.;:’\'"-')


def find_all(text, name):
    """Case-insensitive occurrences of `name`; returns start offsets.

    Whole-word by default. Word boundaries are a property of scripts that separate words with spaces,
    though: in 在杭州的土地 the character before 杭州 is itself a letter, so the boundary never matches
    and a perfectly real place name looks absent — which downstream is read as the model having
    invented it. So for a name written without case (Chinese, Japanese, Korean and the other
    unspaced scripts) fall back to a plain substring search.
    """
    if not name:
        return []
    try:
        pat = re.compile(r'(?<!\w)' + re.escape(name) + r'(?!\w)', re.IGNORECASE)
    except re.error:
        return []
    hits = [m.start() for m in pat.finditer(text)]
    if hits or any(ch.isupper() or ch.islower() for ch in name):
        return hits
    out, i = [], text.find(name)
    while i >= 0:
        out.append(i)
        i = text.find(name, i + 1)
    return out


def snippet(text, start, length):
    a = max(0, start - CONTEXT_CHARS // 2)
    b = min(len(text), start + length + CONTEXT_CHARS // 2)
    snip = re.sub(r'\s+', ' ', text[a:b]).strip()
    return ('…' if a > 0 else '') + snip + ('…' if b < len(text) else '')


def _split_compound(name, text):
    """Split "Kingsbridge, Devon" into its parts.

    qwen3 sometimes returns a place together with its container as a single span, which reconciles as
    neither — on one REQ 2 record it cost all three of the record's place names. Split only when every
    part occurs in the source text in its own right, so a name that merely contains a comma survives.
    """
    if ',' not in name:
        return [name]
    parts = [p.strip(' .;:') for p in name.split(',')]
    parts = [p for p in parts if len(p) >= 2]
    if len(parts) < 2 or not all(find_all(text, p) for p in parts):
        return [name]
    return parts


def _chunks(text):
    """Split on sentence ends so a place name is never cut in half."""
    if len(text) <= CHUNK_CHARS:
        return [text] if text.strip() else []
    out, buf = [], ''
    for part in re.split(r'(?<=[.;\n])\s+', text):
        if buf and len(buf) + len(part) + 1 > CHUNK_CHARS:
            out.append(buf)
            buf = part
        else:
            buf = f'{buf} {part}'.strip() if buf else part
        if len(out) >= MAX_CHUNKS:
            break
    if buf and len(out) < MAX_CHUNKS:
        out.append(buf)
    return out


def _generate(chunk):
    """One Ollama generation → list of raw place names. Raises on transport/HTTP failure."""
    base = (_conf('OLLAMA_URL', '') or '').rstrip('/')
    if not base:
        raise ExtractionUnavailable('OLLAMA_URL is not configured')
    r = requests.post(
        base + '/api/generate',
        json={
            'model': _conf('OLLAMA_MODEL', 'qwen3:0.6b'),
            'system': SYSTEM,
            'prompt': PROMPT % chunk,
            'stream': False,
            # qwen3 is a reasoning model and will spend its whole token budget deliberating over a
            # list of place names if allowed to. Ollama accepts this field on models that cannot
            # think anyway, so it needs no per-model branching.
            'think': bool(_conf('OLLAMA_THINK', False)),
            'format': 'json',      # constrained decoding — a model this size will not emit valid JSON otherwise
            'options': {
                'temperature': 0,  # extraction, not composition: never sample
                'num_predict': 512,
                'num_thread': int(_conf('OLLAMA_NUM_THREAD', 4)),
            },
        },
        timeout=float(_conf('OLLAMA_TIMEOUT', 120)),
    )
    r.raise_for_status()
    raw = (r.json() or {}).get('response') or ''
    try:
        parsed = json.loads(raw)
    except ValueError:
        # A small model that falls into a repetition loop runs out of num_predict mid-array, leaving
        # unterminated JSON. The names before the loop started are usually fine, so recover the quoted
        # strings rather than throwing the whole chunk away; grounding then discards what it invented.
        return [_clean(v) for v in _SALVAGE_RE.findall(raw) if _clean(v)]

    # `format: json` guarantees an object, but the model chooses the shape inside it. Accept the
    # asked-for {"places": […]}, a bare list, and the common near-misses rather than losing a chunk.
    if isinstance(parsed, list):
        items = parsed
    elif isinstance(parsed, dict):
        items = next((parsed[k] for k in ('places', 'place_names', 'placeNames', 'names', 'results')
                      if isinstance(parsed.get(k), list)), [])
    else:
        items = []

    out = []
    for it in items:
        if isinstance(it, str):
            out.append(_clean(it))
        elif isinstance(it, dict):
            out.append(_clean(str(it.get('name') or it.get('place') or '')))
    return [n for n in out if n]


def extract_places(text):
    """text → [{name, count, context, verbatim}, …], most-mentioned first.

    Every name is grounded back in the source text. Grounding matters: a model this small paraphrases
    and occasionally invents. Names found verbatim get a real occurrence count and a context snippet;
    names it produced but that are not in the text are kept — a normalised spelling can still
    reconcile — but marked `verbatim: False` with count 0, so the caller can rank or drop them.

    Raises ExtractionUnavailable if the model host is unconfigured or unreachable.
    """
    seen = {}
    chunk_list = _chunks(text)
    for chunk in chunk_list:
        try:
            names = _generate(chunk)
        except ExtractionUnavailable:
            raise
        except requests.RequestException as e:
            raise ExtractionUnavailable(str(e))
        for raw_name in names:
            for name in _split_compound(raw_name, text):
                if name and len(name) <= 120:
                    seen.setdefault(name.lower(), name)

    entities = []
    for name in seen.values():
        hits = find_all(text, name)
        entities.append({
            'name': text[hits[0]:hits[0] + len(name)] if hits else name,
            'label': 'LLM',
            'count': len(hits),
            'context': snippet(text, hits[0], len(name)) if hits else '',
            'verbatim': bool(hits),
        })
    entities.sort(key=lambda e: (not e['verbatim'], -e['count'], e['name'].lower()))
    return entities


def health():
    """Is the model host up, and is the configured model actually pulled? Never raises."""
    base = (_conf('OLLAMA_URL', '') or '').rstrip('/')
    model = _conf('OLLAMA_MODEL', 'qwen3:0.6b')
    if not base:
        return {'configured': False, 'reachable': False, 'model': model, 'model_present': False}
    try:
        r = requests.get(base + '/api/tags', timeout=5)
        r.raise_for_status()
        tags = [m.get('name', '') for m in (r.json() or {}).get('models') or []]
    except Exception as e:                            # noqa: BLE001 — report, never raise
        logger.warning('extraction health probe failed: %s', e)
        return {'configured': True, 'reachable': False, 'model': model, 'model_present': False,
                'detail': str(e)[:200]}
    present = any(t == model or t.split(':')[0] == model.split(':')[0] for t in tags)
    return {'configured': True, 'reachable': True, 'model': model, 'model_present': present,
            'models': tags}
