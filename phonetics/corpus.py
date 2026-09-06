"""Real place names for a rule set, sampled from WHG's own index.

**Why this exists.** The sandbox is where a mapping stops being abstract: you
type a place name and watch which letters the rules cannot read. Landing on it
with an empty box asks the reviewer to invent a place name in their own script
before they can see anything, which is precisely the friction the page was meant
to remove. So it arrives pre-filled with real names WHG actually holds.

**This is a seed, not a measurement.** Per-rule corpus *frequencies* and the
worked examples that go with them come from the indexing side via
``manage.py import_corpus_stats``, computed over the whole corpus. What is here
is a random handful, good enough to look at and not good enough to prioritise
by — so it never writes ``Rule.corpus_frequency``, and the UI keeps saying "not
counted" until the real figures land.

**Two things about the index that shape the query.** Its ``script`` field holds
coarse names (``LATIN``, ``DEVANAGARI``, ``OTHER``) rather than ISO 15924 codes,
and every script in place#251's table — Myanmar, Sinhala, Gurmukhi, Tibetan,
Lao, Syriac — falls in ``OTHER`` together. Its ``lang`` field is ISO 639-1. So
the filter is by language, and the script test happens here: a name is in this
rule set's script if the rules recognise most of its letters. That test needs no
Unicode block table and is exactly the right question — the script we mean is
the one these rules are written for.
"""

import logging

from django.conf import settings
from django.core.cache import cache

from .iso import _table
from .validation import nfd

logger = logging.getLogger(__name__)

INDEX = 'toponyms'
CACHE_SECONDS = 60 * 60 * 12
# Sampled before filtering. `lang` is not a script tag — a fifth of the names
# under lang=my are romanised — so the pool has to be bigger than the answer.
POOL = 120


def alpha2(code):
    """ISO 639-3 → ISO 639-1, which is what the index is tagged with."""
    for two, three in _table()['alpha2_to_alpha3'].items():
        if three == code:
            return two
    return code[:2] if len(code) == 2 else ''


def _in_script(name, alphabet):
    """True if the rule set's own letters account for most of this name.

    Deliberately not a Unicode-block test. The rules define the script we care
    about; a name written in it will be largely spelled out of their inventory,
    and a romanised name in the same language will not.
    """
    letters = [c for c in nfd(name) if c.isalpha()]
    if not letters:
        return False
    covered = sum(1 for c in letters if c in alphabet)
    return covered >= len(letters) / 2


def sample_names(ruleset, size=10):
    """Up to ``size`` real place names written in this rule set's script.

    Returns ``[]`` — never raises — if the index is unreachable, the language is
    untagged, or nothing matches. The sandbox is useful without it.
    """
    version = ruleset.current_version
    key = f'phonetics:names:{ruleset.slug}:{version.blob_sha[:8] if version else "none"}'
    cached = cache.get(key)
    if cached is not None:
        return cached[:size]

    names = []
    try:
        names = _query(ruleset, size)
    except Exception as exc:  # noqa: BLE001 — a sample is a nicety, never a blocker
        logger.warning('phonetics: could not sample names for %s: %s', ruleset.slug, exc)
    cache.set(key, names, CACHE_SECONDS)
    return names[:size]


def _query(ruleset, size):
    from .models import Rule
    es = getattr(settings, 'ES_CONN', None)
    if es is None:
        return []
    lang = alpha2(ruleset.language_code)
    if not lang:
        return []
    response = es.search(
        index=INDEX, size=POOL, _source=['name'],
        query={'function_score': {'query': {'bool': {'filter': [{'term': {'lang': lang}}]}},
                                  'random_score': {}}})
    alphabet = set()
    for orth in Rule.objects.filter(ruleset=ruleset, present_upstream=True).values_list('orth', flat=True):
        alphabet.update(orth)
    if not alphabet:
        return []

    out, seen = [], set()
    for hit in response['hits']['hits']:
        name = (hit.get('_source') or {}).get('name', '').strip()
        if not name or name in seen or not _in_script(name, alphabet):
            continue
        seen.add(name)
        out.append(name)
        if len(out) >= size * 2:
            break
    return out
