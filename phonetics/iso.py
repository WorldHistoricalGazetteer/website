"""ISO 639-3 / 15924 names, and the browser-language mapping built on them.

Bundled as a static table rather than a runtime dependency: the names never
change between deployments, and a review UI that cannot name the language it is
asking about is worse than useless. Regenerate with
``manage.py phonetics_build_iso_table`` (needs ``pycountry``, a dev-only tool).
"""

import functools
import json
import re
from pathlib import Path

_DATA = Path(__file__).resolve().parent / 'data' / 'iso_names.json'


@functools.lru_cache(maxsize=1)
def _table():
    with _DATA.open(encoding='utf-8') as fh:
        return json.load(fh)


def language_name(code):
    return _table()['languages'].get(code, code)


def script_name(code):
    return _table()['scripts'].get(code, code)


def autonym(code):
    """The language's name in its own language, where that differs from English.

    Empty where the two are the same, so a picker never shows
    "Assyrian Neo-Aramaic — Assyrian Neo-Aramaic". Someone who reads Burmese but
    little English should be able to find မြန်မာ without first knowing that
    English calls it "Burmese".
    """
    return _table().get('autonyms', {}).get(code, '')


def alpha3(code):
    """ISO 639-1 (or -3) → ISO 639-3, which is what the rule sets are keyed by."""
    code = (code or '').lower()
    if len(code) == 3:
        return code
    return _table()['alpha2_to_alpha3'].get(code, code)


_TAG = re.compile(r'^\s*([A-Za-z]{2,3})(?:-([A-Za-z]{4}))?(?:-[A-Za-z0-9]+)*\s*(?:;\s*q=([0-9.]+))?\s*$')


def parse_accept_language(header):
    """``Accept-Language`` → ``[(iso639_3, script_or_'', quality), …]``, best first.

    Used to *offer* likely-relevant rule sets, never to assert competence: the
    browser knows what languages the interface is set to, which is a hint about
    what someone might read and no evidence at all about what they can judge.
    Declared competence remains the thing that routes work.
    """
    out = []
    for part in (header or '').split(','):
        m = _TAG.match(part)
        if not m:
            continue
        lang, script, q = m.group(1), m.group(2) or '', m.group(3)
        try:
            quality = float(q) if q is not None else 1.0
        except ValueError:
            quality = 1.0
        out.append((alpha3(lang), script.title() if script else '', quality))
    seen, deduped = set(), []
    for lang, script, quality in sorted(out, key=lambda t: -t[2]):
        if (lang, script) in seen:
            continue
        seen.add((lang, script))
        deduped.append((lang, script, quality))
    return deduped
