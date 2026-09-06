"""Validation of proposed grapheme→IPA values, against the consumer.

The rule sets reviewed here are consumed by Epitran/PanPhon in the indexing
pipeline. A value that PanPhon cannot use is worthless however good the
linguistic judgement behind it, so every proposal is checked against PanPhon
*before* it is recorded — collecting expert time on a value the pipeline will
silently discard is the worst outcome available (place#252, non-negotiable 2).

Three things here are not obvious and each of them has already bitten this
project (place#251):

1. **Parsing successfully is not enough.** PanPhon accepts ``ⁿɡ`` and hands
   back ``['ɡ']``; ``dʒʰ`` comes back as ``['d', 'ʒ']``. No error is raised —
   the prenasalisation and the aspiration are simply gone. So the check is not
   "did it parse?" but "do the parsed segments still spell what was submitted?"
   See :func:`validate_ipa`.

2. **Normalisation decides the answer, in both directions.** PanPhon segments
   ``ẽ`` when it is decomposed (U+0065 U+0303) and drops the tilde when it is
   composed (U+1EBD) — the same glyph, opposite outcomes. Meanwhile a
   duplicate-grapheme check has to compare NFD because Unicode's composition
   exclusions mean NFC will *not* merge Gurmukhi ``ਸ਼`` written precomposed
   (U+0A36) with the same letter written decomposed (U+0A38 U+0A3C); a rule set
   carrying both fails to load. Everything here is therefore normalised to NFD
   before it is compared or stored.

3. **Confusables render identically.** ASCII ``g`` (U+0067) is not IPA ``ɡ``
   (U+0261) and PanPhon rejects it, but no reviewer will see the difference in
   a form field. They are named explicitly rather than left to the generic
   "unparseable" message, which would tell the reviewer nothing actionable.
"""

import functools
import hashlib
import os
import unicodedata

# The literal EMPTY SET glyph. Epitran's own 139 native rule sets use it zero
# times; the convention for "this grapheme produces nothing" is an empty field.
# Left in a Phon value it is likely emitted straight into the output.
EMPTY_SET = '∅'

# Characters that render as (or are routinely mistaken for) an IPA symbol but
# are a different codepoint. Keyed by the wrong character → (right character,
# human explanation). Deliberately short: every entry is a defect actually
# observed in the shipped rule sets or in review, not a speculative catalogue.
CONFUSABLES = {
    'g': ('ɡ', "ASCII 'g' (U+0067) is not the IPA voiced velar plosive "
                         "'ɡ' (U+0261). They render alike; PanPhon rejects the ASCII one."),
    ':': ('ː', "ASCII colon ':' (U+003A) is not the IPA length mark "
                         "'ː' (U+02D0)."),
    "'": ('ʼ', 'ASCII apostrophe (U+0027) is not the IPA ejective/modifier '
               'letter apostrophe "ʼ" (U+02BC).'),
    '?': ('ʔ', "ASCII question mark '?' (U+003F) is not the IPA glottal stop "
                         "'ʔ' (U+0294)."),
}


def nfd(value):
    """Canonical decomposition — the one normal form used throughout this app.

    NFC is wrong here: composition exclusions leave some letters unmergeable
    under NFC, so two spellings of one grapheme survive as distinct keys and
    the rule set will not load. NFD collapses them.
    """
    return unicodedata.normalize('NFD', value or '')


def codepoints(value):
    """``'ka'`` → ``'U+006B U+0061'``. Shown next to every grapheme in the UI so
    that a confusable or a stray combining mark is visible rather than implied."""
    return ' '.join('U+%04X' % ord(ch) for ch in value or '')


@functools.lru_cache(maxsize=1)
def feature_table():
    """The PanPhon FeatureTable, built once.

    Construction reads a 350KB CSV and compiles a large alternation regex, so
    it must not happen per request.
    """
    import panphon
    return panphon.FeatureTable()


@functools.lru_cache(maxsize=1)
def panphon_provenance():
    """Which PanPhon a verdict was validated against.

    Recorded on every Review. The rules are consumed on a different host by a
    different install, so "it validated" is only meaningful alongside *what*
    validated it — and the segment inventory lives in ``ipa_all.csv``, whose
    digest identifies it far more precisely than a release number does. See
    ``tests.py::PanphonPinTests`` for the pin this app targets.
    """
    import panphon
    from importlib.metadata import version, PackageNotFoundError
    try:
        release = version('panphon')
    except PackageNotFoundError:  # pragma: no cover - packaging accident only
        release = 'unknown'
    path = os.path.join(os.path.dirname(panphon.__file__), 'data', 'ipa_all.csv')
    with open(path, 'rb') as fh:
        digest = hashlib.sha256(fh.read()).hexdigest()
    return {'panphon_version': release, 'ipa_all_sha256': digest}


def segment(value):
    """PanPhon's segmentation of ``value``, after NFD normalisation.

    Returns ``(normalised_value, segments)``. Normalising first is not a
    tidiness measure: without it a composed ``ẽ`` is reported as lossy when it
    is perfectly good.
    """
    value = nfd(value)
    if not value:
        return value, []
    return value, feature_table().ipa_segs(value)


def validate_ipa(value, allow_empty=True):
    """Check a proposed IPA value the way its consumer will.

    Returns ``(normalised_value, errors, segments)`` where ``errors`` is a list
    of ``{'code', 'message'}`` dicts — empty means the value is usable. The
    caller must refuse to store anything with errors.

    An empty value is legitimate: it is how Epitran spells "this grapheme
    contributes nothing", and 37 rows across 24 shipped rule sets rely on it.
    ``allow_empty=False`` is for callers who know the row must produce output.
    """
    value = nfd(value)
    errors = []

    if not value:
        if not allow_empty:
            errors.append({'code': 'empty',
                           'message': 'A value is required for this row.'})
        return value, errors, []

    if EMPTY_SET in value:
        errors.append({
            'code': 'empty_set_glyph',
            'message': "Contains the EMPTY SET glyph '∅' (U+2205). To mean "
                       "'produces nothing', leave the value blank — Epitran's own "
                       "rule sets never use this character, and it is likely to be "
                       "emitted into the transcription.",
        })

    for wrong, (right, explanation) in CONFUSABLES.items():
        if wrong in value:
            errors.append({'code': 'confusable',
                           'message': f'{explanation} Did you mean “'
                                      f'{value.replace(wrong, right)}”?'})

    if errors:
        # A confusable or a stray '∅' fully explains the failure and names the
        # fix. Appending "PanPhon recognises no IPA segment here" on top of that
        # would be true, redundant, and would bury the actionable message.
        return value, errors, []

    try:
        segments = feature_table().ipa_segs(value)
    except Exception as exc:  # pragma: no cover - PanPhon does not normally raise
        return value, errors + [{'code': 'parse_error',
                                 'message': f'PanPhon could not parse this value: {exc}'}], []

    rebuilt = ''.join(segments)
    if rebuilt != value:
        if not segments:
            errors.append({
                'code': 'unparseable',
                'message': 'PanPhon recognises no IPA segment in this value, so the '
                           'row would contribute nothing to matching.',
            })
        else:
            # The silent-truncation case. Naming the surviving segments is the
            # point: "invalid" would leave the reviewer guessing which contrast
            # was the one that failed to survive.
            errors.append({
                'code': 'lossy',
                'message': 'PanPhon parses this without complaint but keeps only '
                           f'“{rebuilt}” ({" + ".join(segments)}). The rest is '
                           'discarded silently, so the distinction you are drawing would '
                           'not reach the matching model.',
            })

    return value, errors, segments
