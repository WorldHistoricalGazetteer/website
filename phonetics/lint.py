"""Machine-detectable defects in a rule set.

Opening every rule set for review hands us a queue that needs no linguistic
judgement at all: values that are simply broken. Surfacing those first is worth
doing on its own terms — it is an uncontroversial win that shows reviewers the
tool is worth their time — but the stronger reason is that they must never
reach a reviewer as a *question*. Asking a Burmese speaker whether ``g`` is the
right value for a letter wastes their attention on a defect a regex can find.

The three classes below were measured across the 115 shipped rule sets:
108 defective rows in 42 files. See place#251 for how each was found, and
:mod:`phonetics.validation` for why "did it parse?" is not one of the checks.
"""

from .validation import (CONFUSABLES, EMPTY_SET, LIGATURES, is_modifier_only,
                         nfd, segment)

# code → (short label, why it matters)
LINT_CODES = {
    'ascii_g': (
        'ASCII g',
        "Uses ASCII 'g' (U+0067) where IPA requires 'ɡ' (U+0261). The two render "
        "identically and the consumer rejects the first outright.",
    ),
    'ligature': (
        'Withdrawn ligature',
        'Uses a tie-bar ligature the IPA withdrew in 1989 (ʤ, ʧ, …). The consumer does '
        'not recognise it; it has to be written as two characters.',
    ),
    'confusable': (
        'Look-alike character',
        'Contains a character that renders like an IPA symbol but is a different '
        'codepoint, so the consumer does not recognise it.',
    ),
    'empty_set_glyph': (
        'Literal ∅',
        "Contains the EMPTY SET glyph '∅' (U+2205) instead of an empty field. "
        "Epitran's own 139 rule sets use it zero times; it is likely emitted into "
        "the transcription.",
    ),
    'lossy': (
        'Silently truncated',
        'Parses without error but the consumer keeps only part of it — the rest is '
        'discarded with nothing reported, so the contrast never reaches the model.',
    ),
    'unparseable': (
        'Not recognised',
        'The consumer finds no IPA segment here, so the row contributes nothing. '
        '(A value that is only a modifier — a length mark, a nasal tilde — is fine, '
        'and is not counted here: it attaches to the sound before it.)',
    ),
    'duplicate_grapheme': (
        'Duplicate grapheme',
        'Another row in this rule set defines the same grapheme once NFD-normalised. '
        'Unicode composition exclusions mean the two spellings look identical but do '
        'not merge, and the rule set fails to load.',
    ),
}


def lint_value(ipa):
    """Defect codes for one Phon value. Empty is legitimate, not a defect."""
    ipa = nfd(ipa)
    codes = []
    if not ipa:
        return codes
    if EMPTY_SET in ipa:
        codes.append('empty_set_glyph')
    if 'g' in ipa:
        codes.append('ascii_g')
    for wrong in CONFUSABLES:
        if wrong != 'g' and wrong in ipa:
            codes.append('confusable')
            break
    if any(ligature in ipa for ligature in LIGATURES):
        codes.append('ligature')
    if codes:
        # A look-alike character already explains the failure; running the
        # segmenter would only add 'unparseable' on top and bury the fix.
        return codes
    _, segs = segment(ipa)
    rebuilt = ''.join(segs)
    if rebuilt != ipa:
        if not segs and is_modifier_only(ipa):
            # A length mark, a nasal tilde or a palatalisation on its own is a
            # correct rule, not a broken one: it modifies the preceding sound.
            # 22 shipped rows are this, and flagging them would hand reviewers
            # 22 non-questions. See validation.is_modifier_only.
            return codes
        codes.append('unparseable' if not segs else 'lossy')
    return codes


def lint_rows(rows):
    """Lint a whole rule set: ``[(orth, phon), …]`` → ``{index: [codes]}``.

    Duplicate detection lives here rather than in :func:`lint_value` because it
    is a property of the set, not of a value — and it is the check that would
    have caught the Gurmukhi rule set that will not load.
    """
    result = {}
    seen = {}
    for i, (orth, phon) in enumerate(rows):
        codes = lint_value(phon)
        key = nfd(orth)
        if key in seen:
            codes = codes + ['duplicate_grapheme']
            result.setdefault(seen[key], [])
            if 'duplicate_grapheme' not in result[seen[key]]:
                result[seen[key]] = result[seen[key]] + ['duplicate_grapheme']
        else:
            seen[key] = i
        result[i] = codes
    return result
