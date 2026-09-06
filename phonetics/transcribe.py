"""Apply a rule set to a name, so a mapping can be judged in use.

⚠ **What this is not.** Epitran does more than apply a grapheme map:
language-specific pre- and post-processors, punctuation and numeral handling,
and for some languages a full FST. This module models *only the map* — the part
under review — as longest-match-first substitution, which is what Epitran's
``SimpleEpitran`` does with these CSVs. Output here is therefore
"what these rules alone would do", not "what the pipeline emits". The
distinction is shown in the UI rather than glossed, because a reviewer told the
sandbox is the pipeline will mistrust the tool the first time the two differ.

The reason it earns its place anyway is *residue*: every character no rule
matched is reported. That is the measurement behind place#251's quality table —
Myanmar at 16.6% of names fully converted — and it is the single most useful
thing to put in front of someone deciding whether a row matters. It also makes
the effect of a proposed correction visible on a real name before it is
submitted.
"""

from .validation import nfd

# Marks a character no rule matched. Chosen because it cannot occur in IPA and
# cannot be mistaken for output; note that the literal ∅ appearing in a *rule*
# is a defect (see phonetics.lint), which is a different thing entirely.
RESIDUE_OPEN, RESIDUE_CLOSE = '(', ')'


def build_map(pairs):
    """``[(orth, phon), …]`` → an NFD-keyed dict, longest key wins at match time."""
    return {nfd(orth): nfd(phon) for orth, phon in pairs if orth}


def transcribe(name, mapping):
    """Longest-match-first application of ``mapping`` to ``name``.

    Returns ``{'output', 'residue', 'complete', 'trace'}`` where ``trace`` is the
    per-step account the UI shows: which rule fired on which characters, and
    where nothing did.
    """
    name = nfd(name)
    if not mapping:
        return {'output': name, 'residue': list(name), 'complete': not name, 'trace': []}
    longest = max(len(k) for k in mapping)
    out, residue, trace = [], [], []
    i = 0
    while i < len(name):
        for size in range(min(longest, len(name) - i), 0, -1):
            chunk = name[i:i + size]
            if chunk in mapping:
                out.append(mapping[chunk])
                trace.append({'orth': chunk, 'ipa': mapping[chunk], 'matched': True})
                i += size
                break
        else:
            ch = name[i]
            residue.append(ch)
            # Unmatched characters are surfaced, not dropped. Dropping them is
            # how a rule set with a large hole in it looks like it is working.
            out.append(f'{RESIDUE_OPEN}{ch}{RESIDUE_CLOSE}')
            trace.append({'orth': ch, 'ipa': '', 'matched': False})
            i += 1
    return {'output': ''.join(out), 'residue': residue,
            'complete': not residue, 'trace': trace}


def compare(name, current_pairs, overrides):
    """Current rules against the same rules with ``overrides`` applied.

    ``overrides`` is ``{orth: proposed_ipa}``. Used both by the sandbox and by
    the row form, so a reviewer can see what their correction does to real names
    before they commit to it.
    """
    current = build_map(current_pairs)
    proposed = dict(current)
    proposed.update({nfd(k): nfd(v) for k, v in (overrides or {}).items()})
    before = transcribe(name, current)
    after = transcribe(name, proposed)
    return {'name': name, 'before': before, 'after': after,
            'changed': before['output'] != after['output']}
