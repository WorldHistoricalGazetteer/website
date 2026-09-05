# Symphonym tokeniser fixtures

Two fixtures, both generated from the **canonical** Symphonym tokeniser — the block between the
`--- BEGIN/END CANONICAL TOKENISER ---` markers in `hf/inference.py` in the
[indexing repo](https://github.com/WorldHistoricalGazetteer/indexing), commit `97a8b31`
(5 September 2026). That is the code the CRC gateway serves and the code the `toponyms` index was
written by. Run `npm run test:symphonym` to check whg3 against them.

## Why they exist

whg3 computes its own `query_vector` in the browser (`whg/webpack/js/recon-symphonym*.js`) and posts
it to `/api/reconcile`, where it is compared against 72.7M vectors written by the canonical code. If
the two tokenise differently the comparison is meaningless — and until 5 September 2026 four
implementations of this tokeniser disagreed. On 46,483,973 documents (63.9% of the index) a query
vector did not match that document's own stored vector, and for CJK/Kana/Hangul the two were
*anti*-correlated (cos −0.30), leaving 3.9M documents effectively unreachable at 0.3% rank-1
self-retrieval. whg3 was the fourth implementation.

The contract is exact equality on `char_ids`, `script_id` and `lang_id`. Do not relax it.

## `symphonym_golden.json` — 35 cases, precision

**Fixture v3**, generated at the indexing repo's working tree. Pins the four named divergences, the
empty-input guard, the FB00–FB17 precedence defect (D5, five cases) and the interpreter-Unicode
question (D6, one case), plus the full 128-value `int8` vector for each. Deliberately *not* a corpus
sample: the equivalence test that failed to catch the script-vote bug ran over 6,013 real names
containing zero digits, so it could not have reached the boundary. A test that cannot reach the
boundary passes for the wrong reason.

Each version has been additive: v2 added six cases to v1's 27, v3 added two more, and neither
renumbered or reordered what came before — only `case` labels and `warning` text were reworded. A
pass/fail mapping against an earlier version still holds.

**The D5 five are a set, not five examples of one thing.** `ﬁ` alone and `ﬀﬂ` are the defect;
`Oﬁce Hill` is the boundary, where the surrounding letters outvote the ligature and the answer comes
out LATIN under *either* precedence rule; `ﬓ` is ARMENIAN for the right reason; `שׁ` (U+FB2A) is the
control just outside the overlap. A fixture containing only the realistic name would have passed
under both rules and taught nobody anything.

**The three D6 cases carry a `warning` field** and the harness echoes each one. Their expected
values are the gateway's (13.0.0); the index writer at 14.0.0 disagrees. Treat them as documentation
of a known disagreement, not as stable expectations, and do not regenerate them on a different
interpreter.

Two of the three discriminate; one deliberately does not, and the fixture says so:

| | | |
|---|---|---|
| `鿽` U+9FFD | OTHER → **CJK** at 14.0.0 | moves `script_id` **and** `char_ids` (`[37574]`→`[1]`), because landing in CJK turns romanisation on — the D6→D1 interaction |
| `ࢵ` U+08B5 | OTHER → **ARABIC** | moves `script_id` only; pairs with the above to separate the two effects |
| `ࡰ` U+0870 | OTHER → OTHER | **moves nothing — cannot fail.** Kept as documentation |

U+0870 is the obvious codepoint to reach for and the wrong one: it is alphabetic at 14.0.0 but sits
outside every named script range (Arabic starts at 0x08A0), so it resolves to OTHER whether the
alpha filter counts it or not. Fixture v2 had it as its only D6 case; substituting `\p{L}` here and
watching all 33 cases still pass is how that was found. It is kept, and labelled, so nobody re-adds
it believing it tests something.

The `int8` vectors come from the fp32 PyTorch reference. `static/webpack/symphonym/symphonym.onnx`
is a *dynamically quantised int8* export, so it cannot reproduce them bit for bit — measured, the
export costs at most 2 of 127 on any component and never drops cosine below 0.9976. The harness
therefore checks the vector against a bound rather than for equality, and the bound's job is to
catch a stale or mismatched ONNX asset, not a tokeniser regression (the ids catch those).

## `symphonym_differential.json` — 15,853 cases, breadth

Generated here, because three further ways to get the tokeniser wrong are not reachable from 27
cases at all:

| Substitution | Differential cases it breaks | Golden cases it breaks |
|---|---|---|
| script table scanned first-match-wins instead of later-entry-wins | 835 | 3 (since v2) |
| `/\s/u` instead of Python's whitespace set | 699 | **0** |
| `\p{L}` instead of the frozen Unicode 13.0 alpha table | 189 | 2 (since v3) |

Each number was measured by making the substitution and re-running, not predicted. **Keep this
column when you change either fixture.** A zero in it is the only thing that distinguishes a case
that documents a divergence from a case that detects one, and it is what found the decorative D6
case in fixture v2. The whitespace substitution is still reachable from this corpus alone.

The precedence one is the least obvious: the canonical Python builds a codepoint→script dict by
iterating its range list, so where two scripts overlap the later entry wins. Exactly one block
overlaps — U+FB00–FB17, Hebrew presentation forms shadowed by the Armenian ligatures — with the
consequence that the Latin ligature `ﬁ` scores as ARMENIAN. The golden fixture reached none of this
before v2; the 835 failures here are what prompted its five D5 cases.

## Regenerating

Both come from the same source. **Change the canonical Python first, regenerate, then re-port** —
editing one side alone is how the four implementations diverged.

    # golden: ask the indexing repo (it needs torch + hf/model.safetensors for the int8 vectors)
    # differential: extract the vendored tokeniser block, which needs neither
    python3 - <<'EOF'
    src = open('<indexing>/hf/inference.py').read()
    a = src.index('# --- BEGIN CANONICAL TOKENISER ---')
    b = src.index('# --- END CANONICAL TOKENISER ---')
    open('canonical_tokeniser.py', 'w').write(src[a:b])
    EOF

then tokenise each case with `canonical_tokeniser.tokenise(text, lang, char_to_id, lang_to_id,
script_to_id)` against the vocab files in `static/webpack/symphonym/`, writing
`{t, l, ids, s, g}` per case. The generator used pools covering every script in the table plus
digits, punctuation, all of Python's whitespace, U+FB00–FB17, fullwidth forms, half-width katakana
and SMP CJK extensions, seed `20260905`. Keep the coverage: confirm any regenerated corpus still
fails all three substitutions above.

## Deployment constraints

These are not notes. Each one, violated, silently changes what every browser query means, with
nothing in the running system to report it.

### `any-ascii` (npm) and `anyascii` (PyPI) must be bumped in lockstep

Both are **0.3.3**, and their transliteration tables were verified byte-identical across 94,624
codepoints spanning every romanised range. That guarantee evaporates the moment either side moves
alone: whg3 would romanise CJK/Kana differently from the code that wrote the index, which is
divergence D1 all over again. Treat a bump on either side as a change requiring the other, plus a
re-run of the 94,624-codepoint comparison. `npm audit fix`, Dependabot and a routine
`pip install -U` are all capable of breaking this without anyone deciding to.

### The Unicode pin follows the GATEWAY, not the index writer

`whg/webpack/js/symphonym-unicode.js` freezes `str.isalpha()` and `str.isspace()` to
**unicodedata 13.0.0**. Three interpreters are in play and two of them disagree — measured, not
assumed:

| | Python | unicodedata |
|---|---|---|
| golden fixture's generator | 3.10.12 | 13.0.0 |
| **CRC gateway (pitt)** | **3.9.25** | **13.0.0** |
| index writer (CRC conda `whg`) | 3.11.13 | 14.0.0 |

13.0.0 is correct for whg3: it is what the gateway receiving `query_vector` runs, and the fixtures
agree with it. It is pinned rather than delegated to `\p{L}` and `/\s/u` because those track the
*browser's* ICU and would drift on a browser update with nothing to catch it.

Note the gateway is not aligned with the corpus it queries: 515 codepoints are alphabetic in 14.0.0
and not in 13.0.0 (Unicode 14 additions — Cypro-Minoan, Tangsa, Vithkuqi, Latin Ext-G, Arabic
Extended-B, Toto, Ethiopic Ext-B, Old Uyghur), none of which occur in 5,307 sampled prod toponyms.
The whitespace sets are identical across all three. That gap belongs to the indexing repo and
closing it implies a re-embed; it is recorded here so nobody re-derives it.

### `static/webpack/symphonym/symphonym.onnx` has no recorded provenance

An 8.36 MB dynamically-quantised int8 asset, self-hosted, with no note anywhere of which commit
exported it or from which weights. It is the file that decides what every browser query means, and
neither this repo nor the indexing repo knows its origin. Until that is established, it cannot be
rebuilt if lost and cannot be shown to correspond to the model the index was embedded with — the
fixture bound (cos ≥ 0.995) is the only evidence that it does. Do not replace it casually, and
record the provenance if you ever learn it.

## One defect reproduced on purpose

The script table's precedence rule sends the Latin ligature `ﬁ` (U+FB01) to ARMENIAN. Both sides
agree that is wrong. The index was written with it, so here *correct* means *identical*; fixing it
is a Symphonym v8 question with a full re-embed attached. Do not "improve" `scriptOfCodepoint`.
