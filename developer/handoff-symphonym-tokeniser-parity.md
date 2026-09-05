# Handoff — the browser's Symphonym tokeniser now agrees with the gateway

**Date:** 2026-09-05 · **State:** in the working tree, **not committed and not deployed** ·
**Origin:** raised by the indexing repo after its own fix (`WorldHistoricalGazetteer/indexing`
commit `97a8b31`)

## What was wrong

whg3 computes its own Symphonym `query_vector` in the browser and posts it to `/api/reconcile`
(`whg/webpack/js/recon-symphonym*.js` → `api/crc_client.py:333`), where the gateway compares it
against 72.7M vectors in the `toponyms` index. Until 5 September 2026 **four** implementations of
the tokeniser those vectors were written by existed and disagreed. On 46,483,973 documents (63.9%
of the index) a query vector did not match that document's own stored vector; for CJK, Kana and
Hangul the two were *anti*-correlated, leaving 3.9M documents effectively unreachable at 0.3%
rank-1 self-retrieval.

The indexing repo has fixed the gateway and now measures 100% rank-1 in every stratum. **whg3 was
the fourth implementation**, and it had none of the four corrections. Once the fixed gateway
deploys, a whg3 vector built the old way would reintroduce the identical bug on the client side,
where it would read as a server regression.

Measured against the golden fixture (v1, 27 cases — the pre-fix code was never run against v2),
the pre-fix whg3 code got `char_ids` wrong in 13, `script_id` in 7, `lang_id` in 2. Cosine against the reference vectors: **−0.307 for 東京**,
0.023 for 서울, 0.951 for "Bury St Edmunds". (The −0.307 sits on top of the −0.3036 the indexing
repo measured independently for the same name.)

## What changed

| File | |
|---|---|
| `whg/webpack/js/recon-symphonym-preprocess.js` | rewritten as a port of the canonical Python |
| `whg/webpack/js/symphonym-unicode.js` | **new** — `str.isalpha()`/`str.isspace()` frozen to the gateway's Unicode version |
| `whg/webpack/js/recon-symphonym-quantise.js` | **new** — int8 quantiser, extracted so tests exercise the shipped function |
| `whg/webpack/js/recon-symphonym.worker.js` | uses both; its ad-hoc empty guard moved into the tokeniser |
| `scripts/check-symphonym-tokeniser.mjs` + `scripts/fixtures/` | **new** — the checks, two fixtures, and `README-symphonym.md` |
| `main/tests.py` | `SymphonymTokeniserTests` — runs the above under `python manage.py test main` |
| `package.json` | `any-ascii` dependency; `npm run test:symphonym` |

Five corrections, not four: preprocess-by-script (romanise CJK/Kana, decompose Hangul to Jamo, NFC
everything else); `U+0020` → `<SPACE>` id 2 with all other whitespace dropped; language tag
lowercased and stripped; script detection counting **only alphabetic** characters; and — found here,
not supplied — the script range table itself, which whg3 had in a different and shorter form and
scanned first-match-wins where the canonical Python is later-entry-wins. Two smaller ones: the
quantiser clipped to `[-128,127]` where the index's writer does not clip, and used `Math.round`
(half-up) where `np.round` is half-to-even.

## Verification

- **35/35** golden cases exact on `char_ids`, `script_id`, `lang_id`, `length` (fixture v3: v1's 27,
  plus five D5 precedence cases, plus three interpreter-dependent D6 cases).
- **15,853/15,853** differential cases exact, generated here from the canonical Python because the
  27 could not reach the precedence rule, the whitespace set, or the Unicode-version question.
- The int8 vector is checked against a **bound** (cos ≥ 0.995, |Δ| ≤ 4), not equality — see below.
  Observed: worst cosine 0.99763, worst |Δ| 2.
- Each of the six divergences was **mutated back in and the check confirmed to fail** (4,215 / 902 /
  4,781 / 2,895 / 112 / 835 differential failures respectively). Since fixture v2 the precedence one
  is caught by the golden cases too (3 of the 5 D5 ones), and since v3 so is the frozen-Unicode one
  (2 of the 3 D6 ones). The whitespace substitution is still reachable only from the differential
  corpus, which is why that corpus stays.

## Three things for the record

### 1. The ONNX asset's provenance is unknown

`static/webpack/symphonym/symphonym.onnx` is 8.36 MB and is a **dynamically quantised int8** export
(not the 33.2 MB fp32 `model.safetensors`). **No note anywhere records which commit exported it or
from which weights** — not in this repo, and the indexing repo does not know either. It is the file
that decides what every browser query means. It cannot currently be rebuilt if lost, nor shown to
correspond to the weights the index was embedded with; the fixture bound is the only evidence that
it does. Worth establishing, and worth writing down when it is.

This is also why the vector check is a bound: a quantised int8 graph cannot reproduce fp32 torch
bitwise. The indexing repo agrees that buying bitwise equality with a 4× asset-size fp32 export
would be a bad trade, since a real tokeniser regression shows up two orders of magnitude away
(0.95 → −0.31), not as rounding.

### 2. `any-ascii` (npm) and `anyascii` (PyPI) are a lockstep deployment constraint

Both are 0.3.3, and their tables were verified byte-identical across **94,624 codepoints** spanning
every romanised range. **That guarantee ends the moment either side is bumped alone** — whg3 would
then romanise CJK/Kana differently from the code that wrote the index, which is the original bug
again. `npm audit fix`, Dependabot and a routine `pip install -U` can all break this without
anyone deciding to. Treat a bump on either side as requiring the other plus a re-run of the
comparison.

### 3. Three interpreters are involved and two of them disagree

Script detection counts `str.isalpha()`, which is a property of whichever Python runs it:

| | Python | unicodedata |
|---|---|---|
| the fixture's generator | 3.10.12 | 13.0.0 |
| **CRC gateway (pitt)** | **3.9.25** | **13.0.0** |
| index writer (CRC conda `whg`) | 3.11.13 | 14.0.0 |

whg3 is pinned to **13.0.0**, which is correct — it is the gateway whg3 talks to. But note the
gateway is not itself aligned with the corpus it queries: 515 codepoints are alphabetic in 14.0.0
and not in 13.0.0 (Unicode 14 additions — Cypro-Minoan, Tangsa, Vithkuqi, Latin Ext-G, Arabic
Extended-B, Toto, Ethiopic Ext-B, Old Uyghur). None occur in 5,307 sampled prod toponyms, so
practical exposure looks nil, but 72.7M documents is a lot of chances. The whitespace sets are
identical across all three. Closing that gap is the indexing repo's, and implies a re-embed; it is
raised there as a follow-up.

## Decision needed: the interim

**Recommendation: have the gateway ignore a client-supplied `query_vector` entirely — every script
— until whg3 ships.** On prod this costs nothing:

- Phonetic matching is **opt-in and defaults to off** (`phoneticEnabled()`,
  `reconciliation.js:4057`), so a vector is only sent when a user ticks the box in Map your Data.
- Prod's `reconciliation.log` records full request payloads. Across its retained window
  (2026-09-03 → 2026-09-05, **2,069** reconcile queries) **zero** carried an `embedding`. The
  absence is meaningful rather than a logging gap: `limit` appears 1,701 times and `mode` 24 times
  in the same lines, so an `embedding` key would have been recorded had one been sent.
- Dev is the only place it is exercised: 50 such requests.
- **Caveat:** the log retains roughly two days. This says nothing about a beta tester who used the
  feature last month.

A blanket ignore is cheaper than a per-script one, removes the whole class of client-side drift
while whg3 waits, and takes nothing away from prod that is currently in use.

## Also open

- The `ﬁ` → ARMENIAN behaviour is a defect both sides agree on, reproduced here deliberately because
  the index was written with it. Fixing it is a Symphonym v8 question with a re-embed attached.
- **Closed.** Fixture v2's only D6 case (U+0870) documented the interpreter-Unicode disagreement but
  could not fail — that codepoint sits outside every named script range, so counting it or not
  leaves the answer OTHER either way. Found by substituting `\p{L}` here and watching all 33 cases
  pass; reported with six verified replacements. Fixture v3 adds U+9FFD (moves `char_ids` as well as
  `script_id`) and U+08B5 (moves `script_id` alone), and keeps U+0870 relabelled so nobody re-adds
  it believing it tests something.
- `whg/webpack/js/clustering-embed.js` also calls `embedNames` but is dead code (see the whg3
  loose-ends note); it inherits the fix regardless.
