# LPF issue #52 — state of play and drafted reply (PARKED)

**Status 2026-08-20: nothing posted.** A 👀 reaction was added to
[LinkedPasts/linked-places-format#52](https://github.com/LinkedPasts/linked-places-format/issues/52)
to acknowledge Karl Grossner's comment; the substantive reply below is drafted and awaiting review.
Do not post it without checking with SG.

## Where it stands

WHG filed two proposals on #52: `certainty` on `links[]`, and the identifier-prefix inconsistency.
Karl replied on 2026-08-19: (1) agreed — an obvious omission that breaks nothing; (2) reframed —
the context document defines the schema's own elements and is not an identifier registry; the README
alias table was probably an error (it listed what WHG happened to support); any implementer must
always accept full URIs; an implementer that *emits* CURIEs must map them back to full URIs or ship a
context that resolves them.

That position is accepted. It puts the defect on WHG rather than on the format.

## What was established while checking it

* **There is no JSON Schema in the LPF repository** — README, contexts, samples and TSV templates
  only. `validation/static/lpf_v2.0.jsonld` in this repo is the only LPF schema in existence, which
  is why "the schema and the context disagree": both lists are downstream of us. It already deviates
  deliberately (links accept `certainty`/`when`/`citations`; prefix enum extended with
  `whg|osm|ohm|po`), each deviation carrying a `comment` field in the file.
* **The two published sources disagree on prefix *expansions*, not just membership.**

  | prefix | README alias table | context v1.1 |
  |---|---|---|
  | `gn` | `http://www.geonames.org/` | `http://www.geonames.org/ontology#` |
  | `wd` | `https://www.wikidata.org/wiki/` | `http://www.wikidata.org/entity/` |
  | `tgn` | `http://vocab.getty.edu/page/tgn/` | `http://vocab.getty.edu/tgn/` |
  | `periodo` | — | `http://n2t.net/ark:/99152/#` |

  Expanded with the published context, `gn:2649808` becomes `http://www.geonames.org/ontology#2649808`
  — a term URI where no place lives. So "map CURIEs back to full URIs" is undefined for the prefix in
  heaviest use.
* **WHG is the non-conforming emitter.** `reconciliation.js:2051` declares only the published LP
  context, while `links[].identifier` carries `osm:`/`ohm:`/`po:`/`whg:` CURIEs it cannot resolve.
* **A context alone cannot fix it.** Prefix expansion is string concatenation, and our identifiers
  fuse the type letter (`osm:n28825933` needs `/node/28825933`) or carry a second colon
  (`whg:<dataset>:<record>`, `iv:IV:IV1680-058-42`). Only 6 of 28 registry namespaces have a
  `web_item` URL template.
* `certainty`, `when` and `citations` are **already** defined as terms in the published context, so
  proposal 1 needs no context change — spec text and schema only.

## Agreed direction (not yet acted on)

1. Concede prefix governance as Karl frames it; drop the "registry in the context" option.
2. Extended `@context` on WHG exports now; full URIs later, as identifiers acquire resolvable forms
   (`whg:` depends on place#172). Reshape `osm:`/`ohm:` suffixes to `osm:node/28825933` so they
   expand correctly.
3. Keep hosting the schema for now, as an interim rather than a permanent fork, and stop deviating
   silently.
4. Do not redefine `gn`/`wd`/`tgn` locally, even though the shared definitions are wrong — forking
   the meaning of a shared prefix is worse than the problem.

Possible PRs, whenever wanted: the `links[]` properties; the README paragraph; the `gn`/`periodo`
expansions (changes meaning, not wording — wants more eyes).

---

## Drafted reply (NOT POSTED)

Thank you — and on (2) you're not missing anything. That's the right principle and I'll take it as
the answer: the context document defines the schema's own elements, it isn't an identifier registry,
and any implementer claiming LPF support must always accept full URIs. Withdrawing my "maintain the
registry in the context" option accordingly.

**Which makes WHG the implementer that has to change, not the format.** Our exports declare only the
published context and then emit `osm:`, `ohm:`, `po:` and `whg:` CURIEs it cannot resolve. By the
rule above that is exactly backwards, and it's ours to fix. We'll do it as you describe — ship a
context that resolves what we emit, and move to full URIs as our own identifiers acquire resolvable
forms.

One wrinkle worth recording, since it caught us out: a context alone can't rescue everything, because
prefix expansion is string concatenation. `osm:n28825933` under any base gives
`…/n28825933`, when OpenStreetMap wants `/node/28825933` — the type letter is fused into our
identifier. So part of the fix is on our side of the wire (emit `osm:node/28825933`), not in anyone's
context file. Identifiers with a second colon (`whg:<dataset>:<record>`) need a URI pattern we don't
have yet. I mention it only because "map the aliases back to full URIs" sounds like a lookup table
and turns out not to be one.

**Two things that would make the rule followable**, both small:

The README's list still reads as normative — *"the aliases indicated (short URI prefixes) should be
used in place of the URI base"* — which is how it ended up hardened into a closed enum in a
validator. Replacing that paragraph with the rule you've just stated would close the gap between
what the spec says and what you've said it means.

And the expansions disagree between the two published sources, so "map back to full URIs" is
currently undefined for the prefix in heaviest use:

| prefix | README table | context v1.1 |
|---|---|---|
| `gn` | `http://www.geonames.org/` | `http://www.geonames.org/ontology#` |
| `wd` | `https://www.wikidata.org/wiki/` | `http://www.wikidata.org/entity/` |
| `tgn` | `http://vocab.getty.edu/page/tgn/` | `http://vocab.getty.edu/tgn/` |
| `periodo` | — | `http://n2t.net/ark:/99152/#` |

Expand a document with the context as published and `gn:2649808` becomes
`http://www.geonames.org/ontology#2649808` — a term URI in the ontology namespace, where no place
lives. The `#` on `periodo` looks like the same kind of slip.

**On (1)** — agreed, and it's cheaper than I thought: `certainty`, `when` and `citations` are already
defined as terms in the context, so nothing there needs to change. It's spec text plus a schema.

**Which brings me back to my unanswered question, with an answer of sorts.** There is no JSON Schema
in this repository — README, contexts, samples and TSV templates, but no schema. So the "two
disagreeing lists" I reported is substantially our own doing: the only schema in existence is the one
WHG hosts and validates against, and it inherited a list from the spec that, as you say, shouldn't
have been there.

We'll keep maintaining it and keep it aligned rather than quietly diverging — but a format whose only
machine-readable definition lives with one implementer is the condition that produced this issue, and
it will produce the next one. I'd rather it became a shared artifact with a version, a changelog and
a permanent URL. Happy to contribute ours as a starting point if that's useful, or to keep it where
it is and reference it from here — your call as to which is more helpful.

Three small things I could open PRs for whenever you'd like them, separately or together: the
`links[]` properties; the README paragraph; and the `gn`/`periodo` expansions (that one changes
meaning rather than wording, so it may want more eyes than mine).

Last, two housekeeping questions. Is `linkedplaces-context-v1.1.jsonld` still current, or does
`lpo_context_5aug.jsonld` supersede it? And documents in the wild — ours included — cite the context
as `raw.githubusercontent.com/LinkedPasts/linked-places-format/**master**/…`, which now resolves only
by redirect since the default branch is `main`. A permanent URL in front of it would make the context
reference survive any future move; I'd be glad to help set one up.
