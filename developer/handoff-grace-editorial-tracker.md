# Handoff — GRACE, the editorial tracker

**Audience:** Claude Code in the `whg3` repo (desktop / PyCharm).
**Branch:** `claude/palak-system-review-gwkmrr` — pushed, nothing uncommitted.
**Status:** design review complete; **no code written, and none should be until
decisions 1–4 below are settled.** This handoff exists so the next session starts
with the findings rather than re-deriving them.

```bash
git fetch origin claude/palak-system-review-gwkmrr
git checkout claude/palak-system-review-gwkmrr
```

---

## 1. What GRACE is

**GRACE** — *Gazetteer Register And Contact Engagement* — is a planned Django app
inside WHG (app label `grace/`, alongside `datasets/`, `collection/`). It tracks the
research world around the platform: the gazetteers we want, the people and projects
behind them, the printed sources that document them, our correspondence, and where
each gazetteer sits on its way in.

It replaces an earlier attempt that ran on Baserow, an external no-code database.
That route is abandoned; §6 lists what it left behind.

Origin: Palak Vashist's design document *"Tracking people, projects, and datasets —
a working design for our editorial workflow"* (August 2026), plus its ER diagram.
The review of that design is the deliverable on this branch:

| Artefact | Where |
|----------|-------|
| Review (print) | `developer/whg-tracker-review.html` + `.pdf` |
| Review (web) | https://claude.ai/code/artifact/5df8c99b-62d7-4d18-b002-f90c38ee0d14 |
| Prior briefing | `developer/whg-workflow-briefing.html` (19 June 2026) |
| Superseded Baserow note | `developer/baserow-workflow-tool.md` |

**Read `developer/whg-tracker-review.html` before doing anything.** This file is a
navigation aid; that file carries the reasoning.

---

## 2. The three registers

Palak's structure, which the review endorses. Names below are the review's proposal
(decision 7) — hers used "Dashboard" for the third, which collides with WHG's
contributor dashboard.

| Register | Holds |
|----------|-------|
| **Catalogue** | Gazetteers (held and prospective), People, Projects, Sources (the bibliography, including printed gazetteers) |
| **Engagement** | Engagements with people, their action items, and a dated interaction log. Plus Content (blog posts etc.) — which is really a fourth register; see the review's §6 nits |
| **Pipeline** | Editorial state of a gazetteer on its way into WHG, and its reviews |

---

## 3. The load-bearing decision: point at the Gazetteer Register

The tracked-gazetteer record carries a **nullable FK to `GazetteerRegistryEntry`**,
not to `datasets.Dataset`:

```python
class TrackedGazetteer(models.Model):
    title    = models.CharField(...)          # what we called it when we first heard of it
    registry = models.ForeignKey('api.GazetteerRegistryEntry',
                                 null=True, blank=True, ...)
    stage    = models.ForeignKey('Stage', ...)      # editorial values ONLY
    owner    = models.ForeignKey(settings.AUTH_USER_MODEL, ...)

    @property
    def is_prospect(self):
        return self.registry_id is None
```

Consequences, all covered in the review's §2:

- **Every machine fact is read through the link**, never stored: published/indexed
  state, licence (already a resolved FK to `licensing.License`), rights holder,
  CRediT contributors, citation text, source URL, record count, temporal extent,
  spatial coverage, reconciliation.
- **A row with no Register link *is* a prospect.** No vocabulary expresses this, and
  nothing is reclassified by hand when a prospect lands.
- **The editorial fields must NOT be added to `GazetteerRegistryEntry`** — three
  reasons in the review, the decisive one being that its PK is namespace-derived
  (`gn`, `whg:1234`), which only exists post-ingest, so prospects can have no row.
- The Register spans authorities *and* WHG datasets, so outreach about TGN or
  Pleiades is expressible. A `Dataset` FK could not do that.

---

## 4. Code you will need — verified line references

Do not re-derive these; they were checked against this branch.

| What | Where | Why it matters |
|------|-------|----------------|
| The Gazetteer Register model | `api/models.py:186` | FK target. Note `entry_class` (authority/dataset), `gazetteer_type` (standard/itinerary/network), `status`, `license` FK, `rights_holder`, `contributors_csl`, `citation_text` |
| Visibility gate | `api/models.py:157` (`…QuerySet.visible_to`) | Embargo / BETA gating on registry reads |
| The ingest push upsert | `api/views_indexing.py:103` | **Read the comment block.** Documents which fields are push-managed and which are protect-by-omission. `status` is push-managed |
| Registry admin | `api/admin.py:20` | Existing curatorial-field pattern to follow |
| User model | `users/models.py:114` | `name`, `given_name`, `surname`, `affiliation`, `web_page`, `orcid`, `email` (**EncryptedTextField** + `email_hash` HMAC), `email_confirmed`, `role`, `is_active`, `news_permitted` |
| Regions (UN M49) | `regions/models.py:7` | Complete hierarchy w/ parents, members, ISO codes, Wikidata, geometry; `RegionLabel` for multilingual names |
| Licence vocabulary | `licensing/models.py:4` | Single source of truth; the Register already resolves SPDX → this |
| Dataset + its status | `datasets/models.py:70`, `main/choices.py:193` | `ds_status`: seed → format_ok → uploaded → reconciling → wd-complete → accessioning → indexed |
| Dataset collaborators | `datasets/models.py:595` | Existing people↔gazetteer relation |
| Comment model | `main/models.py:126` | Candidate for reuse if review comments are built |
| Record suggestions | `workbench/models.py:185` | Existing pending/approved/rejected review vocabulary to align with |
| Public suggestion form | `main/views.py:485`, `whg/urls.py:70`, `main/templates/main/base_webpack.html:323` | Currently 302s to Baserow — see §6 |

---

## 5. Decisions — status as of 27 August 2026

**Decisions 1–5 and 7 are SETTLED** (Stephen Gadd, 27 Aug 2026: the review's
recommendations accepted as written). **Schema work is unblocked.**

**Decision 6 remains open and is a hard gate on *populating*, not on modelling.**
Build the models; do not load real contact data until Ruth has ruled.

| # | Decision | Outcome |
|---|----------|---------|
| 1 | Does GRACE point at `GazetteerRegistryEntry` and read through it? | ✅ **Yes** — nullable FK, local title retained |
| 2 | One `Contact` model with optional `User` link, or a separate People table? | ✅ **One**, optional one-to-one, **no duplicated fields** |
| 3 | Vocabularies as editable lookup tables, or frozen `choices=`? | ✅ **Tables by default** — preserves Palak's self-service |
| 4 | Add an Organisations entity? | ✅ **Yes** — permission to publish is granted by institutions |
| 5 | Is `/contribute/` a suggest-a-source tool or a general front door? | ✅ **Suggest-a-source**, with a visible untriaged intake state |
| 6 | Personal data: lawful basis, consent, erasure, encryption | ⏳ **OPEN — with Ruth.** Settle before populating; match `users.User` |
| 7 | Terminology and register names | ✅ Datasets → Gazetteers; Catalogue · Engagement · Pipeline |

**Decision 3 deserves emphasis when you do build.** Model controlled vocabularies as
their own tables, not `choices=` tuples — a `choices` list needs a developer and a
migration to change, a lookup table is editable in the admin. Reserve `choices` for
lists that code actually branches on, and say so in the model docstring so nobody
later "tidies" them.

**Decision 6 is a hard gate, not advice.** `User.email` is encrypted at rest with an
indexed HMAC for lookups. A `Contact` table holding plain-text addresses for people
who never signed up would be a regression against the platform's own standard, in the
same database.

---

## 6. Loose ends left by the Baserow retreat

Safe to act on independently of the decisions above, except where noted.

| Loose end | Action |
|-----------|--------|
| `/contribute/` → external Baserow form (`main/views.py:485`) | **The live one.** The site's public suggestion door leads out of WHG. Rebuild as a Django form writing to the intake model — but the shape depends on decision 5 |
| `sync_licences_to_baserow` | ✅ **DONE** — deleted (commit `1393f6d42`) |
| `BASEROW_*` settings | ✅ **DONE** — the four bot/sync settings removed from `whg/settings.py` and from `local_settings.py`. `BASEROW_SUBMIT_FORM_URL` deliberately **kept**: it is still the only public suggestion door until the Django intake form replaces `/contribute/`. Credentials also still present in the deployed `.env` on prod and dev — strip after revocation |
| The bibliography | ⚠️ **Now the only copy.** `leads_datasetlead` has already been dropped from both databases, so the ~72 seeded rows are gone from Postgres. Surviving copies: `WHG_gazetteer_bibliography.xlsx` and Baserow's *Gazetteer Bibliography* table. **Export before the workspace is shut down** |
| The `leads` prototype | ✅ **READ** — written up in `developer/grace-leads-prototype-notes.md`. Take the public form, its honeypot + rate-limit, the admin triage pattern and the xlsx importer; leave the `TextChoices` vocabularies, the three overlapping region fields and the JSON rubric |
| `developer/baserow-workflow-tool.md` | ✅ **DONE** — bannered as superseded, not deleted |

---

## 7. Suggested first moves

1. Read `developer/whg-tracker-review.html`, then `api/views_indexing.py:103` and
   `api/models.py:186`. Those two files carry most of the constraints.
2. Then `developer/grace-leads-prototype-notes.md` — the `leads` prototype read
   for you, with what to lift and what to leave.
3. Build order: vocabulary tables → `Contact` + `Organisation` →
   `TrackedGazetteer` + `Stage` → Engagement/ActionItem/Interaction → admin →
   the `/contribute/` rebuild.
4. Do not load real contact data until decision 6 returns.

---

## Notes

- The review is written for the team, not for a developer — it argues rather than
  specifies. Where it and this file disagree on detail, the review is authoritative
  on *what* and this file on *where*.
- The terminology decision is human-facing only: the `Dataset` model name stays.
  "Gazetteer" replaces "dataset"/"collection" in language, not in code.
- The name GRACE is settled. The three register names are decision 7.
