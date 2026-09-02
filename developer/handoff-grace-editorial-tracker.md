# Handoff — GRACE, the editorial tracker

**Audience:** Claude Code in the `whg3` repo (desktop / PyCharm).
**Branch:** `staging`.
**Status:** built, deployed to dev, and **reviewed by the team** (2 Sep 2026).
Palak's seven review points are addressed; §8 records them. Decision 7 was
**overturned** by that review — the pipeline record is a **Dataset**, not a
gazetteer, and Contacts are **People**. This handoff is the map; the reasoning
lives in the review and in the module docstrings.

```bash
git checkout staging      # the work is on staging; the review branch is deleted
```

---

## 1. What GRACE is

**GRACE** — *Gazetteer Register And Contact Engagement* — is a planned Django app
inside WHG (app label `grace/`, alongside `datasets/`, `collection/`). It tracks the
research world around the platform: the datasets we want, the people and projects
behind them, the printed sources that document them, our correspondence, and where
each dataset sits on its way in.

**Terminology (revised 2 Sep 2026).** A contributor brings a *dataset*; once
reconciled and published it is a *gazetteer*. GRACE's pipeline record exists almost
entirely during the first stage, so it is `TrackedDataset`. "Gazetteer" is left to
mean the two things it means everywhere else at WHG: a **printed** gazetteer (a
`Source`) and a **published WHG** gazetteer (a Register entry). See review §8.

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

Palak's structure, which the review endorses. Hers used "Dashboard" for the third,
which collides with WHG's contributor dashboard. Content is a fourth register in the
build — it is *output*, not engagement.

| Register | Holds |
|----------|-------|
| **Catalogue** | People, Organisations, Projects, Sources (the bibliography, including printed gazetteers) |
| **Engagement** | Engagements with people, their action items, and a dated interaction log. Plus Content (blog posts etc.) — which is really a fourth register; see the review's §6 nits |
| **Pipeline** | Datasets on their way into WHG (held and prospective), their reviews, and the public suggestion queue |

---

## 3. The load-bearing decision: point at the Gazetteer Register

The tracked-dataset record carries a **nullable FK to `GazetteerRegistryEntry`**,
not to `datasets.Dataset`:

```python
class TrackedDataset(models.Model):
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

All seven were settled on 27 Aug 2026. **Decision 7 was then overturned** by the
team's review of the working build on 2 Sep — see §8 and review §8.

| # | Decision | Outcome |
|---|----------|---------|
| 1 | Does GRACE point at `GazetteerRegistryEntry` and read through it? | ✅ **Yes** — nullable FK, local title retained |
| 2 | One `Person` model with optional `User` link, or a separate People table? | ✅ **One**, optional one-to-one, **no duplicated fields** |
| 3 | Vocabularies as editable lookup tables, or frozen `choices=`? | ✅ **Tables by default** — preserves Palak's self-service |
| 4 | Add an Organisations entity? | ✅ **Yes** — permission to publish is granted by institutions |
| 5 | Is `/contribute/` a suggest-a-source tool or a general front door? | ✅ **Suggest-a-source**, with a visible untriaged intake state |
| 6 | Personal data: lawful basis, consent, erasure, encryption | ✅ **Legitimate interests**, with the four obligations of review §10 built |
| 7 | Terminology and register names | ⚠️ **REVISED 2 Sep** — pipeline record is a **Dataset**, Contacts are **People**; Catalogue · Engagement · Pipeline unchanged |

**Decision 3 deserves emphasis when you do build.** Model controlled vocabularies as
their own tables, not `choices=` tuples — a `choices` list needs a developer and a
migration to change, a lookup table is editable in the admin. Reserve `choices` for
lists that code actually branches on, and say so in the model docstring so nobody
later "tidies" them.

**Decision 6 is a hard gate, not advice.** `User.email` is encrypted at rest with an
indexed HMAC for lookups. A `Person` table holding plain-text addresses for people
who never signed up would be a regression against the platform's own standard, in the
same database.

---

## 6. Loose ends left by the Baserow retreat

Safe to act on independently of the decisions above, except where noted.

| Loose end | Action |
|-----------|--------|
| `/contribute/` → external Baserow form (`main/views.py:485`) | **The live one.** The site's public suggestion door leads out of WHG. Rebuild as a Django form writing to the intake model — but the shape depends on decision 5 |
| `sync_licences_to_baserow` | ✅ **DONE** — deleted (commit `1393f6d42`) |
| `BASEROW_*` settings | ✅ **DONE** — the four bot/sync settings removed from `whg/settings.py`, from `local_settings.py`, and from the deployed `.env` on **both** prod and dev (backups shredded; verified no `BASEROW_BOT` remains). `BASEROW_SUBMIT_FORM_URL` deliberately **kept** — still the only public suggestion door until the Django intake form replaces `/contribute/`; it was never in `.env`, so it falls back to the settings.py default and `/contribute/` is unaffected |
| The bot account | ✅ **DONE** — `whg-claude@docuracy.co.uk` deletion scheduled (Baserow applies a 30-day grace). ⚠️ **Logging into it would cancel the deletion**, and Chrome has the credentials saved — remove that entry from the password manager |
| The leaked API token | ⏳ **PALAK ONLY.** The bot account held *no* database tokens; the full read/write token circulated by email is under **Palak's personal account**, and Baserow scopes tokens per user. Ask her to delete it: My settings → Database tokens |
| The bibliography | ✅ **EXPORTED** — the whole WHG workspace (498 rows, 4 databases, 14 tables) is in `developer/baserow-export/`, **gitignored** because this repo is public and the export holds names/emails/phones. Print Gazetteer Bibliography = the 72 rows. Baserow left **intact** — Palak's call what happens to it |
| The `leads` prototype | ✅ **READ** — written up in `developer/grace-leads-prototype-notes.md`. Take the public form, its honeypot + rate-limit, the admin triage pattern and the xlsx importer; leave the `TextChoices` vocabularies, the three overlapping region fields and the JSON rubric |
| `developer/baserow-workflow-tool.md` | ✅ **DONE** — bannered as superseded, not deleted |

---

## 7. Build status

**All seven steps are built and deployed to dev.** The app is `grace/`, on the
`staging` branch, live at <https://dev.whgazetteer.org/>.

| Step | State |
|------|-------|
| 1. Vocabulary tables | ✅ `grace/vocabularies.py` — 18 tables, 109 seeded terms |
| 2. Person + Organisation | ✅ optional `User` one-to-one; local copies cleared on save |
| 3. TrackedDataset + Stage | ✅ nullable FK to `api.GazetteerRegistryEntry` |
| 4. Engagement / ActionItem / Interaction | ✅ one-owner rule + staleness alarm enforced |
| 5. Admin | ✅ **`/grace/admin/`** — GRACE's own AdminSite (`grace/admin_site.py`), linked from `/dashboard_admin/` → Tools. Shows only GRACE, leads with what needs attention, calls out the editable vocabularies. Django's `/admin/` keeps them too |
| 6. `/contribute/` rebuild | ✅ redirects to `grace:suggest`; **all `BASEROW_*` settings gone** |
| 7. Import | ✅ **72 sources + 42 datasets (41 prospects) + 209 people + 7 organisations** on dev |
| 8. Post-review work | ✅ Connections panels, the board, Reviews, the new fields — see §8 |

Verified on dev: **99/99 tests pass** (`manage.py test grace users`; run it twice
consecutively, which is the check that matters — see below), migrations apply
clean from zero, the import is idempotent (a second run creates 0),
`/contribute/` 302s to `/grace/suggest/`, the form renders for anonymous
visitors, `grace_retention_review` runs, and `/grace/admin/` renders the board
and the registers with live counts.

### Traps this build already fell into

Recorded because each cost real time and none is obvious:

- **`USE_TZ` is unset in `whg/settings.py`, so it is `False`.** `timezone.now()`
  is naive and **`timezone.localdate()` raises `ValueError`**. Use
  `datetime.date.today()`, as the rest of the codebase does.
- **The test suite shares Redis.** The suggest view's per-IP rate limit lives in
  the cache; every test-client request comes from 127.0.0.1, so the counter
  accumulated across tests *and across runs* — after five anonymous POSTs the
  form silently throttled and tests that passed on a cold cache failed on a warm
  one. The form test classes are pinned to a locmem cache. **Run the suite twice
  and compare** before believing it.
- **`autocomplete_fields` resolves against the same admin site**, so a second
  AdminSite needs its lookup targets registered too or every referencing form
  raises `admin.E039`. See `SUPPORT_MODELS`.
- **Never let an email address reach `Person.name`.** It is plaintext and
  indexed; `Person.email` is encrypted precisely so addresses are not in the
  clear. The importer's `_display_name()` uses the local part as a handle.
- **The admin mirror runs during autodiscovery.** `register_grace_models()` is
  called from the bottom of `grace/admin.py`, which Django imports while walking
  `INSTALLED_APPS` — so whether `licensing.admin` had been imported yet was down
  to app order, and when it had not, every licence autocomplete raised
  `admin.E039`. It now imports the support apps' admin modules itself.
- **`User.__str__` is the username**, which on this project is ORCID-derived.
  Nothing user-facing should render it. `grace/admin_links.py` has `user_label`
  and `registry_label`, and both need fixing in *three* places to take effect:
  the form field, the select2 AJAX endpoint, and the mirrored admin serving it.

### Where the reasoning lives

Read the code, not a spec: the design decisions are argued in the module
docstrings, next to what they constrain. In particular
`grace/vocabularies.py` (why these are tables, not `choices=`),
`grace/privacy.py` (the whole of decision 6), and `TrackedDataset` /
`Person` in `grace/models.py`.

### Two things left for a human

1. **The Article 14 backlog is now real on dev.** 209 people are loaded, none
   has been told we hold their details, and they become overdue a month after
   import. `./manage.py grace_retention_review` lists them; the admin has a bulk
   action to record the notice once sent. Prod has nothing loaded yet.
2. **Palak should walk the vocabularies** in the admin and change whatever is
   wrong. The 109 seeded terms are a starting point, not a claim — that is the
   whole point of decision 3, and the seeder never overwrites an edited label.

### Not built, deliberately

External peer review (review §7, Q5 — sketch and defer). The
`ReviewRecommendation` vocabulary exists so it is ready, and the platform's
existing pending/approved/rejected language is what it should align with.

## 8. The team's review — 2 September 2026

Palak reviewed the build on dev and raised seven points. All are addressed; the
reasoning is in review §13, and only the parts that constrain future work are
repeated here.

| # | Point | What was built |
|---|-------|----------------|
| 1 | Records should show their connections | `grace/admin_links.py` — a read-only **Connections** panel on every change form, both directions, plus reverse counts on the lists. `Source.people` was added: there had been *no* person↔source relation at all |
| 2 | People should be everyone, us included | Six internal roles seeded; `PersonRole.is_internal` exempts them from the Article 14 queue. **Mailing-list membership deliberately not built** — see below |
| 3 | A board anyone can scan | On the `/grace/admin/` landing page, ordered furthest-along first. `reconciliation_status` reads through to `datasets.Dataset`; nothing is copied |
| 4 | Missing dataset fields | `data_format`, `geometry_status`, and the `expected_*` trio — which are **not** copies of Register fields, see below |
| 5 | A way to record reviews | `grace.models.Review`, inline on the dataset. External peer review needs a vocabulary term, not a migration |
| 6 | Reconciled-against and regions vocabularies | `reconciled_against` is an M2M to the Register limited to authorities. Regions were always modelled — `regions.Region` simply had **zero rows** |
| 7 | Datasets ≠ gazetteers; Contacts → People | The rename. See review §8 |

**Three rules this review established, which later work must not undo:**

1. **GRACE is not the mailing list.** The sending platform (Ali's, likely
   Mailchimp) owns subscription state — that is where the unsubscribe link points
   and where bounces are recorded. GRACE holds `Person.email_status` only:
   whether the address still works. Do not add list membership, campaigns or a
   sync. If a sync is ever wanted it is *one-way in*: import an export, match on
   `email_hash`, set `email_status`. And do not pour the mailing list into the
   People register — everyone in it is owed an Article 14 notice.
2. **`expected_*` fields are a different fact, not a second copy.** Review §2
   forbids storing Register facts in GRACE, and a test asserts it. The
   `expected_record_count` / `expected_licence` / `expected_rights_holder` trio
   records what someone *told us* during a negotiation, when there was no
   Register row to read. The Register wins the moment there is one. Do not
   "simplify" them into plain fields.
3. **Reconciliation and publication status are read through, never stored.**
   `TrackedDataset.reconciliation_status` resolves `whg:NNNN` to a
   `datasets.Dataset` and returns its `ds_status`. Authorities have none — we
   ingested them, we did not reconcile them.

### Open, for humans

- **Nothing links a prospect to its Register entry automatically.** 41 of 42 rows
  are prospects; `China Historical GIS (CHGIS)` was linked by hand because it is
  an exact match to registry `chgis`. A fuzzy scan also matched *GeoNames
  Historical Layer* to `gn`, which is almost certainly wrong — which is why
  matching must stay a human decision. A suggest-and-confirm action is not built.
- **No first-indexed date exists to read.** The Register has `updated_at` and
  `reingest_finished_at` only. Raise it against the indexing repo (as a `place`
  issue) rather than adding a field here.
- **The imported data has no engagements**, so every Connections panel on dev is
  empty. That is the data, not the feature.

---

## Notes

- The review is written for the team, not for a developer — it argues rather than
  specifies. Where it and this file disagree on detail, the review is authoritative
  on *what* and this file on *where*.
- The terminology decision is **not** human-facing only, contrary to what this file
  said before 2 Sep: the Python classes were renamed too, because a label that
  disagrees with the code underneath is a trap. `TrackedGazetteer → TrackedDataset`,
  `Contact → Person`, `ContactRole → PersonRole`, `ContactStatus → PersonStatus`.
- The name GRACE is settled, and keeps its expansion — *Gazetteer Register And
  Contact Engagement* — even though the register of people is now People. The
  acronym is the product's name; contact engagement is still what the Engagement
  register records.
