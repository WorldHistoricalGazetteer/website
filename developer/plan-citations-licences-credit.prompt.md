# Sitewide Citations, Licences & Credit — Unified Design

**Status:** Reviewed — decisions locked 2026-06-06; ready to break into implementation plans.
**Scope:** new `licensing` app; `datasets`, `collection`, `persons`, `api` (registry + attestations + serializers), `utils` (DOI/CSL), submission/edit forms, templates, public API responses, plus the out-of-tree indexing repo (`processing/settings.py` `AUTHORITIES`, Batch 11 push) and a handoff doc for it (§10).
**Drivers:**
- Inconsistent, partly-incorrect licence handling sitewide.
- Issue [place#99](https://github.com/WorldHistoricalGazetteer/place/issues/99) — adopt CRediT contributor roles.
- Ingestion rebuild in progress → opportunity to capture richer per-source citation metadata at ingest.

---

## 1. Current state (as built)

### 1.1 Three disconnected attribution systems

| System | Where | Citation | Licence | Attribution |
|--------|-------|----------|---------|-------------|
| **Contributed datasets** | `datasets/models.py:113-122` | free-text `citation` + cached `citation_csl` | hard-coded CC-BY-NC-4.0 | free-text `creator`/`contributors`/`source` strings; **unused** `creators_csl`/`contributors_csl` M2M → `persons.Person` |
| **Collections** | `collection/models.py:102-103,122` | `citation_csl` | hard-coded | `creator`, `contact` strings only (no contributors M2M) |
| **Authority gazetteers** | indexing `processing/settings.py` `AUTHORITIES[].citation` | one free-text blob (citation + licence + URL) → `GazetteerRegistryEntry.description` | **none structured** | none structured |
| **Contributor attestations** | `api/models.py:60-148` | n/a | n/a | records asserting `user` + `justification`, but no credit surfaced anywhere |

### 1.2 The CSL pipeline (datasets/collections)

- `utils/csl_citation_formatter.py::csl_citation()` builds CSL-JSON, cached as `citation_csl` (`datasets/models.py:188`, `collection/models.py:153`).
- Rendered client-side by `whg/webpack/js/citationFormatter.js` (APA/Chicago/Harvard/MLA/Turabian/Vancouver), embedded in download envelopes (`api/download_file.py`), surfaced in `download_modal.html`, `ds_metadata.html`, `place_collection_browse.html`.
- DOIs: `utils/doi.py` → DataCite.

### 1.3 Confirmed defects (not just gaps)

1. **Licence is asserted wrongly.** WHG hard-codes **CC-BY-NC-4.0** as the rights statement in DOI metadata (`utils/doi.py:177-186`), download envelopes (`api/download_file.py`), and export task text (`utils/tasks.py`). The actual upstream licences differ and are often **-NC-incompatible**:
   - Pleiades → CC-BY 3.0 (`settings.py:264`)
   - TGN → ODC-By 1.0 (`:309`)
   - OSM / OHM → ODbL (share-alike!) (`:345`, `:363`)
   - Natural Earth → public domain (`:482`); PeriodO → public domain (`:513`)
   Asserting `-NC` over ODbL or PD data is legally wrong and (for ODbL) a share-alike risk.
2. **Label/href mismatch.** `datasets/templates/datasets/ds_metadata.html:281` labels the link "CC BY-NC 4.0" but `href` points at `/licenses/by/4.0/` (CC-BY 4.0).
3. **Dead structured-author path.** `creators_csl`/`contributors_csl` M2M exist but `csl_citation()` ignores them — it re-parses the free-text strings and even references `person.first/.middle/.last`, fields absent from `persons.Person` (which has `family`/`given`). Structured authorship never reaches a citation.
4. **No `contributors` in DataCite.** `get_doi_metadata()` emits only `creators` — there is no slot for CRediT-typed contributors today.

---

## 2. Design principles

1. **One licence vocabulary, one renderer.** A single `License` representation reused by Dataset, Collection, GazetteerRegistryEntry, and the under-used `Link.license`.
2. **Truthful, per-source rights.** Rights statements in DOIs/downloads/UI come from the object's actual licence, never a hard-coded constant. WHG's own *aggregation/curation* layer can still carry its platform licence separately from the *source* licence.
3. **Structured authorship is the source of truth; strings become a fallback/import path.** CRediT contributions live in a through-model; free-text stays only as a legacy importer.
4. **Capture at ingest.** The indexing `AUTHORITIES` config is the canonical metadata for authorities; Batch 11 pushes structured fields into the registry (per the chosen SoT). New ingestion captures licence + citation + roles up front.
5. **Standards-aligned mappings.** SPDX for licence ids; CRediT (NISO) for roles; DataCite `contributorType` + CSL for output, using the JATS4R CRediT↔DataCite mapping.

---

## 3. Proposed data model

### 3.1 `License` (new — shared lookup)

A small curated table (seeded via migration, editable in admin). Reused everywhere a licence is named.

```python
# new app `licensing` (or fold into `main`)
class License(models.Model):
    spdx_id     = models.CharField(max_length=64, unique=True)   # "CC-BY-4.0", "ODbL-1.0", "ODC-By-1.0", "CC0-1.0"
    label       = models.CharField(max_length=128)               # "Creative Commons Attribution 4.0 International"
    url         = models.URLField()                              # canonical licence deed
    spdx_uri    = models.URLField(default="https://spdx.org/licenses/")
    permits_commercial = models.BooleanField(default=True)       # false for -NC
    share_alike        = models.BooleanField(default=False)      # true for ODbL, -SA
    attribution_required = models.BooleanField(default=True)
    custom      = models.BooleanField(default=False)             # true for non-SPDX/bespoke terms
    notes       = models.TextField(blank=True)
```

Seed set covers the licences already in `AUTHORITIES` + the CC family + CC0/PD. A `custom=True` row plus a free-text `rights_statement` on the owner handles bespoke terms (e.g. Ottoman NFS, Index Villaris).

**Attachment options considered:**
- (a) FK `license = models.ForeignKey(License, null=True, on_delete=PROTECT)` on each owner — chosen. Clean joins, admin dropdown, one SoT.
- (b) Denormalised SPDX string per object — rejected; reproduces today's drift.

Add to: `Dataset`, `Collection`, `GazetteerRegistryEntry`, and adopt the existing `Link.license` CharField → migrate to FK (or keep CharField but validate against SPDX set).

### 3.2 `GazetteerRegistryEntry` — new citation/licence/rights fields

Per the agreed SoT (**indexing `AUTHORITIES` → registry via Batch 11**), add fields that the push populates. These are *push-managed* (not in the admin-protected curatorial set):

```python
citation_text  = models.TextField(null=True, blank=True)   # human citation (was crammed into `description`)
license        = models.ForeignKey('licensing.License', null=True, blank=True, on_delete=models.SET_NULL)
license_url    = models.URLField(null=True, blank=True)     # override when source deviates from the canonical deed
rights_holder  = models.CharField(max_length=255, null=True, blank=True)  # "J. Paul Getty Trust", "ISAW"
source_url     = models.URLField(null=True, blank=True)     # homepage / landing page
# CRediT-style contributions for authorities, if/when sources provide them:
contributors_csl = models.JSONField(default=list, blank=True)  # [{name, role, orcid}] — see §3.3 output shape
```

Migration moves the existing `description`-blob → `citation_text` for current rows where appropriate (keep `description` for genuine prose description).

### 3.3 CRediT contributions (the issue #99 core)

Replace the free-text creator/contributor split with a **through-model** carrying a controlled role. A **generic relation** (`ContentType` + `object_id`) lets one model carry credit for **`Dataset`, `Collection`, *and* `GazetteerRegistryEntry`** (authorities + WHG-dataset registry rows) — confirmed required (resolved decision 5). Two-parallel-FKs was rejected precisely because it can't reach the registry; the generic FK keeps a single credit table and a single renderer across all three targets.

```python
# CRediT taxonomy (NISO) — 14 roles + a WHG-local "Rights holder" if useful
class CreditRole(models.TextChoices):
    CONCEPTUALIZATION = "conceptualization", "Conceptualization"
    DATA_CURATION     = "data-curation",     "Data curation"
    FORMAL_ANALYSIS   = "formal-analysis",   "Formal analysis"
    FUNDING           = "funding-acquisition","Funding acquisition"
    INVESTIGATION     = "investigation",     "Investigation"
    METHODOLOGY       = "methodology",       "Methodology"
    PROJECT_ADMIN     = "project-administration", "Project administration"
    RESOURCES         = "resources",         "Resources"
    SOFTWARE          = "software",          "Software"
    SUPERVISION       = "supervision",       "Supervision"
    VALIDATION        = "validation",        "Validation"
    VISUALIZATION     = "visualization",     "Visualization"
    WRITING_ORIGINAL  = "writing-original-draft", "Writing – original draft"
    WRITING_REVIEW    = "writing-review-editing", "Writing – review & editing"

class Contribution(models.Model):
    person       = models.ForeignKey('persons.Person', on_delete=models.PROTECT, related_name='contributions')
    role         = models.CharField(max_length=32, choices=CreditRole.choices)
    degree       = models.CharField(max_length=8, blank=True, choices=[("lead","Lead"),("equal","Equal"),("supporting","Supporting")])
    is_corresponding = models.BooleanField(default=False)
    order        = models.PositiveSmallIntegerField(default=0)
    # target: Dataset | Collection | GazetteerRegistryEntry (generic FK)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id    = models.PositiveIntegerField()
    target       = GenericForeignKey('content_type', 'object_id')

    class Meta:
        unique_together = [("person", "role", "content_type", "object_id")]
        ordering = ["order"]
```

`persons.Person` keeps `family/given/literal/orcid/affiliation` (already CSL-shaped). ORCiD becomes first-class here rather than regex-extracted.

**Why through-model over free-text:** CRediT is inherently *(person × role)*; a person can hold several roles, a role several people, each with a degree of contribution. Free-text cannot express this and is what the JATS4R/DataCite mappings consume.

**CRediT URIs:** each role has a canonical `https://credit.niso.org/contributor-roles/<slug>/` URI — store the slug, derive the URI for export.

### 3.4 Migration of existing free-text

A one-off importer reuses the existing `parse_names()` logic to seed `Person` rows and `Contribution(role=...)`:
- `creator` → role `Conceptualization` (or a generic "author"/creator mapping)
- `contributors` → role `Data curation` (default; editable afterward)
Free-text fields retained read-only as provenance; new edits go through `Contribution`.

---

## 4. Output / integration changes

### 4.1 CSL (`csl_citation_formatter.py`)
- Rewrite to read `Contribution` rows (and authority `contributors_csl`) instead of re-parsing strings; fall back to `parse_names()` only when no structured contributions exist.
- Fix the `person.first/.middle/.last` bug → use `Person.given/family/literal`.
- Emit CSL `author` (creator-equivalent roles) and CSL `contributor` arrays.

### 4.2 DataCite (`utils/doi.py`)
- Add a `contributors` array alongside `creators`, each with `contributorType` (DataCite enum) derived from CRediT via the JATS4R mapping, plus `nameIdentifiers` (ORCID) and an `affiliation` block.
- **Replace the hard-coded single `rightsList`** with: one entry per source `License` (spdx_id, label, url) **plus** a distinct WHG-overlay entry (CC-BY-NC-4.0, from a setting/constant) — decisions 1+2. The overlay entry is always present; source entries are added per constituent. This makes the legal assertion truthful while preserving WHG's aggregation licence.

### 4.3 Downloads / exports
- `api/download_file.py` and `utils/tasks.py`: emit the actual source licence(s). For aggregates spanning multiple sources, emit a **list** of per-source rights (the download modal already enumerates constituent datasets — reuse that).

### 4.4 Templates
- Fix `ds_metadata.html:281` label/href.
- Single licence/credit partial (`includes/_attribution.html`) rendering: citation (CSL), licence badge (from `License`), contributor list grouped by CRediT role. Reuse across dataset, collection, registry/gazetteer panels, and place-detail source lines.

### 4.5 Registry / authorities surfacing
- `search.views.AtlasPageView` already serialises the registry; extend the serialised payload with `license`, `citation_text`, `rights_holder`, and `contributors_csl` so the Gazetteers offcanvas can show proper attribution + CRediT credit per authority.

### 4.6 Contributor submission UI (datasets & collections)
Contributors must be able to **enter CRediT metadata at submission time**, not only post-hoc.
- Extend the dataset upload/validate forms (`datasets/forms.py`) and collection metadata forms with a **repeatable contributor row**: person (name + ORCID + affiliation) × one-or-more CRediT roles × degree (`lead`/`equal`/`supporting`) × corresponding flag. Backed by `Contribution`/`Person`.
- A **licence selector** (dropdown over the seeded `License` set + "custom" → free-text `rights_statement`) replaces the current fixed "Accept CC BY 4.0" checkbox; the WHG-overlay acceptance stays as a separate explicit consent.
- Inline-formset (Django) or a small JS widget writing structured JSON the view deserialises into `Contribution` rows. Free-text `creator`/`contributors` remain as an optional quick-entry path that the importer (§3.4) expands into `Contribution`s.
- Same widget reused on the edit/metadata screens so credit is maintainable after submission.

### 4.7 API surfacing — attribution must follow the data out
Wherever records are pulled from the indices, the response must carry the relevant rights/credit. To avoid bloating per-record payloads, **aggregate by namespace** (resolved direction from review):
- API responses that return place records (reconciliation, search, place detail, downloads) include a top-level `attribution` block keyed by namespace, e.g.
  ```json
  "attribution": {
    "tgn": { "name": "TGN", "citation": "…", "license": {"id":"ODC-By-1.0","url":"…"}, "rights_holder":"J. Paul Getty Trust", "contributors":[…] },
    "wd":  { … }
  }
  ```
  built from the `GazetteerRegistryEntry` rows for the namespaces present in the result set (one lookup per distinct namespace, cached). Per-record payloads keep only their namespaced id; the client/consumer joins to the `attribution` map.
- The gateway/CRC client (`api/`) and DRF serializers (`api/serializers.py`) gain a shared helper `attribution_for(namespaces)` returning that map; reused by search, reconciliation, place-detail, and the download envelope (§4.3) so there is one code path.
- WHG-overlay licence is included once at the envelope top level, distinct from the per-namespace source rights (decision 2).

---

## 5. Ingestion-side work (indexing repo) — capture richer metadata now

Since ingestion is under active development, bake the structured fields into `AUTHORITIES` and the push:

1. **Extend each `AUTHORITIES` entry** in `processing/settings.py` from a single `citation` blob to structured keys:
   ```python
   {
     'dataset_name': 'TGN',
     'namespace': 'tgn',
     'citation_text': 'The Getty Thesaurus of Geographic Names® (TGN) …',
     'license_spdx': 'ODC-By-1.0',
     'license_url': 'https://opendatacommons.org/licenses/by/1-0/',
     'rights_holder': 'J. Paul Getty Trust',
     'source_url': 'https://www.getty.edu/research/tools/vocabularies/tgn/',
     'contributors': [],   # optional CRediT-shaped list where the source documents roles
     ...
   }
   ```
   Keep `citation` as a derived/back-compat alias during transition.
2. **`push_gazetteer_inventory.py::_authority_meta`** maps these into the inventory payload (today it only sends `name`/`description`). Add `citation_text`, `license_spdx`, `license_url`, `rights_holder`, `source_url`, `contributors`.
3. **Registry inventory endpoint** (`api/views_indexing.GazetteerInventoryView._upsert_one`) accepts the new fields, resolving `license_spdx` → `License` FK (create-on-miss against the seeded set, or reject unknown SPDX ids with a warning).
4. **New contributed datasets** flowing through ingestion capture licence + CRediT contributions at submit time rather than free-text.

---

## 6. Rollout (phased, low-risk ordering)

1. **Phase 0 — correctness hotfix (small, shippable now):** fix `ds_metadata.html` label/href; stop asserting a single hard-coded licence where we already know it's wrong (e.g. drive download/DOI rights from a per-object `License` once present). Tracks the legal risk first.
2. **Phase 1 — `License` model + seed + attach FKs** to Dataset/Collection; admin; render licence badge. No CRediT yet. → detailed build plan: **`developer/plan-citations-phase1-licensing.prompt.md`**. (Registry FK held to Phase 4; legacy Link CharFields held to Phase 5.)
3. **Phase 2 — CRediT:** `persons` first-class ORCiD, `Contribution` generic-relation through-model (targets datasets/collections/authorities), `degree`+`is_corresponding`, CSL + DataCite contributor output, free-text importer, **contributor submission UI (§4.6)**.
4. **Phase 3 — API surfacing (§4.7):** namespace-aggregated `attribution` block + WHG-overlay in search/reconciliation/place-detail/download responses via the shared `attribution_for()` helper.
5. **Phase 4 — authorities SoT (atlas-side):** extend `AUTHORITIES`, extend Batch 11 push + inventory endpoint, backfill registry licences/credit; surface in Atlas offcanvas.
6. **Phase 5 — consolidation:** single `_attribution.html` partial; retire dead `creators_csl`/`contributors_csl` M2M (or repoint them); per-source rights lists in aggregate downloads.
7. **Phase 6 — indexing-repo handoff (§10):** once Phases 1–4 are on `main`, write and execute the handoff doc for Claude Code on the `indexing` repo (upgrade pathway + audit/correct every authority's metadata + all-namespace push).

Each phase is independently shippable and reversible. Phases 1–2 are mostly additive migrations (CRediT moved ahead of authorities so it leapfrogs to `main` first — §7); Phase 2 is the largest. Phase 6 is cross-repo and depends on the endpoint changes from Phase 4 being live.

---

## 7. Branch strategy — leapfrogging `atlas` to land on `main` first

**Goal:** ship citation/licence/CRediT to `main` ahead of the in-flight atlas feature work, without dragging atlas's 183-commit lead along.

**This is feasible cleanly, because the foundations already exist on `main`:** `ContributorAttestation`, `GazetteerRegistryEntry`, the `persons` app, and `Dataset.citation`/`creators_csl`/`contributors_csl` are all present on `main` today. The citation work therefore has **no hard dependency on atlas** — build it against `main`, not on top of `atlas`.

**Recommended workflow:**
1. **Cut the feature branch from `main`** (e.g. `citations`), not from `atlas`. Develop and test there.
2. **Prefer a new `licensing` app for new models** (`License`, `Contribution`, `CreditRole`). Its migrations are wholly independent of `api`, so they cannot collide with atlas's `api/0005`/`0006`. This is now a decisive argument for §3.1's "new app" option (open question 4).
3. **Keep `api` changes minimal and dependency-pinned.** The registry licence/credit fields (§3.2) are the only unavoidable `api` migration. `main` is at `api/0004`; atlas added `0005_admin_reingest_controls` + `0006_remove_stale_dplace_seed`. A citation migration cut from `main` would be a *second* `api/0005`. Two siblings off `0004` is legal in Django and resolves with a one-line `makemigrations --merge` when the branches converge — but to avoid even that, **hold the authorities/registry phase (Phase 4) on atlas**, leapfrogging only Phases 0–3 + 5 (which live in the new `licensing` app + `datasets`/`collection`/`persons`/templates/API, with no `api`-model migration).
4. **Verify per-app migration parity before authoring** migrations in `datasets`/`collection`/`persons`: cut new migrations to depend on each app's `main` HEAD, not its atlas HEAD, so the cherry-pick/merge to `main` is conflict-free.
5. **Merge path:** PR `citations` → `main`. Then `atlas` picks up the citation work on its next merge-from-`main` (or rebase). Because the work is additive (new app + additive fields), the atlas-side reconciliation is limited to the `api/0005` migration sibling (introduced when Phase 4 lands) — handled by `--merge`.

**Net:** Phases 0–3, 5 are leapfrog-friendly to `main`. Phase 4 (authorities/registry + the indexing-repo push) rides on atlas, since it needs the new `api`-model fields *and* is coupled to the ongoing ingestion rebuild. Phase 6 (indexing handoff) follows once Phase 4's endpoint is live. Recommended order to `main`: **License (1) → CRediT incl. submission UI (2) → API surfacing (3)**; authorities (4) + handoff (6) proceed on atlas in parallel.

---

## 8. Resolved decisions (2026-06-06 review)

1. **Aggregate licensing → (a) per-source rights list.** Downloads/DOIs/collections that span multiple sources emit a list of per-source rights, never a single collapsed/"most-restrictive" licence. Reuse the download modal's existing constituent-dataset enumeration.
2. **WHG platform licence = separate overlay.** Keep WHG's own curation/aggregation licence (CC-BY-NC-4.0) asserted *alongside* — not instead of — each source's rights. DOI `rightsList` therefore carries the source licence(s) **plus** a clearly-distinguished WHG-overlay entry. The two are modelled separately (a `License` FK for the source + a constant/setting for the WHG overlay) so they never get conflated again.
3. **Metadata flexibility.** Schema must accept *any available* citation/attribution/credit metadata a source provides — optional fields throughout, no required-field gating beyond a licence. This drives the use of JSON `contributors_csl` on the registry and nullable fields on `License`/`Contribution`.
4. **`License` home → new `licensing` app.** Confirmed (also the cleanest for the leapfrog — see §7).
5. **Credit everywhere.** Contributions (incl. attesting users where appropriate) surface as credit on **datasets, collections, *and* authorities/gazetteers** — see §3.3 (Contribution targets all three) and §9 (authority CRediT).
6. **`degree` + `is_corresponding` → included now.**

## 9. CRediT for authority gazetteers

CRediT is not just for contributed datasets — it must accommodate authorities/gazetteers too (decision 5). The design already supports this with **no extra model**: `Contribution`'s generic relation targets `GazetteerRegistryEntry` exactly as it targets `Dataset`/`Collection`.

Two ingest channels feed authority credit, used as available (decision 3 — flexibility):
- **Structured (preferred):** the indexing `AUTHORITIES[].contributors` list (§5) → pushed as `GazetteerRegistryEntry.contributors_csl` JSON → optionally materialised into `Contribution` rows by the inventory endpoint for uniform querying/rendering.
- **Minimal fallback:** where a source documents no roles, store `rights_holder` + a single provider-level credit (e.g. CRediT *Resources* or a WHG-local "Provider" role). Most authorities will sit here initially; the audit in §10 upgrades them where richer metadata exists.

Rendering is identical to datasets/collections via the shared `_attribution.html` partial (§4.4) and the API `attribution` block (§4.7).

---

## 10. Indexing-repo handoff (deliverable — written after Phases 1–4 land)

Once the Django side is implemented and the registry/inventory endpoint accepts the new fields, produce a **handoff document for Claude Code running in the `indexing` repo** (`/home/stephen/PycharmProjects/indexing`). It must cover:

1. **Upgrade the ingestion pathway:**
   - Migrate every `AUTHORITIES` entry in `processing/settings.py` from the single free-text `citation` blob to the structured keys (`citation_text`, `license_spdx`, `license_url`, `rights_holder`, `source_url`, `contributors`) — §5.
   - Update `push_gazetteer_inventory.py::_authority_meta` (and the payload builder) to emit them.
   - Document the new inventory payload contract + the endpoint's `license_spdx`→`License` resolution and unknown-SPDX handling.
2. **Audit + correct every existing authority's source metadata** (this is real research work, not mechanical):
   - For each namespace (pl, gn, tgn, wd, osm, ohm, dplace, gb, iv, nl, po, clio, chgis, dgsd, tm, ottnfs, ukhc, …), verify the **actual** current upstream licence and citation against the source's site/terms, since the existing blobs are stale/imprecise and several are wrong (e.g. the global hard-coded CC-BY-NC-4.0 must be replaced with the true per-source licence: ODbL, ODC-By, CC-BY-3.0, CC0/PD, etc.).
   - Record SPDX id, deed URL, rights holder, canonical citation, and any documented contributor roles (CRediT) per namespace.
   - Flag bespoke/non-SPDX terms for a `custom` `License` row + `rights_statement`.
3. **All-namespace push of corrected metadata:** run `push_gazetteer_inventory.py` across **all** namespaces (not just a changed subset) so every `GazetteerRegistryEntry` is refreshed against the new contract — an explicit full reconciliation pass, with `--dry-run` first and a diff review before the live `POST`.

The handoff doc lives in the `indexing` repo (e.g. `developer/handoff-citation-metadata.md`) and is itself a tracked artifact; this doc (§10) is the spec for it.
