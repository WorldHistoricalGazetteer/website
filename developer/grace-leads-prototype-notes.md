# The `leads` prototype — what GRACE should take from it

**Read before building GRACE's intake and Sources tables.** The review (§11) and
the handoff both say the `leads` app is worth reading; this note is that reading,
so the next session does not have to go spelunking in `git log --all`.

## Where it is

The app was removed from the tree in `ef5e2c9ee` ("chore(leads): remove retired
DatasetLead app"). It is **not** on the tip of `atlas` — only in history. The last
commit that still has it is **`d4f6173ac`**:

```bash
git show d4f6173ac:leads/models.py
git ls-tree -r --name-only d4f6173ac -- leads/
```

Seventeen files: `models.py`, `admin.py`, `forms.py`, `views.py`, `urls.py`,
two templates, `management/commands/import_leads_xlsx.py`, and two service stubs
(`gapvalue.py`, `zotero.py`).

Design notes for it survive on `atlas` at `developer/dataset-leads-overview.md`
and `developer/dataset-leads-plan.md`.

## ⚠️ Do not run the recorded teardown

`developer/baserow-workflow-tool.md` (now bannered as superseded) records this:

```sql
DROP TABLE IF EXISTS leads_datasetlead CASCADE;
DELETE FROM django_migrations WHERE app = 'leads';
```

**That SQL must not be run — and, checked on 27 August 2026, it is moot: the
table is already gone.** `leads_datasetlead` exists on neither database:

```
postgres_dev-whgazetteer-org_staging  whgv3beta:  leads tables = 0
postgres_whgazetteer-org_main         whgv3beta:  leads tables = 0
```

`django_migrations` has no `leads` rows either, so the teardown (or `migrate
leads zero`) was run at some point after the June note. The **~72 seeded rows are
therefore not recoverable from Postgres.**

That matters more than it looks. Those rows were imported from
`WHG_gazetteer_bibliography.xlsx`, and that bibliography is the review's "obvious
first real content for GRACE". Two copies are believed to survive — the
spreadsheet itself, and Baserow's *Gazetteer Bibliography* table — so:

> **Export the Baserow bibliography before anyone shuts that workspace down.**
> Revoking the API token does not delete data, but abandoning a free-tier
> workspace eventually might, and Postgres no longer holds a fallback.

The SQL still should not be run, because re-running it would now hit a GRACE
table if the app label were ever reused. Treat the whole "Teardown" section as
dead text.

## What GRACE should reuse

### 1. The public form is close to what decision 5 needs

`leads/forms.py::PublicLeadForm` is a `ModelForm` exposing only safe
bibliographic fields — title, author/compiler, publication years, region covered,
source URL, notes, recommended-by — with `status` and `provenance` set
server-side in the view. That is precisely the "suggest-a-source, with a visible
untriaged intake state" shape decision 5 settled on.

Its anti-spam is worth lifting wholesale, because it is already tuned:

- A **honeypot** field (`website`), visually hidden and off the tab order, whose
  presence is gated on an explicit `show_honeypot` flag — `d4f6173ac` and
  `9c5938604` are both bugfixes to that gating, so the version in `d4f6173ac` is
  the debugged one.
- A **per-IP rate limit** (5/hour) via the cache, X-Forwarded-For aware.
- **Both bypassed for authenticated users** (`trusted=True` pops the honeypot
  field entirely so `clean_website()` never runs). Logged-in submitters get no
  friction, and their `recommender_user` FK is set instead of free text.

Note the pairing of `recommended_by` (free text: who suggested it) with
`recommender_user` (FK, set when logged in). That is the same
optional-link-to-`User` pattern the review's decision 2 prescribes for `Contact`,
arrived at independently — which is mild evidence it is the right shape.

### 2. The triage UI was just Django admin, and that was enough

`leads/admin.py` gets a working triage board out of `list_editable`
(status, priority, assignee edited inline from the list), `list_filter`,
`autocomplete_fields`, and two bulk actions. No custom views. Given decision 3
(vocabularies as editable tables, admin-managed), GRACE's Pipeline register can
start the same way and only grow a bespoke UI if the admin genuinely fails.

### 3. The xlsx importer is a working spec for the bibliography migration

`import_leads_xlsx.py` already carries the header map from
`WHG_gazetteer_bibliography.xlsx` — the spreadsheet that later went into Baserow
and now needs to come back out into GRACE's **Sources**. It is idempotent
(matches on `title` + `volume_example`), maps free-text scan status onto an enum,
and splits `"a; b; c"` tags into a list. Adapt it rather than starting over; the
column-name normalisation alone will save an afternoon.

## What GRACE should *not* carry over

- **`status`/`provenance`/`scan_status` as `TextChoices`.** Decision 3 says these
  become lookup tables. `LeadStatus` in particular (suggested → triage →
  approved → in_progress → ingested → rejected → parked) mixes editorial
  judgement with machine facts in the same way the review's §2 objects to:
  *ingested* is derivable from the Register link, not something to type.
- **`region_covered` / `current_area` / `ccodes` as three overlapping fields.**
  Review §6 replaces all of it with an M2M to `regions.Region`.
- **`publication_years` as free text** (e.g. `"1877–1896"`). Same objection as
  the tracker's Time period field: keep the prose, add numeric start/end.
- **`rubric` as a `JSONField` blob.** The comment says it was a blob "to avoid 9
  columns", and the keys are listed in a comment — meaning they cannot be
  filtered, sorted or reported on, and nothing validates them. Ruth's selection
  rubric is a real controlled vocabulary; under decision 3 it should be a table.
- **`gap_value_for()`** — a stub that raises `NotImplementedError`. The idea
  (score how thinly a lead's region is covered in the live `places` index) is
  good and now much easier, since the Gazetteer Register carries `h3_coverage`.
  But it is out of scope until the schema lands.
- **The Zotero layer.** `services/zotero.py` is a stub, and the two-layer
  Zotero-plus-triage design in `dataset-leads-overview.md` was a response to not
  having a bibliography table of our own. GRACE has **Sources**, so the argument
  for an external bibliographic source of truth largely evaporates. Worth a
  conscious decision rather than silent inheritance.

## One-line summary

Take the form, the anti-spam, the admin pattern and the xlsx importer; leave the
vocabularies, the region fields and the JSON rubric behind.
