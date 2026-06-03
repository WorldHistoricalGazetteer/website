# Dataset / Print-Gazetteer Leads Tracker — Implementation Plan

> Branch: `feature/dataset-leads`
> Drafted: 2026-06-03 (handed over from the `indexing` repo session)
> Status: **prototype spec — not yet built**

## Why this exists

Ruth, Palak, and Ali are deciding how to track *leads* on candidate datasets and
print gazetteers — a successor to the old Trello board. The thread floated
Airtable, OJS, and Zotero. The agreed direction:

**Two layers, not one tool:**

1. **Bibliography layer → Zotero** (a shared *group library*). These leads are
   literally bibliographic records (author, year, repository, access URL), and
   Palak already uses Zotero; its web connector grabs Internet Archive /
   HathiTrust catalogue records in one click.
2. **Triage / workflow layer → this Django app** (status, assignee, priority,
   provenance of the lead, public suggestion intake) — lives on the WHG site,
   stays at Pitt, no new subscription, and can do two things no SaaS tool can:
   - **Pull from the Zotero group library** so the bibliography and the tracker
     stay in sync (Django *queries* Zotero — the part Stephen wants).
   - **Auto-compute "WHG gap value"** by querying the live `places` index via
     `settings.CRC_GATEWAY_URL`, so priority isn't all eyeballed.

The source-of-truth spreadsheet (74 candidate rows + a selection rubric) lives at
`indexing` dev's machine:
`~/.config/JetBrains/PyCharm2026.1/scratches/WHG_gazetteer_bibliography.xlsx`.
Its columns and rubric are the field model below.

Two new fields Ruth explicitly asked for, folded in:
- **lead provenance** — own research vs community recommendation, and *who*.
- **priority score** — gap-filling value × difficulty (cleaning, rights, etc.).

## Scope of this prototype

MVP (this branch):
- New `leads` Django app: `DatasetLead` model + admin triage UI.
- Public suggestion form (unauthenticated) on the WHG site.
- Zotero group-library read sync (pull → upsert leads).
- Gap-value helper that queries the CRC ES gateway.
- Seed command to import the 74 existing spreadsheet rows.

Deferred (note in PR, don't build yet):
- Writing *back* to Zotero from Django.
- A richer board/kanban UI beyond Django admin (admin filters are enough to start).
- Public-facing browse of accepted leads.

## App layout (mirror existing apps, e.g. `datasets/`, `placetypes/`)

```
leads/
  __init__.py
  apps.py                 # LeadsConfig
  models.py               # DatasetLead (+ TextChoices enums)
  admin.py                # DatasetLeadAdmin: list_display/filter/search/actions/fieldsets
  forms.py                # PublicLeadForm (ModelForm, honeypot)
  views.py                # suggest_lead view (GET form / POST create)
  urls.py                 # /leads/suggest/
  services/
    zotero.py             # group-library client + upsert_from_zotero()
    gapvalue.py           # query places index → coverage/gap estimate
  management/commands/
    import_leads_xlsx.py  # seed from the bibliography spreadsheet (one-off)
    sync_zotero_leads.py  # pull group library → leads (cron/Celery later)
  templates/leads/
    suggest.html          # extends main/templates/main/base_webpack.html
    suggest_thanks.html
  migrations/
```

Wire-up:
- `whg/settings.py` INSTALLED_APPS → add `'leads.apps.LeadsConfig',`.
- `whg/urls.py` → `path('leads/', include('leads.urls')),`.
- `whg/local_settings.py` (gitignored, same place as `CRC_GATEWAY_URL`) →
  `ZOTERO_GROUP_ID = '...'` and `ZOTERO_API_KEY = '...'` (read-only key).
  Read them in `settings.py` with the same `os.environ.get(...) or globals().get(...)` pattern used for `CRC_GATEWAY_URL`.

## Data model (`leads/models.py`)

Map the spreadsheet columns + rubric + the two new fields. Use
`AUTH_USER_MODEL = 'users.User'` for assignee/recommender FKs.

```python
class LeadStatus(models.TextChoices):
    SUGGESTED  = 'suggested',  'Suggested'      # public/unreviewed default
    TRIAGE     = 'triage',     'In triage'
    APPROVED   = 'approved',   'Approved'
    IN_PROGRESS= 'in_progress','In progress'
    INGESTED   = 'ingested',   'Ingested'
    REJECTED   = 'rejected',   'Rejected'
    PARKED     = 'parked',     'Parked'

class LeadProvenance(models.TextChoices):
    OWN        = 'own',        'Own research'
    COMMUNITY  = 'community',  'Community recommendation'
    PUBLIC_FORM= 'public',     'Public suggestion form'

class ScanStatus(models.TextChoices):
    DOWNLOADABLE = 'downloadable', 'PDF/text downloadable'
    FULLVIEW     = 'fullview',     'Catalogue / full view only'
    NONE         = 'none',         'Not digitised'
    UNKNOWN      = 'unknown',      'Unknown'

class DatasetLead(models.Model):
    # --- bibliographic (spreadsheet columns) ---
    title             = models.CharField(max_length=500)
    volume_example    = models.CharField(max_length=500, blank=True)   # "Volume / district example"
    author_compiler   = models.CharField(max_length=500, blank=True)
    publication_years = models.CharField(max_length=100, blank=True)   # free text e.g. "1877–1896"
    region_covered    = models.TextField(blank=True)
    current_area      = models.CharField(max_length=255, blank=True)   # "Current country/area"
    ccodes            = models.JSONField(default=list, blank=True)     # derived ISO codes for gap-value
    repository        = models.CharField(max_length=255, blank=True)   # "Repository checked"
    source_url        = models.URLField(max_length=1000, blank=True)
    scan_status       = models.CharField(max_length=20, choices=ScanStatus.choices, default=ScanStatus.UNKNOWN)
    tags              = models.JSONField(default=list, blank=True)

    # --- workflow / triage ---
    status            = models.CharField(max_length=20, choices=LeadStatus.choices, default=LeadStatus.SUGGESTED)
    assignee          = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                          on_delete=models.SET_NULL, related_name='assigned_leads')
    next_action       = models.TextField(blank=True)                  # "Suggested next action"
    notes             = models.TextField(blank=True)

    # --- Ruth's two new fields ---
    provenance        = models.CharField(max_length=20, choices=LeadProvenance.choices, default=LeadProvenance.OWN)
    recommended_by    = models.CharField(max_length=255, blank=True)  # free text: who recommended it (public submitter or community member)
    recommender_user  = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                          on_delete=models.SET_NULL, related_name='recommended_leads')
    priority_score    = models.PositiveSmallIntegerField(null=True, blank=True)  # 0-100 composite
    gap_value         = models.PositiveSmallIntegerField(null=True, blank=True)  # auto-computed component
    difficulty        = models.PositiveSmallIntegerField(null=True, blank=True)  # manual: cleaning/rights effort

    # --- selection rubric (sheet 2) — keep as a JSON blob to avoid 9 columns ---
    rubric            = models.JSONField(default=dict, blank=True)
    # keys: regionally_focused, authoritative, out_of_copyright, scans_available,
    #       data_richness, gap_value, workflow_fit, ethics_risk  (each "high"/"med"/"low"/note)

    # --- Zotero link (bibliography layer) ---
    zotero_key        = models.CharField(max_length=32, blank=True, db_index=True)  # item key in the group library
    zotero_version    = models.PositiveIntegerField(null=True, blank=True)          # for incremental sync

    created_at        = models.DateTimeField(auto_now_add=True)
    updated_at        = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-priority_score', '-created_at']

    def __str__(self):
        return self.title
```

Note on rubric: store as JSON now; if the team wants per-criterion filtering in
admin later, promote the hot ones to real columns. The 8 rubric criteria come
from sheet 2 of the spreadsheet ("Selection Rubric").

## Admin = the triage UI (`leads/admin.py`)

This is what replaces Trello for the team — no custom front-end needed for MVP.

```python
@admin.register(DatasetLead)
class DatasetLeadAdmin(admin.ModelAdmin):
    list_display  = ('title', 'status', 'provenance', 'priority_score',
                     'current_area', 'scan_status', 'assignee', 'updated_at')
    list_filter   = ('status', 'provenance', 'scan_status', 'assignee')
    search_fields = ('title', 'author_compiler', 'region_covered', 'current_area', 'recommended_by')
    list_editable = ('status', 'priority_score', 'assignee')   # quick triage from the list
    actions       = ['mark_approved', 'mark_rejected', 'recompute_gap_value', 'sync_from_zotero']
    fieldsets     = (...)  # group: Bibliographic / Workflow / Priority & rubric / Zotero
```

Custom admin actions:
- `recompute_gap_value` → calls `services.gapvalue.gap_value_for(lead)` and saves.
- `sync_from_zotero` → pull/refresh the linked Zotero item.

## Public suggestion form (`leads/forms.py`, `views.py`, `urls.py`)

- `PublicLeadForm(ModelForm)` exposing only safe fields: `title`,
  `author_compiler`, `publication_years`, `region_covered`, `source_url`,
  `notes`, plus a "your name / how you found this" → maps to `recommended_by`.
- Hidden **honeypot** field + simple rate limit; no login required.
- On POST: create with `status=SUGGESTED`, `provenance=PUBLIC_FORM`; render
  `suggest_thanks.html`.
- Mount at `/leads/suggest/`. Template extends
  `main/templates/main/base_webpack.html` (per CLAUDE.md, all pages do).
- Optional: ping Zulip on new public submission (repo already has Sentry→Zulip
  wiring; reuse the same webhook approach) so triage isn't pull-only.

## Zotero sync (`leads/services/zotero.py`)

Goal: Django reads the shared **group library** and upserts leads — the
bibliography layer feeds the tracker.

- Add `pyzotero` to `requirements.txt` (or hit `https://api.zotero.org` with the
  already-present `requests`; `pyzotero` is simpler and handles paging/versioning).
- Client: `Zotero(ZOTERO_GROUP_ID, 'group', ZOTERO_API_KEY)`.
- `upsert_from_zotero(since_version=None)`:
  - Use Zotero's `?since=<version>` incremental API; store the library version
    so syncs are cheap.
  - For each item, match on `zotero_key`; create or update the bibliographic
    fields **without clobbering** workflow fields (status/assignee/priority).
    Same "augment, don't overwrite" discipline as the indexing pipeline.
  - Map Zotero fields → model: `title`, `creators`→`author_compiler`,
    `date`→`publication_years`, `url`→`source_url`, `libraryCatalog`/`archive`
    →`repository`, `tags`→`tags`.
- Expose as: management command `sync_zotero_leads` (manual now), and wrap in a
  Celery task later (`django_celery_beat` is already installed) for periodic sync.

Decision to confirm with the team: **direction of truth.** MVP = Zotero is the
bibliographic source, Django owns workflow. Write-back to Zotero is deferred.

## Gap-value automation (`leads/services/gapvalue.py`)

The rubric's "WHG gap value" criterion is partly computable — this is the
WHG-native advantage.

- Input: a lead's `ccodes` (or `current_area` geocoded to ISO codes).
- Query the `places` index via `settings.CRC_GATEWAY_URL` (the same gateway the
  `api` app already talks to — reuse its client/auth; see `api/` for the
  existing ES connection helper rather than re-rolling one).
- Estimate coverage: e.g. doc counts per `ccodes` / per source namespace, or
  thinness of a region/period. Return a 0–100 `gap_value` (higher = thinner =
  more valuable).
- Keep it advisory: it fills `gap_value`; a human still sets final
  `priority_score` (gap_value vs `difficulty`).

Confirm the exact metric with Ruth — "fills gaps" can mean region, period, or
language thinness. Start with per-country place-density; iterate.

## Seed import (`leads/management/commands/import_leads_xlsx.py`)

One-off to load the 74 existing rows so the team starts with real data, not an
empty board.

- Input: a CSV/XLSX export of the "Candidate Bibliography" sheet (ask Stephen for
  the file or a CSV dump — it's a PyCharm scratch on his machine, not in either repo).
- Column → field map matches the model above. Set `provenance=OWN` (these are
  the team's own research), `status=TRIAGE`. Parse the rubric sheet into `rubric`
  if joinable by title; otherwise leave blank.
- Idempotent: match on `title` (+`volume_example`) to allow re-runs.

## Build order (suggested for the next session)

1. Scaffold `leads/` app, model, `makemigrations`/`migrate`, register in
   INSTALLED_APPS + urls. Get the admin showing an empty list.
2. Seed command + import the 74 rows → team has a working triage board in admin.
3. Public suggestion form + thanks page at `/leads/suggest/`.
4. Zotero read sync (`pyzotero`, management command).
5. Gap-value helper + admin action.
6. Optional Zulip notification on public submission.
7. Wire a nav link (where datasets/collections live) and write a short
   `developer/dataset-leads.md` usage note.

## Open questions for the team (surface in PR description)

- Zotero **group library** id + a read-only API key (Palak likely owns/creates this).
- Final definition of **gap value** (region / period / language thinness?).
- Should accepted leads be **publicly browsable**, or admin-only for now?
- Spam protection appetite for the public form (honeypot only vs captcha).

## References

- Email thread: Mostern / Vashist / Straub, "tracking system for leads on
  datasets and print gazetteers?", 1–2 Jun 2026.
- Spreadsheet schema: `WHG_gazetteer_bibliography.xlsx` (Candidate Bibliography +
  Selection Rubric sheets).
- Sibling repo `indexing` (`WorldHistoricalGazetteer/indexing`) consumes accepted
  leads downstream via the ingestion pipeline — keep `source_url` / `ccodes`
  machine-readable so an approved lead can flow there.
