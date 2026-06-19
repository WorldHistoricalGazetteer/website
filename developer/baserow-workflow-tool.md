# Dataset / Gazetteer Workflow Tool — Baserow (external)

> Branch: `feature/baserow-submit-link` (off `main`)
> Supersedes the in-codebase Django `leads` app (see "Teardown" below).

## Decision

Lead/dataset/submission tracking lives in **Baserow** — an open-source,
free-tier Airtable alternative — built and owned by Palak **outside this
codebase**. We do **not** model any of this in Django. The website's only job is
to **link out** to Baserow's public submission form.

This replaces the earlier Django `leads` app prototype (DatasetLead model, admin
triage UI, public form, Zotero/gap-value services), which was built on `atlas`
and is now being retired.

## Baserow workspace (Palak)

Tables: **Datasets** (intake → publication status), **Projects** (linked to
Datasets), **Contacts/Submitters**, plus the **Gazetteer Bibliography**.
A public form, *"Submit Your Dataset to WHG,"* writes straight into the Datasets
table, and an automation emails the team on each new submission.

Database IDs / token were shared by Palak by email — **not stored in this repo.**

## What this branch adds (the only Django footprint)

A stable internal URL that redirects to the external form:

| Piece | Location |
|-------|----------|
| Config | `whg/settings.py` → `BASEROW_SUBMIT_FORM_URL` (env / `local_settings.py`) |
| View | `main/views.py` → `submit_dataset` (302 to the form; graceful fallback if unset) |
| URL | `whg/urls.py` → `path('contribute/', …, name='submit-dataset')` |
| Nav | `base_webpack.html` → "Submit a Dataset" in the **Data** dropdown |

Why a redirect view rather than a raw external link in the nav: the public form
a stable `/contribute/` lets the external target change without touching
templates, and if the URL is ever unset the view redirects home with an info
message instead of erroring.

### Live URL

The form is **live** — `https://baserow.io/form/VhZUnWUX7JXiP9BYX2VbBFc7tLCdZGWYKtSuoN7RxVU`
("Submit Your Dataset to WHG") — and is baked in as the default for
`BASEROW_SUBMIT_FORM_URL` in `whg/settings.py`. It's a public URL, not a secret,
so the `/contribute/` link works on every deployment as soon as this merges — no
per-server config. To override (e.g. if the form is rebuilt), set
`BASEROW_SUBMIT_FORM_URL` via env var or `local_settings.py`; precedence is
env > local_settings > the baked default.

### Optional later: embedding / API sync

Not built here (we chose "link out only"). If wanted later:
- **Embed** — swap the redirect for a page with an `<iframe>` of the form.
- **API sync** — Palak's token (`Database Token`, full read/write) could let
  Django read submissions or update status flags. If so, it goes in
  `local_settings.py` / secrets **only** — never committed. The token has been
  shared by email in the clear, so ask Palak to **rotate** it before any
  programmatic use.

## Teardown of the old Django `leads` app

The `leads` app exists **only on `atlas`** (commits `c7558b0…` ff). `main` never
had it, so this branch adds the Baserow link **without removing any code** — the
two are independent.

Because new work now goes **straight to `main`, bypassing `atlas`**, the leads
app gets dropped simply by `main` not containing it. Two cleanup items remain:

1. **Deployed dev database.** The dev server (running `atlas`) has a
   `leads_datasetlead` table with ~72 seeded rows. Once the deployment tracks
   `main`, the app code is gone, so you can't `migrate leads zero`. Drop the
   orphaned table directly:
   ```sql
   DROP TABLE IF EXISTS leads_datasetlead CASCADE;
   DELETE FROM django_migrations WHERE app = 'leads';
   ```
   (The data is superseded by Baserow / the bibliography spreadsheet.)
   Alternatively, while still on `atlas`, run `python manage.py migrate leads zero`
   first — the clean path — then abandon the branch.

2. **Obsolete branches.** `feature/dataset-leads` (off `main`, plan docs only,
   local, never pushed) is superseded — safe to delete:
   `git branch -D feature/dataset-leads`. The `leads` commits on `atlas` should
   not be merged forward to `main`.

## References

- Palak's update email + API-token email (Baserow workspace).
- Source bibliography: `WHG_gazetteer_bibliography.xlsx` (now lives in Baserow's
  Gazetteer Bibliography table).
