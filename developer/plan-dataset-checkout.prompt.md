# Plan — Whole-dataset ("Gazetteer") wholesale check-out

**Status:** PLAN (2026-07-09) — not started. The largest and highest-blast-radius piece of the
check-out/publish-back model: editing a whole **published, indexed gazetteer** (`datasets.Dataset`)
through the Workbench. Realises plan-collaborativeCollections **§6 / §6.1** at the top of the
granularity spectrum.

**Relates to / builds on (all shipped):** record-level check-out (`developer/plan-record-checkout.prompt.md`
— "Correct this record"), Gazetteer-Group + Place-Collection check-out, the optimistic conflict guard,
and the targeted single-doc re-index (`workbench.publish._reindex_place`). place#111 (umbrella),
plan-collaborativeCollections §6.1.

---

## 0. Framing — a granularity spectrum, not "load everything into the browser"

§6.1 is explicit that a naïve whole-dataset round-trip is **infeasible for large gazetteers** (tens/
hundreds of thousands of records won't fit in IndexedDB; a full re-upload + full re-index to fix a few
fields is wasteful and risky). So "wholesale check-out" must NOT mean "blindly serialise the entire
dataset to the browser". It is the top of a spectrum we're building bottom-up:

| Granularity | Status | Browser holds | Publish-back |
|---|---|---|---|
| **1 record** | ✅ shipped | one place | apply delta + reindex 1 doc |
| **filtered subset / page** | ← the sensible NEXT step | N places (bounded) | delta of changed records + reindex those |
| **whole dataset** | this plan (top end) | the whole dataset *iff it fits* | delta of changed records (default) or full re-accession (explicit) |

**Core principle:** the tool is **capacity-aware and steers by size.** Below a threshold, whole-dataset
check-out materialises the dataset locally; above it, the tool refuses and **routes the user to subset
or record-level editing** (the §6.1 guardrail). The subset path is the real workhorse and should be
built first — whole-dataset is subset-of-everything with a capacity gate.

---

## 1. Key architectural decisions (resolve at implementation — options + recommendation)

### 1a. Which editor? (the big one)
A gazetteer's places are **LPF features** (per place: multiple names, geometries, temporal spans,
types, links), *not* flat spreadsheet rows. Three options:

- **A — Reuse Map your Data (flatten to a table).** Check the dataset out as a table (one row per
  place; columns for primary name / lng / lat / start / end / type / ccodes) into a `reconciliation`
  project. Cheapest (MyD already does local-first tabular editing + publish-to-dataset via
  `/datasets/validate/`). **Lossy**: multi-name / multi-geometry / rich LPF don't survive the flatten,
  so publish-back could *destroy* structure it didn't round-trip. Acceptable only for genuinely
  tabular datasets, and only if publish-back is a **field-level delta** (never a wholesale overwrite).
- **B — New LPF-feature editor.** A feature-list editor that round-trips full LPF. Faithful but a
  large new build.
- **C (recommended) — Subset of record editors + a dataset shell.** Reuse the just-built **record
  editor** as the per-feature editor, over a **checked-out subset/page** of the dataset, wrapped in a
  dataset-level shell (list + filter + "N changed" + publish-all). This composes what's already built,
  round-trips faithfully per record (no flatten loss), and scales by only holding the working page in
  the browser. Whole-dataset = "select all" over this, capacity permitting.

**Recommendation:** build **C** (subset editing over the record editor) first; treat "whole dataset"
as select-all with a capacity gate. Offer **A** later only for explicitly-tabular datasets where a
MyD round-trip is safe.

### 1b. Transfer (streaming, resumable, capacity-aware) — §6.1
- **Check capacity first:** `navigator.storage.estimate()` before check-out; if the dataset's
  serialised size exceeds available quota (or a hard cap), **refuse and steer to subset/record**.
- **Stream out in chunks**, never one giant JSON. Reuse the **LPF/GeoJSON serialisers**
  (`utils/feature_collection.py`, `utils/mapdata.py`) behind a **paged** endpoint
  (`?page=`/`?after_id=`), so the browser pulls features in batches (mirror of the accession ingest,
  which already streams features with `ijson.items(file,'features.item')` in
  `validation/create_dataset.py`).
- **Resumable** both ways (page cursor survives a flaky connection).
- **Backpressure / caps:** explicit size limits with clear messaging ("this gazetteer is too large to
  edit wholesale — edit records or a filtered subset instead"), mirroring MyD's existing caps.

### 1c. Publish-back — DELTA by default, full re-accession as an explicit choice
- **Delta (default):** send only **changed records** (a diff against the checkout baseline). The server
  applies each changed record with the **record-level apply path already built**
  (`publish_place_record`'s field mutation) and **targeted re-index** — reuse `_reindex_place` per
  changed record, or a **bulk `streaming_bulk`** (as `index_to_pub` does) when many changed. NOT a full
  dataset re-accession. This keeps routine bulk corrections cheap and low-risk.
- **Full re-accession (explicit "I restructured everything"):** re-run the **validation/accession
  pipeline** (`validation.views.validate_file` → `validate_feature_batch` → `create_dataset`) on the
  edited LPF, then re-index. Heavy, re-reconciles, high blast radius — behind an explicit confirmation
  + backup, and only for datasets that were fully checked out.

### 1d. Optimistic locking at scale (§6.1 "record granularity")
Per-record content hash (reuse `checkout.record_state_hash`) captured at check-out. On delta
publish-back, guard **each changed record** individually: concurrent edits to *other* records don't
block a correction, but a conflicting edit to the *same* record surfaces a merge/conflict rather than
clobbering. (The dataset-level `base_version` becomes a map of per-record hashes, or a checkout
manifest.)

### 1e. Backup & reversibility (non-negotiable for whole-dataset)
- Snapshot the pre-edit state so publish-back is reversible: datasets already carry revisioned files
  via **`DatasetFile.rev`** — write the checked-out LPF as a new `DatasetFile` revision before applying
  a publish-back, so a bad bulk edit can be rolled back to the prior revision.
- Keep a `ProjectSnapshot` baseline (already built) as the merge ancestor.
- Nothing destructive without a recoverable prior revision.

---

## 2. Flow (recommended path C + delta publish-back)

1. **"Edit in Workbench"** on a published dataset page — gated on **beta + `Dataset.can_edit`**
   (owner/staff; already added). Capacity check → if too large, steer to subset/record.
2. **Check-out** streams the dataset's features (paged LPF) into a dataset working-copy project
   (a new `dataset_edit` doc-type, or a `reconciliation` project for option A), stamped with a
   per-record hash manifest as `base_version`.
3. **Edit** locally + collaboratively (Teams/realtime already built): filter/search the features,
   correct individual records (reuse the record editor), track "N changed".
4. **Publish-back (delta):** for each changed record, optimistic-guard → apply field delta → collect;
   **bulk re-index** the changed records; write a new `DatasetFile` revision; update the manifest.
   Full re-accession is a separate, confirmed, backed-up action.

---

## 3. Reuse map

| Need | Reuse |
|---|---|
| Stream features OUT (paged LPF) | `utils/feature_collection.py`, `utils/mapdata.py` FeatureCollection builders |
| Stream features IN / re-accession | `validation/create_dataset.py` (ijson), `validation.views.validate_file`, `validate_feature_batch` (Celery batched) |
| Apply a record delta | `publish.publish_place_record` field-mutation logic (name/coord; extend to types/dates) |
| Targeted re-index | `publish._reindex_place` (1 doc) / `datasets.tasks.index_to_pub` `streaming_bulk` (many) |
| Optimistic guard | `checkout.record_state_hash` per record |
| Backup / rollback | `datasets.models.DatasetFile.rev` (revisioned dataset files) |
| Progress on large ops | Celery batched-insert + Redis progress (accession pipeline pattern) |
| Collaboration | Teams / `ProjectYDoc` / share (place#112, shipped) |
| Gate | `Dataset.can_edit(user)` (shipped) |

## 4. Safety & gating

- Beta + `Dataset.can_edit` (owner/staff), button + endpoint (one predicate) — as for record-level.
- **Capacity guard** before check-out; hard size cap with steering to subset/record.
- **Backup revision** (`DatasetFile.rev`) before any publish-back; reversible.
- Delta-by-default keeps blast radius per-record; full re-accession is explicit + confirmed + backed up.
- The single-doc re-index path is **already proven** (record-level, verified live) — bulk is the same
  `makeDoc`+`searchy`-enrichment scaled via `streaming_bulk`.

## 5. Phasing

1. **Subset / filtered check-out** — check out a bounded page/filter of a dataset's records; edit with
   the record editor; delta publish-back + reindex the changed records. (The real workhorse; do first.)
2. **Whole-dataset for datasets that fit** — "select all" over (1) with the capacity gate; the "Edit in
   Workbench" button on the dataset page.
3. **Streaming/resumable transfer + capacity steering** for large datasets.
4. **Full re-accession** option (validation pipeline) for wholesale restructures, behind confirmation +
   backup.

## 6. Open questions (decide with Stephen)

- **Editor choice** (1a: MyD-flatten vs record-editor-subset vs new LPF editor) — recommend
  record-editor-subset (C), but confirm.
- **Capacity threshold** — what record count / byte size flips from "edit wholesale" to "edit a subset"?
- **Full re-accession** — is re-reconciliation on publish-back ever wanted, or is delta-only sufficient
  for v3.3? (Re-reconciliation changes matched authority ids — a semantic decision.)
- **Who may check out a gazetteer owned by others** (place#111 open question — access/permissions for
  legacy gazetteers) — staff-only initially?
- **Field scope** for record edits at scale — name/coord (built) is thin for a gazetteer; likely need
  types, dates, links too (extends `publish_place_record`).

## 7. Files (indicative)

**New:** `workbench/dataset_checkout.py` (paged LPF export + manifest), `workbench/dataset_publish.py`
(delta apply + bulk reindex + DatasetFile-rev backup); a dataset-editor shell
(`whg/webpack/js/wb-dataset.js`) over the record editor; endpoints `checkout/dataset/<id>/…?page=`,
`publish` (delta); `developer` progress note.
**Edited:** `workbench/models.py` (+`dataset_edit` doc-type + per-record manifest field, migration);
`workbench/doctypes.py`; `datasets/…` dataset browse template ("Edit in Workbench" + capacity steer);
`validation/*` to expose the accession pipeline for re-accession.
