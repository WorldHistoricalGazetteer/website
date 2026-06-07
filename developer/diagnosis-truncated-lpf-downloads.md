# Diagnosis: truncated LPF (and TSV) dataset downloads — cache poisoning

**Status:** root cause identified; fix specified below. Ready for a coding agent to
implement, deploy, clear the poisoned caches, and verify.
**Prod access:** `ssh whg` reaches the host running the prod containers
(`web`, `celery_worker`, `redis`, `postgres`, …). Confirm exact names with
`ssh whg 'docker ps'` before running the commands here.

---

## 0. Symptom

LPF downloads of large datasets are silently **truncated** and stay that way on
every re-download. Confirmed on **dataset 14** (HGIS de las Indias — lugares):
the downloaded `.lpf` contains **11,353 of ~13,000** features and ends with

```
…"src_id": "7000012", "url": "https://whgazetteer.org/api/place/178293/"}},
```

i.e. a feature object followed by a **trailing comma and no closing `]}`** — the
JSON array and the FeatureCollection object are never closed. (Dataset 15 /
territorios, 893 features, downloads complete — it's small enough to finish before
the abort.)

The content is otherwise correct (names, links wd/gn/loc/viaf/tgn, AAT types,
`related` containment, geometry) — this is purely a **stream-termination /
caching** defect, not a data defect.

---

## 1. Root cause — `api/download_file.py :: stream_live()`

The download view (`api/views_entity.py`, ~lines 335–350) serves a cached file
when present, otherwise calls `stream_live(..., cache_filepath=cache_path)`
**in the request** as a `StreamingHttpResponse`, which streams the gzip’d LPF to
the client **and writes it to the cache at the same time**:

```python
# views_entity.py (~335)
if FileCache.is_cached(obj_type, obj_id, filetype=filetype):
    response = StreamingHttpResponse(stream_from_file(cache_path), ...)   # serve cached
else:
    if not FileCache.is_building(...):
        if FileCache.acquire_build_lock(...):
            response = StreamingHttpResponse(
                stream_live(obj_type, obj, request, cache_filepath=cache_path, ...))  # live + cache
```

`stream_live()` (`api/download_file.py`, ~lines 147–484):

1. opens `cache_filepath + '.tmp'` (line ~154) — *"so partial outputs never replace the final file"*;
2. emits the header, citation, license, then `,"features":[` (line ~236);
3. loops `for place in qs.iterator():` emitting `,` + `json.dumps(feature)` per place (~250–262);
4. emits the closing `]` and `}` (~263–264);
5. writes the gzip trailer with `compressor.flush(zlib.Z_FINISH)` (~474);
6. **`finally:` closes the temp file and renames it to the final cache path — UNCONDITIONALLY** (~480–484):

```python
    finally:
        if cache_file:
            cache_file.close()
            if os.path.exists(cache_filepath + '.tmp'):
                os.rename(cache_filepath + '.tmp', cache_filepath)   # ← BUG: runs even on abort
```

**The defect:** if the stream is interrupted **before** step 5 — by a client
disconnect (`GeneratorExit` is raised into the generator at the suspended
`yield`, and `finally` blocks run during `GeneratorExit`), a gunicorn/nginx
**request timeout** on the long ~13k-feature stream, or an **exception in
`PlaceFeatureSerializer`** for one record (the `try` has only `finally`, no
`except`) — then:

- `Z_FINISH` and the closing `]}` are never written, so `.tmp` is a **truncated
  gzip whose JSON ends mid-array**;
- the `finally` nonetheless **renames that partial `.tmp` onto the final cache
  path**, publishing it;
- every later request hits `is_cached() == True` and serves the poisoned partial
  via `stream_from_file`. Hence the truncation is **persistent**, not a one-off.

This directly violates the line ~153 intent (*"partial outputs never replace the
final file"*).

### Corroboration: the async builder already does it correctly

`build_cache()` (`api/download_file.py`, ~lines 488–523) — the Celery task —
writes to its own `.tmp` and:

- `os.rename(.tmp → final)` **only after** the loop completes successfully (~515);
- `os.remove(.tmp)` in `except` (~521–522).

It also calls `stream_live(..., cache_filepath=None)` (~506) so `stream_live`’s
own `cache_file` is `None` and its buggy `finally` rename is skipped. **So only
the in-request live path (views_entity → `stream_live` with `cache_filepath=cache_path`)
poisons the cache.** The same `finally` serves both the `lpf` and `tsv` branches,
so TSV exports have the identical bug.

---

## 2. The fix — publish the cache only on successful completion

Mirror the async builder: track completion and, in `finally`, **rename on success,
discard on abort**.

```python
def stream_live(obj_type, obj, request, filetype='lpf', cache_filepath=None):
    cache_file = None
    if cache_filepath:
        cache_file = open(cache_filepath + '.tmp', 'wb')
    completed = False                                   # ← add
    ...
    try:
        ...
        final = compressor.flush(zlib.Z_FINISH)
        if final:
            if cache_file:
                cache_file.write(final)
            yield final
        completed = True                               # ← only reached if the stream finished
    finally:
        if cache_file:
            cache_file.close()
            tmp = cache_filepath + '.tmp'
            if completed and os.path.exists(tmp):
                os.rename(tmp, cache_filepath)         # publish only the complete file
            elif os.path.exists(tmp):
                os.remove(tmp)                         # discard partial — never poison the cache
```

This is the minimal, correct change. It fixes both `lpf` and `tsv`.

### Secondary hardening (recommended — addresses the *trigger*, not just the persistence)

The fix above stops a partial from ever being cached, but a large dataset’s
in-request live stream can still **abort every time** (timeout/disconnect),
leaving users with no cache and a failing download. Prefer building off-request:

- In the download view, when not cached, **dispatch `build_cache.delay(...)` and
  return a “building, retry shortly” response** (202 / poll) instead of live-
  streaming a 13k-feature export in the request. The async task has no request
  timeout and already uses the correct `.tmp` discipline. (`prebuild_cache()` /
  `invalidate_and_rebuild_cache()` already exist for this.)
- Optionally wrap the per-place serialization in `try/except` so a single bad
  record is skipped (and logged) rather than aborting the whole export.
- If the live path is kept for small objects, ensure gunicorn `--timeout` and
  nginx `proxy_read_timeout` exceed the largest expected stream.

---

## 3. Remediate prod (after deploying the fix)

The fix prevents *future* poisoning; the **already-poisoned caches must be
cleared and rebuilt**. Use Django so paths/keys are computed correctly (no
guessing `MEDIA_ROOT`). Adjust container name if `docker ps` differs.

```bash
# Clear poisoned cache files + redis build keys for the affected datasets
ssh whg 'docker exec -i web python manage.py shell' <<'PY'
import os
from api.download_file import FileCache, redis_client
for did in (14, 15):                       # extend to any other large datasets/collections
    for ft in ('lpf', 'tsv'):
        p = FileCache.get_cache_path('dataset', did, ft)
        for f in (p, p + '.tmp'):
            if os.path.exists(f):
                os.remove(f); print('removed', f)
        for k in ('lpf_build_lock','lpf_build_task','lpf_pending_rebuild','lpf_last_rebuild',
                  'tsv_build_lock','tsv_build_task','tsv_pending_rebuild','tsv_last_rebuild'):
            redis_client.delete(f'{k}:dataset:{did}')
    print('cleared redis for dataset', did)
PY
```

Then rebuild **via the async task** (correct `.tmp` discipline, no request timeout):

```bash
ssh whg 'docker exec -i web python manage.py shell' <<'PY'
from api.download_file import build_cache
for did in (14, 15):
    build_cache.delay('dataset', did, 'lpf')
    print('queued lpf build for dataset', did)
PY
```

Watch it run (this is where the *trigger* is diagnosed):

```bash
ssh whg 'docker logs --tail 200 -f celery_worker'   # or `web` if you keep the live path
# look for:  "Streaming LPF dataset:14 - emitted N features so far"  (every 100)
#            the final  "Completed streaming LPF dataset:14 - total features: <N>"
#            or a traceback / WORKER TIMEOUT / broken pipe at some N
```

**Diagnostic read:**
- **Stops at a consistent N every run** → a data/serializer error on the place at
  `N+1`; fix that record/serializer (and the per-place `try/except` hardening covers it).
- **Variable N (only in the in-request path; never in celery)** → request/proxy
  **timeout** — the async-task routing above is the cure.

---

## 4. Verify

```bash
# the cache file should now decompress to a complete FeatureCollection
ssh whg 'docker exec web bash -lc "zcat media/downloads/whg_dataset_14.lpf | tail -c 40"'
#   expect it to end with ...}}]}   (NOT ...}},)

# feature count == dataset place count
ssh whg 'docker exec -i web python manage.py shell' <<'PY'
import gzip, json
from datasets.models import Dataset
from api.download_file import FileCache
n = json.load(gzip.open(FileCache.get_cache_path('dataset',14,'lpf')))['features']
print('cached features:', len(n), 'vs dataset.places:', Dataset.objects.get(id=14).places.count())
PY
```

Both should match (~13,000 for dataset 14). Then a fresh GET of
`https://whgazetteer.org/entity/dataset:14/api` (LPF) returns the complete file.

---

## 5. Scope / files

- **Fix:** `api/download_file.py` → `stream_live()` `finally` block (~480–484) +
  `completed` flag.
- **Hardening (optional):** `api/views_entity.py` download view (~335–350) — route
  large exports through `build_cache.delay()` instead of in-request `stream_live`.
- **No data migration**; no schema change. Affects LPF **and** TSV exports for
  **datasets and place collections**.

*(Diagnosed 2026-06-07 from the indexing-side investigation: dataset 14 LPF
download arrived truncated at 11,353/~13k with no closing `]}`. Self-referential
content was intact; the failure is purely stream termination + unconditional
cache publish.)*
