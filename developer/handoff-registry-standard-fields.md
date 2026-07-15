# Handoff — standardized registry rendering fields (authority + contributed gazetteers)

**Audience:** Claude Code in the `whg3` repo. **Goal:** render every gazetteer —
public *authority* gazetteers (pushed by the indexing repo) **and** WHG-contributed
*datasets/collections* — with **one card component**, by giving the
`GazetteerRegistryEntry` a common, fully-populated field set.

**Status (indexing side):** done. `processing.push_gazetteer_inventory` already
emits the fields below (optional keys; the endpoint ignores unknown keys, so this is
safe to receive before the model changes land). This handoff covers the WHG side.
**Prod access:** `ssh whg` (confirm container names with `ssh whg 'docker ps'`).

---

## 1. The common rendering contract

One card for both `class=authority` and `class=dataset`/`collection`:

| field | meaning | authority source (push) | WHG-dataset source |
|-------|---------|-------------------------|--------------------|
| `name` | display name | `dataset_name` | `Dataset.title` |
| `class` | authority / dataset / collection | set by push | model |
| `description` | **prose blurb** | `AUTHORITIES.description` (falls back to the legacy `citation` blob) | `Dataset.description` |
| `citation_text` | structured human citation | `AUTHORITIES.citation_text` | (contributor / n/a) |
| `license` `{spdx_id,label,url}` | resolved licence | `license_spdx`/`license_url` | `License` FK |
| `rights_holder` | who to attribute | `AUTHORITIES.rights_holder` | model / contributor |
| `contributors` | CRediT `[{name,role,orcid}]` | `AUTHORITIES.contributors` | contributor workflow |
| `source_url` | **homepage / webpage** | `AUTHORITIES.source_url` | `Dataset.webpage` |
| `image` | logo / thumbnail URL | `AUTHORITIES.image` (optional) | `Dataset.image` |
| `last_modified` | ISO date the data was last updated | push-stamped (mtime of the namespace's temporal-extent aggregate ≈ last ingest) | `Dataset.last_modified` |
| `record_count`, `temporal_extent`, `h3_coverage` | counts / coverage | aggregates | model |

`source_url` IS the "webpage" — no separate field. `description` is now
**prose-preferred** (the citation moved to `citation_text` in Phase 4).

---

## 2. WHG-side changes

1. **Model** — `GazetteerRegistryEntry` (API/registry app): add two nullable fields:
   - `image = models.URLField(null=True, blank=True)`
   - `last_modified = models.DateField(null=True, blank=True)`
   (`description`, `source_url`, `citation_text`, licence/rights/contributors already
   exist from Phase 4.) Migration is trivial; both nullable.

2. **Inventory endpoint** — accept + store `image` and `last_modified` (and continue
   to treat `description` as prose). Keep the existing contract guarantees: **only
   keys present in the payload are written** (never null-clobber), and unknown keys
   are ignored. So nothing breaks if an older payload omits them.

3. **WHG-dataset entries** — when the registry row is a `class=dataset`/`collection`,
   populate `image` / `last_modified` / `description` from the `Dataset` model (the
   legacy dataset view already surfaces these), so contributed datasets render with
   the same card as authorities.

4. **Unified card / offcanvas** — render the contract in §1 for *both* classes:
   name · class badge · image · description · citation · licence chip ·
   rights_holder · source_url link · last_modified ("updated …") · record_count ·
   temporal span · spatial coverage. Drive freshness sort off `last_modified`.

5. **`attribution_for()`** — unchanged for the licence/citation block; just ensure the
   card reads `image`/`last_modified`/`description` alongside it.

---

## 3. Verify (after the model + endpoint land)

```bash
# re-push from the indexing repo (crc0):  push_gazetteer_inventory --namespace hgis
# then on WHG:
ssh whg 'docker exec -i web python manage.py shell' <<'PY'
from api.registry.models import GazetteerRegistryEntry as G   # adjust import
e = G.objects.get(id='hgis')
print(e.description[:80], '|', e.image, '|', e.last_modified, '|', e.source_url)
PY
# and the public attribution/card endpoint should now include image + last_modified
```

---

## 4. Notes

- All new keys are **optional**; authorities seed `description`/`image` incrementally
  in `processing/settings.py` `AUTHORITIES` (e.g. `hgis`, `alc` already carry
  `description`; `image` is added per authority as a stable logo URL is confirmed —
  don't invent URLs that 404).
- `last_modified` for authorities is push-stamped from the temporal-extent aggregate
  mtime (≈ last ingest). If you prefer the *source* publication date, add an optional
  `last_modified` to the `AUTHORITIES` entry and have the push prefer it.
- This is the same forward-compatible pattern as the Phase-4 attribution fields
  (`developer/plan-citations-licences-credit.prompt.md` §10).
