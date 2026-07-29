# Carry aggregated source terms in API result sets (and add licence to the attribution block)

**Context.** The licensing design calls for any API response spanning multiple sources to carry an
aggregated statement of terms at the **root of the returned JSON** — one entry per source namespace
present in the results, with that source's licence — so a programmatic consumer can comply without a
second lookup. Checked against the live service on 2026-07-29 while preparing the Office of Innovation
briefing (`developer/whg-licensing-briefing-pitt.html`, §4.3). It is only partly implemented.

## What exists

| Surface | Terms carried? |
|---|---|
| Interactive UI (Atlas gazetteer list, gazetteer detail, map popups) | Full licence + flags + rights holder + deed link |
| Single-record API (`EntityFeatureView` `?variant=popup`; `search.views.atlas_place`) | `attribution` object via `api.attribution.registry_attribution()` — includes `license__spdx_id`, `license__permits_commercial`, `license__share_alike`, `license__attribution_required`, `license__custom` |
| Standalone resolver `GET /api/attribution/?namespaces=…` / `?ids=…` | `{"sources": {...}, "whg": {...overlay...}}` |

## What is missing

1. **No root-level terms block in multi-record result sets.** Verified live:
   `GET /api/index/?name=Abergavenny` returns root keys `['note', 'type', 'pagesize', 'features']` —
   no `attribution`. Feature `properties` carry no licence/rights key either. Same for `/api/db/`
   and `/api/spatial/`.

2. **No terms in reconciliation responses.** `api/reconcile.py` contains no reference to attribution
   or licence; candidates carry id, name, type, score and match flag only. This is the channel where
   it matters most — reconciliation blends candidates from many differently-licensed sources into one
   response, so a consumer currently *cannot* comply with per-source terms even when willing to.

3. **The aggregated block itself omits the licence.** `api/attribution.py::attribution_for()` returns
   only `name`, `citation`, `record_count` per namespace. So even where `attribution_block()` is
   served, it does not state terms. Contrast `registry_attribution()` (single namespace), which does
   select the full licence field set. Embedding the block without fixing this would not solve the
   problem.

## Suggested shape

Root of any multi-record response:

```jsonc
{
  "attribution": {
    "sources": {
      "gn":  { "name": "GeoNames", "citation": "…", "record_count": 13378039,
               "license": { "spdx_id": "CC-BY-4.0", "label": "…", "url": "…",
                            "permits_commercial": true, "share_alike": false,
                            "attribution_required": true, "custom": false },
               "rights_holder": "…", "source_url": "…" },
      "osm": { "…": "…" }
    },
    "whg": { "spdx_id": "CC-BY-NC-4.0", "label": "…", "url": "…" }   // overlay, alongside not instead of
  },
  "features": [ … ]
}
```

Namespaces can be derived from the result ids with the existing
`api.attribution.namespaces_from_ids()`.

## Work items

- [ ] Extend `attribution_for()` to include the licence field set (aligning it with `registry_attribution()`).
- [ ] Attach `attribution_block(namespaces_from_ids(...))` at the root of `/api/index/`, `/api/db/`, `/api/spatial/`.
- [ ] Attach it to `/reconcile` responses, plus per-candidate namespace so a consumer can map candidate → terms.
- [ ] Decide whether gateway (`places`/`toponyms`) responses should echo the namespace set so the Django
      layer can resolve terms without re-deriving them — the only part of this that needs the indexing team.
- [ ] Document the block in the API docs.

## Related data-quality finding

The registry records **OpenHistoricalMap (`ohm`, ~905k records) as `CC0-1.0`**. OHM publishes under
**ODbL**, as OSM does — and our registry records `ODbL-1.0` correctly for `osm`. If this is an ingest
error rather than a deliberate determination it understates a share-alike obligation on ~0.9 m records.
Worth confirming as part of the per-namespace licence audit.
