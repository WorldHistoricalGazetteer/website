# Contract — rename boundary tilesets to match namespaces

> **Scope.** Indexing-side rename of two boundary tilesets so that, for every
> non-outlier authority, the tileset name **equals** the WHG namespace. The
> Atlas UI assumes this identity (so `tileSourceFor()` can collapse to a
> no-op); preserving the `_admin` suffix forces every consumer to carry a
> bespoke mapping table. This contract specifies the indexing-side changes;
> the matching whg3 follow-up is listed at the end so the two repos can be
> coordinated.

## Principle

For every authority `<ns>` whose namespace appears in `processing/settings.py::AUTHORITIES`, the boundary tileset's name **and** TileJSON `id` **and** the source-layer name inside each MVT tile is exactly `<ns>`.

## Renames

| Authority namespace | Current tileset / source-layer | New tileset / source-layer | Notes |
|---|---|---|---|
| `osm` | `osm_admin` | `osm` | OSM administrative boundaries (admin_level "0"–"11") |
| `ohm` | `ohm_admin` | `ohm` | OHM administrative boundaries |
| `osm_misc` | `osm_misc` | `osm_misc` | **Outlier — stays as is.** This is a category cluster (curated `boundary=<tag>` values across both OSM and OHM), not a single namespace, so the identity rule cannot apply. UI code retains `osm_misc` as a literal exception. |
| `po`, `clio`, `nl` | already match | — | No change needed. |

## Indexing-side changes

### 1. `processing/generate_tiles.py`

* The bucket → namespace map at line ~119 currently reads:

  ```python
  "osm_admin": ("osm",),
  "ohm_admin": ("ohm",),
  ```

  Replace the keys so the bucket name **is** the namespace:

  ```python
  "osm": ("osm",),
  "ohm": ("ohm",),
  ```

* Anywhere the bucket name is interpolated into output paths (around the `--output`, `--layer`, `--name`, `--description` arguments to `tippecanoe` / `tile-join` near lines 278–279 and 970), the new bucket name `osm` / `ohm` flows through automatically — no second-place edit needed unless there's a hardcoded `osm_admin` string elsewhere in the same file.
* Search for and update any other literal `osm_admin` / `ohm_admin` strings in this module: the `is_admin_bucket` check around line 776, the bucket-list comment at line 14, the example invocation comment at line 33, and the docstring at line 768 / 1009.

### 2. `scripts/ingest.sh`

If the deploy/upload step references `.mbtiles` files by name (e.g. `rsync osm_admin.mbtiles …`), update those literals.

### 3. Tileserver — `tileboss/tileserver/`

The tileserver's `mbtiles` source URLs in `tileboss/tileserver/styles/whg-context/style.json` reference these by basename:

```jsonc
"osm_admin": {"type": "vector", "url": "mbtiles://{osm_admin}", ...},
"ohm_admin": {"type": "vector", "url": "mbtiles://{ohm_admin}", ...},
```

After rename:

```jsonc
"osm": {"type": "vector", "url": "mbtiles://{osm}", ...},
"ohm": {"type": "vector", "url": "mbtiles://{ohm}", ...},
```

Every layer in the same style.json that uses `"source": "osm_admin"` / `"ohm_admin"` and `"source-layer": "osm_admin"` / `"ohm_admin"` must be updated in the same edit. There are roughly 20 such layers (10 each for osm/ohm covering continental, country, state, district, local — both line and label) plus the two `*-fill` layers added earlier in this branch.

Layer **`id`** strings (`osm_admin-line-continental`, etc.) are arbitrary identifiers; renaming them to `osm-line-continental` is recommended for consistency but optional — they're not joined to the source name in MapLibre, only the `source` and `source-layer` fields are.

After editing, `tileboss/restart_tileserver.sh` (or whichever restart command applies) needs to fire so the tileserver picks up the new style and starts serving from the new mbtiles paths.

### 4. mbtiles file rename on the tileserver host

The actual `.mbtiles` files on the tileserver host need renaming on disk:

```
osm_admin.mbtiles  →  osm.mbtiles
ohm_admin.mbtiles  →  ohm.mbtiles
```

Coordinate with whichever process delivers the mbtiles to the tileserver (rsync from CRC?) — if that delivery path is name-aware, it needs updating too. If not, a one-shot `mv` on the host plus a tileserver restart is enough.

### 5. (Optional) regenerate

A regeneration via `generate_tiles.py` after the rename is the safest sequencing — it produces fresh `osm.mbtiles` / `ohm.mbtiles` directly and the cutover is atomic per file.

## whg3 follow-up (do **not** do as part of the indexing PR — listed for coordination only)

Once the renamed tilesets are live, the following whg3-side simplifications become possible. They aren't blocking — the current code carries a temporary mapping that handles either name — but cleaning them up in a follow-up keeps the codebase honest:

1. **`whg/webpack/js/heroMap.js`** and **`whg/webpack/js/contextMap.js`** — replace:

   ```js
   const BOUNDARY_SOURCE_LAYERS = ['osm_admin', 'ohm_admin', 'osm_misc', 'po', 'clio', 'nl'];
   ```

   with:

   ```js
   const BOUNDARY_SOURCE_LAYERS = ['osm', 'ohm', 'osm_misc', 'po', 'clio', 'nl'];
   ```

2. **`whg/webpack/js/layerSourcesPalette.js`** — `tileSourceFor()` collapses to identity:

   ```js
   function tileSourceFor(sourceId) { return sourceId; }
   ```

   (or it can be deleted entirely and call sites can use `this._activeSource` directly).

3. **`whg/webpack/js/regionSelector.js`** — `NAMESPACE_TO_SOURCE` similarly collapses to identity / can be removed.

4. **`api/migrations/0005_admin_reingest_controls.py`** — no change needed; the registry already keys on namespace (`osm`, `ohm`, …) and never referred to the `_admin` suffix.

The whg3 PR can be merged either before or after the tileset rename: the JS keeps a mapping table that handles `osm → osm_admin` / `ohm → ohm_admin` so the UI works against the current tilesets, and once the tileserver is renamed the mapping becomes a no-op until someone deletes it. There's no breaking moment — only a cleanup opportunity.

## Validation checklist

After the rename:

* `curl https://tiles.whgazetteer.org/data/osm.json` returns a TileJSON whose `vector_layers[0].id == "osm"`.
* Same for `https://tiles.whgazetteer.org/data/ohm.json`.
* `https://tiles.whgazetteer.org/data/osm_admin.json` and `…/ohm_admin.json` return 404 (or, if you choose to keep aliases, identical bodies — but aliases just defer the cleanup, so prefer 404).
* The Atlas page Regions panel — picking "OSM (Modern)" or "OHM (Historical)" — renders boundaries on the map exactly as before. (The whg3 mapping handles the transition; no code change needed for parity.)
* The whg-context style at `https://tiles.whgazetteer.org/styles/whg-context/style.json` lists `osm` and `ohm` (not `osm_admin` / `ohm_admin`) under `sources`, and every layer's `source` / `source-layer` matches.

## Outlier policy: why `osm_misc` stays

`osm_misc` is not a namespace — it's a curated cluster of `boundary=<tag>` values from OSM/OHM data that don't fall under `boundary=administrative` (parishes, civil/political/historical/regional, aboriginal lands, climatic zones, etc., per `plan-consolidateBoundaries-completed`). The records inside still carry `namespace: "osm"` or `namespace: "ohm"` — `osm_misc` is purely a tileset/UI label for the category cluster. Treating it as a namespace alias would conflict with the real `osm` / `ohm` namespaces. The Atlas UI keeps `osm_misc` as the literal exception in two places: the `LayerSourcesPalette` source-id list and the `BOUNDARY_SOURCE_LAYERS` array.
