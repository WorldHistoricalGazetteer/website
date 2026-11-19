# Proposed payload for EDOPS `/whgmodal` (provisional, from WHG)

Status: **a proposal for discussion, not an agreement.** WHG has built a prototype modal
(beta only) against this shape, filled today by adapting `/api/signature` responses
(`api/edops_whgmodal.py`). If your endpoint's shape differs, only that one adapter
function changes. A worked example for a level-8 basin (London, 1600-1700) is in
`scripts/fixtures/edop/whgmodal_basin_london_L8_provisional.json`.

## What the modal needs

One request = one signature unit (a basin, or an area), same parameters as
`/api/signature` (`scope`, `lat`/`lon` or `geom_wkt`, `level`, `from_year`/`to_year`).
JSON in, JSON out; WHG draws every chart in the browser from the numbers (d3-scale and
d3-shape to SVG), so **raw values, not images**. WHG sends no names, ids or other WHG
data, and will not.

## Shape

```
{
  "schema": "whgmodal/0-provisional",
  "status": "ok" | "no_basin" | "unavailable" | ...,   // as today; "unavailable" is never "no data"
  "reason": null | "text",
  "version": { "signature_version": "0.4", "generated": "ISO-8601",
               "profile_version": null | "x.y" },      // shown on the profile so it can be cited
  "scope":   { "type": "basin"|"area", "level": 6|8, "basin_id": 53398,
               "upstream_area_km2": 10797.4, "n_basins": null, "uncovered_pct": null },
  "sources": [ { "id": "temporal_climate", "name": "LMR v2.1 ...",
                 "status": "ok"|"unavailable", "reason": null|"text" } ],  // which answered, and why not
  "profile": null | {                                   // YOUR rule-generated prose; no LLM
      "generated_text": true,
      "generated_by": "edops-rules",                    // WHG's stand-in says "whg-adapter-placeholder"
      "level": 6,                                       // prose only for level 6 at first: null otherwise
      "sections": [ { "id": "climate", "title": "Climate",
                      "scale": "place"|"basin"|"area",  // REQUIRED: which scale this passage describes
                      "text": "...",
                      "caveats": ["..."] } ] },         // hedging carried with the passage
  "signature": { "headline": [ {"label","value","unit","note"} ],
                 "bands": [ { "band": "C", "label": "...", "scale": "basin",
                              "items": [ {"label","value","unit","note"} ] } ],
                 "temporal": { "status": "ok", "years": [1600,1700], "items": [...], "notes": [...] } },
  "series": {                                           // null for scope=area
    "monthly":  { "months": [1..12], "temperature_c": [12], "precipitation_mm": [12],
                  "scale": "basin", "source": "...", "period": "about 1970-2000 CE" },
    "timeline": { "years": [...], "pdsi": [...], "air_anom_k": [...], "prate_anom_mm_day": [...],
                  "reconstructed": true, "scale": "grid_cell", "grid_cell": {"lat","lon"},
                  "source": "LMR v2.1", "caveats": ["EDOPS's own lmr_*_note strings"] },
    "land_use": { "epochs": [ {"year", "cropland_km2", "grazing_km2", "pasture_km2", "rangeland_km2"} ],
                  "basin_area_km2": 1667.5, "reconstructed": true, "scale": "basin", "source": "HYDE 3.4" },
    "volcanic": { "events": [ {"year","month","vssi_tg","location"} ], "scale": "global",
                  "source": "eVolv2k v4" } },
  "caveats": ["area-level caveats"],
  "attribution": { ... }                                // as WHG already shows
}
```

## Editorial points this shape carries (from WHG's editors)

1. **Scale in the text.** Every `profile.sections[]` and every chart says whether it
   describes the place, the basin, an area, or a climate-model grid cell.
2. **Hedging travels with the value.** Reconstructed series are flagged
   `reconstructed: true` and carry EDOPS's own caveat strings, unedited.
3. **Version and date on every profile**, so it can be cited (`version`).
4. **Labelled as generated text**: `generated_text` and `generated_by`; WHG prints
   "Generated text" and that it was produced by rules from data.

## Questions for Karl

1. Is one `series` block per unit acceptable (about 100 numbers x 3 for the timeline)?
   Would you rather WHG request series separately?
2. Level 8 has no prose yet. Should `profile` be `null` there, or a short level-6
   passage about the parent basin (scale `basin`, with the parent id)?
3. Can `sources` list each source with its own status/reason on every response, including
   `no_basin`?
4. For `scope=area`, is there any series you would offer (e.g. per-epoch distributions),
   or is "summaries only" right?
5. Do you want a `citation` string from you, rather than WHG composing one from
   `version`?
