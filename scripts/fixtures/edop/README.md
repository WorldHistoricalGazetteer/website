# EDOPS fixtures (place#325 Stage 3)

Real responses from the live EDOPS service, recorded once so that the tests in
`api/tests_edop_client.py` never call it. Recorded **2026-10-21, 11:07 UTC**,
with `curl`, from `https://computingplace.org` (spec `info.version` 0.4).
The JSON files are byte-for-byte what the service returned; nothing was edited.

| File | HTTP | Request (`GET /api/signature?…`) |
|---|---|---|
| `openapi_v0.4.json` | 200 | `GET /openapi.json` (the contract) |
| `basin_london_ABCDET_1600-1700.json` | 200 | `scope=basin&lat=51.5074&lon=-0.1278&bands=ABCDET&level=6&from_year=1600&to_year=1700` |
| `basin_london_ABCDE.json` | 200 | `scope=basin&lat=51.5074&lon=-0.1278&bands=ABCDE&level=6` |
| `area_london_ABCDE.json` | 200 | `scope=area&geom_wkt=POLYGON((-0.5 51.3,0.3 51.3,0.3 51.7,-0.5 51.7,-0.5 51.3))&bands=ABCDE&level=6` |
| `area_london_ABCDET_1650.json` | 200 | the same polygon, `bands=ABCDET&level=6&from_year=1650&to_year=1650` |
| `404_no_basin.json` | 404 | `scope=basin&lat=0&lon=-30&bands=ABCDE&level=6` (open Atlantic) |
| `422_missing_scope.json` | 422 | `lat=51.5&lon=-0.12` (no `scope`) |
| `422_area_band_t_range.json` | 422 | the polygon, `scope=area&bands=ABCDET&from_year=1600&to_year=1700` |

Timings seen while recording (one sample each, not a benchmark): basin 0.5-0.7 s,
area over that ~0.8° x 0.4° box 2.2 s, 404/422 ~0.25 s.

Note the two 422 bodies differ in shape: `detail` is a list for a missing
parameter (FastAPI validation) and a plain string for the area Band T rule.

Added **2026-10-21, 13:19 UTC** (one call, `curl`, same service, spec still 0.4) for the
ruling "basin level 8 for single points":

| File | HTTP | Request (`GET /api/signature?…`) |
|---|---|---|
| `basin_london_ABCDET_1600-1700_L8.json` | 200 | `scope=basin&lat=51.5074&lon=-0.1278&bands=ABCDET&level=8&from_year=1600&to_year=1700` |

Its key structure is identical to the level-6 response (compared recursively); it differs
in values only (`meta.scope.basin_level` 8, basin `id` 53398, `up_area` 10797.4). 0.72 s.
