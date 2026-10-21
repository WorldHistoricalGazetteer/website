"""Server-side client for EDOPS, the Environmental Dimensions of Place Service
(Karl Grossner, Computing Place), place#325 Stage 3.

The contract is the LIVE service's OpenAPI spec, ``https://computingplace.org/openapi.json``
(``info.version`` 0.4, read 2026-10-21; a copy is in ``scripts/fixtures/edop/``), NOT the
``kgeographer/cedop`` repo, which has no ``scope`` parameter. ``GET /api/signature``:

* ``scope`` is required (422 without it): ``basin`` (raw values for the one basin holding
  ``lat``/``lon``), ``buffer`` (``radius_km``; not used here, "expected to be retired soon"
  per the area schema), ``area`` (``geom_wkt``, SRID 4326, POLYGON or MULTIPOLYGON only);
* ``bands`` (default ``ABCDE``); ``T`` needs ``from_year`` and ``to_year`` (0-1998 CE), and on
  ``scope=area`` they must be EQUAL (422 "Band T requires a single year");
* ``level`` 6 or 8 (default 6); ``flat``, ``place_links`` (basin only); ``detail`` (area only).

The two scopes return different shapes: basin has ``profile_summary`` +
``signature_bands``; area has a flat ``variables`` list of distribution summaries plus
``shortfall`` and ``caveats``. Both carry ``meta``.

What this module promises its callers:

* **Every answer has a status, and "unavailable" is never "no data".** ``ok``; ``no_basin``
  (HTTP 404 whose detail is "No basin covers this point": a finding about the place, e.g.
  open sea); ``unavailable`` (timeout, connection failure, 5xx, any other 404, a body that is
  not JSON: the service could not be asked; says nothing about the place); ``rejected``
  (400/422: the request WE built was refused, i.e. our bug or a contract change);
  ``disabled`` (the off-switch, ``settings.EDOP_ENABLED``).
* **Short timeouts, no retries on user-facing paths.** ``retries`` defaults to 0 and is
  capped at ``MAX_RETRIES``; only ``unavailable`` is retried.
* **Cached on the request.** The key is a hash of the exact query parameters (and the
  base URL and this module's ``CACHE_VERSION``). Signature data is slow-changing
  (BasinATLAS v1.0, LMR v2.1, HYDE 3.4, eVolv2k v4 are static releases), so ``ok`` and
  ``no_basin`` are kept ``CACHE_TTL`` (30 days). ``unavailable`` is cached
  ``FAILURE_TTL`` (30 s), only so a down service costs one timeout per half-minute rather
  than one per page view; it is cached AS unavailable and comes back labelled ``cached``.
  ``rejected`` and ``disabled`` are not cached. What is cached is the response trimmed of
  bulk it never renders (basin polygon, per-year series, area histograms); summaries are
  computed on read, so changing a summary does not need a cache flush.
* **No secrets logged.** The service takes no key today. Logs carry the scope, the
  status and the elapsed time, never the full URL (``geom_wkt`` can be long).

Scope choice (SG, delegated 2026-10-21; see ``plan_concordance`` / ``plan_locus``):
``basin`` for a single point; ``area`` over the hull of the aggregated members' geometries
for a multi-member concordance; for a locus, per-member ``basin``, or one ``area`` only
when the cluster is spatially tight (``LOCUS_TIGHT_KM``).

Tests use a fake ``transport`` and the recorded fixtures; nothing here is called live
from the test suite.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Iterable, Optional

from django.conf import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration (overridable in settings; no settings.py change is needed)
# ---------------------------------------------------------------------------

DEFAULT_BASE_URL = "https://computingplace.org"
SIGNATURE_PATH = "/api/signature"
DOCS_URL = "https://computingplace.org/docs/api/"
CODEBOOK_URL = "https://computingplace.org/docs/codebook/"
HOME_URL = "https://computingplace.org/"

# Passed explicitly so a change of the service's default (it was 8, is now 6) cannot
# silently change what is cached under the same key or what a page means.
LEVEL = 6
BASE_BANDS = "ABCDE"
TEMPORAL_BAND = "T"
YEAR_MIN, YEAR_MAX = 0, 1998          # Band T coverage, per the spec

CONNECT_TIMEOUT = 2.0
# Read timeouts by scope. Seen 2026-10-21: basin 0.5-0.7 s; area over a 0.8 x 0.4 degree box
# 2.2 s. About 8x / 5x headroom, and short enough that the async section gives up well
# inside the page's own fetch timeout.
READ_TIMEOUT = {"basin": 5.0, "area": 12.0}
MAX_RETRIES = 2
RETRY_BACKOFF_S = 0.5

CACHE_VERSION = 1
CACHE_TTL = 30 * 24 * 3600            # ok and no_basin: slow-changing source data
FAILURE_TTL = 30                      # unavailable: just enough to stop a stampede

STATUS_OK = "ok"
STATUS_NO_BASIN = "no_basin"
STATUS_UNAVAILABLE = "unavailable"
STATUS_REJECTED = "rejected"
STATUS_DISABLED = "disabled"
_CACHEABLE = {STATUS_OK: CACHE_TTL, STATUS_NO_BASIN: CACHE_TTL, STATUS_UNAVAILABLE: FAILURE_TTL}

NO_BASIN_DETAIL = "No basin covers this point"

# ---- scope choice -----------------------------------------------------------

# A locus is "tight" when every pair of its members' representative points is within
# this many km (the cluster's diameter, great-circle). Why 50 km: BasinATLAS level 6, the
# level used here, has 16,397 basins over ~134 M km2 of land (Lehner & Grill 2013), a mean
# of ~8,200 km2, i.e. ~90 km across. A cluster no wider than about half a typical basin
# draws an area aggregate from one or a few adjacent basins, so one signature describes an
# environment its members share. Wider than that, members usually sit in different basins,
# and one aggregate would blend them into a profile none of them has, which is what
# place#325 warns against. (Published basin counts, not measured here. Revisit with Karl.)
LOCUS_TIGHT_KM = 50.0
# At most this many per-member basin signatures for one locus; the rest are listed as not
# looked up. Calls run in parallel (LOCUS_WORKERS), each bounded by READ_TIMEOUT["basin"].
LOCUS_MAX_MEMBER_SIGNATURES = 8
LOCUS_WORKERS = 4
# A concordance whose members span more than this is not given one area signature: the
# hull would be continental, slow to compute, and describe no member. It is reported as
# "dispersed" instead (a finding about the concordance; such spans are usually a bad link).
CONCORDANCE_MAX_SPAN_KM = 1000.0
# A hull of collinear points is a line; EDOPS takes polygons only, so it is buffered by this
# many degrees (~1 km at the equator) to make one.
DEGENERATE_HULL_BUFFER_DEG = 0.01
# geom_wkt travels in a GET query string (nginx's default limit is 8 KB); the hull is
# simplified until it has at most this many vertices, and written at 5 decimal places.
MAX_HULL_VERTICES = 64
WKT_DECIMALS = 5
# Members closer than this to each other count as the same location (a single point).
SAME_POINT_KM = 0.05

Transport = Callable[[str, dict, tuple], tuple]


def _setting(name, default):
    return getattr(settings, name, default)


def enabled() -> bool:
    """The off-switch: ``settings.EDOP_ENABLED`` (default True)."""
    return bool(_setting("EDOP_ENABLED", True))


def base_url() -> str:
    return str(_setting("EDOP_BASE_URL", DEFAULT_BASE_URL)).rstrip("/")


def _cache():
    from django.core.cache import caches
    return caches[_setting("EDOP_CACHE_ALIAS", "default")]


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

def band_t_years(span) -> tuple[Optional[tuple[int, int]], Optional[str]]:
    """``((from_year, to_year), note)`` for Band T from a ``[start, end]`` year span, or
    ``(None, reason)`` when Band T cannot be asked for. Clamped to EDOPS's 0-1998 CE, and
    the note says so when it was."""
    if not span or len(span) != 2:
        return None, "undated"
    s, e = span
    if s is None and e is None:
        return None, "undated"
    s = e if s is None else s
    e = s if e is None else e
    try:
        s, e = int(s), int(e)
    except (TypeError, ValueError):
        return None, "undated"
    if s > e:
        s, e = e, s
    if e < YEAR_MIN or s > YEAR_MAX:
        return None, "outside"
    cs, ce = max(s, YEAR_MIN), min(e, YEAR_MAX)
    return (cs, ce), ("clamped" if (cs, ce) != (s, e) else None)


def basin_request(lon: float, lat: float, years: Optional[tuple[int, int]] = None) -> dict:
    """Query parameters for ``scope=basin`` at one point. ``years`` adds Band T."""
    params = {"scope": "basin", "lat": round(float(lat), 6), "lon": round(float(lon), 6),
              "bands": BASE_BANDS, "level": LEVEL}
    if years:
        params["bands"] = BASE_BANDS + TEMPORAL_BAND
        params["from_year"], params["to_year"] = int(years[0]), int(years[1])
    return params


def area_request(geom_wkt: str, year: Optional[int] = None) -> dict:
    """Query parameters for ``scope=area`` over ``geom_wkt``. Band T only for ONE year
    (the spec: on area, ``from_year`` must equal ``to_year``)."""
    params = {"scope": "area", "geom_wkt": geom_wkt, "bands": BASE_BANDS, "level": LEVEL}
    if year is not None:
        params["bands"] = BASE_BANDS + TEMPORAL_BAND
        params["from_year"] = params["to_year"] = int(year)
    return params


def cache_key(params: dict) -> str:
    canon = json.dumps({"u": base_url(), "p": params}, sort_keys=True, separators=(",", ":"))
    return f"edop:v{CACHE_VERSION}:" + hashlib.sha256(canon.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Transport and fetch
# ---------------------------------------------------------------------------

def _running_tests() -> bool:
    """True under ``manage.py test`` (the same test settings.py uses)."""
    return "test" in sys.argv


def _requests_transport(url: str, params: dict, timeout: tuple) -> tuple:
    # Automated tests never call the live service: tests that need EDOPS replace this
    # function with a fake, and any page a browser test opens that reaches it anyway
    # (another suite's Record page, say) gets "unavailable" instead of a network call.
    if _running_tests():
        raise RuntimeError("EDOPS is not called from automated tests")
    import requests
    resp = requests.get(url, params=params, timeout=timeout,
                        headers={"Accept": "application/json",
                                 "User-Agent": "WHG/World-Historical-Gazetteer (+https://whgazetteer.org)"})
    return resp.status_code, resp.text


class _Unavailable(Exception):
    pass


def _trim(raw: dict, scope: str) -> dict:
    """The response without the bulk no summary reads: the basin polygon, per-year
    series, and area histograms."""
    if not isinstance(raw, dict):
        return raw
    out = {k: v for k, v in raw.items() if k != "geom_geojson"}
    bands = out.get("signature_bands")
    if isinstance(bands, dict) and isinstance(bands.get("T"), dict):
        bands = dict(bands)
        bands["T"] = {k: v for k, v in bands["T"].items() if not k.endswith("_series")}
        out["signature_bands"] = bands
    if isinstance(out.get("variables"), list):
        out["variables"] = [{k: v for k, v in var.items() if k != "detail"} if isinstance(var, dict) else var
                            for var in out["variables"]]
    return out


def _detail_text(body) -> str:
    d = body.get("detail") if isinstance(body, dict) else None
    if isinstance(d, str):
        return d
    if isinstance(d, list):
        return "; ".join(str(x.get("msg", x)) if isinstance(x, dict) else str(x) for x in d)
    return ""


def _classify(code: int, text: str, scope: str) -> dict:
    """One HTTP answer -> ``{status, reason, raw?}``. Raises _Unavailable for the
    retryable class."""
    try:
        body = json.loads(text) if text else None
    except (TypeError, ValueError):
        body = None
    if code == 200:
        if not isinstance(body, dict):
            raise _Unavailable("a 200 whose body is not a JSON object")
        return {"status": STATUS_OK, "reason": None, "raw": _trim(body, scope)}
    if code == 404 and _detail_text(body).strip().rstrip(".") == NO_BASIN_DETAIL:
        return {"status": STATUS_NO_BASIN, "reason": NO_BASIN_DETAIL}
    if code in (400, 422):
        return {"status": STATUS_REJECTED, "reason": f"HTTP {code}: {_detail_text(body)[:300]}"}
    raise _Unavailable(f"HTTP {code}")


def fetch_signature(params: dict, *, transport: Optional[Transport] = None, retries: int = 0,
                    use_cache: bool = True) -> dict:
    """Ask EDOPS for one signature. Returns
    ``{"status", "reason", "scope", "request", "raw" (ok only), "cached", "elapsed_ms"}``.

    ``retries`` (default 0, capped at MAX_RETRIES) applies to ``unavailable`` only, and
    must stay 0 on any path a person is waiting on."""
    scope = params.get("scope")
    base = {"scope": scope, "request": dict(params), "cached": False, "elapsed_ms": 0}
    if not enabled():
        return {**base, "status": STATUS_DISABLED, "reason": "EDOP_ENABLED is off"}
    key = cache_key(params)
    if use_cache:
        try:
            hit = _cache().get(key)
        except Exception:  # noqa: BLE001 — a broken cache must not break the page
            hit = None
        if isinstance(hit, dict) and hit.get("status"):
            return {**base, **hit, "cached": True}

    transport = transport or _requests_transport
    timeout = (CONNECT_TIMEOUT, READ_TIMEOUT.get(scope, READ_TIMEOUT["basin"]))
    url = base_url() + SIGNATURE_PATH
    attempts = 1 + max(0, min(int(retries or 0), MAX_RETRIES))
    t0 = time.monotonic()
    result = None
    for attempt in range(attempts):
        try:
            code, text = transport(url, params, timeout)
            result = _classify(int(code), text, scope)
            break
        except _Unavailable as exc:
            result = {"status": STATUS_UNAVAILABLE, "reason": str(exc)}
        except Exception as exc:  # noqa: BLE001 — timeouts, DNS, refused, TLS: all "could not ask"
            result = {"status": STATUS_UNAVAILABLE, "reason": type(exc).__name__}
        if attempt + 1 < attempts:
            time.sleep(RETRY_BACKOFF_S * (attempt + 1))
    elapsed = int((time.monotonic() - t0) * 1000)
    logger.info("EDOPS %s -> %s in %d ms%s", scope, result["status"], elapsed,
                f" ({result['reason']})" if result.get("reason") else "")
    ttl = _CACHEABLE.get(result["status"])
    if use_cache and ttl:
        try:
            _cache().set(key, result, ttl)
        except Exception:  # noqa: BLE001
            pass
    return {**base, **result, "elapsed_ms": elapsed}


# ---------------------------------------------------------------------------
# Readable summaries (from the spec's band structure and the Codebook)
# ---------------------------------------------------------------------------

# Paraphrased from the EDOPS Codebook (/docs/codebook/), so the page says what each band
# can and cannot be used for. D in particular is present-day only.
BAND_INFO = {
    "A": ("Physiographic", "Terrain and geology: stable over geological time, so a fair baseline for any period."),
    "B": ("Hydro-climatic", "Water and soils under the current climatic regime, including the effect of upstream dams."),
    "C": ("Bioclimatic", "Present-day climate (WorldClim, about 1970-2000 CE): long-run averages, not historical values."),
    "D": ("Anthropocene (present-day)", "Present-day human presence only: not evidence about the past."),
    "E": ("Coastality", "How the basin drains to the sea."),
    "T": ("Temporal", "The years used: reconstructed climate anomalies (LMR v2.1), volcanic events (eVolv2k v4) and "
                      "land use (HYDE 3.4)."),
}

# (key, label, unit) per band, in display order. Keys as the basin response names them.
_BASIN_ITEMS = {
    "A": [("elev_point", "Elevation at the point", "m"), ("elev_range", "Basin elevation range", "m"),
          ("slope_avg", "Mean slope", "°"), ("lith_class", "Lithology", None), ("karst", "Karst", "% of basin")],
    "B": [("runoff", "Annual runoff", "mm/yr"), ("discharge_yr", "Mean discharge", "m³/s"),
          ("gw_table_depth", "Groundwater table depth", "cm"),
          ("pnv_majority", "Potential natural vegetation", None), ("soil", "Soil texture", None),
          ("wetland_class", "Wetland class", None)],
    "C": [("biome", "Biome", None), ("ecoregion", "Ecoregion", None), ("temp_yr", "Mean annual temperature", "°C"),
          ("precip_yr", "Annual precipitation", "mm/yr"),
          ("aridity", "Aridity index", "P/PET × 100; 100 means precipitation equals evapotranspiration, higher is wetter"),
          ("freshwater_ecoregion_name", "Freshwater ecoregion", None)],
    "D": [("pop_density", "Population density", "people/km²"), ("human_footprint_09", "Human footprint, 2009", "index"),
          ("cropland_extent", "Cropland", "% of basin"), ("pasture_extent", "Pasture", "% of basin")],
    "E": [("dist_sink", "Flow distance to the outlet", "km"), ("outlet", "Outlet", None)],
}

# Area: (variable, fallback label) per band. Labels prefer the response's friendly_name.
_AREA_VARS = {
    "A": [("elev_min", "Elevation minimum"), ("elev_max", "Elevation maximum"), ("slope_avg", "Mean slope"),
          ("lith_class", "Lithology"), ("karst", "Karst area")],
    "B": [("runoff", "Annual runoff"), ("discharge_yr", "Mean discharge"), ("gw_table_depth", "Groundwater table depth"),
          ("pnv_majority", "Potential natural vegetation"), ("wetland_class", "Wetland class")],
    "C": [("biome", "Biome"), ("eco_id", "Ecoregion"), ("zone_name", "Bioclimate zone"),
          ("land_cover_name", "Land cover"), ("temp_yr", "Mean annual temperature"),
          ("precip_yr", "Annual precipitation"), ("aridity", "Aridity index"),
          ("freshwater_ecoregion_name", "Freshwater ecoregion")],
    "D": [("pop_density", "Population density"), ("human_footprint_09", "Human footprint, 2009"),
          ("cropland_extent", "Cropland"), ("pasture_extent", "Pasture")],
    "E": [("dist_sink", "Flow distance to the outlet"), ("outlet_type", "Outlet"),
          ("coast_fraction", "Coastal share")],
}
_AREA_T_VARS = [("lmr_pdsi", "Drought index (PDSI) anomaly"), ("lmr_air", "Temperature anomaly"),
                ("lmr_prate", "Precipitation anomaly"), ("hyde_cropland", "Cropland (HYDE)"),
                ("hyde_grazing", "Grazing land (HYDE)")]


def _num(v):
    """A number rounded for reading: no false precision, never a bool."""
    if isinstance(v, bool) or not isinstance(v, (int, float)) or (isinstance(v, float) and not math.isfinite(v)):
        return v
    a = abs(v)
    if a >= 100:
        return int(round(v))
    if a >= 10:
        return round(v, 1)
    return round(v, 2)


def _item(label, value, unit=None, note=None):
    return {"label": label, "value": value, "unit": unit, "note": note}


def _basin_values(raw: dict) -> dict:
    vals = {}
    for band in (raw.get("signature_bands") or {}).values():
        if isinstance(band, dict):
            for it in band.get("items") or []:
                if isinstance(it, dict) and "key" in it:
                    vals.setdefault(it["key"], it.get("value"))
    return vals


def _basin_band_items(band: str, v: dict) -> list:
    out = []
    for key, label, unit in _BASIN_ITEMS[band]:
        if key == "elev_range":
            lo, hi = v.get("elev_min"), v.get("elev_max")
            if lo is not None and hi is not None:
                out.append(_item(label, f"{_num(lo)} to {_num(hi)}", unit))
            continue
        if key == "soil":
            parts = [(n, v.get(f"pct_{n}")) for n in ("clay", "silt", "sand")]
            if all(p is not None for _, p in parts):
                out.append(_item(label, " · ".join(f"{n} {_num(p)}%" for n, p in parts)))
            continue
        if key == "outlet":
            endo, coast = v.get("endorheic"), v.get("coast_flag")
            if endo is None and coast is None:
                continue
            if endo:
                txt = "inland (endorheic): no outflow to the sea"
            else:
                txt = "drains to the sea" + (" and the basin touches the coast" if coast else "")
            out.append(_item(label, txt))
            continue
        val = v.get(key)
        if val is None or val == "":
            continue
        note = None
        if key == "temp_yr" and v.get("temp_min") is not None and v.get("temp_max") is not None:
            note = f"basin minimum {_num(v['temp_min'])} °C, maximum {_num(v['temp_max'])} °C"
        out.append(_item(label, _num(val), unit, note))
    return out


def _temporal_basin(t: dict) -> Optional[dict]:
    if not isinstance(t, dict):
        return None
    if t.get("_status") != "ok":
        return {"status": t.get("_status") or "error", "items": [], "notes": []}
    items = []
    if t.get("lmr_status") == "available":
        if t.get("pdsi_mean") is not None:
            items.append(_item("Drought index (PDSI), mean", _num(t["pdsi_mean"]), "anomaly; below 0 is drier",
                               f"range {_num(t.get('pdsi_min'))} to {_num(t.get('pdsi_max'))}"
                               if t.get("pdsi_min") is not None else None))
        if t.get("air_mean_anom_k") is not None:
            items.append(_item("Temperature anomaly, mean", _num(t["air_mean_anom_k"]), "K"))
        if t.get("prate_mean_anom_mm_day") is not None:
            items.append(_item("Precipitation anomaly, mean", _num(t["prate_mean_anom_mm_day"]), "mm/day"))
    else:
        items.append(_item("Reconstructed climate (LMR)", f"not available ({t.get('lmr_status') or 'unknown'})"))
    if t.get("volcanic_event_count") is not None:
        events = [e for e in (t.get("volcanic_events") or []) if isinstance(e, dict)]
        big = max(events, key=lambda e: e.get("vssi_tg") or 0) if events else None
        note = None
        if big:
            where = big.get("location") or "location unknown"
            note = f"largest: {big.get('year_ad')} CE, {where}, {_num(big.get('vssi_tg'))} Tg sulphate"
        items.append(_item("Major volcanic events in the period", t["volcanic_event_count"], None, note))
    hyde = t.get("hyde_land_use") if isinstance(t.get("hyde_land_use"), dict) else {}
    epochs = [e for e in (hyde.get("epochs") or []) if isinstance(e, dict) and e.get("cropland_km2") is not None]
    if epochs:
        first, last = epochs[0], epochs[-1]
        val = f"{_num(first['cropland_km2'])} km² in {first.get('year_ce')}"
        if last is not first:
            val += f", {_num(last['cropland_km2'])} km² in {last.get('year_ce')}"
        items.append(_item("Cropland in the basin (HYDE)", val))
    notes = [t[k] for k in ("lmr_out_of_range_note", "lmr_fidelity_note", "lmr_proxy_bias_note",
                            "volcanic_events_note") if t.get(k)]
    if hyde.get("_note"):
        notes.append(hyde["_note"])
    return {"status": "ok", "items": items, "notes": notes,
            "years": [t.get("year_start"), t.get("year_end")]}


def summarise_basin(raw: dict) -> dict:
    v = _basin_values(raw)
    headline = [_item(it.get("label"), _num(it.get("value")))
                for it in (raw.get("profile_summary") or [])
                if isinstance(it, dict) and it.get("key") in ("ecoregion", "zone_name", "land_cover_name")
                and it.get("value") not in (None, "")]
    bands = []
    for b in "ABCDE":
        items = _basin_band_items(b, v)
        if items:
            bands.append({"band": b, "label": BAND_INFO[b][0], "description": BAND_INFO[b][1], "items": items})
    t = _temporal_basin((raw.get("signature_bands") or {}).get("T"))
    meta = raw.get("meta") or {}
    sc = meta.get("scope") or {}
    return {"scope": "basin", "headline": headline, "bands": bands, "temporal": t,
            "unit": {"type": "basin", "level": sc.get("basin_level"), "basin_id": raw.get("id"),
                     "upstream_area_km2": _num(raw.get("up_area"))},
            "data_sources": meta.get("data_sources") or {}, "caveats": [],
            "signature_version": meta.get("signature_version"), "generated": meta.get("generated")}


def _area_item(var: dict, fallback_label: str):
    if not isinstance(var, dict) or var.get("status") not in (None, "ok") or var.get("score_suppressed"):
        return None
    # Our labels, not the response's friendly_name ("Wetland class ID" for a class name),
    # so basin and area pages read alike.
    label = fallback_label or var.get("friendly_name") or var.get("variable")
    method = var.get("method")
    coh = var.get("coherence")
    cnote = {"concentrated": "similar across the basins", "spread": "varies across the basins",
             "split": "splits into distinct groups", "mixed": "mixed across the basins"}.get(coh, coh)
    if method == "class_mixture":
        if var.get("representative_raw") in (None, ""):
            return None
        return _item(label, var["representative_raw"], None, f"dominant class; {cnote}" if cnote else "dominant class")
    if method == "dominant_basin":
        if var.get("representative_raw") is None:
            return None
        return _item(label, _num(var["representative_raw"]), var.get("units"), "value for the dominant basin")
    if method == "flag_fraction":
        if var.get("representative_raw") is None:
            return None
        return _item(label, _num(100 * var["representative_raw"]), "% of the area")
    if var.get("representative_score") is None:
        return None
    return _item(label, _num(var["representative_score"]), "percentile score", cnote)


def summarise_area(raw: dict) -> dict:
    by_var = {v.get("variable"): v for v in (raw.get("variables") or []) if isinstance(v, dict)}
    headline = []
    for key, lbl in (("biome", "Biome"), ("eco_id", "Ecoregion"), ("zone_name", "Bioclimate zone"),
                     ("land_cover_name", "Land cover")):
        it = _area_item(by_var.get(key), lbl)
        if it:
            headline.append(_item(it["label"], it["value"]))
    bands = []
    for b in "ABCDE":
        items = [i for i in (_area_item(by_var.get(k), lbl) for k, lbl in _AREA_VARS[b]) if i]
        if items:
            bands.append({"band": b, "label": BAND_INFO[b][0], "description": BAND_INFO[b][1], "items": items})
    temporal = None
    t_vars = [by_var.get(k) for k, _ in _AREA_T_VARS if by_var.get(k)]
    if t_vars:
        items = []
        for (k, lbl) in _AREA_T_VARS:
            var = by_var.get(k)
            if not var or var.get("representative_raw") is None:
                continue
            yr = var.get("epoch_year") or var.get("year")
            items.append(_item(lbl, _num(var["representative_raw"]), var.get("units"),
                               f"year {yr}" if yr is not None else None))
        year = next((v.get("year") for v in t_vars if v.get("year") is not None), None)
        temporal = {"status": "ok", "items": items, "notes": [], "years": [year, year]}
    caveats = [str(c) for c in (raw.get("caveats") or {}).values()] if isinstance(raw.get("caveats"), dict) else []
    meta = raw.get("meta") or {}
    sc = meta.get("scope") or {}
    shortfall = raw.get("shortfall")
    return {"scope": "area", "headline": headline, "bands": bands, "temporal": temporal,
            "unit": {"type": "area", "level": (meta.get("query") or {}).get("level", LEVEL),
                     "n_basins": sc.get("n_units"),
                     "uncovered_pct": _num(100 * shortfall) if isinstance(shortfall, (int, float)) else None},
            "data_sources": meta.get("data_sources") or {}, "caveats": caveats,
            "signature_version": meta.get("signature_version"), "generated": meta.get("generated")}


def summarise(result: dict) -> Optional[dict]:
    """The readable summary of an ``ok`` result, else None."""
    if result.get("status") != STATUS_OK or not isinstance(result.get("raw"), dict):
        return None
    return summarise_area(result["raw"]) if result.get("scope") == "area" else summarise_basin(result["raw"])


def public_result(result: dict, members=None) -> dict:
    """What an endpoint may send: status, scope, the request's non-geometry parameters,
    the summary, never the raw payload."""
    req = {k: v for k, v in (result.get("request") or {}).items() if k != "geom_wkt"}
    if (result.get("request") or {}).get("geom_wkt"):
        req["geom_wkt_vertices"] = (result["request"]["geom_wkt"].count(",") + 1)
    out = {"status": result.get("status"), "scope": result.get("scope"), "reason": result.get("reason"),
           "request": req, "cached": bool(result.get("cached")), "summary": summarise(result)}
    if members is not None:
        out["members"] = members
    return out


# ---------------------------------------------------------------------------
# Geometry: spans, tightness, hulls
# ---------------------------------------------------------------------------

def haversine_km(a, b) -> float:
    (lon1, lat1), (lon2, lat2) = a, b
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    h = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * 6371.0088 * math.asin(min(1.0, math.sqrt(h)))


def valid_point(p) -> bool:
    return (isinstance(p, (list, tuple)) and len(p) >= 2
            and all(isinstance(c, (int, float)) and not isinstance(c, bool) and math.isfinite(c) for c in p[:2])
            and -180 <= p[0] <= 180 and -90 <= p[1] <= 90)


def span_km(points: Iterable) -> float:
    """The diameter of a set of [lon, lat] points: the largest great-circle distance
    between any two. 0 for fewer than two."""
    pts = [tuple(p[:2]) for p in points if valid_point(p)]
    best = 0.0
    for i in range(len(pts)):
        for j in range(i + 1, len(pts)):
            best = max(best, haversine_km(pts[i], pts[j]))
    return best


def is_tight(points: Iterable, limit_km: float = LOCUS_TIGHT_KM) -> bool:
    """True when every member lies within ``limit_km`` of every other (see LOCUS_TIGHT_KM)."""
    return span_km(points) <= limit_km


def _vertices(g) -> list:
    """Every [lon, lat] vertex of a GEOS geometry (the hull of a set of geometries is
    the hull of their vertices)."""
    out = []

    def walk(c):
        if c and isinstance(c[0], (int, float)):
            out.append((float(c[0]), float(c[1])))
        else:
            for x in c:
                walk(x)
    walk(json.loads(g.json).get("coordinates") or [])
    return out


def crosses_antimeridian(lons) -> bool:
    """True when the points are closer together across 180 degrees than across 0: their
    longitude range exceeds 180 in [-180, 180] and is smaller once negative longitudes
    are shifted by +360. (A cluster whose great-circle span is tight, but which
    straddles the antimeridian, would otherwise get a hull around the rest of the world.)"""
    lons = list(lons)
    if not lons or max(lons) - min(lons) <= 180:
        return False
    shifted = [x + 360 if x < 0 else x for x in lons]
    return max(shifted) - min(shifted) < max(lons) - min(lons)


def _simplified_hull(hull):
    if hull.geom_type != "Polygon":
        hull = hull.buffer(DEGENERATE_HULL_BUFFER_DEG).convex_hull
    tol = 0.0005
    while hull.num_coords > MAX_HULL_VERTICES + 1 and tol < 5:
        simpler = hull.simplify(tol, preserve_topology=True)
        if simpler.geom_type == "Polygon" and not simpler.empty:
            hull = simpler
        tol *= 2
    return hull


def hull_wkt(geoms) -> Optional[str]:
    """WKT of the convex hull of ``geoms`` (GEOSGeometry, SRID 4326), buffered if
    degenerate and simplified to at most MAX_HULL_VERTICES vertices; None if empty.

    POLYGON normally. Across the antimeridian (``crosses_antimeridian``) the hull is
    taken with negative longitudes shifted by +360, then cut at 180 and the eastern
    part shifted back, giving a MULTIPOLYGON whose parts each stay in [-180, 180]:
    EDOPS takes lon only in [-180, 180] and accepts POLYGON or MULTIPOLYGON (openapi
    v0.4, scripts/fixtures/edop/openapi_v0.4.json), so a 0-360 frame is not sent."""
    from django.contrib.gis.geos import GeometryCollection, MultiPoint, Point, Polygon
    gs = [g for g in geoms if g is not None and not g.empty]
    if not gs:
        return None
    fmt = f"{{:.{WKT_DECIMALS}f}} {{:.{WKT_DECIMALS}f}}"

    def ring_text(poly):
        return "((" + ",".join(fmt.format(x, y) for x, y in poly.exterior_ring.coords) + "))"

    verts = [v for g in gs for v in _vertices(g)]
    if verts and crosses_antimeridian(x for x, _y in verts):
        pts = [Point(x + 360 if x < 0 else x, y, srid=4326) for x, y in verts]
        hull = _simplified_hull(MultiPoint(*pts, srid=4326).convex_hull)
        (x0, y0, x1, y1) = hull.extent
        west = hull.intersection(Polygon.from_bbox((x0 - 1, y0 - 1, 180.0, y1 + 1)))
        east = hull.intersection(Polygon.from_bbox((180.0, y0 - 1, x1 + 1, y1 + 1)))
        parts = []
        for part, shift in ((west, 0.0), (east, -360.0)):
            for poly in (part if part.geom_type == "MultiPolygon" else [part]):
                if poly.geom_type == "Polygon" and not poly.empty and poly.area > 0:
                    moved = Polygon([(x + shift, y) for x, y in poly.exterior_ring.coords], srid=4326)
                    parts.append(moved)
        if len(parts) == 1:
            return "POLYGON" + ring_text(parts[0])
        if parts:
            return "MULTIPOLYGON(" + ",".join(ring_text(p) for p in parts) + ")"
    hull = _simplified_hull(GeometryCollection(*[g.clone() for g in gs], srid=4326).convex_hull)
    return "POLYGON" + ring_text(hull)


# ---------------------------------------------------------------------------
# Plans: which scope for which group
# ---------------------------------------------------------------------------

def _years_block(span):
    years, note = band_t_years(span)
    return years, {"span": list(span) if span else None, "used": list(years) if years else None, "note": note}


def _drop_withheld(members: list):
    """``(members that may be sent, keys of those marked offsite: False)``."""
    keep, withheld = [], []
    for m in members:
        if m.get("offsite") is False:
            withheld.append(m.get("key"))
        else:
            keep.append(m)
    return keep, withheld


def plan_concordance(members: list, span=None) -> dict:
    """The scope for one concordance aggregate. ``members``: dicts with ``repr_point``
    and optional ``geometry`` (GEOSGeometry). ``span``: the group's [start, end] years.

    * no member has a location -> ``no_location`` (nothing asked);
    * one location (one member, or all within SAME_POINT_KM) -> ``basin`` at that point;
    * several -> ``area`` over the hull of their geometries, unless they span more than
      CONCORDANCE_MAX_SPAN_KM -> ``dispersed`` (nothing asked).
    Band T: basin takes the group's years; area only a single year (the spec), so a
    range is reported as not requested on area, with the reason.

    A member marked ``offsite: False`` (``places.record_group.mark_offsite``: not
    visible to an anonymous caller) is never part of a request; its key is listed in
    ``withheld``. The caller should not pass it at all; this is the second guard."""
    members, withheld = _drop_withheld(members)
    located = [m for m in members if valid_point(m.get("repr_point"))]
    years, yblock = _years_block(span)
    if not located:
        return {"kind": "no_location", "requests": [], "years": yblock, "withheld": withheld}
    pts = [m["repr_point"] for m in located]
    spread = span_km(pts)
    if len(located) == 1 or spread <= SAME_POINT_KM:
        lon, lat = pts[0][:2]
        return {"kind": "basin", "span_km": round(spread, 3), "n_located": len(located),
                "requests": [basin_request(lon, lat, years)], "years": yblock, "withheld": withheld}
    if spread > CONCORDANCE_MAX_SPAN_KM:
        return {"kind": "dispersed", "span_km": round(spread, 1), "limit_km": CONCORDANCE_MAX_SPAN_KM,
                "n_located": len(located), "requests": [], "years": yblock, "withheld": withheld}
    from django.contrib.gis.geos import Point
    geoms = [m.get("geometry") or Point(*m["repr_point"][:2], srid=4326) for m in located]
    wkt = hull_wkt(geoms)
    single = years[0] if years and years[0] == years[1] else None
    if years and single is None:
        yblock = {**yblock, "used": None, "note": "area_needs_single_year"}
    return {"kind": "area", "span_km": round(spread, 1), "n_located": len(located),
            "requests": [area_request(wkt, single)], "years": yblock, "withheld": withheld}


def plan_locus(members: list) -> dict:
    """The scope for a locus. ``members``: dicts with ``key``, ``repr_point`` and optional
    ``years`` ([start, end]).

    * no member has a location -> ``no_location``;
    * all within LOCUS_TIGHT_KM of each other -> ONE ``area`` over their hull (or ``basin``
      when they are all at one point), Band T only for a single shared year;
    * otherwise -> per-member ``basin`` (each with its own years), for at most
      LOCUS_MAX_MEMBER_SIGNATURES members; the rest are listed as not looked up.

    Members marked ``offsite: False`` are withheld, as in ``plan_concordance``."""
    members, withheld = _drop_withheld(members)
    located = [m for m in members if valid_point(m.get("repr_point"))]
    if not located:
        return {"kind": "no_location", "requests": [], "tight_km": LOCUS_TIGHT_KM, "withheld": withheld}
    pts = [m["repr_point"] for m in located]
    spread = span_km(pts)
    # ``years`` is None or [start, end] with both ints (the caller normalises).
    spans = [m["years"] for m in located if m.get("years")]
    union = [min(s[0] for s in spans), max(s[1] for s in spans)] if spans else None
    base = {"span_km": round(spread, 1), "tight_km": LOCUS_TIGHT_KM, "n_located": len(located),
            "n_members": len(members), "max_member_signatures": LOCUS_MAX_MEMBER_SIGNATURES,
            "withheld": withheld}
    if len(located) == 1 or spread <= SAME_POINT_KM:
        years, yblock = _years_block(union)
        lon, lat = pts[0][:2]
        return {**base, "kind": "basin", "requests": [basin_request(lon, lat, years)],
                "groups": [[m["key"] for m in located]], "years": yblock}
    if spread <= LOCUS_TIGHT_KM:
        from django.contrib.gis.geos import Point
        years, yblock = _years_block(union)
        single = years[0] if years and years[0] == years[1] else None
        if years and single is None:
            yblock = {**yblock, "used": None, "note": "area_needs_single_year"}
        wkt = hull_wkt([Point(*p[:2], srid=4326) for p in pts])
        return {**base, "kind": "area", "requests": [area_request(wkt, single)],
                "groups": [[m["key"] for m in located]], "years": yblock}
    asked = located[:LOCUS_MAX_MEMBER_SIGNATURES]
    reqs, groups, yblocks = [], [], []
    for m in asked:
        years, yblock = _years_block(m.get("years"))
        reqs.append(basin_request(m["repr_point"][0], m["repr_point"][1], years))
        groups.append([m["key"]])
        yblocks.append(yblock)
    return {**base, "kind": "per_member", "requests": reqs, "groups": groups, "years_each": yblocks,
            "not_looked_up": [m["key"] for m in located[LOCUS_MAX_MEMBER_SIGNATURES:]],
            "no_location": [m["key"] for m in members if not valid_point(m.get("repr_point"))]}


def run_requests(requests_: list, *, transport: Optional[Transport] = None, workers: int = LOCUS_WORKERS) -> list:
    """Fetch several signatures in parallel (no retries: user-facing)."""
    if not requests_:
        return []
    if len(requests_) == 1:
        return [fetch_signature(requests_[0], transport=transport)]
    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(requests_)))) as pool:
        return list(pool.map(lambda p: fetch_signature(p, transport=transport), requests_))


ATTRIBUTION = {
    "service": "EDOPS (Environmental Dimensions of Place Service)",
    "provider": "Computing Place (Karl Grossner)",
    "url": HOME_URL,
    "docs_url": DOCS_URL,
    "codebook_url": CODEBOOK_URL,
    "note": "A research prototype. Values are EDOPS's, unchanged except for rounding; WHG chose which to show.",
}
