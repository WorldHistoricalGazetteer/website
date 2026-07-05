// recon-coords.js
// Coordinate-format detection + conversion to WGS84 for the Gazetteer Workbench.
// Inspired by Locolligo (proj4 + geodesy grid-ref parsing): detect the likely format of a
// coordinate column across a wide range, show what was detected, and let the user override when
// ambiguous. Everything runs in the browser. See developer/plan-gazetteerWorkbench.prompt.md §1d.

import proj4 from 'proj4';

// Projected CRS defs (Helmert/towgs84 → ~few-metre accuracy, ample for reconciliation hints).
proj4.defs('EPSG:27700', // OSGB36 / British National Grid
  '+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.9996012717 +x_0=400000 +y_0=-100000 +ellps=airy ' +
  '+towgs84=446.448,-125.157,542.06,0.15,0.247,0.842,-20.489 +units=m +no_defs');
proj4.defs('EPSG:29903', // TM65 / Irish Grid
  '+proj=tmerc +lat_0=53.5 +lon_0=-8 +k=1.000035 +x_0=200000 +y_0=250000 +ellps=mod_airy ' +
  '+towgs84=482.5,-130.6,564.6,-1.042,-0.214,-0.631,8.15 +units=m +no_defs');

// Formats offered in the override dropdown (id → human label).
export const COORD_FORMATS = [
  ['latlon', 'Decimal degrees (lat, lon)'],
  ['lonlat', 'Decimal degrees (lon, lat)'],
  ['dms', 'Degrees-minutes-seconds'],
  ['osgb', 'OS National Grid (GB, e.g. SK690965)'],
  ['irish', 'Irish Grid (e.g. N866278)'],
  ['utm', 'UTM (zone easting northing)'],
  ['wkt', 'WKT POINT(lon lat)'],
  ['none', 'Not coordinates / ignore'],
];

const inLat = (v) => Number.isFinite(v) && v >= -90 && v <= 90;
const inLon = (v) => Number.isFinite(v) && v >= -180 && v <= 180;
function toWgs84(fromEPSG, easting, northing) {
  const [lon, lat] = proj4(fromEPSG, 'WGS84', [easting, northing]);
  return inLat(lat) && inLon(lon) ? { lat, lon } : null;
}

// ── Grid references ─────────────────────────────────────────────────────────
function osgbToEN(ref) {
  const s = String(ref).toUpperCase().replace(/\s+/g, '');
  const m = s.match(/^([A-HJ-Z])([A-HJ-Z])(\d+)$/);
  if (!m || m[3].length % 2 !== 0) return null;
  let l1 = m[1].charCodeAt(0) - 65, l2 = m[2].charCodeAt(0) - 65;
  if (l1 > 7) l1--; if (l2 > 7) l2--;                 // skip 'I'
  const e100 = ((l1 - 2) % 5) * 5 + (l2 % 5);
  const n100 = (19 - Math.floor(l1 / 5) * 5) - Math.floor(l2 / 5);
  if (e100 < 0 || e100 > 6 || n100 < 0 || n100 > 12) return null; // outside GB
  const h = m[3].length / 2;
  const easting = e100 * 100000 + Number(m[3].slice(0, h).padEnd(5, '0'));
  const northing = n100 * 100000 + Number(m[3].slice(h).padEnd(5, '0'));
  return { easting, northing };
}
function irishToEN(ref) {
  const s = String(ref).toUpperCase().replace(/\s+/g, '');
  const m = s.match(/^([A-HJ-Z])(\d+)$/);              // single letter, 'I' excluded
  if (!m || m[2].length % 2 !== 0) return null;
  let l = m[1].charCodeAt(0) - 65;
  if (l > 8) l--;                                      // skip 'I'
  const e100 = l % 5, n100 = 4 - Math.floor(l / 5);
  const h = m[2].length / 2;
  const easting = e100 * 100000 + Number(m[2].slice(0, h).padEnd(5, '0'));
  const northing = n100 * 100000 + Number(m[2].slice(h).padEnd(5, '0'));
  return { easting, northing };
}

// ── Scalar parsers (return {lat,lon} | null) ────────────────────────────────
function twoNumbers(value) {
  const nums = String(value).match(/-?\d+(?:\.\d+)?/g);
  return nums && nums.length >= 2 ? [parseFloat(nums[0]), parseFloat(nums[1])] : null;
}
const P = {
  latlon(v) { const n = twoNumbers(v); if (!n) return null; return inLat(n[0]) && inLon(n[1]) ? { lat: n[0], lon: n[1] } : null; },
  lonlat(v) { const n = twoNumbers(v); if (!n) return null; return inLat(n[1]) && inLon(n[0]) ? { lat: n[1], lon: n[0] } : null; },
  osgb(v) { const en = osgbToEN(v); return en ? toWgs84('EPSG:27700', en.easting, en.northing) : null; },
  irish(v) { const en = irishToEN(v); return en ? toWgs84('EPSG:29903', en.easting, en.northing) : null; },
  wkt(v) {
    const m = String(v).match(/POINT\s*\(\s*(-?\d+(?:\.\d+)?)\s+(-?\d+(?:\.\d+)?)\s*\)/i);
    if (!m) return null;
    const lon = parseFloat(m[1]), lat = parseFloat(m[2]);
    return inLat(lat) && inLon(lon) ? { lat, lon } : null;
  },
  utm(v) {
    const m = String(v).trim().match(/^(\d{1,2})\s*([C-HJ-NP-X])?\s+(\d+(?:\.\d+)?)\s+(\d+(?:\.\d+)?)\s*([NS])?$/i);
    if (!m) return null;
    const zone = parseInt(m[1], 10);
    if (zone < 1 || zone > 60) return null;
    const band = (m[2] || '').toUpperCase();
    const hemi = (m[5] || '').toUpperCase();
    const south = hemi === 'S' || (!hemi && band && band < 'N');
    const def = `+proj=utm +zone=${zone} ${south ? '+south ' : ''}+datum=WGS84 +units=m +no_defs`;
    try { return toWgs84(def, parseFloat(m[3]), parseFloat(m[4])); } catch (_) { return null; }
  },
  dms(v) {
    // Two DMS tokens like 52°21'00"N 0°30'00"W (separators flexible).
    const re = /(\d+(?:\.\d+)?)[°:\s]+(?:(\d+(?:\.\d+)?)['′:\s]+)?(?:(\d+(?:\.\d+)?)["″]?\s*)?([NSEW])/gi;
    const parts = [];
    let m;
    while ((m = re.exec(String(v))) && parts.length < 2) {
      let dd = parseFloat(m[1]) + (m[2] ? parseFloat(m[2]) / 60 : 0) + (m[3] ? parseFloat(m[3]) / 3600 : 0);
      const hemi = m[4].toUpperCase();
      if (hemi === 'S' || hemi === 'W') dd = -dd;
      parts.push({ dd, axis: (hemi === 'N' || hemi === 'S') ? 'lat' : 'lon' });
    }
    if (parts.length < 2) return null;
    const lat = parts.find((p) => p.axis === 'lat'), lon = parts.find((p) => p.axis === 'lon');
    if (!lat || !lon || !inLat(lat.dd) || !inLon(lon.dd)) return null;
    return { lat: lat.dd, lon: lon.dd };
  },
};

// Parse a single value in a known format → {lat, lon} | null.
export function parseCoord(format, value) {
  if (!value || format === 'none' || !P[format]) return null;
  return P[format](value);
}

// Parse a lat/lon pair held in two separate columns (decimal degrees), with a swap option.
export function parseLatLonPair(latVal, lonVal, swapped) {
  const a = parseFloat(latVal), b = parseFloat(lonVal);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return null;
  const lat = swapped ? b : a, lon = swapped ? a : b;
  return inLat(lat) && inLon(lon) ? { lat, lon } : null;
}

// Detect the most likely format for a single coordinate column by trial-parsing samples.
// Returns { format, parsed, total, ranked:[{format,parsed}], ambiguous }.
export function detectCoordFormat(samples) {
  const vals = samples.filter((s) => s != null && String(s).trim() !== '').slice(0, 200);
  const total = vals.length;
  const order = ['osgb', 'irish', 'wkt', 'dms', 'utm', 'latlon', 'lonlat'];
  const ranked = order.map((format) => ({
    format,
    parsed: vals.reduce((n, v) => n + (P[format](v) ? 1 : 0), 0),
  })).sort((a, b) => b.parsed - a.parsed);
  const best = ranked[0];
  // latlon vs lonlat are structurally identical when all points fall within ±90 both ways — flag it.
  const latlon = ranked.find((r) => r.format === 'latlon') || { parsed: 0 };
  const lonlat = ranked.find((r) => r.format === 'lonlat') || { parsed: 0 };
  const ambiguous = best.parsed > 0 &&
    ((best.format === 'latlon' && lonlat.parsed === latlon.parsed) ||
     (best.format === 'lonlat' && latlon.parsed === lonlat.parsed));
  return { format: best.parsed > 0 ? best.format : 'none', parsed: best.parsed, total, ranked, ambiguous };
}
