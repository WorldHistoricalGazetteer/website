// recon-calendars.js
// Global / non-Western calendar recognition + conversion to proleptic-Gregorian ISO intervals,
// for the Gazetteer Workbench date parser. Uses the MIT Temporal polyfill for the built-in calendars
// (Islamic, Hebrew, Indian/Saka, Persian, Coptic, Ethiopic, Japanese, ROC, Buddhist) and inline
// arithmetic for those Temporal lacks (French Republican, Byzantine Anno Mundi, Seleucid, Ab urbe
// condita, Olympiads). Recognition is MARKER-GATED — a bare number stays Gregorian; only explicit
// era markers (AH, BE, an VIII, Śaka, 民國, …) trigger a non-Gregorian reading. Research-sourced;
// systems that need ephemeris tables (full Chinese/Hindu lunisolar day-level) return a year span.
//
// Every result is an interval { startISO, endISO, system, gran } — honest about the fact that a
// non-Gregorian "year" usually straddles two Gregorian years.

import { Temporal } from '@js-temporal/polyfill';

function fmt(pd) {
  const g = pd.withCalendar('iso8601');
  return `${g.year < 0 ? '-' : ''}${String(Math.abs(g.year)).padStart(4, '0')}-${String(g.month).padStart(2, '0')}-${String(g.day).padStart(2, '0')}`;
}
// Gregorian span of one calendar year (calendar's month 1 day 1 → day before next year's).
function temporalYearSpan(calendar, year, era) {
  const base = era ? { era, eraYear: year, calendar } : { year, calendar };
  const s = Temporal.PlainDate.from({ ...base, month: 1, day: 1 });
  const nextBase = era ? { era, eraYear: year + 1, calendar } : { year: year + 1, calendar };
  const e = Temporal.PlainDate.from({ ...nextBase, month: 1, day: 1 }).subtract({ days: 1 });
  return { startISO: fmt(s), endISO: fmt(e) };
}
function temporalDate(calendar, year, month, day, era) {
  const base = era ? { era, eraYear: year, calendar } : { year, calendar };
  const p = Temporal.PlainDate.from({ ...base, month, day });
  const i = fmt(p);
  return { startISO: i, endISO: i };
}

// A signed-year Gregorian span from a plain year number (for arithmetic eras returning a year).
function gregYearSpan(y) {
  const p = (n) => `${n < 0 ? '-' : ''}${String(Math.abs(n)).padStart(4, '0')}`;
  return { startISO: `${p(y)}-01-01`, endISO: `${p(y)}-12-31` };
}

// ── French Republican (inline; Temporal has no such calendar) ────────────────
const FR_MONTHS = ['vendemiaire', 'brumaire', 'frimaire', 'nivose', 'pluviose', 'ventose',
  'germinal', 'floreal', 'prairial', 'messidor', 'thermidor', 'fructidor'];
const FR_EPOCH_JDN = 2375840; // 1 Vendémiaire an I = 22 Sep 1792 (Gregorian)
const frLeap = (y) => y % 4 === 3;               // sextile years an III, VII, XI (historical window)
const frDaysInYear = (y) => 365 + (frLeap(y) ? 1 : 0);
function frDaysBefore(y) { let d = 0; for (let k = 1; k < y; k++) d += frDaysInYear(k); return d; }
function jdnToGregISO(j) {
  let a = j + 32044, b = Math.floor((4 * a + 3) / 146097), c = a - Math.floor(146097 * b / 4);
  let d = Math.floor((4 * c + 3) / 1461), e = c - Math.floor(1461 * d / 4), m = Math.floor((5 * e + 2) / 153);
  const day = e - Math.floor((153 * m + 2) / 5) + 1, month = m + 3 - 12 * Math.floor(m / 10), year = 100 * b + d - 4800 + Math.floor(m / 10);
  return `${String(year).padStart(4, '0')}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
}
function frToISO(year, monthIdx, day) { // monthIdx 0-11, or null for a whole year
  if (monthIdx == null) {
    const s = FR_EPOCH_JDN + frDaysBefore(year);
    return { startISO: jdnToGregISO(s), endISO: jdnToGregISO(s + frDaysInYear(year) - 1) };
  }
  const j = FR_EPOCH_JDN + frDaysBefore(year) + monthIdx * 30 + (day - 1);
  const i = jdnToGregISO(j);
  return { startISO: i, endISO: i };
}

const roman = (s) => {
  const M = { i: 1, v: 5, x: 10, l: 50, c: 100, d: 500, m: 1000 }; s = s.toLowerCase();
  if (!/^[ivxlcdm]+$/.test(s)) return null;
  let t = 0; for (let i = 0; i < s.length; i++) { const a = M[s[i]], b = M[s[i + 1]]; t += (b && a < b) ? -a : a; } return t;
};
const numOrRoman = (s) => (/^\d+$/.test(s) ? parseInt(s, 10) : roman(s));

// Japanese nengō era → Temporal era id (only the modern eras Temporal knows reliably).
const NENGO = { meiji: 'meiji', taisho: 'taisho', taishō: 'taisho', showa: 'showa', shōwa: 'showa', heisei: 'heisei', reiwa: 'reiwa' };

// ── Recognisers (marker-gated). Each returns {startISO,endISO,system,gran} | null. ──
function tryCalendar(raw) {
  const s = raw.trim().replace(/\s+/g, ' ');
  const wrap = (system, gran, r) => (r ? { ...r, system, gran } : null);
  const safe = (fn) => { try { return fn(); } catch (_) { return null; } };
  let m;

  // Islamic / Hijri — require "AH"/"A.H." (avoid bare "H"; collides with regnal "8 H 6").
  if ((m = s.match(/^(\d{1,4})(?:-(\d{1,2})(?:-(\d{1,2}))?)?\s*(?:AH|A\.H\.)$/i)) || (m = s.match(/^A\.?H\.?\s*(\d{1,4})$/i))) {
    const y = parseInt(m[1], 10), mo = m[2] ? +m[2] : null, d = m[3] ? +m[3] : null;
    return safe(() => wrap('Islamic (Hijri, tabular)', mo ? 'day' : 'year',
      mo ? temporalDate('islamic-civil', y, mo, d || 1) : temporalYearSpan('islamic-civil', y)));
  }
  // Thai Buddhist Era
  if ((m = s.match(/^(\d{3,4})\s*(?:BE|B\.E\.|พ\.ศ\.)$/i)) || (m = s.match(/^(?:BE|พ\.ศ\.)\s*(\d{3,4})$/i)))
    return safe(() => wrap('Thai Buddhist Era', 'year', temporalYearSpan('buddhist', parseInt(m[1], 10))));
  // Indian national / Śaka
  if ((m = s.match(/^(?:s[h]?aka(?:\s+samvat)?|śaka)\s+(\d{3,4})$/i)) || (m = s.match(/^(\d{3,4})\s+(?:s[h]?aka|śaka)$/i)))
    return safe(() => wrap('Śaka (Indian national)', 'year', temporalYearSpan('indian', parseInt(m[1], 10))));
  // Persian / Solar Hijri
  if ((m = s.match(/^(\d{3,4})\s*(?:SH|A\.?P\.?|HS)$/i)) || (m = s.match(/^(?:persian|jalali)\s+(\d{3,4})$/i)))
    return safe(() => wrap('Persian (Solar Hijri)', 'year', temporalYearSpan('persian', parseInt(m[1], 10))));
  // Republic of China (Minguo)
  if ((m = s.match(/^(?:民國|ROC|Minguo|R\.O\.C\.)\s*(\d{1,3})(?:年)?$/i)))
    return safe(() => wrap('Republic of China (Minguo)', 'year', temporalYearSpan('roc', parseInt(m[1], 10))));
  // Coptic (Era of Martyrs) — require explicit marker
  if ((m = s.match(/^(?:coptic|A\.?M\.?\s*martyr\w*|anno martyrum)\s+(\d{3,4})$/i)) || (m = s.match(/^(\d{3,4})\s+(?:coptic|A\.?M\.?\s*martyr\w*|anno martyrum)$/i)))
    return safe(() => wrap('Coptic (Anno Martyrum)', 'year', temporalYearSpan('coptic', parseInt(m[1], 10))));
  // Ethiopian — require explicit marker
  if ((m = s.match(/^(?:ethiopian|ethiopic|ዓ\.?ም)\s+(\d{3,4})$/i)) || (m = s.match(/^(\d{3,4})\s+(?:ethiopian|ethiopic|ዓ\.?ም)$/i)))
    // Ethiopian (Amete Mihret) via Coptic: same structure, offset 276 years (Coptic epoch 284 CE,
    // Ethiopian 8 CE). This polyfill's 'ethiopic' only exposes Amete Alem, so route through Coptic.
    return safe(() => wrap('Ethiopian', 'year', temporalYearSpan('coptic', parseInt(m[1], 10) - 276)));
  // Hebrew / Byzantine Anno Mundi — disambiguate by magnitude; "Anno Mundi" or "AM".
  if ((m = s.match(/^(?:anno mundi|A\.?M\.?)\s+(\d{3,4})$/i)) || (m = s.match(/^(\d{3,4})\s+(?:anno mundi|A\.?M\.?)$/i))) {
    const y = parseInt(m[1], 10);
    if (y >= 5000 && y <= 6200) return safe(() => wrap('Hebrew (Anno Mundi)', 'year', temporalYearSpan('hebrew', y)));
    if (y >= 6600 && y <= 7600) return { ...byzantineAM(y), system: 'Byzantine (Anno Mundi)', gran: 'year' };
    return null; // ambiguous magnitude → leave to Gregorian
  }
  // Japanese nengō, e.g. "Meiji 45", "昭和 20"
  if ((m = s.match(/^([A-Za-zŌōŌ一-鿿]+)\s*(\d{1,2})(?:年)?$/))) {
    const era = NENGO[m[1].toLowerCase()];
    if (era) return safe(() => wrap('Japanese (nengō)', 'year', temporalYearSpan('japanese', parseInt(m[2], 10), era)));
  }
  // French Republican, e.g. "18 Brumaire an VIII", "an II"
  if ((m = s.match(/^(?:(\d{1,2})(?:er|e)?\s+([a-zéèôû]+)\s+)?an\s+([ivxlcdm]+|\d{1,2})$/i))) {
    const y = numOrRoman(m[3]); if (!y) return null;
    if (!m[2]) return { ...frToISO(y, null, null), system: 'French Republican', gran: 'year' };
    const mi = FR_MONTHS.indexOf(m[2].toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, ''));
    if (mi < 0) return null;
    return { ...frToISO(y, mi, parseInt(m[1], 10)), system: 'French Republican', gran: 'day' };
  }
  // Seleucid (Anno Graecorum)
  if ((m = s.match(/^(?:seleucid|anno graecorum|A\.?G\.?)\s+(\d{1,4})$/i)) || (m = s.match(/^(\d{1,4})\s+(?:seleucid|A\.?G\.?)$/i)))
    return { ...gregYearSpan(parseInt(m[1], 10) - 312), system: 'Seleucid (approx.)', gran: 'year' };
  // Ab urbe condita
  if ((m = s.match(/^(?:a\.?u\.?c\.?)\s*(\d{1,4})$/i)) || (m = s.match(/^(\d{1,4})\s+a\.?u\.?c\.?$/i)))
    return { ...gregYearSpan(parseInt(m[1], 10) - 753), system: 'Ab urbe condita', gran: 'year' };
  // Olympiad, e.g. "Ol. 175.2"
  if ((m = s.match(/^ol(?:\.|ymp\w*)?\s*(\d{1,3})(?:\.(\d))?$/i))) {
    const ol = parseInt(m[1], 10), yr = m[2] ? parseInt(m[2], 10) : 1;
    const bce = 776 - (ol - 1) * 4 - (yr - 1);
    return { ...gregYearSpan(-bce + 1), system: 'Olympiad (approx.)', gran: 'year' };
  }
  // Vikram Samvat (no Temporal calendar; year-level ≈ −57)
  if ((m = s.match(/^(?:vikram\s+samvat|bikram\s+sambat|v\.?s\.?|samvat|संवत्?)\s+(\d{3,4})$/i)) || (m = s.match(/^(\d{3,4})\s+(?:vikram\s+samvat|v\.?s\.?|samvat)$/i)))
    return { ...gregYearSpan(parseInt(m[1], 10) - 57), system: 'Vikram Samvat (approx.)', gran: 'year' };

  return null;
}

function byzantineAM(y) { // year start 1 Sept; AD = AM − 5508 (Sep–Dec) .. AM − 5509 start
  return gregYearSpan(y - 5508);
}

export function parseGlobalCalendar(input) {
  if (input == null || String(input).trim() === '') return null;
  return tryCalendar(String(input));
}
