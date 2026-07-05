// recon-dates.js
// Historical date parser for the Gazetteer Workbench. Normalises a messy date string to ISO
// start/end bounds (granularity-correct) and integer start/end years for the /reconcile temporal
// hint. Format set agreed 2026-07-05 (see developer/plan-gazetteerWorkbench.prompt.md §1d / #111).
//
// Supported: ISO (Y, Y-M, Y-M-D); numeric d/m/y or m/d/y (UK dd/mm default, auto-detect when a part
// is >12, flag when genuinely ambiguous); month names (full/abbrev) with day, month-day-year, and
// month-year; ordinals; year-only; centuries; CE/BCE/AD/BC eras incl. a leading "-" minus
// (space-optional); ranges via "to" / hyphen / en–em dash / "Y/Y" / the ISO delimiter-collision
// case (Y-M-D-Y-M-D); approximate qualifiers (c., circa, trailing ?); open-ended (from/after/
// before/to and a dangling "Y-"); and reversed-range normalisation.
// NOT handled (future): regnal years (8 Henry VI), feast-day / Michaelmas customs-year dating,
// two-digit-year century inference.

const MONTHS = {
  jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, sept: 9, oct: 10, nov: 11, dec: 12,
  january: 1, february: 2, march: 3, april: 4, june: 6, july: 7, august: 8, september: 9, october: 10, november: 11, december: 12,
};
const isLeap = (y) => (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
const daysInMonth = (y, m) => [31, isLeap(y) ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][m - 1];
const pad = (n, w) => String(Math.abs(n)).padStart(w, '0');
function iso(y, m, d) { return `${y < 0 ? '-' : ''}${pad(y, 4)}-${pad(m, 2)}-${pad(d, 2)}`; }
// Sortable numeric key from an ISO string (handles negative/BCE years, where string order is wrong).
function isoNum(s) {
  const neg = s[0] === '-';
  const [y, m, d] = (neg ? s.slice(1) : s).split('-').map(Number);
  const v = y * 10000 + m * 100 + d;
  return neg ? -v : v;
}

// Parse a single (non-range) date token → { y, m, d, gran, ambiguous } | null. y is era-signed.
function parseSingle(raw, opts) {
  let s = String(raw).trim().replace(/\s+/g, ' ');
  if (!s) return null;
  let ambiguous = false;

  // Era: BCE/BC → negative; CE/AD → positive; both space-optional and either side.
  let sign = 1;
  if (/\b(bce|bc)\b/i.test(s) || /\d\s*(bce|bc)\b/i.test(s)) sign = -1;
  // Strip era, whether glued to the digits ("200BCE") or standalone ("500 CE", "AD 500").
  s = s.replace(/(\d)\s*(bce|bc|ce|ad)\b/ig, '$1').replace(/\b(bce|bc|ce|ad)\b/ig, ' ').replace(/\s+/g, ' ').trim();
  // A leading minus directly on the year also means BCE.
  const applyEra = (y) => (sign < 0 ? -Math.abs(y) : (y < 0 ? y : y));

  let m;

  // Century, e.g. "15th century"
  if ((m = s.match(/^(\d{1,2})\s*(?:st|nd|rd|th)?\s*century$/i))) {
    const c = parseInt(m[1], 10);
    return { y: applyEra((c - 1) * 100), m: null, d: null, gran: 'century', ambiguous };
  }

  // ISO: Y, Y-M, Y-M-D (year may be negative)
  if ((m = s.match(/^(-?\d{1,4})(?:-(\d{1,2})(?:-(\d{1,2}))?)?$/))) {
    const y0 = parseInt(m[1], 10);
    const y = sign < 0 ? -Math.abs(y0) : y0;
    if (m[3] != null) return { y, m: +m[2], d: +m[3], gran: 'day', ambiguous };
    if (m[2] != null) return { y, m: +m[2], d: null, gran: 'month', ambiguous };
    return { y, m: null, d: null, gran: 'year', ambiguous };
  }

  // Numeric d/m/y or m/d/y with / . - separators
  if ((m = s.match(/^(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{1,4})$/))) {
    let a = +m[1], b = +m[2]; const y = applyEra(+m[3]);
    let day, mon, order;
    if (a > 12 && b <= 12) { day = a; mon = b; order = 'uk'; }          // unambiguous → dd/mm
    else if (b > 12 && a <= 12) { mon = a; day = b; order = 'us'; }     // unambiguous → mm/dd
    else {                                                             // ambiguous → locale default
      order = 'ambiguous'; ambiguous = a <= 12 && b <= 12;
      if (opts.locale === 'us') { mon = a; day = b; } else { day = a; mon = b; }
    }
    if (mon < 1 || mon > 12 || day < 1 || day > daysInMonth(Math.abs(y) || 4, mon)) return null;
    return { y, m: mon, d: day, gran: 'day', ambiguous, order };
  }

  // Day Month Year, e.g. "3 June 1431", "3rd May 1425", "18 Aug 1419"
  if ((m = s.match(/^(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\.?\s+(-?\d{1,4})$/))) {
    const mon = MONTHS[m[2].toLowerCase()];
    if (mon) return { y: applyEra(+m[3]), m: mon, d: +m[1], gran: 'day', ambiguous };
  }

  // Month Day, Year, e.g. "November 19, 1495", "May 3rd, 1425"
  if ((m = s.match(/^([A-Za-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(-?\d{1,4})$/))) {
    const mon = MONTHS[m[1].toLowerCase()];
    if (mon) return { y: applyEra(+m[3]), m: mon, d: +m[2], gran: 'day', ambiguous };
  }

  // Month Year, e.g. "July 1423"
  if ((m = s.match(/^([A-Za-z]+)\.?\s+(-?\d{1,4})$/))) {
    const mon = MONTHS[m[1].toLowerCase()];
    if (mon) return { y: applyEra(+m[2]), m: mon, d: null, gran: 'month', ambiguous };
  }

  return null;
}

const startOf = (p) => p.gran === 'day' ? iso(p.y, p.m, p.d)
  : p.gran === 'month' ? iso(p.y, p.m, 1)
  : p.gran === 'century' ? iso(p.y, 1, 1)
  : iso(p.y, 1, 1);
const endOf = (p) => p.gran === 'day' ? iso(p.y, p.m, p.d)
  : p.gran === 'month' ? iso(p.y, p.m, daysInMonth(p.y, p.m))
  : p.gran === 'century' ? iso(p.y + 99, 12, 31)
  : iso(p.y, 12, 31);

function splitRange(s) {
  let m;
  if ((m = s.split(/\s+to\s+/i)).length === 2) return m;
  if ((m = s.split(/\s*[–—]\s*/)).length === 2) return m;            // en/em dash
  if ((m = s.match(/^(-?\d{1,4}-\d{1,2}-\d{1,2})\s*-\s*(-?\d{1,4}-\d{1,2}-\d{1,2})$/))) return [m[1], m[2]]; // ISO collision
  if ((m = s.match(/^(-?\d{1,4})\s*-\s*(-?\d{1,4})$/))) return [m[1], m[2]];   // Y-Y
  if ((m = s.split(/\s+-\s+/)).length === 2) return m;               // spaced hyphen
  if ((m = s.match(/^(\d{3,4})\/(\d{3,4})$/))) return [m[1], m[2]];  // Y/Y
  return null;
}

// Main entry. Returns { startISO, endISO, startYear, endYear, granularity, approximate,
// openStart, openEnd, ambiguous, reversed } | null.
export function parseDate(input, opts = {}) {
  const locale = opts.locale === 'us' ? 'us' : 'uk';
  let s = String(input == null ? '' : input).trim().replace(/\s+/g, ' ');
  if (!s) return null;

  let approximate = false;
  if (/^(circa|ca\.?|c\.)\s*/i.test(s)) { approximate = true; s = s.replace(/^(circa|ca\.?|c\.)\s*/i, '').trim(); }
  if (/\?\s*$/.test(s)) { approximate = true; s = s.replace(/\?\s*$/, '').trim(); }
  // qualifiers may sit inside range parts too (circa … – circa …) — strip per part below.

  const stripCirca = (t) => t.replace(/^(circa|ca\.?|c\.)\s*/i, '').replace(/\?\s*$/, '').trim();

  // Open-ended
  let mm;
  if ((mm = s.match(/^(?:from|after)\s+(.+)$/i))) {
    const p = parseSingle(stripCirca(mm[1]), { locale }); if (!p) return null;
    return { startISO: startOf(p), endISO: null, startYear: p.y, endYear: null, granularity: p.gran, approximate, openStart: false, openEnd: true, ambiguous: p.ambiguous, reversed: false };
  }
  if ((mm = s.match(/^(?:before|to|until)\s+(.+)$/i))) {
    const p = parseSingle(stripCirca(mm[1]), { locale }); if (!p) return null;
    return { startISO: null, endISO: endOf(p), startYear: null, endYear: p.y, granularity: p.gran, approximate, openStart: true, openEnd: false, ambiguous: p.ambiguous, reversed: false };
  }
  if ((mm = s.match(/^(.+?)\s*-\s*$/)) && !/\s/.test(mm[1]) && parseSingle(mm[1], { locale })) { // dangling "1420-"
    const p = parseSingle(mm[1], { locale });
    return { startISO: startOf(p), endISO: null, startYear: p.y, endYear: null, granularity: p.gran, approximate, openStart: false, openEnd: true, ambiguous: p.ambiguous, reversed: false };
  }

  // Range
  const parts = splitRange(s);
  if (parts) {
    let a = parseSingle(stripCirca(parts[0]), { locale });
    let b = parseSingle(stripCirca(parts[1]), { locale });
    if (!a || !b) return null;
    // Range-internal locale consistency: if one numeric part is unambiguous (dd/mm or mm/dd),
    // re-parse the other ambiguous part with the same order — a range uses one convention.
    const firm = (a.order === 'us' || a.order === 'uk') ? a.order
      : (b.order === 'us' || b.order === 'uk') ? b.order : null;
    if (firm) {
      if (a.order === 'ambiguous') a = parseSingle(stripCirca(parts[0]), { locale: firm });
      if (b.order === 'ambiguous') b = parseSingle(stripCirca(parts[1]), { locale: firm });
    }
    let s1 = startOf(a), e2 = endOf(b), y1 = a.y, y2 = b.y, reversed = false;
    if (isoNum(s1) > isoNum(e2)) { // reversed → normalise to start ≤ end (numeric, BCE-safe)
      const A = startOf(b), B = endOf(a); s1 = A; e2 = B; const t = y1; y1 = y2; y2 = t; reversed = true;
    }
    return { startISO: s1, endISO: e2, startYear: y1, endYear: y2, granularity: `${a.gran}/${b.gran}`, approximate, openStart: false, openEnd: false, ambiguous: a.ambiguous || b.ambiguous, reversed };
  }

  // Single
  const p = parseSingle(s, { locale });
  if (!p) return null;
  return { startISO: startOf(p), endISO: endOf(p), startYear: p.y, endYear: p.y, granularity: p.gran, approximate, openStart: false, openEnd: false, ambiguous: p.ambiguous, reversed: false };
}
