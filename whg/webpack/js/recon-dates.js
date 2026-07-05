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

import { parseGlobalCalendar } from './recon-calendars.js';

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

// ── Calendrical extensions: regnal years, feast days, Julian/Gregorian ───────
// Data verified against Cheney, *Handbook of Dates for Students of British History* (CUP),
// the Univ. of Nottingham dating guide, and Wikipedia's regnal-years table.

// Julian Day Number conversions (integer arithmetic) — for computus + Julian/Gregorian conversion.
function gregToJDN(Y, M, D) {
  const a = Math.floor((14 - M) / 12), y = Y + 4800 - a, m = M + 12 * a - 3;
  return D + Math.floor((153 * m + 2) / 5) + 365 * y + Math.floor(y / 4) - Math.floor(y / 100) + Math.floor(y / 400) - 32045;
}
function jdnToGreg(j) {
  let a = j + 32044, b = Math.floor((4 * a + 3) / 146097), c = a - Math.floor(146097 * b / 4);
  let d = Math.floor((4 * c + 3) / 1461), e = c - Math.floor(1461 * d / 4), m = Math.floor((5 * e + 2) / 153);
  return [100 * b + d - 4800 + Math.floor(m / 10), m + 3 - 12 * Math.floor(m / 10), e - Math.floor((153 * m + 2) / 5) + 1];
}
function julToJDN(Y, M, D) {
  return 367 * Y - Math.floor(7 * (Y + 5001 + Math.floor((M - 9) / 7)) / 4) + Math.floor(275 * M / 9) + D + 1729777;
}
function jdnToJul(j) {
  let c = j + 32082, d = Math.floor((4 * c + 3) / 1461), e = c - Math.floor(1461 * d / 4), m = Math.floor((5 * e + 2) / 153);
  return [d - 4800 + Math.floor(m / 10), m + 3 - 12 * Math.floor(m / 10), e - Math.floor((153 * m + 2) / 5) + 1];
}
// Convert an Old-Style (Julian) date to a proleptic-Gregorian date (same physical day).
export function julianToGregorian(Y, M, D) { return jdnToGreg(julToJDN(Y, M, D)); }
export function gregorianToJulian(Y, M, D) { return jdnToJul(gregToJDN(Y, M, D)); }

// Easter Sunday on the Julian calendar (Meeus) — used for movable feasts in OS English dating.
function julianEaster(Y) {
  const a = Y % 4, b = Y % 7, c = Y % 19, d = (19 * c + 15) % 30, e = (2 * a + 4 * b - d + 34) % 7;
  const f = d + e + 114;
  return [Math.floor(f / 31), (f % 31) + 1]; // [month, day]
}

// Monarch name variants (lowercased, dots stripped) → canonical key. Researched & cited: English
// scribal + statute-citation forms, and Latin nominative AND genitive (the genitive is what appears
// in roll dating clauses, e.g. "anno regni regis Henrici Sexti octavo"). Sources: TNA Latin guide
// ("How to decline personal names"); Statutes Project chronological table; Cheney, Handbook of Dates.
const MONARCH_KEY = {
  edward: 'edward', edwardus: 'edward', edwardi: 'edward', edw: 'edward', edwd: 'edward', ed: 'edward', e: 'edward',
  henry: 'henry', henricus: 'henry', henrici: 'henry', hen: 'henry', henr: 'henry', hy: 'henry', h: 'henry',
  richard: 'richard', ricardus: 'richard', ricardi: 'richard', rich: 'richard', ricd: 'richard', ric: 'richard', rd: 'richard', r: 'richard',
  john: 'john', johannes: 'john', johannis: 'john', joh: 'john', jo: 'john',
  elizabeth: 'elizabeth', elizabetha: 'elizabeth', elizabethae: 'elizabeth', elizab: 'elizabeth', eliz: 'elizabeth',
  mary: 'mary', maria: 'mary', mariae: 'mary', mar: 'mary',
  james: 'james', jacobus: 'james', jacobi: 'james', jas: 'james', jac: 'james', ja: 'james',
  charles: 'charles', carolus: 'charles', caroli: 'charles', chas: 'charles', char: 'charles', cha: 'charles', car: 'charles',
  william: 'william', gulielmus: 'william', gulielmi: 'william', willelmus: 'william', willelmi: 'william', will: 'william', wm: 'william', gul: 'william', w: 'william',
  anne: 'anne', anna: 'anne', annae: 'anne', ann: 'anne',
  george: 'george', georgius: 'george', georgii: 'george', geo: 'george', geor: 'george',
};
// Latin ordinal words (single-word forms) → integer, for the monarch ordinal ("Sexti") and the
// regnal year ("octavo"/"8vo"). Compound tens ("vicesimo primo") are noted as future.
const LATIN_ORD = {
  primo: 1, primi: 1, primus: 1, secundo: 2, secundi: 2, tertio: 3, tertii: 3, quarto: 4, quarti: 4,
  quinto: 5, quinti: 5, sexto: 6, sexti: 6, septimo: 7, septimi: 7, octavo: 8, octavi: 8, nono: 9, noni: 9,
  decimo: 10, decimi: 10, undecimo: 11, duodecimo: 12, tertiodecimo: 13, quartodecimo: 14, quintodecimo: 15,
  sextodecimo: 16, septimodecimo: 17, octavodecimo: 18, nonodecimo: 19,
  vicesimo: 20, vigesimo: 20, tricesimo: 30, trigesimo: 30, quadragesimo: 40, quinquagesimo: 50,
};
// Accession dates (Old Style / Julian), keyed "canonical|ordinal". Regnal year N runs from the
// (N−1)th to the Nth anniversary of accession.
const ACCESSION = {
  'edward|1': [1272, 11, 20], 'edward|2': [1307, 7, 8], 'edward|3': [1327, 1, 25],
  'richard|2': [1377, 6, 22], 'richard|3': [1483, 6, 26],
  'henry|4': [1399, 9, 30], 'henry|5': [1413, 3, 21], 'henry|6': [1422, 9, 1], 'henry|7': [1485, 8, 22], 'henry|8': [1509, 4, 22],
  'edward|4': [1461, 3, 4], 'edward|5': [1483, 4, 9], 'edward|6': [1547, 1, 28],
  'mary|1': [1553, 7, 6], 'elizabeth|1': [1558, 11, 17],
  'james|1': [1603, 3, 24], 'charles|1': [1625, 3, 27], 'charles|2': [1649, 1, 30], // Charles II: legal reign from 1649
  'james|2': [1685, 2, 6], 'william|3': [1689, 2, 13], 'mary|2': [1689, 2, 13], 'anne|1': [1702, 3, 8],
  'george|1': [1714, 8, 1], 'george|2': [1727, 6, 11],
};

function romanToInt(s) {
  const M = { i: 1, v: 5, x: 10, l: 50, c: 100, d: 500, m: 1000 };
  s = s.toLowerCase(); if (!/^[ivxlcdm]+$/.test(s)) return null;
  let t = 0; for (let i = 0; i < s.length; i++) { const a = M[s[i]], b = M[s[i + 1]]; t += (b && a < b) ? -a : a; }
  return t;
}
const asInt = (s) => {
  const l = String(s).toLowerCase().replace(/[.º°]/g, '');
  if (/^\d+$/.test(l)) return parseInt(l, 10);
  if (LATIN_ORD[l] != null) return LATIN_ORD[l];
  return romanToInt(l);
};
function dayBefore(y, m, d) {
  if (d > 1) return [y, m, d - 1];
  if (m > 1) return [y, m - 1, daysInMonth(y, m - 1)];
  return [y - 1, 12, 31];
}

// Regnal-year dating → the regnal year's anniversary range. Handles both the English/statute order
// "<year> <name> <ordinal>" (8 Henry VI · 27 Hen. 8 · 12 Cha. II) and the Latin roll-clause order
// "<name> <ordinal> <year>" (anno regni regis Henrici Sexti octavo). year/ordinal may be Arabic,
// Roman (incl. IIII), or a Latin ordinal word. NB Charles II is counted from 1649 (legal fiction).
function parseRegnal(s) {
  const t = s.trim()
    .replace(/^anno\s+(regni\s+)?(regis\s+|regine\s+|reginae\s+)?/i, '')
    .replace(/,/g, ' ').replace(/\s+/g, ' ').trim();
  const toks = t.split(' ');
  const nameKey = (tok) => (tok ? MONARCH_KEY[tok.toLowerCase().replace(/\./g, '')] : null);

  // Locate the monarch name (followed by its ordinal) anywhere in the string.
  let ni = -1, key = null, ord = null;
  for (let i = 0; i < toks.length; i++) {
    const k = nameKey(toks[i]);
    if (k) { const o = asInt(toks[i + 1] || ''); if (o) { ni = i; key = k; ord = o; break; } }
  }
  if (ni < 0) return null;

  // Regnal year: precedes the name (English/statute) or follows the ordinal (Latin clause).
  let year = null, dateToks = null;
  const before = asInt(toks[ni - 1] || '');
  if (before) { year = before; dateToks = toks.slice(0, ni - 1); }             // [day month] <year> <name> <ordinal>
  else { const after = asInt(toks[ni + 2] || ''); if (after) { year = after; dateToks = toks.slice(0, ni); } } // <name> <ordinal> <year>
  if (!year) return null;

  const acc = ACCESSION[`${key}|${ord}`];
  if (!acc) return null;
  const [ay, am, ad] = acc;
  const Y0 = ay + year - 1; // calendar year in which the regnal year begins

  // Bare regnal year → the whole anniversary span.
  if (!dateToks || dateToks.length === 0) {
    const [ey, em, ed] = dayBefore(ay + year, am, ad);
    return { gran: 'regnal', y: Y0, startISO: iso(Y0, am, ad), endISO: iso(ey, em, ed) };
  }

  // Day/month or fixed feast within the regnal year → a specific date. The regnal year straddles
  // two calendar years: a date on/after the accession month-day is in Y0, else in Y0+1.
  const monthOf = (tok) => MONTHS[String(tok).toLowerCase().replace(/\.$/, '')] || null;
  const dt = dateToks.join(' ');
  let M = null, D = null, mm;
  if ((mm = dt.match(/^(\d{1,2})\s+([a-z]+)\.?$/i)) && monthOf(mm[2])) { D = +mm[1]; M = monthOf(mm[2]); }
  else if ((mm = dt.match(/^([a-z]+)\.?\s+(\d{1,2})(?:st|nd|rd|th)?$/i)) && monthOf(mm[1])) { M = monthOf(mm[1]); D = +mm[2]; }
  else if (FEAST_FIXED[normFeast(dt)]) { [M, D] = FEAST_FIXED[normFeast(dt)]; }
  else return null;
  if (M < 1 || M > 12 || D < 1 || D > daysInMonth(2000, M)) return null;
  const inFirstYear = M > am || (M === am && D >= ad);
  return { gran: 'day', y: inFirstYear ? Y0 : Y0 + 1, m: M, d: D };
}

const FEAST_FIXED = {
  'circumcision': [1, 1], 'new year': [1, 1], 'epiphany': [1, 6], 'twelfth day': [1, 6],
  'candlemas': [2, 2], 'purification': [2, 2], 'st matthias': [2, 24],
  'lady day': [3, 25], 'ladymas': [3, 25], 'annunciation': [3, 25], 'st george': [4, 23],
  'may day': [5, 1], 'invention of the cross': [5, 3], 'midsummer': [6, 24], 'nativity of st john the baptist': [6, 24],
  'st john the baptist': [6, 24], 'ss peter and paul': [6, 29], 'st swithin': [7, 15], 'st mary magdalene': [7, 22],
  'st james': [7, 25], 'lammas': [8, 1], 'assumption': [8, 15], 'st bartholomew': [8, 24],
  'nativity of the bvm': [9, 8], 'holy cross': [9, 14], 'holy rood': [9, 14], 'st matthew': [9, 21],
  'michaelmas': [9, 29], 'st michael': [9, 29], 'st luke': [10, 18], 'ss simon and jude': [10, 28],
  'all saints': [11, 1], 'all hallows': [11, 1], 'hallowmas': [11, 1], 'all souls': [11, 2],
  'martinmas': [11, 11], 'st martin': [11, 11], 'st edmund': [11, 20], 'st catherine': [11, 25],
  'st andrew': [11, 30], 'st nicholas': [12, 6], 'conception of the bvm': [12, 8], 'st thomas': [12, 21],
  'christmas': [12, 25], 'nativity': [12, 25], 'st stephen': [12, 26], 'st john the evangelist': [12, 27],
  'holy innocents': [12, 28], 'childermas': [12, 28], 'st silvester': [12, 31],
};
// Movable feasts — offset in days from Easter Sunday.
const FEAST_MOVABLE = {
  'septuagesima': -63, 'sexagesima': -56, 'quinquagesima': -49, 'shrove tuesday': -47, 'ash wednesday': -46,
  'quadragesima': -42, 'palm sunday': -7, 'maundy thursday': -3, 'good friday': -2, 'holy saturday': -1,
  'easter': 0, 'easter sunday': 0, 'easter monday': 1, 'hock monday': 15, 'hock tuesday': 16,
  'rogation sunday': 35, 'ascension': 39, 'ascension day': 39, 'whitsun': 49, 'whitsunday': 49, 'pentecost': 49,
  'trinity': 56, 'trinity sunday': 56, 'corpus christi': 60,
};
function normFeast(s) { return s.toLowerCase().replace(/[.'’]/g, '').replace(/\bthe\b/g, '').replace(/&/g, 'and').replace(/\s+/g, ' ').trim(); }

// "Michaelmas 1505", "Easter 1450", "Whitsun 1432" → the feast's date in that year (OS/Julian).
function parseFeast(s) {
  const m = s.match(/^(.+?)\s+(\d{3,4})$/);
  if (!m) return null;
  const name = normFeast(m[1]), yr = parseInt(m[2], 10);
  if (FEAST_FIXED[name]) return { gran: 'day', y: yr, m: FEAST_FIXED[name][0], d: FEAST_FIXED[name][1] };
  if (FEAST_MOVABLE[name] != null) {
    const [em, ed] = julianEaster(yr);
    const [fy, fm, fd] = jdnToJul(julToJDN(yr, em, ed) + FEAST_MOVABLE[name]);
    return { gran: 'day', y: fy, m: fm, d: fd };
  }
  return null;
}

// Parse a single (non-range) date token → { y, m, d, gran, ambiguous } | null. y is era-signed.
function parseSingle(raw, opts) {
  {
    const rg = parseRegnal(String(raw).trim()); if (rg) return rg;
    const ft = parseFeast(String(raw).trim().replace(/\s+/g, ' ')); if (ft) return ft;
  }
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

  // Old-Style/New-Style dual dating, e.g. "1641/2", "1650/51" → the New-Style (1-Jan) year.
  if ((m = s.match(/^(\d{3,4})\/(\d{1,2})$/))) {
    const y1 = parseInt(m[1], 10), base = Math.pow(10, m[2].length);
    let ns = Math.floor(y1 / base) * base + parseInt(m[2], 10);
    if (ns <= y1) ns += base;
    return { y: ns, m: null, d: null, gran: 'year', ambiguous: false };
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

const startOf = (p) => p.startISO ? p.startISO
  : p.gran === 'day' ? iso(p.y, p.m, p.d)
  : p.gran === 'month' ? iso(p.y, p.m, 1)
  : iso(p.y, 1, 1);
const endOf = (p) => p.endISO ? p.endISO
  : p.gran === 'day' ? iso(p.y, p.m, p.d)
  : p.gran === 'month' ? iso(p.y, p.m, daysInMonth(p.y, p.m))
  : p.gran === 'century' ? iso(p.y + 99, 12, 31)
  : iso(p.y, 12, 31);

function splitRange(s) {
  let m;
  if ((m = s.split(/\s+to\s+/i)).length === 2) return m;
  if ((m = s.split(/\s*[–—]\s*/)).length === 2) return m;            // en/em dash
  if ((m = s.match(/^(-?\d{1,4}-\d{1,2}-\d{1,2})\s*-\s*(-?\d{1,4}-\d{1,2}-\d{1,2})$/))) return [m[1], m[2]]; // ISO collision
  if ((m = s.match(/^(-?\d{3,4})\s*-\s*(-?\d{3,4})$/))) return [m[1], m[2]];   // Y-Y (both full years; avoids eating ISO "YYYY-MM")
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

  // Non-Western / global calendars (marker-gated: AH, BE, Śaka, an VIII, 民國, Anno Mundi, …).
  const gc = parseGlobalCalendar(s);
  if (gc) {
    const isoYear = (t) => (t ? parseInt(t, 10) : null);
    return {
      startISO: gc.startISO, endISO: gc.endISO, startYear: isoYear(gc.startISO), endYear: isoYear(gc.endISO),
      granularity: gc.gran, calendar: gc.system, approximate, openStart: false, openEnd: false, ambiguous: false, reversed: false,
    };
  }

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
