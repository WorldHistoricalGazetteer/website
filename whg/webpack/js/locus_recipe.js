// locus_recipe.js — browser twin of places/locus_recipe.py (place#172, place#325 Stage 1).
//
// ONE serialisation serves both the unminted soft-cluster URL (a token of the canonical
// bytes, re-run on every visit) and the locus PID (sha256 of exactly those bytes). The
// specification lives in the Python module's docstring; this file implements the same
// rules so a cluster card can build the same token the server would. Parity is proved by
// scripts/check-locus-recipe.mjs against scripts/fixtures/locus_recipe_vectors.json — if
// you change a rule here, change it there and regenerate the vectors.
//
// No DOM, no dependencies. Runs in the browser and under Node (>= 20).

export const RECIPE_VERSION = 1;

const SCALAR_DP = 4;
const COORD_DP = 6;
const SIZE_MAX = 10000;
const YEAR_ABS_MAX = 1000000;
const TOKEN_MAX_BYTES = 65536;
const PID_HASH_BITS = 54;      // 31**11 > 2**54, so 11 base-31 digits hold them exactly
const PID_DIGITS = 11;

const MODES = ['exact', 'starts', 'in', 'fuzzy', 'phonetic'];
const CONTAINMENTS = ['fuzzy', 'exact'];
const RELATIONS = ['intersects', 'within'];
const TEMPORAL_MODES = ['possibly', 'definitely'];
const WEIGHT_KEYS = ['link', 'name', 'spatial', 'temporal', 'type'];

export const PID_ALPHABET = '0123456789ABCDEFGHJKMNPQRSTVWXY';   // Crockford base32 minus Z: 31 symbols
export const PID_RE = /^[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}-[0-9A-HJKMNP-TV-Y]{4}$/;
const LEGACY_RE = /^1[0-9]{7}$/;

export const FORBIDDEN_KEYS = new Set([
	'edges', 'edge', 'links', 'hard_links', 'merges', 'merge', 'forced_merges',
	'accept', 'accepted', 'acceptances', 'reject', 'rejected', 'rejections',
	'must_link', 'cannot_link', 'pins', 'pinned', 'attestations',
]);

const TOP_KEYS = new Set(['v', 'retrieval', 'clustering', 'members']);
const RETRIEVAL_KEYS = new Set(['query', 'mode', 'size', 'ccodes', 'types', 'aat_types', 'namespaces',
	'bounds', 'contained_in', 'containment', 'relation', 'temporal']);
const TEMPORAL_KEYS = new Set(['start', 'end', 'mode', 'undated']);
const CLUSTERING_KEYS = new Set(['weights', 'theta', 'tau_name', 'tau_link', 'same_ns_penalty',
	'respect_distinct']);
// Ruled out of the recipe (place#325 Stage 1 ruling 3, SG 2026-10-21): the generic-name
// stoplist is gateway data, taken from the gateway response at resolve time, not hashed.
// Rejected with a reason so an old caller fails loudly. Same text as the Python module.
const RETIRED_CLUSTERING_KEYS = {
	stoplist: 'is not part of a recipe: the generic-name stoplist is gateway data, taken '
		+ 'from the gateway response at resolve time (place#325 ruling 3, 2026-10-21)',
};

// Exactly \s, written out so it is visibly the same class as the Python regex.
const WS_RE = /[\t\n\v\f\r \u00a0\u1680\u2000-\u200a\u2028\u2029\u202f\u205f\u3000\ufeff]+/g;
const CCODE_RE = /^[A-Z]{2}$/;
const NAMESPACE_RE = /^[a-z0-9_]+$/;
const PLACE_ID_RE = /^([A-Za-z0-9_]+):(\S+)$/;
const TOKEN_RE = /^[A-Za-z0-9_-]+$/;

export class RecipeError extends Error {
	constructor(path, message) {
		super(path ? `${path}: ${message}` : message);
		this.name = 'RecipeError';
		this.path = path;
		this.msg = message;
	}
}

// ----------------------------------------------------------------------------- scalars

const isPlainObject = (x) => x !== null && typeof x === 'object' && !Array.isArray(x);
const isNumber = (x) => typeof x === 'number';

/** Round half-up on the shortest decimal repr (String(x)) to `dp` places; returns a Number. */
function quantise(x, dp) {
	if (Math.abs(x) < 0.5 * Math.pow(10, -dp)) return 0;   // also kills -0 and 1e-7-style exponents
	let s = String(x);
	if (s.includes('e') || s.includes('E')) throw new RecipeError('', 'number out of range');
	let neg = false;
	if (s[0] === '-') { neg = true; s = s.slice(1); }
	let [int, frac = ''] = s.split('.');
	if (frac.length > dp) {
		const roundUp = frac.charCodeAt(dp) >= 53; // '5'
		frac = frac.slice(0, dp);
		if (roundUp) {
			// Propagate the carry through frac then int, as decimal strings.
			const digits = (int + frac).split('');
			let i = digits.length - 1;
			while (i >= 0) {
				if (digits[i] === '9') { digits[i] = '0'; i--; } else { digits[i] = String.fromCharCode(digits[i].charCodeAt(0) + 1); break; }
			}
			if (i < 0) digits.unshift('1');
			const joined = digits.join('');
			int = joined.slice(0, joined.length - frac.length);
			frac = joined.slice(joined.length - frac.length);
		}
	}
	frac = frac.replace(/0+$/, '');
	const out = Number(frac ? `${int}.${frac}` : int);
	return neg && out !== 0 ? -out : out;
}

function number(x, path, dp, lo, hi) {
	if (!isNumber(x)) throw new RecipeError(path, 'must be a number');
	if (!Number.isFinite(x)) throw new RecipeError(path, 'must be finite');
	if (x < lo || x > hi) throw new RecipeError(path, `must be between ${lo} and ${hi}`);
	const q = quantise(x, dp);
	if (q < lo || q > hi) throw new RecipeError(path, `must be between ${lo} and ${hi}`);
	return q;
}

function integer(x, path, lo, hi) {
	if (!isNumber(x) || !Number.isInteger(x)) throw new RecipeError(path, 'must be an integer');
	if (x < lo || x > hi) throw new RecipeError(path, `must be between ${lo} and ${hi}`);
	return x;
}

function boolean(x, path) {
	if (typeof x !== 'boolean') throw new RecipeError(path, 'must be true or false');
	return x;
}

/** Canonical JSON text of an already-quantised number. */
function numText(v) {
	if (Object.is(v, -0) || v === 0) return '0';
	const s = String(v);
	if (s.includes('e') || s.includes('E')) throw new RecipeError('', 'number out of range');
	return s;
}

// ----------------------------------------------------------------------------- strings

function text(x, path, { lower = false, allowEmpty = false } = {}) {
	if (typeof x !== 'string') throw new RecipeError(path, 'must be a string');
	let s = x.normalize('NFC').replace(WS_RE, ' ').trim();
	if (/\p{Cs}/u.test(s)) throw new RecipeError(path, 'contains a lone surrogate');
	if (/\p{Cc}/u.test(s)) throw new RecipeError(path, 'contains a control character');
	if (lower) s = s.toLowerCase().normalize('NFC');
	if (!s && !allowEmpty) throw new RecipeError(path, 'must not be empty');
	return s;
}

function enumValue(x, path, allowed) {
	const s = text(x, path, { lower: true });
	if (!allowed.includes(s)) throw new RecipeError(path, 'must be one of ' + allowed.join(', '));
	return s;
}

function placeId(x, path) {
	let s = text(x, path);
	if (s.startsWith('place:')) s = s.slice('place:'.length);
	const m = PLACE_ID_RE.exec(s);
	if (!m) throw new RecipeError(path, 'must be a place id of the form <namespace>:<id>');
	return m[1].toLowerCase() + ':' + m[2];
}

/** Code-point order (what Python's str sort and UTF-8 byte order give), not UTF-16 unit order. */
export function compareCodePoints(a, b) {
	const ia = a[Symbol.iterator](), ib = b[Symbol.iterator]();
	for (;;) {
		const x = ia.next(), y = ib.next();
		if (x.done && y.done) return 0;
		if (x.done) return -1;
		if (y.done) return 1;
		const cx = x.value.codePointAt(0), cy = y.value.codePointAt(0);
		if (cx !== cy) return cx < cy ? -1 : 1;
	}
}

function sortedUniqueStrings(items, path, each) {
	if (!Array.isArray(items)) throw new RecipeError(path, 'must be a list');
	const set = new Set(items.map((v, i) => each(v, `${path}[${i}]`)));
	return Array.from(set).sort(compareCodePoints);
}

// ----------------------------------------------------------------------------- geometry

function position(p, path) {
	if (!Array.isArray(p) || p.length !== 2) throw new RecipeError(path, 'must be a [longitude, latitude] pair');
	return [number(p[0], `${path}[0]`, COORD_DP, -540, 540), number(p[1], `${path}[1]`, COORD_DP, -90, 90)];
}

function ring(r, path) {
	if (!Array.isArray(r) || r.length < 4) throw new RecipeError(path, 'a ring needs at least four positions');
	const out = r.map((p, i) => position(p, `${path}[${i}]`));
	const a = out[0], z = out[out.length - 1];
	if (a[0] !== z[0] || a[1] !== z[1]) throw new RecipeError(path, 'a ring must be closed (first position == last)');
	return out;
}

function polygonCoords(c, path) {
	if (!Array.isArray(c) || !c.length) throw new RecipeError(path, 'a Polygon needs at least one ring');
	return c.map((r, i) => ring(r, `${path}[${i}]`));
}

function geometry(g, path, nested = false) {
	if (!isPlainObject(g)) throw new RecipeError(path, 'must be a GeoJSON geometry object');
	const gtype = g.type;
	if (typeof gtype !== 'string') throw new RecipeError(`${path}.type`, 'must be a string');
	if (gtype === 'GeometryCollection') {
		onlyKeys(g, path, new Set(['type', 'geometries']));
		if (nested) throw new RecipeError(path, 'a GeometryCollection may not contain a GeometryCollection');
		if (!Array.isArray(g.geometries)) throw new RecipeError(`${path}.geometries`, 'must be a list');
		const parsed = [];
		g.geometries.forEach((m, i) => {
			const pm = geometry(m, `${path}.geometries[${i}]`, true);
			if (pm !== null) parsed.push(pm);
		});
		if (!parsed.length) return null;
		if (parsed.length === 1) return parsed[0];
		const uniq = new Map();
		for (const pm of parsed) uniq.set(dumps(pm), pm);
		const keys = Array.from(uniq.keys()).sort(compareCodePoints);
		return { geometries: keys.map((k) => uniq.get(k)), type: 'GeometryCollection' };
	}
	if (gtype === 'Polygon' || gtype === 'MultiPolygon') {
		onlyKeys(g, path, new Set(['type', 'coordinates']));
		const cpath = `${path}.coordinates`;
		if (gtype === 'Polygon') return { coordinates: polygonCoords(g.coordinates, cpath), type: 'Polygon' };
		if (!Array.isArray(g.coordinates) || !g.coordinates.length) throw new RecipeError(cpath, 'a MultiPolygon needs at least one polygon');
		return { coordinates: g.coordinates.map((pc, i) => polygonCoords(pc, `${cpath}[${i}]`)), type: 'MultiPolygon' };
	}
	throw new RecipeError(`${path}.type`, 'must be Polygon, MultiPolygon or GeometryCollection');
}

// ----------------------------------------------------------------------------- blocks

function onlyKeys(obj, path, allowed) {
	for (const k of Object.keys(obj)) {
		if (allowed.has(k)) continue;
		const where = path ? `${path}.${k}` : k;
		if (FORBIDDEN_KEYS.has(k)) throw new RecipeError(where, 'user acceptances and rejections cannot enter a recipe (place#306)');
		throw new RecipeError(where, 'unknown key');
	}
}

function object(x, path, allowed) {
	if (!isPlainObject(x)) throw new RecipeError(path, 'must be an object');
	onlyKeys(x, path, allowed);
	return x;
}

const has = (o, k) => Object.prototype.hasOwnProperty.call(o, k) && o[k] !== null && o[k] !== undefined;

function parseTemporal(t, path) {
	t = object(t, path, TEMPORAL_KEYS);
	const out = {};
	if (has(t, 'start')) out.start = integer(t.start, `${path}.start`, -YEAR_ABS_MAX, YEAR_ABS_MAX);
	if (has(t, 'end')) out.end = integer(t.end, `${path}.end`, -YEAR_ABS_MAX, YEAR_ABS_MAX);
	if (!('start' in out) && !('end' in out)) throw new RecipeError(path, 'needs a start year, an end year, or both');
	if ('start' in out && 'end' in out && out.start > out.end) throw new RecipeError(path, 'start must not be after end');
	if (!Object.prototype.hasOwnProperty.call(t, 'mode')) throw new RecipeError(`${path}.mode`, 'is required');
	out.mode = enumValue(t.mode, `${path}.mode`, TEMPORAL_MODES);
	if (has(t, 'undated') && boolean(t.undated, `${path}.undated`)) out.undated = true;
	return out;
}

function parseRetrieval(r, path) {
	r = object(r, path, RETRIEVAL_KEYS);
	const out = {};
	if (has(r, 'query')) {
		const q = text(r.query, `${path}.query`, { allowEmpty: true });
		if (q) out.query = q;
	}
	if (!Object.prototype.hasOwnProperty.call(r, 'mode')) throw new RecipeError(`${path}.mode`, 'is required');
	out.mode = enumValue(r.mode, `${path}.mode`, MODES);
	if (!Object.prototype.hasOwnProperty.call(r, 'size')) throw new RecipeError(`${path}.size`, 'is required');
	out.size = integer(r.size, `${path}.size`, 1, SIZE_MAX);

	if (has(r, 'ccodes')) {
		const vals = sortedUniqueStrings(r.ccodes, `${path}.ccodes`, (v, p) => {
			const s = text(v, p).toUpperCase();
			if (!CCODE_RE.test(s)) throw new RecipeError(p, 'must be a two-letter country code');
			return s;
		});
		if (vals.length) out.ccodes = vals;
	}
	if (has(r, 'types')) {
		const vals = sortedUniqueStrings(r.types, `${path}.types`, (v, p) => text(v, p));
		if (vals.length) out.types = vals;
	}
	if (has(r, 'aat_types')) {
		if (!Array.isArray(r.aat_types)) throw new RecipeError(`${path}.aat_types`, 'must be a list');
		const vals = Array.from(new Set(r.aat_types.map((v, i) => integer(v, `${path}.aat_types[${i}]`, 1, 1e12)))).sort((a, b) => a - b);
		if (vals.length) out.aat_types = vals;
	}
	if (has(r, 'namespaces')) {
		const vals = sortedUniqueStrings(r.namespaces, `${path}.namespaces`, (v, p) => {
			const s = text(v, p, { lower: true });
			if (!NAMESPACE_RE.test(s)) throw new RecipeError(p, 'must be a namespace code ([a-z0-9_])');
			return s;
		});
		if (vals.length) out.namespaces = vals;
	}
	if (has(r, 'bounds')) {
		const g = geometry(r.bounds, `${path}.bounds`);
		if (g !== null) out.bounds = g;
	}
	if (has(r, 'contained_in')) {
		const vals = sortedUniqueStrings(r.contained_in, `${path}.contained_in`, placeId);
		if (vals.length) out.contained_in = vals;
	}
	if ('contained_in' in out) {
		if (!has(r, 'containment')) throw new RecipeError(`${path}.containment`, 'is required with contained_in');
		out.containment = enumValue(r.containment, `${path}.containment`, CONTAINMENTS);
	} else if (has(r, 'containment')) {
		throw new RecipeError(`${path}.containment`, 'is meaningless without contained_in');
	}
	if (has(r, 'relation')) {
		if (!('bounds' in out) && !('contained_in' in out)) throw new RecipeError(`${path}.relation`, 'is meaningless without bounds or contained_in');
		out.relation = enumValue(r.relation, `${path}.relation`, RELATIONS);
	}
	if (has(r, 'temporal')) out.temporal = parseTemporal(r.temporal, `${path}.temporal`);
	if (!('query' in out) && !('bounds' in out) && !('contained_in' in out)) {
		throw new RecipeError(path, 'needs a query, bounds or contained_in: the retrieval would be unbounded');
	}
	return out;
}

function parseClustering(c, path) {
	if (isPlainObject(c)) {
		for (const [k, why] of Object.entries(RETIRED_CLUSTERING_KEYS)) {
			if (Object.prototype.hasOwnProperty.call(c, k)) throw new RecipeError(`${path}.${k}`, why);
		}
	}
	c = object(c, path, CLUSTERING_KEYS);
	for (const k of Array.from(CLUSTERING_KEYS).sort()) {
		if (!has(c, k)) throw new RecipeError(`${path}.${k}`, 'is required (clustering parameters are always literal)');
	}
	const w = object(c.weights, `${path}.weights`, new Set(WEIGHT_KEYS));
	const weights = {};
	for (const k of WEIGHT_KEYS) {
		if (!has(w, k)) throw new RecipeError(`${path}.weights.${k}`, 'is required');
		weights[k] = number(w[k], `${path}.weights.${k}`, SCALAR_DP, 0, 1);
	}
	return {
		respect_distinct: boolean(c.respect_distinct, `${path}.respect_distinct`),
		same_ns_penalty: number(c.same_ns_penalty, `${path}.same_ns_penalty`, SCALAR_DP, 0, 1),
		tau_link: number(c.tau_link, `${path}.tau_link`, SCALAR_DP, 0, 1),
		tau_name: number(c.tau_name, `${path}.tau_name`, SCALAR_DP, 0, 1),
		theta: number(c.theta, `${path}.theta`, SCALAR_DP, 0, 1),
		weights,
	};
}

/** Validate and return the canonical recipe object. Throws RecipeError. Idempotent. */
export function parseRecipe(obj) {
	obj = object(obj, '', TOP_KEYS);
	if (!Object.prototype.hasOwnProperty.call(obj, 'v')) throw new RecipeError('v', 'is required');
	if (!(isNumber(obj.v) && Number.isInteger(obj.v) && obj.v === RECIPE_VERSION)) {
		throw new RecipeError('v', `unsupported recipe version (this code speaks ${RECIPE_VERSION})`);
	}
	for (const k of ['retrieval', 'clustering', 'members']) {
		if (!has(obj, k)) throw new RecipeError(k, 'is required');
	}
	const members = sortedUniqueStrings(obj.members, 'members', placeId);
	if (!members.length) throw new RecipeError('members', 'needs at least one member id');
	return {
		clustering: parseClustering(obj.clustering, 'clustering'),
		members,
		retrieval: parseRetrieval(obj.retrieval, 'retrieval'),
		v: RECIPE_VERSION,
	};
}

// ----------------------------------------------------------------------------- canonical bytes

function dumps(v) {
	if (v === true) return 'true';
	if (v === false) return 'false';
	if (v === null || v === undefined) throw new RecipeError('', 'null has no canonical form (absent means absent)');
	if (isNumber(v)) return numText(v);
	if (typeof v === 'string') return JSON.stringify(v);
	if (Array.isArray(v)) return '[' + v.map(dumps).join(',') + ']';
	if (isPlainObject(v)) {
		const keys = Object.keys(v).sort(compareCodePoints);
		return '{' + keys.map((k) => JSON.stringify(k) + ':' + dumps(v[k])).join(',') + '}';
	}
	throw new RecipeError('', `cannot serialise ${typeof v}`);
}

export function canonicalJson(recipe) { return dumps(parseRecipe(recipe)); }

export function canonicalBytes(recipe) { return new TextEncoder().encode(canonicalJson(recipe)); }

// ----------------------------------------------------------------------------- PID

/** The first PID_HASH_BITS bits of the digest as PID_DIGITS base-31 digits, most significant first. */
function pidDigits(digest) {
	const nBytes = Math.ceil(PID_HASH_BITS / 8);
	let value = BigInt(0);
	for (let i = 0; i < nBytes; i++) value = (value << BigInt(8)) | BigInt(digest[i]);
	value >>= BigInt(8 * nBytes - PID_HASH_BITS);
	const digits = [];
	for (let i = 0; i < PID_DIGITS; i++) {
		digits.push(Number(value % BigInt(31)));
		value /= BigInt(31);
	}
	return digits.reverse();
}

const checkDigit = (digits) => digits.reduce((acc, d, i) => acc + (i + 1) * d, 0) % 31;

function formatPid(digits) {
	const s = digits.map((d) => PID_ALPHABET[d]).join('') + PID_ALPHABET[checkDigit(digits)];
	return `${s.slice(0, 4)}-${s.slice(4, 8)}-${s.slice(8, 12)}`;
}

export function pidFromDigest(digest) {
	if (!(digest instanceof Uint8Array) || digest.length < 8) throw new Error('digest must be at least 8 bytes');
	return formatPid(pidDigits(digest));
}

/** sha256 of exactly the canonical bytes (WebCrypto; async). */
export async function recipeDigest(recipe) {
	const buf = await crypto.subtle.digest('SHA-256', canonicalBytes(recipe));
	return new Uint8Array(buf);
}

export async function recipePid(recipe) { return pidFromDigest(await recipeDigest(recipe)); }

export function normalisePid(s) {
	if (typeof s !== 'string') throw new Error('PID must be a string');
	const raw = s.trim().toUpperCase().replace(/-/g, '').replace(/[IL]/g, '1').replace(/O/g, '0');
	if (raw.length !== PID_DIGITS + 1) throw new Error('PID must be twelve characters');
	const digits = Array.from(raw).map((ch) => PID_ALPHABET.indexOf(ch));
	if (digits.includes(-1)) throw new Error('PID contains a character outside the locus alphabet');
	if (digits[PID_DIGITS] !== checkDigit(digits.slice(0, PID_DIGITS))) throw new Error('PID check digit does not match');
	return formatPid(digits.slice(0, PID_DIGITS));
}

export const isLegacyLocusId = (s) => typeof s === 'string' && LEGACY_RE.test(s.trim());

// ----------------------------------------------------------------------------- token

function b64urlEncode(bytes) {
	let bin = '';
	for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
	return btoa(bin).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function b64urlDecode(token) {
	const b64 = token.replace(/-/g, '+').replace(/_/g, '/') + '='.repeat((4 - token.length % 4) % 4);
	const bin = atob(b64);
	const out = new Uint8Array(bin.length);
	for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
	return out;
}

export function encodeRecipeToken(recipe) { return b64urlEncode(canonicalBytes(recipe)); }

export function decodeRecipeToken(token) {
	if (typeof token !== 'string') throw new RecipeError('', 'token must be a string');
	token = token.trim();
	if (!token || !TOKEN_RE.test(token)) throw new RecipeError('', 'token is not base64url');
	if (token.length > TOKEN_MAX_BYTES) throw new RecipeError('', 'token is too long');
	let obj;
	try {
		obj = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(b64urlDecode(token)));
	} catch (e) {
		throw new RecipeError('', 'token does not decode to JSON');
	}
	return parseRecipe(obj);
}

// ----------------------------------------------------------------------------- adapter

const BODY_FIXED = new Set(['geom', 'include_hard_links', 'include_clustering_fields', 'include_embeddings']);
const BODY_DIRECT = new Set(['query', 'mode', 'size', 'ccodes', 'types', 'aat_types', 'namespaces',
	'bounds', 'contained_in', 'containment', 'relation']);
const BODY_TEMPORAL = { start_year: 'start', end_year: 'end', undated: 'undated', temporal_mode: 'mode' };
const BODY_REJECT = new Set(['browse', 'offset']);

/** The `retrieval` block from the gateway POST /api/search body (mirror of the Python adapter). */
export function retrievalFromSearchBody(body) {
	if (!isPlainObject(body)) throw new RecipeError('body', 'must be an object');
	const out = {};
	const temporal = {};
	for (const [k, v] of Object.entries(body)) {
		if (BODY_FIXED.has(k) || v === null || v === undefined) continue;
		if (BODY_REJECT.has(k)) throw new RecipeError(`body.${k}`, 'a browse page is not a cluster recipe');
		if (BODY_DIRECT.has(k)) out[k] = v;
		else if (k in BODY_TEMPORAL) temporal[BODY_TEMPORAL[k]] = v;
		else throw new RecipeError(`body.${k}`, 'unknown search body key');
	}
	if (Object.keys(temporal).length) out.temporal = temporal;
	return out;
}

export default {
	RECIPE_VERSION, RecipeError, parseRecipe, canonicalJson, canonicalBytes,
	recipeDigest, recipePid, pidFromDigest, normalisePid, isLegacyLocusId,
	encodeRecipeToken, decodeRecipeToken, retrievalFromSearchBody, compareCodePoints,
};
