// /whg/webpack/js/boundaryId.js
/**
 * Encode / decode boundary feature IDs for MVT tiles.
 *
 * tippecanoe requires the GeoJSON top-level `id` to be an integer,
 * and MapLibre uses it as the MVT feature ID.  We pack namespace +
 * relation_id into a single integer that fits within JavaScript's
 * Number.MAX_SAFE_INTEGER (2^53 − 1).
 *
 * Layout (53 usable bits):
 *   bits [52..49]  — 4-bit namespace code  (0–15)
 *   bits [48..0]   — 49-bit relation_id    (0 – 562,949,953,421,311)
 *
 * This gives room for 16 namespaces and relation IDs up to ~563 trillion,
 * which is more than enough for OSM/OHM/M49 identifiers.
 *
 * The same encoding is implemented in Python in utils/boundary_id.py
 * so that the tile-generation pipeline and the client agree.
 */

/**
 * Namespace string → 4-bit code.
 * Keep in sync with utils/boundary_id.py NAMESPACE_CODES.
 */
const NAMESPACE_CODES = {
    osm: 1,
    ohm: 2,
    m49: 3,
};

/** Reverse lookup: code → namespace string. */
const CODE_TO_NAMESPACE = {};
for (const [ns, code] of Object.entries(NAMESPACE_CODES)) {
    CODE_TO_NAMESPACE[code] = ns;
}

/** Number of bits reserved for the relation_id. */
const ID_BITS = 49;
// JS bitwise ops are 32-bit, so we use multiplication instead of shift.
const ID_MULTIPLIER = Math.pow(2, ID_BITS);  // 562949953421312

/**
 * Encode a namespace + relation_id into a single safe integer.
 *
 * @param {string} namespace — 'osm', 'ohm', or 'm49'
 * @param {number} relationId — positive integer (e.g. OSM relation ID)
 * @returns {number} packed integer suitable for GeoJSON `id` / MVT feature ID
 * @throws {Error} if namespace is unknown or relationId is out of range
 */
export function encodeBoundaryId(namespace, relationId) {
    const nsCode = NAMESPACE_CODES[namespace];
    if (nsCode === undefined) {
        throw new Error(`Unknown boundary namespace: "${namespace}"`);
    }
    if (!Number.isInteger(relationId) || relationId < 0 || relationId >= ID_MULTIPLIER) {
        throw new Error(`relation_id out of range: ${relationId}`);
    }
    return nsCode * ID_MULTIPLIER + relationId;
}

/**
 * Decode a packed boundary ID back to namespace + relation_id.
 *
 * @param {number} packedId — the integer from the MVT feature `id`
 * @returns {{ namespace: string, relationId: number }}
 * @throws {Error} if the namespace code is unknown
 */
export function decodeBoundaryId(packedId) {
    if (!Number.isFinite(packedId) || packedId < 0) {
        throw new Error(`Invalid packed boundary ID: ${packedId}`);
    }
    const nsCode = Math.floor(packedId / ID_MULTIPLIER);
    const relationId = packedId % ID_MULTIPLIER;
    // Use Math.round to avoid floating-point drift
    const namespace = CODE_TO_NAMESPACE[nsCode];
    if (!namespace) {
        throw new Error(`Unknown namespace code ${nsCode} in packed ID ${packedId}`);
    }
    return { namespace, relationId: Math.round(relationId) };
}

