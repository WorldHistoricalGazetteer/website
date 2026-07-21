// /whg/webpack/js/toponyms.js
//
// Shared name-variant helpers — ONE implementation of "extract the alternate
// name forms, drop the canonical title, truncate with a '+N more' indicator"
// so toponym variants read consistently wherever they appear: the Atlas cluster
// cards, the Gazetteer Explore place list, and the map popups.
//
// The gateway packs surface forms into a comma-joined ``names[].label`` (and
// sometimes ships plain strings), so extraction splits on commas, trims and
// de-dupes case-insensitively, and always excludes the record's headline title.

function esc(s) {
    if (s == null) return '';
    return String(s)
        .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/**
 * Distinct alternate name strings for a record, EXCLUDING its canonical title.
 * @param {Array} names  ``names[]`` (objects with ``.label`` or plain strings)
 * @param {string} canonicalTitle  the headline name to drop from the list
 * @returns {string[]}
 */
export function variantLabels(names, canonicalTitle) {
    const titleKey = String(canonicalTitle || '').trim().toLowerCase();
    const seen = new Set();
    const out = [];
    (names || []).forEach((n) => {
        const label = (n && typeof n === 'object') ? n.label : n;
        String(label || '').split(',').forEach((t) => {
            const s = t.trim();
            const k = s.toLowerCase();
            if (s && k !== titleKey && !seen.has(k)) { seen.add(k); out.push(s); }
        });
    });
    return out;
}

/**
 * Truncated inline variants HTML, mirroring the cluster cards' rule: up to 5
 * names shown inline; beyond that the first 3 plus a "+N more" indicator. The
 * full set is offered via the container's ``title`` tooltip (so it works in a
 * fixed-height virtualised row that can't expand). Returns '' for an empty list.
 *
 * @param {string[]} variants
 * @param {object} [opts]
 * @param {string} [opts.className]  extra class on the wrapper (per-site styling)
 * @param {string} [opts.prefix]     small lead word rendered before the names (e.g. "also")
 * @param {boolean}[opts.tooltip]    set false to omit the full-list title tooltip
 *                                   (redundant when the names are shown in full)
 */
export function variantsHtml(variants, opts = {}) {
    if (!variants || !variants.length) return '';
    const cls = 'toponym-variants' + (opts.className ? ' ' + opts.className : '');
    const lead = opts.prefix ? `<span class="tv-lead">${esc(opts.prefix)}</span> ` : '';
    let inner;
    if (variants.length <= 5) {
        inner = `<span class="tv-names">${variants.map(esc).join(', ')}</span>`;
    } else {
        const shown = variants.slice(0, 3).map(esc).join(', ');
        const more = variants.length - 3;
        inner = `<span class="tv-names">${shown}</span><span class="tv-more">+${more} more</span>`;
    }
    const titleAttr = (opts.tooltip === false) ? '' : ` title="${esc(variants.join(', '))}"`;
    return `<div class="${cls}"${titleAttr}>${lead}${inner}</div>`;
}

export default { variantLabels, variantsHtml };
