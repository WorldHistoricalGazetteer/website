// clustering-embed.js — Workbench self-embed path for client-side clustering.
//
// clustering.js scores the name signal (s.n) from each record's `phon_emb`
// (an int8 128-vector), agnostic to where it came from:
//   • Atlas    — the CRC gateway ships precomputed `names[].phon_emb`
//                (include_embeddings=true); no worker, no client model.
//   • Workbench — the user's records are private and never reach the server, so
//                there is nothing to ship; the browser self-embeds them here via
//                the same Symphonym worker already loaded for Map-your-Data
//                reconciliation (embedNames → int8, quantised to match the
//                gateway's KNN vectors, so both paths score alike).
//
// This module is the Workbench half of that abstraction: embed records' toponyms
// locally and attach `phon_emb` in the shape clustering.js reads, so the same
// clusterHits() runs identically on public (Atlas) and private (Workbench) data.

import { embedNames } from './recon-symphonym.js';

const defaultNameOf = (r) =>
	r.title || r.name ||
	(Array.isArray(r.names) && r.names[0] && (r.names[0].label || r.names[0].toponym)) ||
	'';

/**
 * Embed each record's representative toponym locally and attach `phon_emb`
 * (a plain `number[128]`, int8-quantised) to it, in place. Records with no
 * name are left untouched (their name signal simply degrades out in the
 * scorer). Returns the same records array.
 *
 * @param {Array}    records          the private records to cluster
 * @param {object}   [opts]
 * @param {function} [opts.nameOf]    record → toponym string (default: title/name/names[0])
 * @param {string}   [opts.lang]      language hint for language-conditioned embedding
 * @param {function} [opts.onProgress](done,total) progress callback
 */
export async function attachSelfEmbeddings(records, opts = {}) {
	const nameOf = opts.nameOf || defaultNameOf;
	const idx = [];
	const names = [];
	records.forEach((r, i) => {
		const n = (nameOf(r) || '').trim();
		if (n) { idx.push(i); names.push(n); }
	});
	if (!names.length) return records;

	// Int8Array(names.length * 128) — one 128-d row per name, in input order.
	const flat = await embedNames(names, { lang: opts.lang, onProgress: opts.onProgress });
	for (let k = 0; k < idx.length; k++) {
		const start = k * 128;
		// Plain Array (not Int8Array) so clustering.js's Array.isArray(phon_emb)
		// check matches — same shape as the gateway's JSON-decoded vectors.
		records[idx[k]].phon_emb = Array.from(flat.subarray(start, start + 128));
	}
	return records;
}

export default { attachSelfEmbeddings };
