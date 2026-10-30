#!/usr/bin/env node
/**
 * Contract check for the Link sheet's pure helpers (place#337): the shipped browser
 * module whg/webpack/js/attest.js, imported as is (no DOM, so only what needs none).
 *
 * What it pins: which ids may be linked; the POST /api/links body (place_a, place_b,
 * relation_type, optional justification; never dataset_id or status, which the
 * server derives); closeMatch as the default (ruling Q1); what each HTTP answer tells
 * the user; the "Linked by" provenance parts.
 *
 * Run with `node scripts/check-attest.mjs`. Exits non-zero listing every failing case.
 * Before any real case it proves its own comparator reports a mismatch.
 */
import path from 'path';
import { fileURLToPath } from 'url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
const mod = await import(path.join(HERE, '..', 'whg', 'webpack', 'js', 'attest.js'));
const { isLinkableId, linkPayload, LinkError, describeLinkResponse, attestationProvenance,
    attestationProvenanceHtml, RELATIONS, DEFAULT_RELATION, JUSTIFICATION_MAX, linkButtonHtml } = mod;

const failures = [];
let checks = 0;
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
function expect(name, got, want, sink = failures) {
    checks++;
    if (!same(got, want)) sink.push(`${name}\n    got  ${JSON.stringify(got)}\n    want ${JSON.stringify(want)}`);
}
function throws(name, fn, re) {
    checks++;
    try {
        fn();
        failures.push(`${name}: did not throw`);
    } catch (e) {
        if (!(e instanceof LinkError) || !re.test(e.message)) failures.push(`${name}: threw ${e && e.message}`);
    }
}

// The comparator itself: a mismatch must be recorded, a match must not.
const probe = [];
expect('probe mismatch', { a: 1 }, { a: 2 }, probe);
expect('probe match', { a: 1 }, { a: 1 }, probe);
if (probe.length !== 1) {
    console.error('check-attest: the comparator cannot tell a mismatch from a match; nothing below can be trusted.');
    process.exit(2);
}

// ids
for (const id of ['wd:Q90', 'gn:2988507', 'whg:12', 'whg:12:a1', 'tgn:7008038', 'WD:Q90']) {
    expect(`linkable ${id}`, isLinkableId(id), true);
}
for (const id of ['', 'Q90', 'wd:', 'wd:None', 'wd:null', 'gn:nan', 'https://www.wikidata.org/wiki/Q90',
    'wd:Q 90', null, undefined, 42, ':Q90']) {
    expect(`not linkable ${JSON.stringify(id)}`, isLinkableId(id), false);
}

// the POST body
expect('default relation is closeMatch', DEFAULT_RELATION, 'closeMatch');
expect('exactMatch is advanced only', [RELATIONS.closeMatch.advanced, RELATIONS.distinct.advanced, RELATIONS.exactMatch.advanced],
    [false, false, true]);
expect('user-visible labels', Object.values(RELATIONS).map((r) => r.label),
    ['Same place', 'Same place, exactly', 'Not the same place']);
expect('payload, default relation', linkPayload({ subject: 'whg:5', target: 'wd:Q90' }),
    { place_a: 'whg:5', place_b: 'wd:Q90', relation_type: 'closeMatch' });
expect('payload, distinct with reason (trimmed)',
    linkPayload({ subject: 'whg:5', target: 'gn:1', relation: 'distinct', justification: '  different river  ' }),
    { place_a: 'whg:5', place_b: 'gn:1', relation_type: 'distinct', justification: 'different river' });
expect('payload, blank reason omitted', Object.keys(linkPayload({ subject: 'whg:5', target: 'gn:1', justification: '   ' })),
    ['place_a', 'place_b', 'relation_type']);
const exact = linkPayload({ subject: 'whg:5', target: 'gn:1', relation: 'exactMatch',
    justification: 'x'.repeat(JUSTIFICATION_MAX) });
expect('payload never carries dataset_id or status', ['dataset_id', 'status', 'legacy_v3_2'].filter((k) => k in exact), []);
expect('reason at the limit is kept', exact.justification.length, JUSTIFICATION_MAX);
throws('reason over the limit', () => linkPayload({ subject: 'whg:5', target: 'gn:1', justification: 'x'.repeat(JUSTIFICATION_MAX + 1) }), /limit is 2000/);
throws('self-link', () => linkPayload({ subject: 'gn:1', target: 'gn:1' }), /itself/);
throws('unknown relation', () => linkPayload({ subject: 'whg:5', target: 'gn:1', relation: 'sameAs' }), /Unknown/);
throws('placeholder target', () => linkPayload({ subject: 'whg:5', target: 'wd:None' }), /identifier/);

// what the user is told
const kind = (s, b, o) => describeLinkResponse(s, b, o).kind;
expect('201', kind(201, { id: 1, status: 'active' }), 'created');
expect('200', kind(200, { id: 1, status: 'active' }), 'exists');
expect('201 pending says so', /visible to you and the dataset owners until it is published/i
    .test(describeLinkResponse(201, { id: 1, status: 'pending' }).message), true);
expect('201 active does not', /visible to you and the dataset owners/i.test(describeLinkResponse(201, { id: 1, status: 'active' }).message), false);
expect('401', kind(401, null), 'signin');
expect('404', kind(404, { error: 'not found' }), 'notfound');
expect('404 text', describeLinkResponse(404, { error: 'not found' }).message,
    'One of these places could not be found or is not visible to you, so they cannot be linked.');
expect('429 uses the server text', describeLinkResponse(429, { error: 'Daily limit of 200 links reached.' }).message,
    'Daily limit of 200 links reached.');
expect('429 without text', kind(429, null), 'limit');
expect('400 uses the server text', describeLinkResponse(400, { error: 'relation_type must be one of …' }),
    { kind: 'invalid', message: 'relation_type must be one of …' });
expect('500', kind(500, null), 'error');
expect('network', kind(0, null), 'error');
expect('withdraw', kind(200, { deleted: 1, status: 'withdrawn' }, { action: 'withdraw' }), 'withdrawn');
expect('vocabulary: no Attest/accession/reconcile in any message',
    [201, 200, 401, 404, 429, 400, 500, 0].map((s) => describeLinkResponse(s, null).message)
        .concat(Object.values(RELATIONS).flatMap((r) => [r.label, r.help]))
        .filter((m) => /attest|accession|reconcil/i.test(m)), []);

// provenance
expect('provenance parts', attestationProvenance({ attested_by: { name: 'Ada Lovelace', orcid: '0000-0002-1825-0097' },
    asserted_at: '2026-10-29T12:00:00Z', status: 'pending', mine: false }),
{ name: 'Ada Lovelace', orcid: 'https://orcid.org/0000-0002-1825-0097', date: '2026-10-29', pending: true, mine: false });
expect('a malformed orcid is not linked', attestationProvenance({ attested_by: { name: 'X', orcid: 'javascript:alert(1)' } }).orcid, '');
expect('no attester', attestationProvenance({ attested_by: null }).name, 'a WHG contributor');
const html = attestationProvenanceHtml({ attested_by: { name: '<b>Eve</b>', orcid: '' }, asserted_at: '2026-01-02', status: 'active' });
expect('provenance html is escaped', html.includes('&lt;b&gt;Eve&lt;/b&gt;') && !html.includes('<b>Eve'), true);
expect('provenance html reads "Linked by"', /Linked by .*, 2026-01-02/.test(html), true);

// no DOM: linking is not enabled, so no button
expect('no button without the page switch', linkButtonHtml({ target: 'wd:Q90' }), '');

if (failures.length) {
    console.error(`check-attest: ${failures.length} of ${checks} checks FAILED\n  ${failures.join('\n  ')}`);
    process.exit(1);
}
console.log(`check-attest: all ${checks} checks passed`);
