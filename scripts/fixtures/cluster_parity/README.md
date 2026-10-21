# Clusterer parity fixtures (place#325)

Inputs and oracle outputs that prove `search/clustering.py` equals `whg/webpack/js/clustering.js`
(`npm run test:cluster-parity`, `search.tests_cluster_parity`). Regenerate the oracle with
`npm run test:cluster-parity -- --write` after ANY change to `clustering.js` or `locus_resolve.js`.

The three `real_*` fixtures were captured read-only on 2026-10-21 from the WHG gateway (request
bodies and dates are in each file's `note`) and trimmed of geometries and unused fields. They contain
records from authority gazetteers only: GeoNames (gn), Wikidata (wd), OpenStreetMap (osm),
OpenHistoricalMap (ohm) and Getty TGN (tgn). Each of these namespaces is a published,
`redistributable` row in WHG's gazetteer registry (checked 2026-10-21); the data remain the
property of, and are attributed to, their sources under their own licences (see https://whgazetteer.org/licenses/).
No contributed (dataset) records are included. The rest of the fixtures are synthetic.
