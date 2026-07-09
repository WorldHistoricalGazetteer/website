// wb-place-collection.js — Collaborative Workbench editor entry for a Place Collection (doc-type #2).
// plan-collaborativeCollections §4.1. Thin entry: the whole editor lives in the shared
// wb-collection-editor core (also used by the Itinerary editor). Local-first (IndexedDB); publishes
// into the existing collection.Collection via the beta-gated publish endpoint. A Place Collection
// references places that EXIST in WHG (§8.1) — external (non-indexed) candidates are flagged and
// reported unresolved at publish time rather than forging place identity.

import '../css/reconciliation.css';
import { mountCollectionEditor } from './wb-collection-editor.js';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

mountCollectionEditor({
  docType: 'place_collection',
  dbName: 'whg-wb-place-collection',
  sequenced: false,
  memberWord: 'place',
  browse: (id) => `/collections/${id}/browse_pl`,
});
