// wb-itinerary.js — Collaborative Workbench editor entry for an Itinerary (doc-type #3).
// plan-collaborativeCollections §4.3: an Itinerary is a Place Collection where the ORDER is
// meaningful — a journey through places. Same storage, same publish path (publish_place_collection
// with sequenced derived server-side from doc_type='itinerary'); the editor just emphasises order.
// Thin entry over the shared wb-collection-editor core.

import '../css/reconciliation.css';
import { mountCollectionEditor } from './wb-collection-editor.js';

// eslint-disable-next-line camelcase, no-undef
__webpack_public_path__ = '/static/webpack/';

mountCollectionEditor({
  docType: 'itinerary',
  dbName: 'whg-wb-itinerary',
  sequenced: true,
  memberWord: 'stop',
  // An itinerary publishes into a Place Collection to the public site; the sequence drives the
  // ordered presentation. Same browse page as a Place Collection.
  browse: (id) => `/collections/${id}/browse_pl`,
});
