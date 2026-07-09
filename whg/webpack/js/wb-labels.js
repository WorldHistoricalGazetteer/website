// wb-labels.js — client mirror of the server terminology map (main/labels.py).
//
// v3.3 display-layer relabel (plan-collaborativeCollections §2 & §11): "Dataset" → "Gazetteer",
// "Dataset Collection" → "Gazetteer Group"; "Place Collection" kept. Route every user-facing
// Workbench string that names one of these concepts through LABELS so the wording lives in one
// place. KEEP IN LOCK-STEP with main/labels.py — the two are one source of truth.

export const LABELS = {
  gazetteer: 'Gazetteer',
  gazetteer_plural: 'Gazetteers',
  gazetteer_group: 'Gazetteer Group',
  gazetteer_group_plural: 'Gazetteer Groups',
  place_collection: 'Place Collection',
  place_collection_plural: 'Place Collections',
  itinerary: 'Itinerary',
  itinerary_plural: 'Itineraries',
  map_your_data: 'Map your Data',
  route: 'Route',
  network: 'Network',
  workbench: 'Collaborative Workbench',
};

// Look up a label by key (falls back to the key itself if unknown).
export const label = (key) => (Object.prototype.hasOwnProperty.call(LABELS, key) ? LABELS[key] : key);
