// wb-labels.js — client mirror of the server terminology map (main/labels.py).
//
// Naming rule (2026-09-02; site-wide 2026-11-17): a contributor brings a "dataset"; once reconciled
// and published it is a "gazetteer" ("Gazetteer Group" for a group of published ones); the
// authorities matched against are "source gazetteers". "Place Collection" kept. Route every user-facing
// Workbench string that names one of these concepts through LABELS so the wording lives in one
// place. KEEP IN LOCK-STEP with main/labels.py — the two are one source of truth.

export const LABELS = {
  dataset: 'Dataset',
  dataset_plural: 'Datasets',
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
