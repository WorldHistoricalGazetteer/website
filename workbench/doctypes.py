"""
Workbench doc-type registry — the extensibility seam for the Collaborative Workbench
(plan-collaborativeCollections §3.2).

One Workbench, many doc-types. Each doc-type declares, in one place:

  * ``label``            — display name (routed through the terminology map, main/labels.py).
  * ``enabled``          — whether users may CREATE this doc-type yet. ``route``/``network`` ship
                           as registry entries with ``enabled=False`` (v4 placeholders, §4.4).
  * ``editor``           — the client editor chunk that mounts for this doc-type (a lazy webpack
                           chunk name; the shell picks it by ``doc_type``).
  * ``validate``         — server-side structural validation of a snapshot of this type. The
                           browser shares an Ajv/jsonschema validator (as Map-your-Data already
                           does for LPF); this is the authoritative server check.
  * ``publish``          — dotted path to the server function that writes a snapshot into the
                           canonical model(s) and triggers re-indexing (workbench/publish.py, §9).
  * ``checkout``         — dotted path to the server function that materialises an already-published
                           item into a snapshot for editing (workbench/checkout.py, §6). ``None``
                           until published-editing is enabled for that doc-type.

Adding a doc-type = one registry entry + one editor chunk + one publish/checkout pair. Nothing
else in the envelope (team, version, share, collab) changes.
"""
from importlib import import_module

from .models import (DOC_RECONCILIATION, DOC_GAZETTEER_GROUP, DOC_PLACE_COLLECTION,
                     DOC_ITINERARY, DOC_PLACE_RECORD, DOC_ROUTE, DOC_NETWORK)


# ── snapshot validators ───────────────────────────────────────────────────────
# Lightweight, dependency-free structural checks. They return a list of human-readable error
# strings (empty ⇒ valid). The client-side Ajv schema is the richer, first-line check; this is the
# server backstop that a determined/broken client can't bypass. Kept deliberately permissive about
# extra keys (snapshots also carry editor UI state) — we validate only what publish depends on.

def _errs_place_collection(snap):
    """Place Collection / Itinerary snapshot:
    ``{title, description?, image?, places:[{id, note?, relation?[], seq?, when?}], keywords?,
       license?, citation?}``. Members MUST resolve to indexed place ids (plan §8.1) — that rule is
    enforced at publish time (unresolved ids are reported), not here."""
    errs = []
    if not isinstance(snap, dict):
        return ['snapshot must be an object']
    if not str(snap.get('title') or '').strip():
        errs.append('a title is required')
    places = snap.get('places')
    if not isinstance(places, list):
        errs.append('"places" must be a list')
    else:
        for i, p in enumerate(places):
            if not isinstance(p, dict) or p.get('id') in (None, ''):
                errs.append(f'place[{i}] must be an object with an "id"')
    return errs


def _errs_itinerary(snap):
    """Itinerary = sequenced Place Collection (§4.3): the Place-Collection shape, but ``seq`` is
    meaningful. We don't hard-require ``seq`` per row (publish falls back to list order), but the
    shape is otherwise identical."""
    return _errs_place_collection(snap)


def _errs_gazetteer_group(snap):
    """Gazetteer Group (was Dataset Collection) snapshot:
    ``{title, description?, gazetteers:[{dataset_id}], keywords?, license?, citation?}``."""
    errs = []
    if not isinstance(snap, dict):
        return ['snapshot must be an object']
    if not str(snap.get('title') or '').strip():
        errs.append('a title is required')
    gaz = snap.get('gazetteers')
    if not isinstance(gaz, list):
        errs.append('"gazetteers" must be a list')
    else:
        for i, g in enumerate(gaz):
            if not isinstance(g, dict) or g.get('dataset_id') in (None, ''):
                errs.append(f'gazetteer[{i}] must be an object with a "dataset_id"')
    return errs


def _errs_place_record_stub(snap):
    """Record-correction snapshot: ``{record_id, title, lng?, lat?, point_editable, …}``. Must carry
    a record_id (the place being corrected). Created only via check-out, so this is a light backstop."""
    if not isinstance(snap, dict):
        return ['snapshot must be an object']
    return [] if snap.get('record_id') else ['a record_id is required']


def _errs_reconciliation(snap):
    """Map-your-Data reconciliation project — the existing free-form browser ``project`` object
    (columns/rows/matches/decisions/geom/scope). It publishes through the /datasets/validate/
    pipeline, not through this registry, so we only sanity-check it is an object."""
    return [] if isinstance(snap, dict) else ['snapshot must be an object']


def _errs_disabled(snap):
    return ['this doc-type is not available yet']


# ── registry ──────────────────────────────────────────────────────────────────
class DocType:
    def __init__(self, key, label_key, enabled, editor, validate,
                 publish=None, checkout=None, sequenced=False):
        self.key = key
        self.label_key = label_key        # key into main.labels.LABELS
        self.enabled = enabled            # may users CREATE this doc-type?
        self.editor = editor              # client chunk name
        self.validate = validate          # fn(snapshot) -> [error str]
        self._publish = publish           # dotted path str or None
        self._checkout = checkout         # dotted path str or None
        self.sequenced = sequenced        # Itinerary: sequence is meaningful

    @property
    def label(self):
        from main.labels import label
        return label(self.label_key)

    def _resolve(self, dotted):
        if not dotted:
            return None
        mod, fn = dotted.rsplit('.', 1)
        return getattr(import_module(mod), fn)

    @property
    def publish_fn(self):
        return self._resolve(self._publish)

    @property
    def checkout_fn(self):
        return self._resolve(self._checkout)


REGISTRY = {
    DOC_RECONCILIATION: DocType(
        DOC_RECONCILIATION, 'map_your_data', enabled=True, editor='reconciliation',
        validate=_errs_reconciliation,
        # Reconciliation publishes via the existing /datasets/validate/ pipeline, not here.
        publish=None, checkout=None),
    DOC_PLACE_COLLECTION: DocType(
        DOC_PLACE_COLLECTION, 'place_collection', enabled=True, editor='wb-place-collection',
        validate=_errs_place_collection,
        publish='workbench.publish.publish_place_collection',
        checkout='workbench.checkout.checkout_place_collection'),
    DOC_ITINERARY: DocType(
        DOC_ITINERARY, 'itinerary', enabled=True, editor='wb-itinerary',
        validate=_errs_itinerary, sequenced=True,
        publish='workbench.publish.publish_place_collection',
        checkout='workbench.checkout.checkout_place_collection'),
    DOC_GAZETTEER_GROUP: DocType(
        DOC_GAZETTEER_GROUP, 'gazetteer_group', enabled=True, editor='wb-gazetteer-group',
        validate=_errs_gazetteer_group,
        publish='workbench.publish.publish_gazetteer_group',
        checkout='workbench.checkout.checkout_gazetteer_group'),
    # Record-level correction (§6.1). Created ONLY via record-level check-out (not the "New…" picker),
    # so enabled=False for creation; checkout + publish + editor are wired.
    DOC_PLACE_RECORD: DocType(
        DOC_PLACE_RECORD, 'place_record', enabled=False, editor='wb-place-record',
        validate=_errs_place_record_stub,
        publish='workbench.publish.publish_place_record',
        checkout='workbench.checkout.checkout_place_record'),
    # ── v4 placeholders (§4.4): reserved doc-types, creation gated OFF. No models, no editors yet.
    DOC_ROUTE: DocType(
        DOC_ROUTE, 'route', enabled=False, editor=None, validate=_errs_disabled),
    DOC_NETWORK: DocType(
        DOC_NETWORK, 'network', enabled=False, editor=None, validate=_errs_disabled),
}


def get(doc_type):
    """Return the DocType for ``doc_type`` or None if unknown."""
    return REGISTRY.get(doc_type)


def validate_snapshot(doc_type, snapshot):
    """Validate ``snapshot`` for ``doc_type``. Returns a list of error strings ([] ⇒ valid).
    Unknown doc-type ⇒ a single 'unknown doc type' error."""
    dt = REGISTRY.get(doc_type)
    if dt is None:
        return [f'unknown doc type: {doc_type}']
    return dt.validate(snapshot)


def creatable():
    """Doc-types users may create right now (enabled), in display order."""
    order = [DOC_PLACE_COLLECTION, DOC_ITINERARY, DOC_GAZETTEER_GROUP, DOC_RECONCILIATION]
    return [REGISTRY[k] for k in order if REGISTRY[k].enabled]
