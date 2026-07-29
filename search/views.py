# various search.views

import logging
import re
from datetime import datetime

import simplejson as json
import sys
from django.conf import settings
from django.http import JsonResponse
from django.views.generic import View
from django.views.generic.base import TemplateView

from api.reconcile import DOCS_URL
from elastic.health import index_available
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset
from datasets.tasks import get_bounds_filter
from places.models import Place, PlaceGeom
from sitemap.models import Toponym
from utils.regions_countries import get_regions_countries
from whg.settings import STANDARD_FIELDS

logger = logging.getLogger(__name__)


def index_health(request):
    """Lightweight liveness check of the Elasticsearch indexing server.

    Consumed by the client-side sniffer on the search page, which disables the
    UI when the index is unreachable (without it, search is non-functional).
    Unauthenticated and deliberately cheap: a short-timeout request to the
    cluster health endpoint. Always returns HTTP 200 so the client only needs
    to read the JSON `status` field.
    """
    return JsonResponse({'status': 'up' if index_available() else 'down'})


# new
class SearchPageView(TemplateView):
    template_name = 'search/search.html'

    def get(self, request, *args, **kwargs):
        if request.GET.get('filter'):
            return JsonResponse({
                "result": [],
                "message": "OpenRefine legacy search call: no results. Use /suggest/entity endpoint instead. See documentation: " + DOCS_URL
            })

        return super().get(request, *args, **kwargs)

    def get_context_data(self, *args, **kwargs):
        # return list of datasets
        dslist = Dataset.objects.filter(public=True)

        context = super(SearchPageView, self).get_context_data(*args, **kwargs)

        context['toponym'] = kwargs.get('toponym', None)

        if context['toponym']:
            toponym_qs = Toponym.objects.filter(name=context['toponym'])

            if toponym_qs.exists():
                toponym = toponym_qs.first()

                yearspans = toponym.yearspans
                if yearspans:
                    earliest_date = min(span[0] for span in yearspans)
                    latest_date = max(span[1] for span in yearspans)
                else:
                    earliest_date, latest_date = None, None  # Handle case with no dates

                context['unique_countries'] = toponym.ccodes
                context['yearspans'] = yearspans
                context['earliest_date'] = earliest_date
                context['latest_date'] = latest_date
            else:
                # Handle the case where the toponym does not exist in the database table
                context['toponym'] = None

        # Ensure the temporal/spatial context vars always exist so the template's
        # {% if earliest_date %} / {% if unique_countries %} guards don't emit a
        # noisy VariableDoesNotExist on every search render (place#120).
        context.setdefault('earliest_date', None)
        context.setdefault('latest_date', None)
        context.setdefault('unique_countries', None)
        context.setdefault('yearspans', None)

        context['media_url'] = settings.MEDIA_URL
        context['dslist'] = dslist
        context['search_params'] = self.request.session.get('search_params')
        context['es_whg'] = settings.ES_WHG
        # context['bboxes'] = bboxes
        context['dropdown_data'] = get_regions_countries()  # Used for spatial filter

        context['adv_filters'] = [
            ["A", "Administrative entities"],
            ["P", "Cities, towns, hamlets"],
            ["S", "Sites, buildings, complexes"],
            ["R", "Roads, routes, rail..."],
            ["L", "Regions, landscape areas"],
            ["T", "Terrestrial landforms"],
            ["H", "Water bodies"],
        ]

        user_areas = []
        if self.request.user.is_authenticated:
            qs = Area.objects.filter(owner=self.request.user, type__in=['ccodes', 'copied', 'drawn']).values('id',
                                                                                                             'title',
                                                                                                             'type',
                                                                                                             'description',
                                                                                                             'geojson')
            for a in qs:
                feature = {
                    "type": "Feature",
                    "properties": {"id": a['id'], "title": a['title'], "type": a['type'],
                                   "description": a['description']},
                    "geometry": a['geojson']
                }
                user_areas.append(feature)

        context['has_areas'] = len(user_areas) > 0
        context['user_areas'] = user_areas

        return context


# ── Gazetteers-panel row enrichment (place#136) ─────────────────────────────
# The Atlas Gazetteers offcanvas surfaces, per gazetteer: a colour-coded licence
# badge, a citation tool, an expandable metadata section, and a download button.
# These helpers derive the display data from the registry ``.values()`` dicts.

def _licence_pill(row):
    """Server-side twin of ``gazetteerInteraction.js::licenceClass`` +
    ``renderAttribution`` for the offcanvas rows. Returns a small dict
    (``cls``/``code``/``deed``/``tip``) or ``None`` when there's nothing to show.
    Kept in sync with the JS: any change to the colour buckets there should be
    mirrored here (and vice versa)."""
    spdx = row.get('license__spdx_id') or ''
    label = row.get('license__label') or ''
    deed = row.get('license__url') or row.get('license_url') or ''
    rights = row.get('rights_holder') or ''
    if not spdx and not label and not rights:
        return None
    up = spdx.upper()
    pc = row.get('license__permits_commercial')
    ar = row.get('license__attribution_required')
    custom = row.get('license__custom')
    if not spdx and not label:
        cls = 'lic-unknown'
    elif ('CC0' in up or 'PDDL' in up or up.startswith('PUBLIC')
          or (pc is True and ar is False)):
        cls = 'lic-open'
    # `permits_commercial` is tri-state: None means the source grants nothing
    # either way. Tested BEFORE the open/restrictive buckets so it can never be
    # read as a permission, and kept distinct from them so it doesn't read as a
    # prohibition either (place#157).
    elif pc is None:
        cls = 'lic-unknown'
    elif pc is False or '-NC' in up or 'ARR' in up or custom is True:
        cls = 'lic-restrictive'
    else:
        cls = 'lic-attribution'
    tip = [label or spdx or 'Licence not specified']
    if rights:
        tip.append('© ' + rights)
    if deed:
        tip.append('Click for licence terms')
    return {'cls': cls, 'code': spdx or '©', 'deed': deed, 'tip': ' · '.join(tip)}


def _csl_json(row):
    """A minimal CSL-JSON object for the citation tool (citationFormatter.js
    reads it off ``data-csl-json`` and adds the accessed date + URL client-side).
    Built from the registry attribution fields; ``type: dataset``."""
    csl = {'type': 'dataset', 'title': row.get('name') or ''}
    if row.get('rights_holder'):
        csl['publisher'] = row['rights_holder']
    if row.get('source_url'):
        csl['URL'] = row['source_url']
    authors = [{'literal': c['name']} for c in (row.get('contributors_csl') or [])
               if isinstance(c, dict) and c.get('name')]
    if authors:
        csl['author'] = authors
    elif row.get('rights_holder'):
        csl['author'] = [{'literal': row['rights_holder']}]
    return json.dumps(csl)


def _dom_slug(prefix, s):
    """A DOM-id / selector-safe slug for a gazetteer row (e.g. ``ref-gn`` or
    ``con-whg-657``); registry ids like ``whg:657`` contain a colon."""
    return f"{prefix}-{re.sub(r'[^a-z0-9]+', '-', str(s).lower()).strip('-')}"


def _authority_download(row):
    """Download affordance for an *authority* gazetteer row. Authorities live in
    the CRC gateway (no local queryset to stream), so the button is always shown
    but disabled — the tooltip explains why, driven by the place#136 flags.
    ``downloadable`` datasets (Specialist Gazetteers) get a real, enabled button
    via ``_specialist_download`` instead."""
    reason = row.get('download_blocked_reason') or ''
    if row.get('downloadable') is False:
        if reason == 'licence-restricted':
            tip = ('Not available for download — licence-restricted. '
                   'Available for search & reconciliation.')
        elif reason == 'volume-exceeds-cap':
            tip = ('Not available for download — dataset too large to re-host. '
                   'Available for search & reconciliation.')
        else:
            tip = ('Not available for download. '
                   'Available for search & reconciliation.')
    else:
        tip = ('Bulk download of this authority isn’t offered here yet — its '
               'records are available for search & reconciliation.')
    return {'enabled': False, 'entity': '', 'filetypes': [], 'tip': tip}


def _specialist_download(row, dataset_downloadable, user_authenticated):
    """Download affordance for a Specialist Gazetteer (a WHG-namespaced local
    ``Dataset``, registry id ``whg:<pk>``). Real, enabled download via the shared
    ``/entity/dataset:<pk>/api`` streaming endpoint when the dataset is
    downloadable and the user is signed in."""
    reg_id = str(row.get('id') or '')
    pk = reg_id.split(':', 1)[1] if ':' in reg_id else ''
    if not pk or dataset_downloadable is None:
        return {'enabled': False, 'entity': '', 'filetypes': [],
                'tip': 'Download unavailable.'}
    if dataset_downloadable is False:
        return {'enabled': False, 'entity': '', 'filetypes': [],
                'tip': ('Not available for download — obtain it from its '
                        'original source.')}
    if not user_authenticated:
        return {'enabled': False, 'entity': f'dataset:{pk}', 'filetypes': [],
                'tip': 'Sign in to download this gazetteer.'}
    return {
        'enabled': True,
        'entity': f'dataset:{pk}',
        'filetypes': ['lpf', 'tsv'],
        'tip': 'Download this gazetteer as Linked Places Format (LPF) or TSV.',
    }


class AtlasPageView(TemplateView):
    """
    The Atlas (Explorer) page — a hero-map spatio-temporal explorer
    served at /atlas/, parallel to the existing /search/ page.
    """
    template_name = 'search/atlas.html'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context['toponym'] = kwargs.get('toponym', None)
        context['media_url'] = settings.MEDIA_URL
        context['es_whg'] = settings.ES_WHG
        context['dropdown_data'] = get_regions_countries()
        context['index_places'] = getattr(settings, 'INDEX_PLACES_LABEL', 'millions of')
        context['index_toponyms'] = getattr(settings, 'INDEX_TOPONYMS_LABEL', 'millions of')

        # Available spatial data sources for the Layer Sources palette,
        # driven by ``GazetteerRegistryEntry.region_source``. Staff toggle
        # entries on/off via the Django admin (api/admin.py); previously
        # this list was hardcoded inline. See migration 0005 for seeds.
        from api.models import GazetteerRegistryEntry
        # Region Source list is driven solely by the ``region_source`` flag.
        # ``no_explore`` is independent — it gates the Gazetteers offcanvas
        # Explore mode (see atlas.js::applyTilesetGating), not this panel.
        # Display order matches the panel's existing order so admin toggles
        # don't visually reshuffle. Unknown ids fall to the end alphabetically.
        REGION_SOURCE_ORDER = ['osm', 'ohm', 'osm_misc', 'po', 'clio', 'nl']
        region_source_rows = (
            GazetteerRegistryEntry.objects
            .filter(region_source=True, status='published')
            .values('id', 'name', 'description')
        )
        ordered = sorted(
            region_source_rows,
            key=lambda r: (
                REGION_SOURCE_ORDER.index(r['id']) if r['id'] in REGION_SOURCE_ORDER else len(REGION_SOURCE_ORDER),
                r['id'],
            ),
        )
        context['available_sources'] = json.dumps([
            {
                'id': row['id'],
                'label': row['name'],
                'description': row['description'] or '',
                'enabled': True,
            }
            for row in ordered
        ])

        user_areas = []
        if self.request.user.is_authenticated:
            qs = Area.objects.filter(
                owner=self.request.user,
                type__in=['ccodes', 'copied', 'drawn'],
            ).values('id', 'title', 'type', 'description', 'geojson')
            for a in qs:
                feature = {
                    "type": "Feature",
                    "properties": {
                        "id": a['id'], "title": a['title'],
                        "type": a['type'], "description": a['description'],
                    },
                    "geometry": a['geojson'],
                }
                user_areas.append(feature)

        context['has_areas'] = len(user_areas) > 0
        context['user_areas'] = json.dumps(user_areas)

        # Gazetteer registry — the standard offcanvas list (authorities) and
        # the Specialist Gazetteers expansion (WHG-namespaced datasets).
        # Falls back to an empty queryset before the inventory push has run;
        # the data migration in api/migrations/0003 seeds the ten current
        # authorities so the page renders even on a fresh DB.
        # Attribution / licence / download fields feed the per-row licence
        # badge, citation tool, metadata section and download button (place#136).
        _reg_fields = (
            'citation_text', 'rights_holder', 'source_url', 'contributors_csl',
            'license__spdx_id', 'license__label', 'license__url', 'license_url',
            'license__permits_commercial', 'license__attribution_required',
            'license__custom',
            # Download-legality + volume flags (place#136).
            'downloadable', 'redistributable', 'download_blocked_reason',
            # temporal_extent is tiny ([min, max]) so it's inlined for the
            # metadata section; h3_coverage_coarse stays out (served/cached
            # separately via atlas_registry_coverage()).
            'temporal_extent',
        )
        gazetteer_inventory = list(
            GazetteerRegistryEntry.objects
            .filter(entry_class='authority')
            .order_by('name')
            .values(
                'id', 'name', 'description', 'namespace', 'core',
                'no_explore', 'gazetteer_type', 'record_count', *_reg_fields,
            )
        )
        for row in gazetteer_inventory:
            row['licence_pill'] = _licence_pill(row)
            row['csl_json'] = _csl_json(row)
            row['download'] = _authority_download(row)
            row['gaz_group'] = 'reference'
            row['dom_id'] = _dom_slug('ref', row.get('id'))

        specialist_gazetteers = list(
            GazetteerRegistryEntry.objects
            .filter(entry_class='dataset', namespace='whg')
            .order_by('name')
            .values(
                'id', 'name', 'description', 'namespace', 'gazetteer_type',
                'record_count', *_reg_fields,
            )
        )
        # Resolve local Dataset.downloadable for the specialist rows (registry id
        # ``whg:<pk>``) so their download buttons reflect the real dataset state.
        _spec_pks = []
        for row in specialist_gazetteers:
            rid = str(row.get('id') or '')
            if ':' in rid and rid.split(':', 1)[1].isdigit():
                _spec_pks.append(int(rid.split(':', 1)[1]))
        _ds_downloadable = dict(
            Dataset.objects.filter(pk__in=_spec_pks)
            .values_list('pk', 'downloadable')
        )
        _authed = self.request.user.is_authenticated
        for row in specialist_gazetteers:
            rid = str(row.get('id') or '')
            pk = int(rid.split(':', 1)[1]) if (
                ':' in rid and rid.split(':', 1)[1].isdigit()) else None
            row['licence_pill'] = _licence_pill(row)
            row['csl_json'] = _csl_json(row)
            row['download'] = _specialist_download(
                row, _ds_downloadable.get(pk), _authed)
            row['gaz_group'] = 'contributed'
            row['dom_id'] = _dom_slug('con', row.get('id'))
            # contributors_csl is a JSON list; the json_script payload keeps it,
            # but the licence FK spanned fields are only needed for the pill and
            # can be dropped to keep the payload lean.
            for k in ('license__permits_commercial',
                      'license__attribution_required', 'license__custom'):
                row.pop(k, None)

        context['gazetteer_inventory'] = gazetteer_inventory
        context['specialist_gazetteers'] = specialist_gazetteers

        # My Gazetteers (place#136): the signed-in user's OWN registry entries,
        # enriched like the others and grouped by status into Published /
        # Private / Pending sections (empty sections show a placeholder in the
        # template). Shown under the "Mine" group pill.
        my_gazetteers = {'published': [], 'private': [], 'pending': []}
        if _authed:
            mine = list(
                GazetteerRegistryEntry.objects
                .filter(owner=self.request.user)
                .order_by('name')
                .values('id', 'name', 'description', 'namespace',
                        'gazetteer_type', 'record_count', 'status', *_reg_fields)
            )
            _mine_pks = [int(str(r['id']).split(':', 1)[1]) for r in mine
                         if ':' in str(r.get('id', ''))
                         and str(r['id']).split(':', 1)[1].isdigit()]
            _mine_ds = dict(
                Dataset.objects.filter(pk__in=_mine_pks)
                .values_list('pk', 'public')
            )
            _mine_dl = dict(
                Dataset.objects.filter(pk__in=_mine_pks)
                .values_list('pk', 'downloadable')
            )
            _PENDING = {'draft', 'submitted', 'pending', 'rejected'}
            for row in mine:
                rid = str(row.get('id') or '')
                pk = int(rid.split(':', 1)[1]) if (
                    ':' in rid and rid.split(':', 1)[1].isdigit()) else None
                row['licence_pill'] = _licence_pill(row)
                row['csl_json'] = _csl_json(row)
                row['download'] = _specialist_download(row, _mine_dl.get(pk), True)
                row['gaz_group'] = 'mine'
                row['dom_id'] = _dom_slug('mine', rid)
                status = row.get('status') or 'published'
                if status in _PENDING:
                    section = 'pending'
                    row['status_badge'] = {
                        'cls': _STATUS_BADGE_CLS.get(status, 'bg-secondary'),
                        'label': status,
                    }
                elif _mine_ds.get(pk) is False:
                    section = 'private'
                    row['status_badge'] = {'cls': 'bg-secondary', 'label': 'private'}
                else:
                    section = 'published'
                    row['status_badge'] = {'cls': 'bg-success', 'label': 'published'}
                my_gazetteers[section].append(row)
        context['my_gazetteers'] = my_gazetteers

        # Version stamp for the client's IndexedDB coverage cache — the browser
        # only re-fetches atlas_registry_coverage() when this changes.
        context['registry_version'] = _registry_version()
        return context


# Bootstrap badge classes per registry status, for the My-Gazetteers section
# badges (place#136).
_STATUS_BADGE_CLS = {
    'published': 'bg-success',
    'submitted': 'bg-info text-dark',
    'pending':   'bg-info text-dark',
    'draft':     'bg-warning text-dark',
    'rejected':  'bg-danger',
}


def _registry_version() -> str:
    """A cheap change-stamp for the authority registry: the latest updated_at
    plus the row count. `updated_at` is auto_now, so any inventory push bumps it,
    invalidating the client's cached coverage maps."""
    from django.db.models import Max, Count
    from api.models import GazetteerRegistryEntry
    agg = (GazetteerRegistryEntry.objects
           .filter(entry_class='authority')
           .aggregate(m=Max('updated_at'), n=Count('id')))
    return f"{agg['m'].isoformat() if agg['m'] else '0'}|{agg['n']}"


def atlas_registry_coverage(request):
    """Coverage maps for the Atlas Gazetteers filters, cached client-side in
    IndexedDB keyed by ``version``:
      { version, temporal: {ns: [earliest, latest]}, h3: {ns: "global"|[cells]} }
    Moved out of the inline page context so the ~200 KB coarse-H3 payload is
    fetched once per registry version rather than on every Atlas page load."""
    from api.models import GazetteerRegistryEntry
    rows = (GazetteerRegistryEntry.objects
            .filter(entry_class='authority')
            .values('namespace', 'temporal_extent', 'h3_coverage_coarse'))
    temporal = {}
    h3 = {}
    for g in rows:
        ns = g['namespace']
        temporal[ns] = g['temporal_extent'] or []
        h3[ns] = g['h3_coverage_coarse'] if g['h3_coverage_coarse'] else []
    return JsonResponse({
        'version': _registry_version(),
        'temporal': temporal,
        'h3': h3,
    })


def atlas_status(request):
    """Lightweight CRC-gateway liveness for the Atlas UI, so it can warn about
    limited functionality when the gateway (Pitt CRC VM) is unreachable.

    ``{"gateway": true|false}`` — ``false`` when the VM is down/unconfigured or
    the user can't use the gateway. Deliberately cheap (short-timeout probe) so
    it can be polled on page load without blocking. Not beta-gated beyond
    requiring auth: a signed-in user gets an honest status either way."""
    if not request.user.is_authenticated:
        return JsonResponse({"gateway": False})
    from api.crc_client import crc_health
    return JsonResponse({"gateway": bool(crc_health(user=request.user))})


def atlas_search(request):
    """Atlas search — proxy to the CRC gateway ``POST /api/search`` with the
    clustering fuel, returning the full response for the browser clusterer
    (whg/webpack/js/clustering.js).

    Routes Atlas through the gateway (via Django, since the Pitt firewall only
    admits the DO app server) instead of the legacy Django ``/search/index/``
    ES path. BETA-gated while the client-side clustering UI is built.
    """
    if not (request.user.is_authenticated and request.user.can_access_beta):
        return JsonResponse({"error": "beta access required"}, status=403)
    if request.method != "POST":
        return JsonResponse({"error": "POST required"}, status=405)
    try:
        options = json.loads(request.body or b"{}")
    except (ValueError, TypeError):
        options = {}

    from api.crc_client import crc_search
    data = crc_search(options, user=request.user)
    if data is None:
        # Gateway unconfigured / unreachable — empty but well-formed so the
        # client renders "no results" rather than erroring.
        return JsonResponse({
            "hits": [], "total": 0, "edges": [],
            "clustering_params": None, "toponym_stoplist": [], "gateway": False,
        })
    data["gateway"] = True
    return JsonResponse(data)


def atlas_place(request):
    """Dynamic Atlas portal — resolve one place on demand from the CRC gateway
    (`/api/places`) and enrich it with the source's registry attribution.

    Per-place, not per-cluster: Atlas clusters are dynamic/client-side with no
    persistent cluster_id, so the portal resolves a place by id and the browser
    supplies the live cluster context. BETA-gated.
    """
    if not (request.user.is_authenticated and request.user.can_access_beta):
        return JsonResponse({"error": "beta access required"}, status=403)
    pid = request.GET.get("id") or request.GET.get("pid")
    if not pid:
        return JsonResponse({"error": "missing id"}, status=400)

    from api.crc_client import crc_places
    data = crc_places([pid], user=request.user)
    places = (data or {}).get("places") or []
    if not places:
        return JsonResponse({"error": "not found", "id": pid}, status=404)
    place = places[0]

    # Enrich with the source authority's attribution (registry, per-namespace).
    ns = place.get("namespace") or (pid.split(":", 1)[0] if ":" in pid else "")
    from api.attribution import registry_attribution
    attribution = registry_attribution(ns)
    if attribution:
        place["attribution"] = attribution
    return JsonResponse(place)


def fetchArea(request):
    aid = request.GET.get('pk')
    area = Area.objects.filter(id=aid)
    return JsonResponse(area)


def makeGeom(pid, geom):
    # TODO: account for non-point
    geomset = []
    if len(geom) > 0:
        for g in geom:
            geomset.append(
                {"type": g['location']['type'],
                 "coordinates": g['location']['coordinates'],
                 "properties": {"pid": pid}}
            )
    return geomset


"""
  format search result items
"""


def suggestionItem(s):
    h = s['hit']
    unique_children = list(set(h['children']))
    timespans = []
    for span in h.get('timespans', []):
        if 'gte' in span and 'lte' in span:
            timespans.append([span['gte'], span['lte']])
        else:
            timespans.append(span)
    item = {
        "whg_id": h.get('whg_id', ''),
        "pid": h['place_id'],
        "index": s['_index'],
        "children": unique_children,
        "linkcount": s['linkcount'],
        "title": h['title'],
        "variants": [n for n in h['searchy'] if n != h['title']],
        "ccodes": h['ccodes'],
        "fclasses": h['fclasses'],
        # TODO: 'label' is an AAT value; sourceLabel is probably preferred if available
        "types": [t['label'] for t in h.get('types', []) if 'label' in t],
        "geom": makeGeom(h['place_id'], h['geoms']),
        "timespans": timespans
    }
    return item


"""
  performs the ES search of 'whg' and 'pub'
"""


def suggester(q, indices):
    # print('suggester q', q)
    # print('suggester indices', indices)
    try:
        es = settings.ES_CONN
    except:
        logger.debug(f'ES query failed: {sys.exc_info()}')

    suggestions = []

    try:
        # Search across multiple indices
        res = es.search(index=','.join(indices), body=q)
        hits = res['hits']['hits']
        if len(hits) > 0:
            for h in hits:
                suggestions.append(
                    {
                        "_id": h['_id'],
                        "_index": h['_index'],
                        "linkcount": len(set(h['_source']['children'])),
                        "hit": h['_source'],
                        "timespans": h['_source'].get('timespans', [])
                    }
                )
    except Exception as e:
        logger.debug(f"ES query failed: {e}")
        return []

    sortedsugs = sorted(suggestions, key=lambda x: x['linkcount'], reverse=True)
    # print('sortedsugs', sortedsugs)
    # TODO: there may be parents and children
    return sortedsugs


def TypeaheadSuggestions(request):
    """
    Typeahead suggestions using SearchViewV3 query building logic
    """
    q = request.GET.get('q', '')
    mode = request.GET.get('mode', 'default')

    if not q:
        return JsonResponse([], safe=False)

    # Build minimal params for SearchViewV3
    params = {
        "qstr": q,
        "mode": mode,
    }

    # Use SearchViewV3's query builder
    query_body = SearchViewV3.build_search_query(params)
    query_body["size"] = 20  # Limit typeahead results

    es = settings.ES_CONN
    indices = [settings.ES_WHG, settings.ES_PUB]

    try:
        res = es.search(index=','.join(indices), body=query_body)
        hits = res['hits']['hits']

        # Extract unique titles for typeahead
        unique_titles = list({
            hit['_source']['title']
            for hit in hits
            if 'title' in hit['_source']
        })

        return JsonResponse(unique_titles, safe=False)

    except Exception as e:
        logger.error(f"Typeahead ES query failed: {e}")
        return JsonResponse([], safe=False)


class SearchViewV3(View):
    """
    /search/index/?
    Performs Elastic search with extended parameters.
    """

    @staticmethod
    def build_search_query(params):
        qstr = params["qstr"]
        fields = STANDARD_FIELDS
        # Lexical search component (affects score)
        search_query = {"multi_match": {"query": qstr, "fields": fields, "fuzziness": "AUTO"}}

        # Initialize the query with the lexical search in the 'must' clause
        q = {
            "size": 100,
            "query": {
                "bool": {
                    "must": [
                        search_query
                    ],
                    "filter": [
                        # Required for all records
                        {"exists": {"field": "whg_id"}}
                    ]
                }
            }
        }

        # Reference the filter list for easier manipulation
        filters = q['query']['bool']['filter']

        # --- 1. FEATURE CLASSES (FCLASS) ---
        if params.get("fclasses"):
            fclist = params["fclasses"].split(',')
            fclist.append('X')  # Include the 'Other/Unknown' class
            filters.append({"terms": {"fclasses": fclist}})

        # --- 2. TEMPORAL FILTERS ---
        if params.get("temporal"):
            current_year = datetime.now().year
            start_year = str(params["start"])
            end_year = str(params.get("end", current_year))
            temporal_range = {"range": {"timespans": {"gte": start_year, "lte": end_year}}}

            if params.get("undated"):
                filters.append({
                    "bool": {"should": [temporal_range, {"bool": {"must_not": {"exists": {"field": "timespans"}}}}]}
                })
            else:
                filters.append(temporal_range)

        # --- 3. COUNTRY CODES ---
        if params.get("countries"):
            countries = params["countries"]
            filters.append({"terms": {"ccodes": countries}})

        # --- 4. GEOMETRY FILTERS (Bounds/User Areas) ---
        geometry_filters = []

        if params.get("bounds"):
            bounds = params["bounds"]["geometries"]
            for geometry in bounds:
                geometry_filters.append({
                    "geo_shape": {
                        "geoms.location": {
                            "shape": {
                                "type": geometry['type'],
                                "coordinates": geometry['coordinates']
                            },
                            "relation": "intersects"
                        }
                    }
                })

        if params.get("userareas"):
            userareas = params["userareas"]
            for userarea_id in userareas:
                # Fetch user area by ID (assuming Area model is available)
                user_area = Area.objects.filter(id=userarea_id).values('geojson').first()
                if user_area:
                    geometry_filters.append({
                        "geo_shape": {
                            "geoms.location": {
                                "shape": user_area['geojson'],
                                "relation": "intersects"
                            }
                        }
                    })

        if len(geometry_filters) > 0:
            # Geometry constraints are combined using a strict 'should' clause (must match at least one area)
            filters.append({"bool": {"should": geometry_filters, "minimum_should_match": 1}})

        return q

    @staticmethod
    def handle_request(request):
        """
        args in request.POST:
            [string] qstr: query string
            [string] mode: search mode (starts, in, fuzzy)
            [string] idx: index to be queried
            [string] fclasses: filter on geonames class (A,H,L,P,S,T)
            [string] temporal: text of boolean for consideration of temporal parameters
            [string] start: filter for timespans
            [string] end: filter for timespans
            [string] undated: text of boolean for inclusion of undated results
            [string] bounds: text of JSON geometry
            [string] countries: text of JSON cccodes array
            [string] userareas: text of JSON userareas array
        """

        if request.method == 'GET':
            return JsonResponse({'error': 'GET requests are not allowed for this endpoint'}, status=400)

        qstr = request.POST.get('qstr')
        idx = request.POST.get('idx') or settings.ES_WHG
        fclasses = request.POST.get('fclasses')
        temporal = request.POST.get('temporal')
        start = request.POST.get('start')
        end = request.POST.get('end')
        undated = request.POST.get('undated')
        bounds = request.POST.get('bounds')
        spatial = request.POST.get('spatial')
        regions = request.POST.get('regions')
        countries = request.POST.get('countries')
        userareas = request.POST.get('userareas')
        mode = request.POST.get('mode')

        params = {
            "qstr": qstr,
            "idx": idx,
            "fclasses": fclasses,
            "temporal": temporal,
            "start": start,
            "end": end,
            "undated": undated,
            "bounds": bounds,
            "spatial": spatial,  # Spatial search mode (pass-through as not used in search)
            "regions": regions,  # Array of region codes (pass-through as not used in search)
            "countries": countries,  # Array of country codes
            "userareas": userareas,  # Array of userarea ids
            "method": request.method,
            "mode": mode
        }
        request.session["search_params"] = params

        if params.get("fclasses") == "":  # Return empty result if no feature classes are given
            result = {'parameters': params, 'suggestions': []}
        else:
            q = SearchViewV3.build_search_query(params)
            suggestions = suggester(q, [idx, 'pub'])
            suggestions = [suggestionItem(s) for s in suggestions]
            result = {'parameters': params, 'suggestions': suggestions}

        return JsonResponse(result, safe=False)

    def post(self, request):
        # Attempt to parse and decode any JSON data from the request body
        try:
            json_data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)

        # Merge any JSON data with request.POST
        request.POST = request.POST.copy()
        request.POST.update(json_data)

        return self.handle_request(request)


class SearchView(View):
    @staticmethod
    def handle_request(request):
        # gets calling ip, useful for django-debug-toolbar
        """
          args in request.GET:
            [string] qstr: query string
            # [string] doc_type: place or trace
            # [string] scope: suggest or search
            [string] idx: index to be queried
            [int] year: filter for timespans including this
            [string[]] fclasses: filter on geonames class (A,H,L,P,S,T)
            [string] temporal: text of boolean for temporal filtering
            [int] start: filter for timespans
            [int] end: filter for timespans
            [string] undated: text of boolean for inclusion of undated results
            [string] bounds: text of JSON geometry
            [string] countries: text of JSON cccodes array
            [string] userareas: text of JSON userareas array
        """
        qstr = request.GET.get('qstr') or request.POST.get('qstr')
        mode = request.GET.get('mode') or request.POST.get('mode')
        idx = settings.ES_WHG
        fclasses = request.GET.get('fclasses') or request.POST.get('fclasses')
        temporal = request.GET.get('temporal') or request.POST.get('temporal')
        start = request.GET.get('start') or request.POST.get('start')
        end = request.GET.get('end') or request.POST.get('end')
        undated = request.GET.get('undated') or request.POST.get('undated')
        bounds = request.GET.get('bounds') or request.POST.get('bounds')
        countries = request.GET.get('countries') or request.POST.get('countries')
        userareas = request.GET.get('userareas') or request.POST.get('userareas')

        params = {
            "qstr": qstr,
            "mode": mode,
            "idx": idx,
            "fclasses": fclasses,
            "temporal": temporal,
            "start": start,
            "end": end,
            "undated": undated,
            "bounds": bounds,
            "countries": countries,  # Array of country codes
            "userareas": userareas  # Array of userarea codes
        }
        request.session["search_params"] = params

        q = {"size": 100,
             "query": {"bool": {
                 "must": [
                     {"exists": {"field": "whg_id"}},
                     {"multi_match": {
                         "query": qstr,
                         "fields": ["title^3", "names.toponym", "searchy"]
                     }}
                 ]
             }}}

        if fclasses:
            fclist = fclasses.split(',')
            fclist.append('X')
            q['query']['bool']['must'].append({"terms": {"fclasses": fclist}})

        if start:
            timespan_filter = {"range": {"timespans": {"gte": start, "lte": end if end else 2005}}}
            if undated:  # Include records with empty timespans
                q['query']['bool']['must'].append({
                    "bool": {"should": [timespan_filter, {"bool": {"must_not": {"exists": {"field": "timespans"}}}}]}
                })
            else:
                q['query']['bool']['must'].append(timespan_filter)

        if bounds:
            if request.method == 'GET':
                bounds = json.loads(bounds)
                # q['query']['bool']["filter"] = get_bounds_filter(bounds, 'whg')
                q['query']['bool']["filter"] = get_bounds_filter(bounds, settings.ES_WHG)
            elif request.method == 'POST' and len(bounds['geometries']) > 0:
                filters = []
                for geometry in bounds['geometries']:
                    filters.append({
                        "geo_shape": {
                            "geoms.location": {
                                "shape": {
                                    "type": geometry['type'],
                                    "coordinates": geometry['coordinates']
                                },
                                "relation": "intersects"
                            }
                        }
                    })
                q['query']['bool']["filter"] = {"bool": {"should": filters}}

        if countries:
            if request.method == 'GET':
                countries = json.loads(countries)
            q['query']['bool']['must'].append({
                "terms": {
                    "ccodes": countries
                }
            })

        suggestions = suggester(q, [idx, 'pub'])
        suggestions = [suggestionItem(s) for s in suggestions]
        # print('suggestions', suggestions)
        # return query params for ??
        result = {'parameters': params, 'suggestions': suggestions}

        return JsonResponse(result, safe=False)

    def get(self, request):
        return self.handle_request(request)

    def post(self, request):
        # Extract JSON data from the request body
        try:
            json_data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)

        # Merge JSON data with request.POST for compatibility with existing code
        request.POST = request.POST.copy()
        request.POST.update(json_data)

        # Call the handle_request method
        return self.handle_request(request)


# speculative, adding mode to search
class SearchViewNew(View):
    @staticmethod
    def build_search_query(params, search_type):
        qstr = params["qstr"]
        fields = ["title^3", "names.toponym", "searchy"]

        # Default search query
        search_query = {"multi_match": {
            "query": qstr,
            "fields": fields
        }}

        # Modify search query based on search_type
        if search_type == "starts":
            search_query = {"bool": {"should": [{"prefix": {field: qstr}} for field in fields]}}
        elif search_type == "in":
            search_query = {"bool": {"should": [{"wildcard": {field: f"*{qstr}*"}} for field in fields]}}
        elif search_type == "fuzzy":
            search_query = {"multi_match": {
                "query": qstr,
                "fields": fields,
                "fuzziness": 2
            }}

        # Construct the full query with additional filters
        q = {
            "size": 100,
            "query": {"bool": {
                "must": [
                    {"exists": {"field": "whg_id"}},
                    search_query
                ]
            }}
        }

        if params.get('fclasses'):
            fclist = params.fclasses.split(',')
            fclist.append('X')
            q['query']['bool']['must'].append({"terms": {"fclasses": fclist}})

        if params.get('start'):
            timespan_filter = {"range": {"timespans": {"gte": params.start, "lte": params.end if params.end else 2005}}}
            if params.undated:  # Include records with empty timespans
                q['query']['bool']['must'].append({
                    "bool": {"should": [timespan_filter, {"bool": {"must_not": {"exists": {"field": "timespans"}}}}]}
                })
            else:
                q['query']['bool']['must'].append(timespan_filter)

        if params.get('bounds'):
            if params.request.method == 'GET':
                bounds = json.loads(params.bounds)
                # q['query']['bool']["filter"] = get_bounds_filter(bounds, 'whg')
                q['query']['bool']["filter"] = get_bounds_filter(bounds, settings.ES_WHG)
            elif params.request.method == 'POST' and len(params.bounds['geometries']) > 0:
                filters = []
                for geometry in params.bounds['geometries']:
                    filters.append({
                        "geo_shape": {
                            "geoms.location": {
                                "shape": {
                                    "type": geometry['type'],
                                    "coordinates": geometry['coordinates']
                                },
                                "relation": "intersects"
                            }
                        }
                    })
                q['query']['bool']["filter"] = {"bool": {"should": filters}}

        if params.get('countries'):
            if params.request.method == 'GET':
                countries = json.loads(params.countries)
            q['query']['bool']['must'].append({
                "terms": {
                    "ccodes": countries
                }
            })

        return q

    @staticmethod
    def handle_request(request):
        """
          args in request.GET:
            [string] qstr: query string
            # [string] doc_type: place or trace
            # [string] scope: suggest or search
            [string] idx: index to be queried
            [int] year: filter for timespans including this
            [string[]] fclasses: filter on geonames class (A,H,L,P,S,T)
            [int] start: filter for timespans
            [int] end: filter for timespans
            [string] undated: text of boolean for inclusion of undated results
            [string] bounds: text of JSON geometry
            [string] countries: text of JSON cccodes array
        """
        qstr = request.GET.get('qstr') or request.POST.get('qstr')
        idx = settings.ES_WHG
        fclasses = request.GET.get('fclasses') or request.POST.get('fclasses')
        start = request.GET.get('start') or request.POST.get('start')
        end = request.GET.get('end') or request.POST.get('end')
        undated = request.GET.get('undated') or request.POST.get('undated')
        bounds = request.GET.get('bounds') or request.POST.get('bounds')
        countries = request.GET.get('countries') or request.POST.get('countries')
        mode = request.GET.get('mode') or request.POST.get('mode')

        params = {
            "qstr": qstr,
            "idx": idx,
            "fclasses": fclasses,
            "start": start,
            "end": end,
            "undated": undated,
            "bounds": bounds,
            "countries": countries,  # Array of country codes
            "method": request.method,
            "mode": mode
        }
        request.session["search_params"] = params

        q = SearchViewNew.build_search_query(params, params.get('mode', 'default'))
        suggestions = suggester(q, [idx, 'pub'])
        suggestions = [suggestionItem(s) for s in suggestions]
        # return query params for ??
        result = {'parameters': params, 'suggestions': suggestions}

        return JsonResponse(result, safe=False)

    def get(self, request):
        return self.handle_request(request)

    def post(self, request):
        # Extract JSON data from the request body
        try:
            json_data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)

        # Merge JSON data with request.POST for compatibility with existing code
        request.POST = request.POST.copy()
        request.POST.update(json_data)

        # Call the handle_request method
        return self.handle_request(request)


"""
  executes search on db.places /search/db
"""


class SearchDatabaseView(View):
    @staticmethod
    def get(request):
        pagesize = 200
        """
          args in request.GET:
            [string] name: query string
            [string] fclasses: geonames class (A,H,L,P,S,T)
            [int] year: within place.minmax timespan
            [string] bounds: text of JSON geometry
            [int] dsid: dataset.id
            
        """
        name = request.GET.get('name')
        name_contains = request.GET.get('name_contains') or None
        fclasses = request.GET.get('fclasses').split(',')
        year = request.GET.get('year')
        bounds = request.GET.get('bounds')
        dsid = request.GET.get('dsid')
        ds = Dataset.objects.get(pk=int(dsid)) if dsid else None

        from django.contrib.gis.geos import GEOSGeometry
        if bounds:
            bounds = json.loads(bounds)
            area = Area.objects.get(id=bounds['id'][0])
            ga = GEOSGeometry(json.dumps(area.geojson))

        qs = Place.objects.filter(dataset__public=True)

        if bounds:
            qs = qs.filter(geoms__geom__within=ga)

        if fclasses and len(fclasses) < 7:
            qs.filter(fclasses__overlap=fclasses)

        if name_contains:
            qs = qs.filter(title__icontains=name_contains)
        elif name and name != '':
            # qs = qs.filter(title__istartswith=name)
            qs = qs.filter(names__jsonb__toponym__istartswith=name).distinct()

        qs = qs.filter(dataset=ds.label) if ds else qs
        count = len(qs)

        filtered = qs[:pagesize]

        # normalizes place.geoms objects for results display
        def dbsug_geoms(pobjs):
            suglist = []
            for p in pobjs:
                g = p.jsonb
                if 'citation' in g: del g['citation']
                g['src'] = 'db'
                g["properties"] = {"pid": p.place_id, "title": p.title}
                suglist.append(g)
            return suglist

        # mimics suggestion items from SearchView (index)
        suggestions = []
        for place in filtered:
            ds = place.dataset
            try:
                suggestions.append({
                    "pid": place.id,
                    "ds": {"id": ds.id, "label": ds.label, "title": ds.title},
                    "name": place.title,
                    "variants": [n.jsonb['toponym'] for n in place.names.all()],
                    "ccodes": place.ccodes,
                    "fclasses": place.fclasses,
                    "types": [t.jsonb['sourceLabel'] or t.jsonb['src_label'] for t in place.types.all()],
                    "geom": dbsug_geoms(place.geoms.all())
                })
            except:
                logger.debug(f"db sugbuilder error: {place.id} {sys.exc_info()}")

        result = {'get': request.GET, 'count': count, 'suggestions': suggestions}
        return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})


'''
  returns 8000 index docs in current map viewport
  OR if task == 'count': count of features in area
'''


def contextSearch(idx, doctype, q, task):
    es = settings.ES_CONN
    count_hits = 0
    result_obj = {"hits": []}
    # TODO: convert calling map(s) to MapLibre.js to handle large datasets
    if task == 'count':
        res = es.count(index=idx, body=q)
        return {'count': res['count']}
    elif task == 'features':
        res = es.search(index=idx, body=q, size=8000)
    hits = res['hits']['hits']
    # TODO: refactor this bit
    if len(hits) > 0:
        # print('contextSearch() hit0 _source: ',hits[0]["_source"])
        for hit in hits:
            count_hits += 1
            if idx.startswith("whg"):
                # why normalize here?
                result_obj["hits"].append(hit['_source'])
            else:
                # this is traces
                result_obj["hits"].append(hit["_source"]['body'])
    result_obj["count"] = count_hits
    return result_obj


class FeatureContextView(View):
    @staticmethod
    def get(request):
        """
        args in request.GET:
            [string] idx: index to be queried
            [string] extent: geometry to intersect
            [string] doc_type: 'place' in this case
            [string] task: 'features' or 'count'
        """
        idx = request.GET.get('idx')
        extent = request.GET.get('extent')  # coordinates string
        doctype = request.GET.get('doc_type')
        task = request.GET.get('task')
        q_context_all = {"query": {
            "bool": {
                "must": [{"match_all": {}}],
                "filter": {"geo_shape": {
                    "geoms.location": {
                        "shape": {
                            "type": "polygon",
                            "coordinates": json.loads(extent)
                        },
                        "relation": "within"
                    }
                }}
            }
        }}
        response = contextSearch(idx, doctype, q_context_all, task)
        return JsonResponse(response, safe=False)


''' 
  Returns places in a trace body
'''


def getGeomCollection(idx, doctype, q):
    # q includes list of place_ids from a trace record
    es = settings.ES_CONN
    # try:
    # res = es.search(index='whg', body=q, size=300)
    res = es.search(index=settings.ES_WHG, body=q, size=300)
    # res = es.search(index='whg', body=q, size=300)
    # except:
    # print(sys.exc_info()[0])
    hits = res['hits']['hits']
    # geoms=[]
    collection = {"type": "FeatureCollection", "feature_count": len(hits), "features": []}
    for h in hits:
        if len(h['_source']['geoms']) > 0:
            # print('hit _source from getGeomCollection',h['_source'])
            collection['features'].append(
                {"type": "Feature",
                 "geometry": h['_source']['geoms'][0]['location'],
                 "properties": {
                     "title": h['_source']['title']
                     , "place_id": h['_source']['place_id']
                     , "whg_id": h['_id']
                 }
                 }
            )
    # print(str(len(collection['features']))+' features')
    return collection


class CollectionGeomView(View):
    @staticmethod
    def get(request):
        # print('CollectionGeomView GET:',request.GET)
        """
        args in request.GET:
            [string] coll_id: collection to be queried
        """
        coll_id = request.GET.get('coll_id')
        coll = Collection.objects.get(id=coll_id)
        pids = [p.id for p in coll.places_all]
        placegeoms = PlaceGeom.objects.filter(place_id__in=pids)
        features = [{"type": "Feature",
                     "geometry": pg.jsonb,
                     "properties": {"pid": pg.place_id, "title": pg.title}
                     } for pg in placegeoms]
        fcoll = {"type": "FeatureCollection", "features": features}

        return JsonResponse(fcoll, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


class TraceGeomView(View):
    @staticmethod
    def get(request):
        # print('TraceGeomView GET:',request.GET)
        """
        args in request.GET:
            [string] idx: index to be queried
            [string] search: whg_id
            [string] doc_type: 'trace' in this case
        """
        idx = request.GET.get('idx')
        trace_id = request.GET.get('search')
        doctype = request.GET.get('doc_type')
        q_trace = {"query": {"bool": {"must": [{"match": {"_id": trace_id}}]}}}

        # using contextSearch() to get bodyids (i.e. place_ids)
        bodies = contextSearch(idx, doctype, q_trace, 'features')['hits'][0]

        bodyids = [b['place_id'] for b in bodies if b['place_id']]
        q_geom = {"query": {"bool": {"must": [{"terms": {"place_id": bodyids}}]}}}
        geoms = getGeomCollection(idx, doctype, q_geom)
        geoms['bodies'] = bodies

        return JsonResponse(geoms, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})
        # return JsonResponse(geoms, safe=False)
