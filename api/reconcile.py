# whg/api/reconcile.py

"""
WHG Gazetteer Reconciliation API

Provides endpoints for place reconciliation and property extension according to Reconciliation Service API v0.2.
Supports POST requests to /reconcile/ to retrieve candidate places with canonical names, alternative names,
normalized match scores, exact match flags, and full GeoJSON geometries.
Also includes /reconcile/extend/propose for suggesting additional properties and /reconcile/extend for computing
property values. Authentication is via API token or session/CSRF.
Batch requests are supported.

See documentation: https://docs.whgazetteer.org/content/technical/apis.html#reconciliation-service-api
"""

import json
import logging
import math
import os
import re
import urllib

from django.contrib.postgres.search import TrigramSimilarity
from django.db import models
from django.db.models import Count
from django.http import JsonResponse
from django.utils.decorators import method_decorator
from django.views.decorators.csrf import csrf_exempt
from drf_spectacular.utils import extend_schema, extend_schema_view, OpenApiParameter
from geopy.distance import geodesic
from rest_framework.authentication import SessionAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.views import APIView

from periods.models import Period, Chrononym
from datasets.models import Dataset
from places.models import Place
from .authentication import AuthenticatedAPIView, TokenQueryOrBearerAuthentication
from .crc_client import crc_reconcile_search, crc_suggest_search, crc_extend
from .querysets import place_feature_queryset, period_public_queryset
from .reconcile_helpers import make_candidate, format_extend_row, es_search, \
    extract_entity_type, is_crc_place_id, create_type_guessing_dummies, parse_schema, \
    parse_namespaces, parse_delimited_param, filter_hits_by_namespace, WHG_NAMESPACE, \
    resolve_legacy_place_pks
from .schemas import reconcile_schema, propose_properties_schema, suggest_entity_schema, suggest_property_schema, \
    authority_datasets_schema
from .serializers_api import PeriodPreviewSerializer

logger = logging.getLogger('reconciliation')

LOG_MAX_LEN = 2000  # max characters per logged payload/response


def _log_json(label, data):
    """Log a JSON-serialisable object, truncating if needed."""
    try:
        text = json.dumps(data, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        text = str(data)
    if len(text) > LOG_MAX_LEN:
        text = text[:LOG_MAX_LEN] + f"... ({len(text)} chars total)"
    logger.debug("%s: %s", label, text)


DOMAIN = os.environ.get('URL_FRONT', 'https://whgazetteer.org').rstrip('/')
DOCS_URL = "https://docs.whgazetteer.org/content/technical/apis.html#reconciliation-service-api"
TILESERVER_URL = os.environ.get('TILEBOSS', 'https://tiles.whgazetteer.org').rstrip('/')
MAX_EARTH_RADIUS_KM = math.pi * 6371  # ~20015 km
SCHEMA_FILE = "/static/whg_schema.jsonld"
SCHEMA_SPACE = DOMAIN + SCHEMA_FILE
PROPOSE_PROPERTIES, VALID_FCLASSES = parse_schema(f"validation{SCHEMA_FILE}")

SERVICE_METADATA = {
    "versions": ["0.2"],
    "name": "World Historical Gazetteer Reconciliation Service",
    "identifierSpace": DOMAIN + "/",
    "schemaSpace": SCHEMA_SPACE,
    "defaultTypes": [
        {"id": SCHEMA_SPACE + "#Place", "name": "Place"},
        {"id": SCHEMA_SPACE + "#Period", "name": "Period"},
    ],
    "documentation": DOCS_URL,
    "logo": DOMAIN + "/static/images/whg_logo_80.png",
    "view": {  # human-readable page
        "url": DOMAIN + "/entity/{{id}}/",
    },
    "feature_view": {  # machine-readable place representation
        "url": DOMAIN + "/entity/{{id}}/api",
    },
    "preview": {  # HTML preview snippet
        "url": DOMAIN + "/entity/{{id}}/preview?token={{token}}",
        "width": 400,
        "height": 300,
    },
    "suggest": {
        "entity": {
            "service_url": DOMAIN,
            "service_path": "/suggest/entity?token={{token}}",
        },
        "property": {
            "service_url": DOMAIN,
            "service_path": "/suggest/property?token={{token}}",
        }
    },
    "extend": {
        "propose_properties": {
            "service_url": DOMAIN,
            "service_path": "/reconcile/properties"
        },
        "property_settings": [
            {
                "name": "limit",
                "label": "Limit",
                "type": "number",
                "default": 0,
                "help_text": "Maximum number of values to return per row (0 for no limit)"
            },
            {
                "name": "content",
                "label": "Content",
                "type": "select",
                "default": "literal",
                "help_text": "Content type: ID or literal",
                "choices": [
                    {
                        "value": "id",
                        "name": "ID"
                    },
                    {
                        "value": "literal",
                        "name": "Literal"
                    }
                ]
            }
        ]
    },
    "authority_datasets": {
        "service_url": DOMAIN,
        "service_path": "/reconcile/authority-datasets?token={{token}}"
    },
    "batch_size": 50,
    "authentication": {
        "type": "apiKey",
        "name": "token",
        "in": "query",
    }
}


def json_error(message, status=400):
    return JsonResponse({"error": f"{message} See documentation: {DOCS_URL}"}, status=status)


@method_decorator(csrf_exempt, name="dispatch")
@reconcile_schema()
class ReconciliationView(APIView):
    # GET (service metadata) is public; POST requires authentication.
    # Authentication classes must be set at class level for DRF to use them.
    # Anonymous requests will still work for GET; POST checks explicitly below.
    authentication_classes = [TokenQueryOrBearerAuthentication, SessionAuthentication]

    def get(self, request, *args, **kwargs):
        logger.info("GET /reconcile from %s (user=%s)", request.META.get('REMOTE_ADDR'), request.user)

        token = request.GET.get("token")

        metadata = json.loads(json.dumps(SERVICE_METADATA))

        if token:
            def inject_token(obj):
                if isinstance(obj, dict):
                    return {k: inject_token(v) for k, v in obj.items()}
                elif isinstance(obj, str):
                    return obj.replace("{{token}}", token)
                else:
                    return obj

            metadata = inject_token(metadata)

        return JsonResponse(metadata)

    def post(self, request, *args, **kwargs):

        if not request.user or not request.user.is_authenticated:
            logger.warning("POST /reconcile unauthenticated from %s", request.META.get('REMOTE_ADDR'))
            return json_error("Authentication required. Provide a valid API token.", status=401)

        logger.info("POST /reconcile from %s (user=%s, content_type=%s)",
                     request.META.get('REMOTE_ADDR'), request.user, request.content_type)

        try:
            payload = parse_request_payload(request)
        except ValueError as e:
            logger.warning("POST /reconcile parse error from %s: %s", request.META.get('REMOTE_ADDR'), e)
            return json_error(str(e))

        _log_json("POST /reconcile payload", payload)

        # Data extension requests
        extend = payload.get("extend", {})
        if extend:
            entity_ids = extend.get("ids", [])
            logger.info("POST /reconcile extend: %d entity IDs, properties=%s",
                        len(entity_ids), [p.get("id") if isinstance(p, dict) else p
                                          for p in extend.get("properties", [])])
            if not entity_ids:
                logger.debug("POST /reconcile extend: no entity IDs, returning empty")
                return JsonResponse({"rows": {}, "meta": []})

            try:
                entity_type, ids = extract_entity_type(entity_ids)
            except ValueError as e:
                return json_error(str(e))

            properties = extend.get("properties", [])

            rows = {}

            if entity_type == "place":
                # Partition into WHG ids (local DB) and CRC ids (gateway)
                whg_ids = [raw_id for raw_id in ids if not is_crc_place_id(raw_id)]
                crc_ids = [raw_id for raw_id in ids if is_crc_place_id(raw_id)]

                # WHG places — fetch from Django ORM. Rows are keyed by the id the
                # CALLER sent (OpenRefine matches the response against its own ids),
                # which may be any form we have emitted: whg:<dataset_id>:<src_id>,
                # whg:<pk>, or a bare numeric pk.
                if whg_ids:
                    pk_by_raw = resolve_legacy_place_pks(whg_ids)
                    qs = place_feature_queryset(request.user).filter(id__in=set(pk_by_raw.values()))
                    obj_by_pk = {obj.id: obj for obj in qs}
                    for raw, pk in pk_by_raw.items():
                        obj = obj_by_pk.get(pk)
                        if obj:
                            rows[f"place:{raw}"] = format_extend_row(obj, properties, request=request)

                # CRC places — forward to CRC gateway /api/extend
                if crc_ids:
                    crc_full_ids = [f"place:{crc_id}" for crc_id in crc_ids]
                    crc_rows = crc_extend(crc_full_ids, properties, user=request.user)
                    rows.update(crc_rows)

                    # Fill in any IDs that the gateway didn't return
                    for crc_id in crc_ids:
                        key = f"place:{crc_id}"
                        if key not in rows:
                            rows[key] = {
                                (p.get("id") if isinstance(p, dict) else p): []
                                for p in properties
                            }

            else:  # period
                qs = period_public_queryset(request.user).filter(id__in=ids)
                for obj in qs:
                    rows[f"period:{obj.id}"] = format_extend_row(obj, properties, request=request)

            # Meta block required by OpenRefine
            meta = [
                {"id": prop.get("id") if isinstance(prop, dict) else prop,
                 "name": prop.get("id") if isinstance(prop, dict) else prop}
                for prop in properties
            ]

            response_data = {"meta": meta, "rows": rows}
            _log_json("POST /reconcile extend response", response_data)
            return JsonResponse(response_data)

        # Reconciliation queries
        queries = payload.get("queries", {})
        if queries:
            logger.info("POST /reconcile queries: %d queries", len(queries))
            try:
                entity_type, _ = extract_entity_type(queries, from_queries=True)
            except ValueError as e:
                logger.warning("POST /reconcile entity_type error: %s", e)
                return json_error(str(e))

            if not entity_type:
                # Check if any query has real query text — if so, default to "place"
                # rather than assuming this is an OpenRefine type-guessing probe.
                has_real_query = any(
                    q.get("query", "").strip()
                    for q in queries.values()
                    if isinstance(q, dict)
                )
                if has_real_query:
                    entity_type = "place"
                else:
                    # No type and no query text: OpenRefine type guessing — return dummies
                    all_candidates = create_type_guessing_dummies(SERVICE_METADATA)
                    first_query_id = next(iter(queries))
                    results = {
                        first_query_id: {"result": all_candidates}
                    }
                    _log_json("POST /reconcile type-guessing response", results)
                    return JsonResponse(results)

            # Period reconciliation
            batch_size = SERVICE_METADATA.get("batch_size", 50)
            if entity_type == "period":
                if len(queries) > batch_size:
                    queries = dict(list(queries.items())[:batch_size])

                results = {}
                for key, params in queries.items():
                    results[key] = self.reconcile_chrononym_to_period(params)

                _log_json("POST /reconcile period response", results)
                return JsonResponse(results)

            # Place reconciliation
            results = process_queries(queries, batch_size=batch_size, user=request.user)
            _log_json("POST /reconcile place response", results)
            return JsonResponse(results)

        return json_error("Missing 'queries' or 'extend' parameter")

    def reconcile_chrononym_to_period(self, params):
        """Reconcile a single chrononym query to periods"""
        query_text = params.get("query", "").strip()
        limit = min(int(params.get("limit", 5)), 20)
        type_filter = params.get("type")

        if not query_text:
            return {"result": []}

        if type_filter and "period" not in type_filter.lower():
            return {"result": []}

        # Step 1: Find matching chrononyms using the same multi-tier strategy
        matching_chrononyms = []

        # 1. Exact matches (highest priority)
        exact_matches = Chrononym.objects.filter(label__iexact=query_text)
        for chrononym in exact_matches:
            matching_chrononyms.append((chrononym, 100, True))

        # 2. Prefix matches
        if len(matching_chrononyms) < limit * 3:  # Get more candidates than final limit
            prefix_matches = Chrononym.objects.filter(
                label__istartswith=query_text
            ).exclude(
                label__iexact=query_text
            )[:limit * 3]

            for chrononym in prefix_matches:
                score = self.calculate_prefix_score(query_text, chrononym.label)
                matching_chrononyms.append((chrononym, score, False))

        # 3. Trigram similarity matches
        if len(matching_chrononyms) < limit * 3:
            existing_ids = [c[0].id for c in matching_chrononyms]

            similarity_matches = Chrononym.objects.annotate(
                similarity=TrigramSimilarity('label', query_text)
            ).filter(
                similarity__gt=0.3
            ).exclude(
                id__in=existing_ids
            ).order_by('-similarity')[:limit * 2]

            for chrononym in similarity_matches:
                similarity_score = getattr(chrononym, 'similarity', 0)
                score = min(int(similarity_score * 85), 85)
                matching_chrononyms.append((chrononym, score, False))

        # Step 2: Get unique periods from matching chrononyms
        period_scores = {}  # period_id -> (period_obj, best_score, is_match, matching_chrononyms)

        # Pre-fetch periods with their chrononyms to avoid N+1 queries
        chrononym_ids = [c[0].id for c in matching_chrononyms]
        periods_with_chrononyms = Period.objects.filter(
            chrononyms__id__in=chrononym_ids
        ).prefetch_related('chrononyms').distinct()

        # Build lookup from chrononym to periods
        chrononym_to_periods = {}
        for period in periods_with_chrononyms:
            for chrononym in period.chrononyms.all():
                if chrononym.id not in chrononym_to_periods:
                    chrononym_to_periods[chrononym.id] = []
                chrononym_to_periods[chrononym.id].append(period)

        # Process matching chrononyms and aggregate by period
        for chrononym, score, is_match in matching_chrononyms:
            periods = chrononym_to_periods.get(chrononym.id, [])

            for period in periods:
                if period.id not in period_scores:
                    period_scores[period.id] = {
                        'period': period,
                        'best_score': score,
                        'is_match': is_match,
                        'matching_chrononyms': []
                    }
                else:
                    # Update with better score if found
                    if score > period_scores[period.id]['best_score']:
                        period_scores[period.id]['best_score'] = score
                        period_scores[period.id]['is_match'] = is_match

                # Add this chrononym to the matching list
                period_scores[period.id]['matching_chrononyms'].append({
                    'label': chrononym.label,
                    'languageTag': chrononym.languageTag,
                    'score': score
                })

        # Step 3: Format results and sort by score
        results = []
        for period_data in period_scores.values():
            period = period_data['period']

            # Filter out the canonical chrononym from aliases
            canonical = period.chrononym or ""
            aliases = []
            for c in period_data['matching_chrononyms']:
                if c['label'] != canonical:
                    lang_suffix = f" ({c['languageTag']})" if c['languageTag'] else ""
                    aliases.append(f"{c['label']}{lang_suffix}")

            # Create description with period ID and aliases
            if aliases:
                description = f"Period {period.id}. Aliases: {', '.join(aliases)}"
            else:
                description = f"Period {period.id}"

            result = {
                "id": f"period:{period.id}",  # Prefix to distinguish from places
                "name": period.chrononym or period_data['matching_chrononyms'][0]['label'],
                "type": [{
                    "id": f"{SCHEMA_SPACE}#Period",
                    "name": "Period"
                }],
                "score": period_data['best_score'],
                "match": period_data['is_match'],
                "description": description,
            }
            results.append(result)

        # Sort by score and return top results
        results = sorted(results, key=lambda x: x['score'], reverse=True)

        return {"result": results[:limit]}

    def calculate_prefix_score(self, query, label):
        """Calculate score for prefix matches"""
        query_len = len(query)
        label_len = len(label)

        if query_len == label_len:
            return 95
        elif query_len > label_len * 0.8:
            return 90
        elif query_len > label_len * 0.5:
            return 80
        else:
            return 70


def _years_from_label(label):
    """Best-effort [start, stop] years parsed from a PeriodO label. Many labels embed
    their span, e.g. "Ming dynasty, 1368-1644", "AD 1099-1750", "Iron Age (800 BC - AD 43)".
    Returns (start, stop) ints (negative = BCE) or (None, None). Display/seeding only —
    the authoritative bounds live in the gateway record; labels are the cheap fallback."""
    if not label:
        return None, None
    # A dash BETWEEN two year numbers is a range separator, not a minus sign
    # ("1368-1644" → "1368 1644"); a genuine leading "-500" is left intact.
    s = re.sub(r'(\d)\s*[-–—]\s*(?=\d)', r'\1 ', label)
    # Grab signed year tokens, each optionally followed by an era marker.
    toks = re.findall(r'(-?\d{1,4})\s*(BCE|BC|CE|AD)?', s, flags=re.IGNORECASE)
    years = []
    for num, era in toks:
        try:
            y = int(num)
        except ValueError:
            continue
        if era and era.upper() in ('BC', 'BCE') and y > 0:
            y = -y
        years.append(y)
    if not years:
        return None, None
    if len(years) == 1:
        return years[0], years[0]
    return min(years), max(years)


def _bounds_from_extend(value):
    """[begin, end] years from a gateway ``whg:temporal_objects`` extend value, e.g.
    ``[{"str": "{\\"begin\\": 1368, \\"end\\": 1644}"}]``. (None, None) if unavailable."""
    if not value:
        return None, None
    for entry in value:
        s = entry.get('str') if isinstance(entry, dict) else None
        if not s:
            continue
        try:
            o = json.loads(s)
        except (ValueError, TypeError):
            continue
        b, e = o.get('begin'), o.get('end')
        if b is not None or e is not None:
            try:
                return (int(b) if b is not None else None, int(e) if e is not None else None)
            except (ValueError, TypeError):
                continue
    return None, None


@extend_schema(
    tags=["Reconciliation API"],
    summary="Suggest PeriodO periods for a dataset's scope",
    description=(
        "Suggest canonical PeriodO periods for a whole dataset's scope, matched against its "
        "geographic (bounding box / country codes) and temporal (year range) extent and/or a name "
        "fragment. Backed by PeriodO indexed as gazetteer namespace `po` in the CRC gateway. Returns "
        "`{id, uri (PeriodO ARK), label, start, stop, ccodes, coverage, has_geom, score}`. A name-less "
        "query requires a spatial constraint (`bounds` or `contained_in`). Powers the Map-your-Data "
        "Scope picker's period hints."
    ),
    parameters=[
        OpenApiParameter("q", str, description="Period name fragment (matched against period labels)."),
        OpenApiParameter("bounds", str, description="GeoJSON Polygon (JSON string) — the dataset's bounding box."),
        OpenApiParameter("contained_in", str, description="Bare place_id of a WHG region to contain results within."),
        OpenApiParameter("ccodes", str, description="Comma/space-separated ISO-2 country codes (ranking signal)."),
        OpenApiParameter("start", int, description="Start year of the temporal scope (negative = BCE)."),
        OpenApiParameter("end", int, description="End year of the temporal scope (negative = BCE)."),
        OpenApiParameter("limit", int, description="Max results (default 12, max 50)."),
    ],
)
class PeriodSuggestView(APIView):
    """
    Suggest PeriodO periods for a whole dataset's *scope* — matched against the
    dataset's geographic (bounding box / country codes) and temporal (year-range)
    scope and/or an optional name fragment. Powers the "When → historical period"
    hints in the Map-your-Data Scope picker.

    Queries PeriodO as the ES gazetteer namespace ``po`` via the CRC gateway (the
    same infra place reconciliation uses) — so geographic matching runs natively
    against period geometry, and the canonical PeriodO ARK comes from the record id.
    (Superseded the earlier Django-``periods``-DB implementation once ``po`` was
    indexed in the gateway — see developer notes.)

    GET params:
      q            optional name fragment (period label)
      bounds       optional GeoJSON Polygon (the dataset's bounding box) — JSON string
      contained_in optional bare place_id of a WHG region (the scope region)
      ccodes       comma/space ISO-2 codes (the dataset's country scope)
      start / end  integer years (negative = BCE) — the dataset's temporal scope
      limit        max results (default 12, max 50)

    A name-less query needs a spatial constraint (bounds or contained_in); with
    neither q nor a region, returns ``[]`` (the client then offers curated seeds).
    """
    authentication_classes = [SessionAuthentication, TokenQueryOrBearerAuthentication]
    permission_classes = []  # public reference data

    def get(self, request, *args, **kwargs):
        raw_cc = request.GET.get('ccodes', '') or ''
        ccodes = [c for c in (parse_delimited_param(raw_cc, upper=True) or []) if len(c) == 2]

        def _int(name):
            v = request.GET.get(name)
            if v is None or str(v).strip() == '':
                return None
            try:
                return int(float(v))
            except (TypeError, ValueError):
                return None

        start = _int('start')
        end = _int('end')
        q = (request.GET.get('q') or '').strip()
        contained_in = (request.GET.get('contained_in') or '').strip()
        bounds = None
        raw_bounds = request.GET.get('bounds')
        if raw_bounds:
            try:
                b = json.loads(raw_bounds)
                if isinstance(b, dict) and b.get('type') and b.get('coordinates'):
                    bounds = b
            except (ValueError, TypeError):
                bounds = None
        try:
            limit = max(1, min(int(request.GET.get('limit', 12)), 50))
        except (TypeError, ValueError):
            limit = 12

        # The gateway rejects a name-less query that has no spatial constraint.
        if not q and not bounds and not contained_in:
            return JsonResponse({'result': []})

        # 'default' (best_fields) gives the best name recall — incl. single words like "Ming",
        # which 'fuzzy'/'starts' miss. Mode is irrelevant for a name-less (spatial-only) suggest.
        raw = {'namespaces': ['po'], 'mode': 'default' if q else 'fuzzy'}
        if ccodes:
            raw['countries'] = ccodes
        if start is not None:
            raw['start'] = start
        if end is not None:
            raw['end'] = end
        if contained_in:
            raw['contained_in'] = [contained_in]
            raw['containment'] = 'fuzzy'
            raw['relation'] = 'intersects'
        nq = {'query_text': q, 'raw': raw, 'size': min(limit * 4, 100)}
        if bounds:
            nq['bounds'] = bounds

        try:
            hits = crc_reconcile_search(nq, user=request.user)
        except Exception as e:
            logger.warning('PeriodSuggest gateway error: %s', e)
            hits = []

        def temporal_score(pstart, pstop):
            if start is None and end is None:
                return 0.0
            qs_ = start if start is not None else (pstart if pstart is not None else -10**7)
            qe_ = end if end is not None else (pstop if pstop is not None else 10**7)
            if pstart is None or pstop is None or pstop < pstart:
                return 0.0
            overlap = min(qe_, pstop) - max(qs_, pstart)
            if overlap <= 0:
                return 0.0
            return overlap / max(1, max(pstop - pstart, qe_ - qs_))

        results, seen = [], set()
        for h in hits:
            src = h.get('_source', {})
            pid = str(src.get('place_id', ''))          # e.g. "po:p0fp7wv2s8c"
            if not pid or pid in seen:
                continue
            seen.add(pid)
            periodo_id = pid.split(':', 1)[1] if ':' in pid else pid
            label = src.get('title') or ''
            pstart, pstop = _years_from_label(label)
            hit_cc = src.get('ccodes') or []
            results.append({
                'id': f'place:po:{periodo_id}',
                'uri': f'http://n2t.net/ark:/99152/{periodo_id}',
                'label': label,
                'start': pstart,
                'stop': pstop,
                'ccodes': hit_cc,
                'coverage': ', '.join(hit_cc[:6]) if hit_cc else '',
                'has_geom': bool(src.get('has_geom')),
                '_gw': float(h.get('_score') or 0.0),
            })

        def geo_score(r):
            if ccodes and r['ccodes']:
                inter = len(set(r['ccodes']) & set(ccodes))
                if inter:
                    return inter / len(ccodes)
            return 0.0

        # Provisional rank (gateway relevance + geography) → shortlist, so we only fetch temporal
        # bounds for what we'll actually return.
        results.sort(key=lambda r: r['_gw'] + 1.5 * geo_score(r), reverse=True)
        shortlist = results[:limit]

        # Authoritative temporal bounds: PeriodO labels often omit years (e.g. bare "Ming dynasty"),
        # so batch-fetch the real begin/end from the gateway (extend: whg:temporal_objects) for the
        # rows still lacking years, and prefer them over the label parse. A single batched call is safe
        # now that the gateway resolves each id independently (WorldHistoricalGazetteer/place#114 fixed
        # in indexing@8c74228 — null ES fields no longer 500 and poison the whole batch).
        need = [r for r in shortlist if r['start'] is None or r['stop'] is None]
        if need:
            try:
                ext = crc_extend([r['id'] for r in need], [{'id': 'whg:temporal_objects'}],
                                 user=request.user) or {}
            except Exception as e:
                logger.warning('PeriodSuggest temporal extend error: %s', e)
                ext = {}
            for r in need:
                b, e = _bounds_from_extend((ext.get(r['id']) or {}).get('whg:temporal_objects'))
                if b is not None:
                    r['start'] = b
                if e is not None:
                    r['stop'] = e

        # Final rank now that temporal overlap is known.
        for r in shortlist:
            r['score'] = round(r.pop('_gw') + 3.0 * temporal_score(r['start'], r['stop']) + 1.5 * geo_score(r), 3)
        shortlist.sort(key=lambda r: r['score'], reverse=True)
        return JsonResponse({'result': shortlist})


@method_decorator(csrf_exempt, name="dispatch")
@propose_properties_schema()
class ExtendProposeView(APIView):

    def get(self, request, *args, **kwargs):
        return JsonResponse({
            "properties": PROPOSE_PROPERTIES
        })


@method_decorator(csrf_exempt, name="dispatch")
@suggest_entity_schema()
class SuggestEntityView(AuthenticatedAPIView):

    def get(self, request, *args, **kwargs):
        prefix = request.GET.get("prefix", "").strip()

        logger.info("GET /suggest/entity from %s (user=%s, prefix=%r, params=%s)",
                     request.META.get('REMOTE_ADDR'), request.user, prefix,
                     {k: v for k, v in request.GET.items() if k != 'token'})

        if not prefix:
            return JsonResponse({"result": []})

        # --- 1. Get Parameters ---
        exact = request.GET.get("exact", "false").lower() == "true"
        mode = request.GET.get("mode", "all").lower()
        type = request.GET.get("type", "all").lower()
        namespaces = parse_namespaces(request.GET.get("namespaces"))
        ccodes = parse_delimited_param(request.GET.get("countries"), upper=True)
        fclasses = parse_delimited_param(request.GET.get("fclasses"), upper=True)
        types = parse_delimited_param(request.GET.get("types"))

        try:
            limit = int(request.GET.get("limit", 10))
            cursor = int(request.GET.get("cursor", 0))
        except (ValueError, TypeError):
            return json_error("Invalid 'limit' or 'cursor' parameter. They must be integers.")

        # --- 2. Search for Places (Elasticsearch) ---
        place_candidates = []
        if type in ("all", "place"):
            # Legacy WHG search — skip when the caller excluded "whg"
            if namespaces is None or WHG_NAMESPACE in namespaces:
                raw_params = {"query": prefix, "size": 50}
                if ccodes:
                    raw_params["countries"] = ccodes
                if fclasses:
                    raw_params["fclasses"] = fclasses
                if types:
                    raw_params["types"] = types
                query = normalise_query_params(raw_params)
                query["mode"] = "starts" if exact else "fuzzy"

                place_hits = es_search(query=query)

                # Max score is used for normalizing subsequent scores
                max_score = place_hits[0].get("_score", 1.0) if place_hits else 1.0

                for hit in place_hits:
                    candidate = make_candidate(hit, query["query_text"], max_score, SCHEMA_SPACE)
                    place_candidates.append(candidate)

            # --- 2b. CRC gateway search (new places/toponyms indexes) ---
            crc_namespaces = None
            if namespaces is not None:
                crc_namespaces = namespaces - {WHG_NAMESPACE}
            if namespaces is None or crc_namespaces:
                crc_mode = "starts" if exact else "fuzzy"
                crc_hits = crc_suggest_search(prefix, mode=crc_mode, limit=50, user=request.user,
                                              namespaces=crc_namespaces, ccodes=ccodes,
                                              fclasses=fclasses, types=types)
                if crc_hits:
                    crc_max_score = crc_hits[0].get("_score", 1.0)
                    # Deduplicate against existing candidates by name
                    existing_names = {c["name"].lower() for c in place_candidates}
                    for hit in crc_hits:
                        candidate = make_candidate(hit, prefix, crc_max_score, SCHEMA_SPACE)
                        if candidate["name"].lower() not in existing_names:
                            place_candidates.append(candidate)
                            existing_names.add(candidate["name"].lower())

        # --- 3. Search for Periods (Database - Chrononym) ---
        period_candidates = []
        if type in ("all", "period"):
            period_limit = 60  # Limit the database query size

            # Search by prefix or substring using trigram similarity
            similarity_threshold = 0.3  # tweak between 0 (any match) and 1 (exact)
            chrononyms = Chrononym.objects.annotate(
                similarity=TrigramSimilarity('label', prefix)
            ).filter(similarity__gte=similarity_threshold).order_by('-similarity') # Collect all related period IDs

            period_qs = Period.objects.filter(
                chrononyms__in=chrononyms
            ).distinct().prefetch_related( 'chrononyms', 'spatialCoverage', )

            if not mode == "nosort":
                period_qs = period_qs.order_by('chrononym')[:period_limit]

            PERIOD_SCHEMA_ID = SCHEMA_SPACE + "#Period"

            for period in period_qs:
                # Aggregate all chrononyms
                chron_labels = [f"{c.label} (<i>{c.languageTag}</i>)" for c in period.chrononyms.all()]

                # Spatial coverage description
                spatial_desc = period.spatialCoverageDescription or ""

                # Spatial coverage countries
                ccodes = sorted(set(period.ccodes or []))

                # Format start/stop bounds using serializer helpers
                serializer = PeriodPreviewSerializer(period)
                start = serializer.get_start(period) or "Unknown"
                stop = serializer.get_stop(period) or "Unknown"
                bounds_str = f"{start} – {stop}" if start or stop else ""

                # Compose description
                description_parts = []
                if chron_labels:
                    description_parts.append(", ".join(chron_labels))
                if spatial_desc:
                    description_parts.append(spatial_desc)
                if ccodes:
                    description_parts.append("<em>" + "</em>, <em>".join(ccodes) + "</em>")
                if bounds_str:
                    description_parts.append(f'<span class="text-primary">{bounds_str}</span>')
                description = "; ".join(description_parts)

                period_candidates.append({
                    "id": f"period:{period.id}",
                    "name": period.chrononym or period.id,
                    "score": 100,
                    "match": True,
                    "description": description,
                    "type": [{"id": PERIOD_SCHEMA_ID, "name": "Period"}],
                })

        # --- 4. Combine and Sort Results ---
        combined_candidates = place_candidates + period_candidates

        # Sort primarily by score (descending), then alphabetically by name (ascending)
        combined_candidates.sort(key=lambda x: (x.get('score', 0), x.get('name', '')), reverse=True)

        # --- 5. Apply Cursor and Limit (Pagination) ---
        start_index = cursor
        end_index = cursor + limit
        final_candidates = combined_candidates[start_index:end_index]

        # --- 6. Handle JSONP (If present) ---
        callback = request.GET.get('callback')
        results = {"result": final_candidates}

        logger.info("GET /suggest/entity response: %d candidates", len(final_candidates))
        _log_json("GET /suggest/entity response", results)

        if callback:
            response_data = f"{callback}({json.dumps(results)})"
            return JsonResponse(response_data, safe=False, content_type='application/javascript')

        return JsonResponse(results)


@method_decorator(csrf_exempt, name="dispatch")
@suggest_property_schema()
class SuggestPropertyView(AuthenticatedAPIView):

    def get(self, request, *args, **kwargs):
        logger.info("GET /suggest/property from %s (user=%s, params=%s)",
                     request.META.get('REMOTE_ADDR'), request.user,
                     {k: v for k, v in request.GET.items() if k != 'token'})
        try:
            query_text = (request.GET.get("prefix") or request.GET.get("query") or "").strip().lower()
            limit = int(request.GET.get("limit", 10))
            # Get cursor value, defaulting to 0 if not provided
            cursor = int(request.GET.get("cursor", 0))
        except (ValueError, TypeError):
            return json_error("Invalid query parameters")

        # Filter the global constant PROPOSE_PROPERTIES
        if query_text:
            matches = [prop for prop in PROPOSE_PROPERTIES if
                       prop['name'].lower().startswith(query_text)
                       # If prop['id'] contains a colon, check the part after the colon
                       or (':' in prop['id'] and prop['id'].split(':', 1)[1].lower().startswith(query_text))
                       # Also check the full id
                       or prop['id'].lower().startswith(query_text)]
        else:
            matches = PROPOSE_PROPERTIES

        # Apply cursor and limit to the list of matches
        start_index = cursor
        end_index = cursor + limit
        paginated_matches = matches[start_index:end_index]

        return JsonResponse({"result": paginated_matches})


@method_decorator(csrf_exempt, name="dispatch")
@authority_datasets_schema()
class AuthorityDatasetsView(AuthenticatedAPIView):
    """Discovery endpoint for the indexing rebuild's ``whg-places.py``.

    Default behaviour (back-compat) returns only ``authority=True``
    datasets. The indexing rebuild also needs ``ds_status`` + ``public`` +
    ``description`` + ``owner_user_id`` per entry so it can derive the
    ``dataset_status`` / ``dataset_id`` fields written into every staged
    place doc (see ``../indexing/developer/plan-ingestionRebuild.execution.md``
    Batch 4c Phase 4).

    Pass ``?include_pending=true`` to also enumerate datasets that are NOT
    ``authority=True`` but are eligible for the WHG ingestion path under
    ``dataset_status='pending'``. The eligibility rule for pending entries
    is ``ds_status not in {'seed', 'format_error'}`` so half-uploaded /
    invalid datasets stay out of the index.
    """

    def get(self, request, *args, **kwargs):
        logger.info("GET /reconcile/authority-datasets from %s (user=%s)",
                    request.META.get('REMOTE_ADDR'), request.user)

        include_pending = request.GET.get(
            "include_pending", ""
        ).lower() in ("1", "true", "yes")

        if include_pending:
            qs = Dataset.objects.filter(
                models.Q(authority=True)
                | (~models.Q(ds_status__in=("seed", "format_error")))
            )
        else:
            qs = Dataset.objects.filter(authority=True)

        datasets = list(
            qs.annotate(place_count=Count("places", distinct=True))
              .order_by("id")
              .values(
                  "id", "title", "label", "description",
                  "ds_status", "public", "authority",
                  "owner_id", "place_count",
                  # place#121: per-item "view at source" template — flows on to
                  # the registry (indexing whg-places sidecar → inventory push).
                  "web_item",
              )
        )

        # Add a derived ``dataset_status`` so callers don't have to
        # re-implement the (authority/public/ds_status) → published|pending
        # mapping. The ingestion rebuild's whg-places.py reads this.
        for d in datasets:
            published = (
                d.get("authority") is True
                and d.get("public") is True
                and d.get("ds_status") in {"accessioning", "indexed"}
            )
            d["dataset_status"] = "published" if published else "pending"

        results = {"result": datasets}
        _log_json("GET /reconcile/authority-datasets response", results)
        return JsonResponse(results)


@method_decorator(csrf_exempt, name="dispatch")
@extend_schema_view(
    get=extend_schema(exclude=True),
    post=extend_schema(exclude=True),
)
class DummyView(APIView):
    def get(self, request, *args, **kwargs):
        return JsonResponse({
            "result": [],
            "message": "OpenRefine legacy search call: no results. Use /suggest/entity endpoint instead. See documentation: " + DOCS_URL
        })

    def post(self, request, *args, **kwargs):
        return self.get(request, *args, **kwargs)


def parse_request_payload(request):
    """
    Parse request body based on Content-Type.
    - Supports form-encoded 'queries' (OpenRefine style) or 'extend' (OpenRefine data extension).
    - Supports raw JSON body.
    Returns: dict payload
    Raises: ValueError with a message if parsing fails.
    """
    content_type = request.content_type or ""

    # Form-encoded (application/x-www-form-urlencoded)
    if content_type.startswith("application/x-www-form-urlencoded"):
        if "queries" in request.POST:
            queries_param = request.POST["queries"]
            try:
                return {"queries": json.loads(queries_param)}
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON in 'queries' parameter")

        elif "extend" in request.POST:
            extend_raw = request.POST["extend"]
            try:
                extend_json = urllib.parse.unquote_plus(extend_raw)
                return {"extend": json.loads(extend_json)}
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON in 'extend' parameter")

        else:
            raise ValueError("Missing 'queries' or 'extend' parameter in form-encoded request")

    # JSON body
    elif content_type.startswith("application/json"):
        try:
            return json.loads(request.body)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON body")

    else:
        raise ValueError(f"Unsupported Content-Type: {content_type}")


def process_queries(queries, batch_size=50, user=None):
    """
    Enforce batch limit, normalise each query, and return a dict of results.
    """
    messages = []

    if len(queries) > batch_size:
        # Slice rather than reject
        queries = dict(list(queries.items())[:batch_size])
        messages.append(f"Batch size limit exceeded; processing first {batch_size} queries.")
        logger.info("process_queries: batch limit exceeded, truncated to %d", batch_size)

    results = {}
    for key, params in queries.items():
        try:
            query = normalise_query_params(params)
            results[key] = reconcile_place_es(query, user=user)
        except ValueError as e:
            logger.warning("process_queries: query '%s' failed: %s", key, e)
            results[key] = {"error": str(e), "result": []}

    out = {**results, "messages": messages} if messages else dict(results)
    out["attribution"] = _attribution_for_results(results)
    return out


def _attribution_for_results(results):
    """Aggregated source terms for a batch of reconciliation results (place#157).

    Reconciliation is the channel where this matters most: a single response
    blends candidates drawn from many differently-licensed gazetteers, so
    without it a consumer cannot comply with per-source terms even when willing.
    Each candidate carries a ``namespace`` (see ``make_candidate``) that resolves
    against ``sources`` here — or, for legacy ``whg`` candidates, against
    ``datasets``.

    Note the reconciliation response is otherwise a map of query-key → result;
    ``attribution`` is a non-query key alongside the existing ``messages``, and
    clients (OpenRefine included) ignore keys they don't recognise.
    """
    from api.attribution import safe_attribution_block, datasets_from_place_ids

    candidates = [c
                  for r in results.values() if isinstance(r, dict)
                  for c in (r.get("result") or [])]
    namespaces = {c.get("namespace") for c in candidates if c.get("namespace")}
    # Prefer what was SEARCHED over what was returned: a source that matched
    # nothing on this query still has terms the consumer may need, and the
    # gateway reports its scope even on empty result sets (place#157). Union
    # rather than replace, so the legacy-only path — which the gateway never
    # sees — keeps contributing its namespaces.
    for r in results.values():
        if isinstance(r, dict):
            namespaces.update(r.get("namespaces_searched") or ())
    # "place:whg:1234:19799" → "whg:1234:19799"; only WHG places resolve to a
    # contributing dataset, and their identifier now names it directly.
    whg_ids = [str(c.get("id", "")).split(":", 1)[-1]
               for c in candidates if c.get("namespace") == WHG_NAMESPACE]
    return safe_attribution_block(
        namespaces=namespaces,
        datasets=datasets_from_place_ids(whg_ids),
    )


def normalise_query_params(params):
    """
    Validate and normalise a single reconcile query dict.
    Ensures geometry filters, nearby circle, and query_text are handled consistently.

    OpenRefine sends additional column mappings as a ``properties`` array
    inside each query, e.g.::

        {"query": "rome", "properties": [{"pid": "whg:countries_codes", "v": "IT"}]}

    This function extracts recognised filter properties and merges them
    into the top-level params before processing.

    Returns a dict with clean values ready for downstream use.
    """
    # --- Extract OpenRefine-style properties into top-level params ---
    or_props = params.get("properties", [])
    if or_props and isinstance(or_props, list):
        PROPERTY_FILTER_MAP = {
            "whg:namespaces": "namespaces",
            "whg:countries_codes": "countries",
            "whg:classes_codes": "fclasses",
            "whg:types_objects": "types",
        }
        for prop in or_props:
            if not isinstance(prop, dict):
                continue
            pid = prop.get("pid") or prop.get("p") or prop.get("id")
            val = prop.get("v") if "v" in prop else prop.get("value")
            if pid and val is not None and pid in PROPERTY_FILTER_MAP:
                target_key = PROPERTY_FILTER_MAP[pid]
                # Only set if not already explicitly provided at top level
                if target_key not in params:
                    params[target_key] = val

    query_text = params.get("query", "").strip() or None
    size = int(params.get("limit", params.get("size", 100)))

    bounds = None

    # Nearby (circle)
    lat = lng = radius = None
    has_nearby = False
    if all(k in params for k in ("lat", "lng", "radius")):
        try:
            lat = float(params["lat"])
            lng = float(params["lng"])
            radius = float(params["radius"])
            if not (-90 <= lat <= 90):
                raise ValueError("Latitude must be between -90 and 90 degrees.")
            if not (-180 <= lng <= 180):
                raise ValueError("Longitude must be between -180 and 180 degrees.")
            if radius <= 0:
                raise ValueError("Radius must be positive.")
            if radius > MAX_EARTH_RADIUS_KM:
                raise ValueError(f"Radius exceeds maximum allowed distance of {MAX_EARTH_RADIUS_KM:.0f} km.")
            has_nearby = True
        except (ValueError, TypeError):
            raise ValueError("Parameters 'lat', 'lng', 'radius' must be numeric.")

    if has_nearby:
        bounds = circle_to_polygon(lat, lng, radius)
    else:
        # Bounds (GeoJSON geometry)
        if "bounds" in params:
            try:
                raw_bounds = params["bounds"]
                if isinstance(raw_bounds, str):
                    raw_bounds = json.loads(raw_bounds)
                # Accept plain GeoJSON geometry (Polygon, MultiPolygon, etc.)
                if raw_bounds.get("type") in ("Polygon", "MultiPolygon"):
                    bounds = {"type": raw_bounds["type"], "coordinates": raw_bounds["coordinates"]}
                # Accept GeometryCollection with a geometries array
                elif raw_bounds.get("geometries"):
                    geom = raw_bounds["geometries"][0]
                    bounds = {"type": geom["type"], "coordinates": geom["coordinates"]}
                else:
                    raise ValueError(
                        "Bounds must be a GeoJSON Polygon/MultiPolygon or a "
                        "GeometryCollection with a 'geometries' array"
                    )
            except (ValueError, KeyError, IndexError, TypeError) as e:
                raise ValueError(f"Invalid bounds: {e}")

    has_dataset = "dataset" in params
    has_contained_in = bool(params.get("contained_in"))

    if not query_text and not (bounds or has_nearby or has_dataset or has_contained_in):
        raise ValueError("Empty query requires bounds, nearby circle, a contained_in region, or dataset filter.")

    fclasses = None
    if "fclasses" in params:
        raw_fclasses = params["fclasses"]
        if isinstance(raw_fclasses, str):
            raw_fclasses = [x.strip() for x in raw_fclasses.split(",") if x.strip()]
        fclasses = [x.strip().upper() for x in raw_fclasses if x.strip()]
        invalid = [x for x in fclasses if x not in VALID_FCLASSES]
        if invalid:
            raise ValueError(
                f"Invalid feature class(es): {', '.join(invalid)}. "
                f"Allowed values are: {', '.join(sorted(VALID_FCLASSES))}. "
                f"See https://www.geonames.org/source-code/javadoc/org/geonames/FeatureClass.html"
            )

    namespaces = parse_namespaces(params.get("namespaces"))
    types = parse_delimited_param(params.get("types"))

    return {
        "query_text": query_text,
        "size": size,
        "bounds": bounds,
        "lat": lat,
        "lng": lng,
        "radius": radius,
        "has_nearby": bool(has_nearby),
        "has_dataset": bool(has_dataset),
        "fclasses": fclasses,
        "types": types,
        "namespaces": namespaces,
        "raw": params,  # keep original for additional filters
    }


def reconcile_place_es(query, user=None):
    """
    Execute a reconciliation query against Elasticsearch.

    Searches the legacy WHG indexes and, if enabled, the CRC gateway's
    new places/toponyms indexes.  Results are merged, deduplicated, and
    re-ranked by score.

    When ``query["namespaces"]`` is set, only sources whose namespace is
    in the set are searched / returned.  The pseudo-namespace ``"whg"``
    represents legacy WHG places (numeric IDs); any other value (e.g.
    ``"gn"``, ``"tgn"``) is forwarded to the CRC gateway.

    query: dict from normalise_query_params
    user:  Django User instance (used for CRC gateway access check)
    """
    namespaces = query.get("namespaces")  # None ⇒ all

    # A `contained_in` region scope is a HARD constraint (issue #143). The legacy WHG index cannot
    # enforce it — containment targets are gateway-resident place ids with no counterpart here, and the
    # legacy `build_es_query` has no containment field to filter on — so unfiltered legacy hits would
    # leak places OUTSIDE the region and get re-ranked into the results (making scope look like mere
    # weighting). When the gateway path is also serving this query (which DOES enforce containment), we
    # therefore drop the unverifiable legacy hits so the scope is respected. Only when the caller has
    # explicitly restricted to legacy WHG (`namespaces == {"whg"}`, no gateway path) do we keep them,
    # since suppressing would otherwise return nothing. See the gateway-side handoff (place#144).
    contained_in = bool((query.get("raw") or {}).get("contained_in"))
    crc_namespaces = None  # None ⇒ don't filter on the gateway side
    if namespaces is not None:
        crc_namespaces = namespaces - {WHG_NAMESPACE}
    gateway_in_play = namespaces is None or bool(crc_namespaces)
    suppress_legacy = contained_in and gateway_in_play

    # 1. Legacy ES search — skip when the caller excluded "whg", or when an unenforceable containment
    #    scope is active and the gateway will serve the (properly scoped) results.
    legacy_hits = []
    if (namespaces is None or WHG_NAMESPACE in namespaces) and not suppress_legacy:
        legacy_hits = es_search(query=query)
    elif suppress_legacy:
        logger.info("reconcile: suppressing legacy hits — contained_in scope is enforced gateway-side only")

    # 2. CRC gateway search (fail-safe: returns [] on error)
    #    Compute the CRC-specific namespace set (everything except "whg").
    crc_hits = []
    crc_meta = {}
    # Only call the gateway when at least one CRC namespace is wanted
    # (or when no namespace filter was given at all).
    if gateway_in_play:
        crc_hits = crc_reconcile_search(query, user=user, namespaces=crc_namespaces, meta=crc_meta)

    # The gateway fails CLOSED on an explicitly scoped query it cannot constrain: rather than
    # answering with unscoped results it returns none and reports scope.applied = False. Legacy hits
    # can't honour that scope either, so they must not resurrect the leak through the back door —
    # drop them and let the empty result stand as the deliberate answer (place#144).
    scope_info = crc_meta.get("scope")
    # If the gateway never answered but a scope WAS requested, we have already suppressed the legacy
    # hits (they can't honour it), so returning a bare empty list would look like "no matches". Report
    # it as an unapplied scope instead, so the client explains the empty result rather than implying
    # the data is at fault.
    if scope_info is None and crc_meta.get("error") and (contained_in or query.get("bounds")):
        scope_info = {
            "requested": True,
            "applied": False,
            "mode": "none",
            "approximate": False,
            "containers_polygon": [], "containers_linked": [],
            "containers_approximated": [], "containers_unresolved": [],
            "message": "The gazetteer service is temporarily unavailable, so the region you chose "
                       "could not be applied. No results were returned, rather than results from "
                       "outside your chosen region. Please try again shortly.",
        }

    if scope_info is not None and scope_info.get("applied") is False and legacy_hits:
        logger.info("reconcile: dropping legacy hits — gateway reports scope.applied=false")
        legacy_hits = []

    # Response-level metadata passed through to the client. Absent entirely on an older gateway, so
    # the client can distinguish "no scope reported" from "scope not applied".
    extra = {}
    if scope_info is not None:
        extra["scope"] = scope_info
    if crc_meta.get("variants_used") is not None:
        extra["variants_used"] = crc_meta["variants_used"]

    # Which sources were actually searched (place#157). Deriving this from the
    # returned ids under-reports: a source can be searched, match nothing, and
    # still be one whose terms the consumer needs — so the root attribution block
    # is built from this, not from the candidates. The gateway echoes its own
    # scope on every return path, including the empty ones; `[]` from the gateway
    # means unrestricted, in which case the honest answer is the full set it
    # holds, and falling back to whatever the hits happen to show is closer to
    # the truth than claiming to know. Absent on an older gateway → the caller
    # falls back to id-derivation.
    searched = set()
    if namespaces is None or WHG_NAMESPACE in namespaces:
        searched.add(WHG_NAMESPACE)
    gw_searched = crc_meta.get("namespaces_searched")
    if gw_searched:
        searched.update(gw_searched)
    elif crc_namespaces:
        # Gateway didn't echo (older build), but we know what we asked it for.
        searched.update(crc_namespaces)
    if crc_meta.get("namespaces"):
        # Present-in-results, a subset of the above but authoritative when the
        # request was unrestricted and the gateway echoed `[]` for `searched`.
        searched.update(crc_meta["namespaces"])
    if searched:
        extra["namespaces_searched"] = sorted(searched)

    # 3. Merge: legacy first, then CRC
    all_hits = legacy_hits + crc_hits

    if not all_hits:
        return {"result": [], "geojson": None, **extra}

    # 4. Deduplicate by place_id (prefer legacy hits)
    seen_ids = set()
    deduped = []
    for hit in all_hits:
        pid = str(hit.get("_source", {}).get("place_id", hit.get("_id", "")))
        if pid not in seen_ids:
            seen_ids.add(pid)
            deduped.append(hit)

    # 5. Re-sort by score descending
    deduped.sort(key=lambda h: h.get("_score", 0), reverse=True)

    # 6. Trim to requested size
    deduped = deduped[:query.get("size", 100)]

    # 7. Post-filter by namespace (safety net)
    deduped = filter_hits_by_namespace(deduped, namespaces)

    max_score = deduped[0]["_score"] if deduped else 1
    results = []

    for hit in deduped:
        candidate = make_candidate(hit, query["query_text"], max_score, SCHEMA_SPACE)
        results.append(candidate)

    return {
        "result": results,
        **extra,
    }


def circle_to_polygon(lat, lng, radius_km, n_points=32):
    """
    Generate a GeoJSON-like polygon approximating a circle, with edges tangent.
    """
    # Expand radius so polygon edges are tangent
    adjusted_radius = radius_km / math.cos(math.pi / n_points)
    coords = []

    for i in range(n_points):
        bearing = (360 / n_points) * i
        dest = geodesic(kilometers=adjusted_radius).destination((lat, lng), bearing)
        coords.append([dest.longitude, dest.latitude])

    # Close the polygon
    coords.append(coords[0])

    return {
        "type": "Polygon",
        "coordinates": [coords]
    }
