# api/views_entity.py
import logging
import os
from urllib.parse import quote as urlquote

from django.http import Http404, HttpResponse, HttpResponseRedirect, StreamingHttpResponse
from django.shortcuts import get_object_or_404
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils.decorators import method_decorator
from django.views.decorators.clickjacking import xframe_options_exempt
from django.views.decorators.csrf import csrf_exempt
from django.views.generic import TemplateView
from drf_spectacular.utils import extend_schema
from rest_framework import status
from rest_framework.exceptions import APIException, NotAuthenticated
from rest_framework.permissions import AllowAny
from rest_framework.renderers import JSONRenderer, StaticHTMLRenderer
from rest_framework.response import Response
from rest_framework.throttling import AnonRateThrottle

from api.authentication import AuthenticatedAPIView
from api.crc_client import crc_fetch_places
from api.gateway_errors import GatewayUnavailable, raise_if_gateway_failed
from api.download_file import (
    FileCache, stream_live, stream_from_file,
    build_streaming_download_response,
)
from api.reconcile_helpers import is_crc_place_id, resolve_legacy_place_pk
from api.schemas import entity_schema, TYPE_MAP

logger = logging.getLogger('reconciliation')


def _fetch_crc_place(place_id: str, user=None,
                     allow_anonymous: bool = False,
                     meta: dict | None = None) -> dict | None:
    """Fetch a single CRC place from the gateway, or return None.

    ``allow_anonymous`` is passed through to ``crc_client._is_enabled``, which
    otherwise refuses an unauthenticated caller outright. Set it only for
    persistent-identifier resolution — see ``PublicEntityReadAPIView``.

    ``meta`` is populated in place with ``error`` when the gateway failed. Pass
    it and call ``raise_if_gateway_failed()`` before treating ``None`` as
    "no such record" — see that function for why (place#272).
    """
    result = crc_fetch_places([place_id], user=user,
                              allow_anonymous=allow_anonymous,
                              meta=meta)
    return result.get(place_id)


class SourceNotRedistributable(APIException):
    """451 for a record whose source we may index but may not re-serve (place#269).

    `/entity/place:<id>/api` is the citable, w3id-published representation of an
    authority record, and since place#271 it answers anonymous linked-data
    clients. That makes it a **redistribution** surface, not merely a search one —
    so a source held under terms that forbid re-hosting must not be served through
    it, however freely we may index and search it.

    🛑 The distinction the registry already draws (place#136): indexing, search and
    reconciliation run server-side and never expose raw source data, and are
    permitted for every authority regardless. `redistributable` governs whether WHG
    may hand the source's own content to a third party. This endpoint does exactly
    that.

    ⚠️ **451, not 403.** The refusal is a legal constraint on the content, not a
    permission failure of the caller — no credential would change the answer, and
    403 invites a client to retry with one. 451 also carries a documented
    `Link: rel="blocked-by"` convention, so the rights holder is nameable.

    The body names the source and its terms, because a consumer who cannot have the
    data can still legitimately need to know whose it is and where to get it.
    """

    status_code = 451
    default_code = "source_not_redistributable"

    def __init__(self, namespace="", attribution=None):
        a = attribution or {}
        name = a.get("name") or namespace or "this source"
        detail = {
            "detail": (
                f"{name} is indexed and searchable through WHG, but its terms do not "
                f"permit WHG to redistribute its records. Obtain the data from the "
                f"source under its own terms."
            ),
            "namespace": namespace,
            "source": {
                "name": a.get("name"),
                "rights_holder": a.get("rights_holder"),
                "source_url": a.get("source_url"),
                "license": a.get("license__spdx_id") or a.get("license__label"),
                "license_url": a.get("license__url") or a.get("license_url"),
            },
        }
        super().__init__(detail)


def _namespace_of(crc_place, obj_id):
    return (crc_place or {}).get("namespace") or (
        obj_id.split(":", 1)[0] if ":" in obj_id else "")


def _crc_timespans_to_when(timespans_raw) -> dict:
    """Convert the gateway's ``timespans`` into an LPF ``when``.

    The gateway sends bare integer years — ``[{"start": 1744, "end": 1747}]``,
    negative for BCE — while LPF wants each bound as an object keyed by the
    kind of knowledge it represents. These are the extents an authority
    asserts, not estimates bracketed by uncertainty, so both bounds are ``in``.

    Until 2026-09-10 this was hardcoded to ``{}``, so every authority record
    resolved through a persistent identifier arrived undated even though the
    gateway had sent the dates. That matters most for exactly the sources whose
    content *is* temporal: a Cliopatria polity is one dated extent of a polity
    that had many, and without ``when`` there is nothing to say which.
    """
    timespans = []
    for span in timespans_raw or []:
        if not isinstance(span, dict):
            continue
        bounds = {}
        for bound in ("start", "end"):
            year = span.get(bound)
            if year is None or isinstance(year, bool):
                continue
            if isinstance(year, dict):  # already an LPF bound — pass it through
                bounds[bound] = year
                continue
            try:
                bounds[bound] = {"in": str(int(year))}
            except (TypeError, ValueError):
                continue
        if bounds:
            timespans.append(bounds)
    return {"timespans": timespans} if timespans else {}


def _crc_place_to_lpf(crc_place: dict, request=None) -> dict:
    """
    Convert CRC gateway place data to a Linked Places Format (LPF) feature.
    """
    place_id = str(crc_place.get("place_id", ""))
    title = crc_place.get("title", "")
    names_raw = crc_place.get("names", [])
    ccodes = crc_place.get("ccodes", [])
    fclasses = crc_place.get("fclasses", [])
    types_raw = crc_place.get("types", [])
    geometries = crc_place.get("geometries", [])
    links_raw = crc_place.get("links", [])
    timespans_raw = crc_place.get("timespans", [])

    # Build a proper URI for @id
    if request is not None:
        entity_uri = request.build_absolute_uri(f"/entity/place:{place_id}/api")
    else:
        entity_uri = place_id

    # Build names in LPF format
    names = []
    for n in names_raw:
        label = n.get("label", "")
        if label:
            name_obj = {"toponym": label}
            lang = n.get("lang")
            if lang:
                name_obj["lang"] = lang
            names.append(name_obj)

    # Build geometry
    geojson_geoms = []
    for g in geometries:
        if isinstance(g, dict):
            if g.get("type") and g.get("coordinates"):
                geojson_geoms.append(g)
            elif isinstance(g.get("location"), dict):
                loc = g["location"]
                if loc.get("type") and loc.get("coordinates"):
                    geojson_geoms.append(loc)
    # Fall back to repr_point
    if not geojson_geoms:
        rp = crc_place.get("repr_point")
        if rp and len(rp) == 2:
            geojson_geoms.append({"type": "Point", "coordinates": rp})

    if len(geojson_geoms) == 1:
        geometry = geojson_geoms[0]
    elif geojson_geoms:
        geometry = {"type": "GeometryCollection", "geometries": geojson_geoms}
    else:
        geometry = None

    # Build links in LPF format
    links = []
    for link in links_raw:
        if isinstance(link, dict):
            links.append(link)
        elif isinstance(link, str):
            links.append({"type": "closeMatch", "identifier": link})

    feature = {
        "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
        "type": "Feature",
        "properties": {
            "title": title,
            "ccodes": ccodes,
            "source_id": place_id,
        },
        "@id": entity_uri,
        "names": names,
        "types": types_raw,
        "geometry": geometry,
        "links": links,
        "descriptions": [],
        "depictions": [],
        "relations": [],
        "when": _crc_timespans_to_when(timespans_raw),
    }

    if fclasses:
        feature["properties"]["fclasses"] = fclasses

    return feature


def _legacy_place_to_lpf(serialized: dict, request=None) -> dict:
    """
    Transform PlaceFeatureSerializer output into a proper LPF v1.1 Feature.

    The serializer returns a flat dict with Django model fields; this reshapes
    it into the Linked Places Format with ``@context``, ``type: "Feature"``,
    ``@id``, top-level ``names``/``types``/``links``/``when``, and a standard
    GeoJSON ``geometry``.
    """
    # The namespaced identifier — `whg:<dataset_id>:<src_id>`, the dataset that
    # contributed the record then that dataset's own id for it. Falls back to the
    # primary key where a serialization omits either leaf.
    ds_id, src_id = serialized.get("dataset_id"), str(serialized.get("src_id") or "").strip()
    place_id = f"whg:{ds_id}:{src_id}" if ds_id and src_id else f"whg:{serialized.get('id', '')}"

    # Build @id as a dereferenceable URI
    if request is not None:
        entity_uri = request.build_absolute_uri(f"/entity/place:{place_id}/api")
    else:
        entity_uri = serialized.get("url") or f"place:{place_id}"

    # Build geometry from serialized geoms list
    geojson_geoms = []
    for g in serialized.get("geoms", []):
        geojson = g.get("geojson") or g.get("geom")
        if isinstance(geojson, dict) and geojson.get("type") and geojson.get("coordinates"):
            geojson_geoms.append(geojson)

    if len(geojson_geoms) == 1:
        geometry = geojson_geoms[0]
    elif geojson_geoms:
        geometry = {"type": "GeometryCollection", "geometries": geojson_geoms}
    else:
        geometry = None

    # Build temporal "when" from serialized "whens" list
    whens = serialized.get("whens", [])
    when = {}
    if whens:
        # Merge all timespan entries into a single "when" object
        timespans = []
        for w in whens:
            timespans.extend(w.get("timespans", []))
        if timespans:
            when = {"timespans": timespans}

    return {
        "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
        "type": "Feature",
        "@id": entity_uri,
        "properties": {
            "title": serialized.get("title", ""),
            "ccodes": serialized.get("ccodes", []),
            "fclasses": serialized.get("fclasses", []),
            "dataset": serialized.get("dataset", ""),
            "dataset_id": serialized.get("dataset_id"),
            "src_id": serialized.get("src_id"),
        },
        "geometry": geometry,
        "names": serialized.get("names", []),
        "types": serialized.get("types", []),
        "links": serialized.get("links", []),
        "relations": serialized.get("related", []),
        "descriptions": serialized.get("descriptions", []),
        "depictions": serialized.get("depictions", []),
        "when": when,
    }


def _crc_place_to_preview(crc_place: dict) -> dict:
    """
    Convert CRC gateway place data to a dict compatible with the
    place preview template.
    """
    place_id = str(crc_place.get("place_id", ""))
    title = crc_place.get("title", "")
    names_raw = crc_place.get("names", [])
    ccodes = crc_place.get("ccodes", [])
    types_raw = crc_place.get("types", [])

    # Build names in the same format as PlacePreviewSerializer
    names = []
    for n in names_raw:
        label = n.get("label", "")
        if label:
            names.append({"toponym": label})

    # Place types (AAT / source place-type wording). Prefer the source's own
    # ``sourceLabel`` (e.g. "human settlement"), as the map popup does — the CRC
    # ``label`` can carry the source *name* ("indexvillaris") rather than a type,
    # so it must not win (place#128). ``format_list`` reads ``label``.
    types = []
    for t in types_raw:
        if isinstance(t, dict):
            lbl = t.get("sourceLabel") or t.get("label") or t.get("identifier")
        else:
            lbl = t
        if lbl:
            types.append({"label": str(lbl)})

    # Timespans → "start-end" strings, mirroring PlacePreviewSerializer
    # .get_year_ranges. The CRC place carries flat ``timespans:[{start,end}]``;
    # previously this was hard-coded to [] so Timespans always read "N/A" (place#128).
    year_ranges = []
    for ts in (crc_place.get("timespans") or []):
        if not isinstance(ts, dict):
            continue
        start, end = ts.get("start"), ts.get("end")
        if start is not None and end is not None:
            year_ranges.append(f"{start}-{end}")
        elif start is not None:
            year_ranges.append(f"{start}-")
        elif end is not None:
            year_ranges.append(f"-{end}")

    # Derive namespace label for "dataset" field
    ns = place_id.split(":", 1)[0] if ":" in place_id else "CRC"

    return {
        "id": place_id,
        "title": title,
        "names": names,
        "types": types,
        "ccodes": ccodes,
        "year_ranges": year_ranges,
        "dataset": f"[{ns.upper()}]",
    }


@extend_schema(tags=["Schema"])
class CustomSwaggerUIView(TemplateView):
    template_name = "swagger_ui.html"


def _place_lookup_id(obj_type, raw_id):
    """Postgres pk for a WHG place identifier, for the ORM lookups below.

    Place ids are namespaced — ``whg:<dataset_id>:<src_id>`` — while the ORM keys
    on the primary key; ids emitted before namespacing (``whg:<pk>``, or a bare
    number) resolve too, so links already held keep working. Non-place types and
    unresolvable ids pass through untouched, to 404 as they did before.
    """
    if obj_type != "place":
        return raw_id
    pk = resolve_legacy_place_pk(raw_id)
    return pk if pk is not None else raw_id


# ---------------------------------------------------------------------------
# Persistent-identifier resolution
# ---------------------------------------------------------------------------
#
# WHG identifiers are published through w3id.org, which 303-redirects here:
#
#   whg:place:clio:<id>
#     -> https://w3id.org/whg/id/place:clio:<id>
#     -> /entity/place:clio:<id>/      (Accept: text/html)
#     -> /entity/place:clio:<id>/api   (Accept: application/ld+json)
#
# A client following that chain arrives with no cookie, no API token and no
# CSRF header, so `IsAuthenticated` made every published identifier answer 401.
# An identifier that requires an API key is not a persistent identifier.
#
# Anonymous access is opened for exactly one case: a place id belonging to an
# AUTHORITY gazetteer (`clio:`, `gn:`, `tgn:`, …), served from the CRC gateway
# and already public reference data.
#
# It is NOT opened for anything else, and the reason is specific rather than
# merely cautious: the Postgres-backed querysets behind the other types are not
# owner-scoped. `api/querysets.py::place_feature_queryset` returns
# `Place.objects` unfiltered, and `api/schemas.py::TYPE_MAP` defines no
# `feature_queryset` at all for `dataset` or `collection` — so serving those
# anonymously would publish unpublished contributed places and private datasets.


class LinkedDataJSONRenderer(JSONRenderer):
    """Serve the same JSON to clients asking for JSON-LD.

    `DEFAULT_RENDERER_CLASSES` is JSONRenderer alone, which advertises only
    `application/json`. w3id's content negotiation sends a linked-data client
    here with `Accept: application/ld+json` — the media type LPF actually is —
    and DRF answered 406 before authentication was even reached. Declared
    per-view rather than in settings, so it does not appear as a spurious
    format option throughout the Swagger UI.
    """
    media_type = 'application/ld+json'
    format = 'jsonld'


class LinkedDataHTMLRenderer(JSONRenderer):
    """Show the same LPF to a browser, as escaped, readable text.

    The detail view 303s a browser on to `/api` (an authority place has no
    Django detail page), and the browser repeats its original `Accept:
    text/html` — so without this the HTML arm of a resolved identifier ended
    at a 406 one redirect after succeeding.

    The body is escaped and wrapped rather than served as raw JSON under a
    `text/html` content type: gazetteer records carry contributed free text,
    and a browser told to treat that as HTML would execute any markup in it.

    This is a stopgap, not a landing page. A human resolving an identifier
    deserves better than a JSON dump; there is currently no id-addressable
    human page for a gateway-backed authority place to send them to.
    """
    media_type = 'text/html'
    format = 'html'

    def render(self, data, accepted_media_type=None, renderer_context=None):
        from django.utils.html import escape
        body = super().render(data, 'application/json', renderer_context)
        return (b'<!doctype html><meta charset="utf-8">'
                b'<title>World Historical Gazetteer</title>'
                b'<pre style="white-space:pre-wrap;word-break:break-word">'
                + escape(body.decode('utf-8')).encode('utf-8')
                + b'</pre>')


class EntityResolveAnonThrottle(AnonRateThrottle):
    """Per-IP ceiling on anonymous identifier resolution.

    The project configures no `DEFAULT_THROTTLE_RATES` — there is no DRF
    throttle and no nginx `limit_req` anywhere — so the rate is declared here
    rather than in settings: `SimpleRateThrottle.__init__` honours an explicit
    `rate` and never consults the scope. Keeping it local also avoids editing
    `whg/settings.py`, which must never be promoted to `main` wholesale.

    Authenticated callers are unaffected — `AnonRateThrottle` returns None for
    them, and their existing daily quota continues to apply.
    """
    rate = "60/min"


def anonymous_resolution_allowed(request, entity_id) -> bool:
    """True when this request may proceed without authentication."""
    user = getattr(request, "user", None)
    if user is not None and user.is_authenticated:
        return True
    obj_type, _, obj_id = str(entity_id or "").partition(":")
    return obj_type == "place" and bool(obj_id) and is_crc_place_id(obj_id)


class PublicEntityReadAPIView(AuthenticatedAPIView):
    """Read-only entity view that anonymous callers may use for authority records.

    Authentication itself is unchanged: a token or session still identifies the
    caller and still counts against their quota. This only stops an
    *unauthenticated* GET being rejected outright when the identifier names an
    authority place.
    """
    permission_classes = [AllowAny]
    throttle_classes = [EntityResolveAnonThrottle]
    renderer_classes = [JSONRenderer, LinkedDataJSONRenderer, LinkedDataHTMLRenderer]

    def initial(self, request, *args, **kwargs):
        super().initial(request, *args, **kwargs)
        if not anonymous_resolution_allowed(request, kwargs.get("entity_id")):
            raise NotAuthenticated()


@method_decorator(csrf_exempt, name='dispatch')
@entity_schema('detail')
class EntityDetailView(PublicEntityReadAPIView):
    """
    Human-readable detail page for any object type, typically within the main web app.
    /{entity_id}/
    """

    # This view only ever redirects or 404s, but content negotiation still runs
    # first — and w3id sends browsers here with `Accept: text/html`, which
    # JSONRenderer alone cannot satisfy. Without an HTML renderer every browser
    # request 406'd before reaching any of the logic below. StaticHTMLRenderer
    # suffices here because nothing is ever rendered through it.
    renderer_classes = [JSONRenderer, LinkedDataJSONRenderer, StaticHTMLRenderer]

    def get(self, request, entity_id, *args, **kwargs):

        try:
            obj_type, id = entity_id.split(":", 1)
        except ValueError:
            raise Http404(f"Invalid entity_id format: {entity_id}")

        config = TYPE_MAP.get(obj_type)
        if not config:
            raise Http404(f"Unsupported object type: {obj_type}")

        # CRC places have no Django detail page — redirect to the feature API
        if obj_type == "place" and is_crc_place_id(id):
            return HttpResponseRedirect(
                reverse("entity-api", kwargs={"entity_id": entity_id})
            )

        # Use the appropriate queryset function, defaulting to all objects
        qs_fn = config.get("detail_queryset") or config.get("preview_queryset") or (
            lambda user: config["model"].objects)
        obj = get_object_or_404(qs_fn(request.user), pk=_place_lookup_id(obj_type, id))

        # Special case: periods redirect to PeriodO website
        if obj_type == "period":
            return HttpResponseRedirect(f"http://n2t.net/ark:/99152/{obj.id}")

        # special case: collections branch on collection_class
        if obj_type == "collection":
            if obj.collection_class == "dataset":
                url_name = "collection:ds-collection-browse"
            elif obj.collection_class == "place":
                url_name = "collection:place-collection-browse"
            else:
                raise Http404(f"Unknown collection_class '{obj.collection_class}'")
        else:
            url_name = config.get("detail_url")

        if not url_name:
            raise Http404(f"No detail_url defined for {obj_type}")

        url = reverse(url_name, kwargs={"pid" if obj_type == "place" else "id": obj.pk})

        return HttpResponseRedirect(url)


@method_decorator(csrf_exempt, name='dispatch')
@entity_schema('feature')
class EntityFeatureView(PublicEntityReadAPIView):
    """
    Returns a machine-readable LPF or TSV representation.
    /{obj_type}/api/{id}/?filetype=lpf|tsv
    """

    def get(self, request, entity_id, *args, **kwargs):
        try:
            obj_type, obj_id = entity_id.split(":", 1)
        except ValueError:
            raise Http404(f"Invalid entity_id format: {entity_id}")

        config = TYPE_MAP.get(obj_type)
        if not config:
            raise Http404(f"Unsupported object type: {obj_type}")

        filetype = request.GET.get('filetype', 'lpf').lower()
        if filetype not in ['lpf', 'tsv']:
            filetype = 'lpf'

        # CRC places — fetch from gateway.
        if obj_type == "place" and is_crc_place_id(obj_id):
            crc_meta: dict = {}
            crc_place = _fetch_crc_place(
                obj_id, user=request.user,
                allow_anonymous=not request.user.is_authenticated,
                meta=crc_meta)
            if not crc_place:
                # 503 first: only a gateway that ANSWERED can support a 404.
                raise_if_gateway_failed(crc_meta, place_id=obj_id)
                raise Http404(f"CRC place not found: {obj_id}")
            # variant=popup → return the RAW gateway PlaceDetail dict. The Atlas
            # gazetteer-feature popup (whg/webpack/js/gazetteerInteraction.js)
            # renders directly from its native fields (title, names[].label,
            # types, geometries, relations, links, descriptions) — the LPF
            # conversion renames them (names[].toponym, …), which is why the
            # popup previously showed only the type chip. Default → LPF.
            # place#269 — resolved ONCE for both variants. The registry row carries the
            # licence AND `redistributable`, so the gate and the credit come from the
            # same lookup; splitting them is how one ships without the other.
            from api.attribution import registry_attribution
            ns = _namespace_of(crc_place, obj_id)
            attribution = registry_attribution(ns)

            # 🛑 Refuse before rendering anything. This endpoint HANDS OVER the source's
            # own content — it is the citable representation, served anonymously since
            # place#271 — so a source we may index but may not re-serve must be refused
            # here even though searching it is permitted.
            if attribution and attribution.get("redistributable") is False:
                raise SourceNotRedistributable(namespace=ns, attribution=attribution)

            if request.GET.get('variant') == 'popup':
                if attribution:
                    crc_place["attribution"] = attribution
                return Response(crc_place, status=status.HTTP_200_OK)
            if filetype != 'lpf':
                raise Http404("TSV export is not available for CRC places.")
            lpf = _crc_place_to_lpf(crc_place, request=request)
            # ⚠️ The LPF path carried NO attribution at all — and it is the half that is
            # published through w3id and read by machines, i.e. the one where an unattributed
            # record is most likely to be copied onward without its terms. The popup had it
            # since place#121; this is the same block on the representation that matters.
            if attribution:
                lpf["attribution"] = attribution
            return Response(lpf, status=status.HTTP_200_OK)

        queryset_fn = config.get("feature_queryset", lambda user: config["model"].objects)
        qs = queryset_fn(request.user)
        obj = get_object_or_404(qs, pk=_place_lookup_id(obj_type, obj_id))

        # Datasets may be flagged non-downloadable (very large bulk/authority
        # datasets to be obtained upstream). Block every export path.
        if obj_type == 'dataset' and not getattr(obj, 'downloadable', True):
            return Response(
                {'detail': 'This dataset is not available for download; '
                           'please obtain it from its original source.'},
                status=status.HTTP_403_FORBIDDEN,
            )

        # Non-streaming serializers (e.g., for certain object types)
        serializer_class = config.get("feature_serializer", None)
        if serializer_class and filetype == 'lpf':
            serializer = serializer_class(obj, context={"request": request})
            data = serializer.data
            # Transform legacy place serializer output into proper LPF
            if obj_type == "place":
                data = _legacy_place_to_lpf(data, request=request)
            return Response(data, status=status.HTTP_200_OK)

        # Cache-aware streaming response (shared with the Atlas gazetteer-panel
        # download path — see api.download_file.build_streaming_download_response).
        return build_streaming_download_response(request, obj_type, obj, filetype)


@method_decorator(csrf_exempt, name='dispatch')
@method_decorator(xframe_options_exempt, name="dispatch")
@entity_schema('preview')
class EntityPreviewView(AuthenticatedAPIView):
    """
    Returns a preview snippet for reconciliation API or human browsing.
    /{obj_type}/preview/{id}/
    """

    def get(self, request, entity_id, *args, **kwargs):

        try:
            obj_type, id = entity_id.split(":", 1)
        except ValueError:
            raise Http404(f"Invalid entity_id format: {entity_id}")

        config = TYPE_MAP.get(obj_type)
        if not config:
            return HttpResponse(f"Unsupported object type: {obj_type}", status=404)

        # CRC places — fetch from gateway and render preview from dict
        if obj_type == "place" and is_crc_place_id(id):
            crc_meta: dict = {}
            crc_place = _fetch_crc_place(id, user=request.user, meta=crc_meta)
            if not crc_place:
                # HTML preview: same distinction as the API path (place#272), but
                # rendered rather than raised, since this endpoint returns markup.
                if crc_meta.get("error"):
                    return HttpResponse(
                        "The gazetteer service did not answer, so this record "
                        "could not be retrieved. It has not been shown to be "
                        "absent — please try again shortly.",
                        status=503,
                        headers={"Retry-After": "30"},
                    )
                return HttpResponse(f"CRC place not found: {id}", status=404)
            preview_data = _crc_place_to_preview(crc_place)
            html = render_to_string(
                f"preview/{obj_type}.html",
                {"object": preview_data},
                request=request,
            )
            return HttpResponse(html, content_type="text/html; charset=UTF-8")

        queryset_fn = config.get("preview_queryset", lambda user: config["model"].objects)
        qs = queryset_fn(request.user)
        obj = get_object_or_404(qs, pk=_place_lookup_id(obj_type, id))

        serializer_class = config["preview_serializer"]
        serializer = serializer_class(obj, context={"request": request})

        html = render_to_string(
            f"preview/{obj_type}.html",
            {"object": serializer.data},
            request=request,
        )
        return HttpResponse(html, content_type="text/html; charset=UTF-8")


@method_decorator(csrf_exempt, name='dispatch')
@entity_schema('create')
class EntityCreateView(AuthenticatedAPIView):
    """
    Create a new object.
    """

    def post(self, request, entity_id, *args, **kwargs):
        # TODO: use forms or DRF serializers depending on workflow
        return Response(
            {"message": f"Create not implemented for {entity_id}"},
            status=status.HTTP_501_NOT_IMPLEMENTED,
        )


@method_decorator(csrf_exempt, name='dispatch')
@entity_schema('update')
class EntityUpdateView(AuthenticatedAPIView):
    """
    Replace (overwrite) an object with new data.
    """

    def put(self, request, entity_id, *args, **kwargs):

        try:
            obj_type, id = entity_id.split(":", 1)
        except ValueError:
            raise Http404(f"Invalid entity_id format: {entity_id}")

        return Response(
            {"message": f"Replace not implemented for {obj_type} id={id}"},
            status=status.HTTP_501_NOT_IMPLEMENTED,
        )

    # optionally, also allow PATCH for partial updates
    def patch(self, request, obj_type, id, *args, **kwargs):
        return Response(
            {"message": f"Partial replace not implemented for {obj_type} id={id}"},
            status=status.HTTP_501_NOT_IMPLEMENTED,
        )


@method_decorator(csrf_exempt, name='dispatch')
@entity_schema('delete')
class EntityDeleteView(AuthenticatedAPIView):
    """
    Delete an object.
    """

    def delete(self, request, entity_id, *args, **kwargs):  # <-- change from post to delete

        try:
            obj_type, id = entity_id.split(":", 1)
        except ValueError:
            raise Http404(f"Invalid entity_id format: {entity_id}")

        config = TYPE_MAP.get(obj_type)
        if not config:
            raise Http404(f"Unsupported object type: {obj_type}")

        return Response(
            {"message": f"Delete not implemented for {obj_type} id={id}"},
            status=status.HTTP_501_NOT_IMPLEMENTED,
        )
