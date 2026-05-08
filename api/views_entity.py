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
from rest_framework.response import Response

from api.authentication import AuthenticatedAPIView
from api.crc_client import crc_fetch_places
from api.download_file import FileCache, stream_live, stream_from_file
from api.reconcile_helpers import is_crc_place_id
from api.schemas import entity_schema, TYPE_MAP

logger = logging.getLogger('reconciliation')


def _fetch_crc_place(place_id: str, user=None) -> dict | None:
    """Fetch a single CRC place from the gateway, or return None."""
    result = crc_fetch_places([place_id], user=user)
    return result.get(place_id)


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
        "when": {},
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
    place_id = serialized.get("id", "")

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


def _crc_place_to_popup(crc_place: dict, place_id: str) -> dict:
    """Shape a CRC gateway place dict for the Atlas popup template.

    Surfaces the popup-relevant fields and rewrites awkward keys (notably
    ``depictions[].@id``, which Django templates can't access via dot
    notation) into template-friendly aliases.
    """
    title = crc_place.get("title", "") or place_id
    namespace = crc_place.get("namespace") or (
        place_id.split(":", 1)[0] if ":" in place_id else ""
    )

    # Primary type label — first types[].label or sourceLabel
    primary_type = ""
    other_types: list[str] = []
    for t in crc_place.get("types") or []:
        if not isinstance(t, dict):
            continue
        label = t.get("label") or t.get("sourceLabel") or ""
        if not label:
            continue
        if not primary_type:
            primary_type = label
        else:
            other_types.append(label)

    # Description — first non-empty value, truncated by template
    description = ""
    for d in crc_place.get("descriptions") or []:
        if isinstance(d, dict) and d.get("value"):
            description = d["value"]
            break

    # First depiction with a usable URL
    depiction = None
    for dep in crc_place.get("depictions") or []:
        if not isinstance(dep, dict):
            continue
        url = dep.get("@id") or dep.get("id") or ""
        if url:
            depiction = {
                "url": url,
                "title": dep.get("title", ""),
                "license": dep.get("license", ""),
            }
            break

    # Timespan — _collapse_timespans returns at most one {start, end}
    timespan = None
    timespans = crc_place.get("timespans") or []
    if timespans and isinstance(timespans[0], dict):
        timespan = {
            "start": timespans[0].get("start"),
            "end": timespans[0].get("end"),
        }

    return {
        "place_id": place_id,
        "entity_id": f"place:{place_id}",
        "title": title,
        "namespace": namespace,
        "primary_type": primary_type,
        "other_types": other_types,
        "ccodes": crc_place.get("ccodes") or [],
        "boundary": crc_place.get("boundary"),
        "timespan": timespan,
        "population": crc_place.get("population"),
        "description": description,
        "depiction": depiction,
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
    fclasses = crc_place.get("fclasses", [])
    types_raw = crc_place.get("types", [])

    # Build names in the same format as PlacePreviewSerializer
    names = []
    for n in names_raw:
        label = n.get("label", "")
        if label:
            names.append({"toponym": label})

    # Build types — match PlaceTypeSerializer output shape
    types = []
    for t in types_raw:
        if isinstance(t, dict):
            types.append(t)
        elif isinstance(t, str):
            types.append({"label": t})

    # Derive namespace label for "dataset" field
    ns = place_id.split(":", 1)[0] if ":" in place_id else "CRC"

    return {
        "id": place_id,
        "title": title,
        "names": names,
        "types": types,
        "ccodes": ccodes,
        "fclasses": fclasses,
        "year_ranges": [],
        "dataset": f"[{ns.upper()}]",
    }


@extend_schema(tags=["Schema"])
class CustomSwaggerUIView(TemplateView):
    template_name = "swagger_ui.html"


@method_decorator(csrf_exempt, name='dispatch')
@entity_schema('detail')
class EntityDetailView(AuthenticatedAPIView):
    """
    Human-readable detail page for any object type, typically within the main web app.
    /{entity_id}/
    """

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
        obj = get_object_or_404(qs_fn(request.user), pk=id)

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
class EntityFeatureView(AuthenticatedAPIView):
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

        # CRC places — fetch from gateway and return LPF
        if obj_type == "place" and is_crc_place_id(obj_id):
            if filetype != 'lpf':
                raise Http404("TSV export is not available for CRC places.")
            crc_place = _fetch_crc_place(obj_id, user=request.user)
            if not crc_place:
                raise Http404(f"CRC place not found: {obj_id}")
            lpf = _crc_place_to_lpf(crc_place, request=request)
            return Response(lpf, status=status.HTTP_200_OK)

        queryset_fn = config.get("feature_queryset", lambda user: config["model"].objects)
        qs = queryset_fn(request.user)
        obj = get_object_or_404(qs, pk=obj_id)

        # Non-streaming serializers (e.g., for certain object types)
        serializer_class = config.get("feature_serializer", None)
        if serializer_class and filetype == 'lpf':
            serializer = serializer_class(obj, context={"request": request})
            data = serializer.data
            # Transform legacy place serializer output into proper LPF
            if obj_type == "place":
                data = _legacy_place_to_lpf(data, request=request)
            return Response(data, status=status.HTTP_200_OK)

        # Determine cache path
        cache_path = FileCache.get_cache_path(obj_type, obj_id, filetype=filetype)
        filename = f"whg_{obj_type}_{obj_id}.{filetype}"

        # Stream from cache if available
        if FileCache.is_cached(obj_type, obj_id, filetype=filetype):
            logger.debug(f"Serving cached {filetype.upper()} for {obj_type}:{obj_id}")
            response = StreamingHttpResponse(
                stream_from_file(cache_path),
                content_type="application/geo+json" if filetype == 'lpf' else "text/tab-separated-values"
            )
            response["Content-Length"] = str(os.path.getsize(cache_path))

        else:
            # Check if another request is building the cache
            if not FileCache.is_building(obj_type, obj_id, filetype=filetype):
                if FileCache.acquire_build_lock(obj_type, obj_id, filetype=filetype):
                    logger.debug(f"Acquired build lock for {filetype.upper()} {obj_type}:{obj_id}")
                    # Stream live while building cache
                    response = StreamingHttpResponse(
                        stream_live(obj_type, obj, request, cache_filepath=cache_path, filetype=filetype),
                        content_type="application/geo+json" if filetype == 'lpf' else "text/tab-separated-values"
                    )
                else:
                    logger.debug(f"Failed to acquire build lock for {filetype.upper()} {obj_type}:{obj_id}")
                    # Someone else got the lock - stream live without caching
                    response = StreamingHttpResponse(
                        stream_live(obj_type, obj, request, filetype=filetype),
                        content_type="application/geo+json" if filetype == 'lpf' else "text/tab-separated-values"
                    )
            else:
                logger.debug(f"Cache is being built for {filetype.upper()} {obj_type}:{obj_id}, streaming live")
                response = StreamingHttpResponse(
                    stream_live(obj_type, obj, request, filetype=filetype),
                    content_type="application/geo+json" if filetype == 'lpf' else "text/tab-separated-values"
                )

        response['Content-Disposition'] = f'attachment; filename="{urlquote(filename)}"'
        response['Content-Encoding'] = 'gzip'
        response['X-Format'] = 'Linked Places Format (LPF)' if filetype == 'lpf' else 'Tab-separated values (TSV)'
        response['X-Format-Version'] = 'v1.1' if filetype == 'lpf' else 'v1'
        response['X-Compatible-With'] = 'GeoJSON' if filetype == 'lpf' else 'WHG TSV consumer'

        return response


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

        variant = request.GET.get("variant")

        # CRC places — fetch from gateway and render preview from dict
        if obj_type == "place" and is_crc_place_id(id):
            crc_place = _fetch_crc_place(id, user=request.user)
            if not crc_place:
                return HttpResponse(f"CRC place not found: {id}", status=404)
            if variant == "popup":
                popup_data = _crc_place_to_popup(crc_place, id)
                html = render_to_string(
                    "preview/place_popup.html",
                    {"place": popup_data},
                    request=request,
                )
                return HttpResponse(html, content_type="text/html; charset=UTF-8")
            preview_data = _crc_place_to_preview(crc_place)
            html = render_to_string(
                f"preview/{obj_type}.html",
                {"object": preview_data},
                request=request,
            )
            return HttpResponse(html, content_type="text/html; charset=UTF-8")

        queryset_fn = config.get("preview_queryset", lambda user: config["model"].objects)
        qs = queryset_fn(request.user)
        obj = get_object_or_404(qs, pk=id)

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
