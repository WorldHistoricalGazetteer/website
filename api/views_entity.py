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
from api.download_file import FileCache, stream_live, stream_from_file
from api.schemas import entity_schema, TYPE_MAP

logger = logging.getLogger('reconciliation')


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

        queryset_fn = config.get("feature_queryset", lambda user: config["model"].objects)
        qs = queryset_fn(request.user)
        obj = get_object_or_404(qs, pk=obj_id)

        # Non-streaming serializers (e.g., for certain object types)
        serializer_class = config.get("feature_serializer", None)
        if serializer_class and filetype == 'lpf':
            serializer = serializer_class(obj, context={"request": request})
            return Response(serializer.data, status=status.HTTP_200_OK)

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
