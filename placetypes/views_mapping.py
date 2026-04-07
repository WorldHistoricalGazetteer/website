# placetypes/views_mapping.py
"""
Type mapping UI — allows authenticated users to map GeoNames, Wikidata,
OSM, and OHM types to AAT concepts via the ES types index.
"""

import json
import logging

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from .mapping_utils import (
    get_geonames_types,
    get_wikidata_types,
    get_osm_ohm_types,
    search_aat_types,
    save_mapping,
    remove_mapping,
    get_mapping_stats,
)
from .models import TypeMappingLog

logger = logging.getLogger(__name__)


@login_required
def mapping_dashboard(request):
    """Main mapping dashboard page."""
    return render(request, 'placetypes/mapping_dashboard.html')


@login_required
@require_GET
def api_geonames_types(request):
    """Return all GeoNames feature codes with current mappings."""
    try:
        types = get_geonames_types()
        return JsonResponse(types, safe=False)
    except Exception as e:
        logger.exception("Error loading GeoNames types")
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_GET
def api_wikidata_types(request):
    """Return all Wikidata P31 Q-items with current mappings."""
    try:
        types = get_wikidata_types()
        return JsonResponse(types, safe=False)
    except Exception as e:
        logger.exception("Error loading Wikidata types")
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_GET
def api_osm_ohm_types(request):
    """Return all OSM + OHM tag values (merged) with current mappings."""
    try:
        tag_key = request.GET.get('tag_key')
        types = get_osm_ohm_types(tag_key_filter=tag_key)
        return JsonResponse(types, safe=False)
    except Exception as e:
        logger.exception("Error loading OSM/OHM types")
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_GET
def api_aat_search(request):
    """Search AAT concepts in the types index."""
    q = request.GET.get('q', '').strip()
    if not q or len(q) < 2:
        return JsonResponse([], safe=False)
    try:
        results = search_aat_types(q)
        return JsonResponse(results, safe=False)
    except Exception as e:
        logger.exception("Error searching AAT types")
        return JsonResponse({"error": str(e)}, status=500)


@login_required
@require_POST
def api_save_mapping(request):
    """Save a type → AAT mapping to the ES types index."""
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    source_vocab = data.get('source_vocab')
    source_id = data.get('source_id')
    aat_id = data.get('aat_id')
    confidence = data.get('confidence', 'exact')

    if not all([source_vocab, source_id, aat_id]):
        return JsonResponse({'error': 'Missing required fields'}, status=400)
    if source_vocab not in ('geonames', 'wikidata', 'osm', 'ohm', 'osm_ohm'):
        return JsonResponse({'error': 'Invalid source_vocab'}, status=400)
    if confidence not in ('exact', 'close', 'review'):
        confidence = 'exact'

    try:
        result = save_mapping(source_vocab, source_id, int(aat_id), confidence=confidence)
        TypeMappingLog.objects.create(
            user=request.user,
            action='save',
            source_vocab=source_vocab,
            source_id=source_id,
            aat_id=result.get('aat_id'),
            aat_term=result.get('aat_term', ''),
            confidence=confidence,
        )
        logger.info(
            "User %s mapped %s:%s → aat:%s (confidence=%s)",
            request.user, source_vocab, source_id, aat_id, confidence,
        )
        return JsonResponse(result)
    except Exception as e:
        logger.exception("Error saving mapping")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_POST
def api_remove_mapping(request):
    """Remove a type → AAT mapping from the ES types index."""
    try:
        data = json.loads(request.body)
    except json.JSONDecodeError:
        return JsonResponse({'error': 'Invalid JSON'}, status=400)

    source_vocab = data.get('source_vocab')
    source_id = data.get('source_id')
    aat_id = data.get('aat_id')

    if not all([source_vocab, source_id, aat_id]):
        return JsonResponse({'error': 'Missing required fields'}, status=400)

    try:
        result = remove_mapping(source_vocab, source_id, int(aat_id))
        TypeMappingLog.objects.create(
            user=request.user,
            action='remove',
            source_vocab=source_vocab,
            source_id=source_id,
            aat_id=int(aat_id),
        )
        logger.info(
            "User %s removed mapping %s:%s from aat:%s",
            request.user, source_vocab, source_id, aat_id,
        )
        return JsonResponse(result)
    except Exception as e:
        logger.exception("Error removing mapping")
        return JsonResponse({'error': str(e)}, status=500)


@login_required
@require_GET
def api_mapping_stats(request):
    """Return mapping coverage statistics."""
    try:
        stats = get_mapping_stats()
        return JsonResponse(stats)
    except Exception as e:
        logger.exception("Error getting mapping stats")
        return JsonResponse({"error": str(e)}, status=500)




