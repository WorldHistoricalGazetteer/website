import gzip
import io
import json
import logging
import os
import time

import redis
from celery import shared_task
from django.conf import settings

from api.schemas import TYPE_MAP
from api.serializers_api import PlaceFeatureSerializer

# Redis client for coordination
redis_client = redis.Redis.from_url(settings.CELERY_BROKER_URL)

# File storage paths
CACHE_DIR = os.path.join(settings.MEDIA_ROOT, 'downloads')
os.makedirs(CACHE_DIR, exist_ok=True)

logger = logging.getLogger('reconciliation')

# Constants for TSV geometry handling
MAX_WKT_LENGTH = 10000  # Max characters for WKT field
SIMPLIFY_TOLERANCE = 0.01  # Degrees for geometry simplification


class FileCache:
    """Generic cache manager for LPF or TSV exports"""

    @staticmethod
    def get_cache_path(obj_type, obj_id, filetype='lpf'):
        ext = 'tsv.gz' if filetype == 'tsv' else 'lpf.gz'
        return os.path.join(CACHE_DIR, f"whg_{obj_type}_{obj_id}.{ext}")

    @staticmethod
    def get_build_lock_key(obj_type, obj_id, filetype='lpf'):
        return f"{filetype}_build_lock:{obj_type}:{obj_id}"

    @staticmethod
    def get_build_task_key(obj_type, obj_id, filetype='lpf'):
        return f"{filetype}_build_task:{obj_type}:{obj_id}"

    @staticmethod
    def get_last_rebuild_key(obj_type, obj_id, filetype='lpf'):
        return f"{filetype}_last_rebuild:{obj_type}:{obj_id}"

    @staticmethod
    def is_cached(obj_type, obj_id, filetype='lpf'):
        return os.path.exists(FileCache.get_cache_path(obj_type, obj_id, filetype))

    @staticmethod
    def is_building(obj_type, obj_id, filetype='lpf'):
        lock_key = FileCache.get_build_lock_key(obj_type, obj_id, filetype)
        return redis_client.exists(lock_key)

    @staticmethod
    def acquire_build_lock(obj_type, obj_id, filetype='lpf', timeout=3600):
        lock_key = FileCache.get_build_lock_key(obj_type, obj_id, filetype)
        return redis_client.set(lock_key, "building", ex=timeout, nx=True)

    @staticmethod
    def release_build_lock(obj_type, obj_id, filetype='lpf'):
        lock_key = FileCache.get_build_lock_key(obj_type, obj_id, filetype)
        redis_client.delete(lock_key)

    @staticmethod
    def store_build_task_id(obj_type, obj_id, task_id, filetype='lpf'):
        task_key = FileCache.get_build_task_key(obj_type, obj_id, filetype)
        redis_client.set(task_key, task_id, ex=3600)

    @staticmethod
    def get_build_task_id(obj_type, obj_id, filetype='lpf'):
        task_key = FileCache.get_build_task_key(obj_type, obj_id, filetype)
        task_id = redis_client.get(task_key)
        return task_id.decode() if task_id else None

    @staticmethod
    def clear_build_task_id(obj_type, obj_id, filetype='lpf'):
        task_key = FileCache.get_build_task_key(obj_type, obj_id, filetype)
        redis_client.delete(task_key)

    @staticmethod
    def should_throttle_rebuild(obj_type, obj_id, filetype='lpf', throttle_seconds=300):
        last_key = FileCache.get_last_rebuild_key(obj_type, obj_id, filetype)
        last_rebuild = redis_client.get(last_key)
        if last_rebuild:
            last_time = float(last_rebuild.decode())
            if time.time() - last_time < throttle_seconds:
                return True
        return False

    @staticmethod
    def get_pending_rebuild_key(obj_type, obj_id, filetype='lpf'):
        return f"{filetype}_pending_rebuild:{obj_type}:{obj_id}"

    @staticmethod
    def mark_pending_rebuild(obj_type, obj_id, filetype='lpf'):
        pending_key = FileCache.get_pending_rebuild_key(obj_type, obj_id, filetype)
        redis_client.set(pending_key, str(time.time()), ex=86400)

    @staticmethod
    def has_pending_rebuild(obj_type, obj_id, filetype='lpf'):
        pending_key = FileCache.get_pending_rebuild_key(obj_type, obj_id, filetype)
        return redis_client.exists(pending_key)

    @staticmethod
    def clear_pending_rebuild(obj_type, obj_id, filetype='lpf'):
        pending_key = FileCache.get_pending_rebuild_key(obj_type, obj_id, filetype)
        redis_client.delete(pending_key)

    @staticmethod
    def record_rebuild_time(obj_type, obj_id, filetype='lpf'):
        last_key = FileCache.get_last_rebuild_key(obj_type, obj_id, filetype)
        redis_client.set(last_key, str(time.time()), ex=86400)

    @staticmethod
    def delete_cache(obj_type, obj_id, filetype='lpf'):
        path = FileCache.get_cache_path(obj_type, obj_id, filetype)
        if os.path.exists(path):
            os.remove(path)
            return True
        return False

    @staticmethod
    def cancel_current_build(obj_type, obj_id, filetype='lpf'):
        task_id = FileCache.get_build_task_id(obj_type, obj_id, filetype)
        if task_id:
            try:
                from celery import current_app
                current_app.control.revoke(task_id, terminate=True)
                FileCache.release_build_lock(obj_type, obj_id, filetype)
                FileCache.clear_build_task_id(obj_type, obj_id, filetype)
                return True
            except Exception as e:
                logger.warning(f"Error cancelling {filetype} build task {task_id}: {e}")
        return False


def stream_from_file(filepath):
    """Stream a pre-built gzipped cache file"""
    with open(filepath, 'rb') as f:
        while chunk := f.read(8192):
            yield chunk


def stream_live(obj_type, obj, request, filetype='lpf', cache_filepath=None):
    """
    Generic streaming generator for LPF or TSV
    """
    cache_file = None
    if cache_filepath:
        cache_file = open(cache_filepath + '.tmp', 'wb')

    buffer = io.BytesIO()
    gzip_file = gzip.GzipFile(fileobj=buffer, mode='w')

    def write_and_yield(data: str):
        gzip_file.write(data.encode('utf-8'))
        gzip_file.flush()
        buffer.seek(0)
        chunk = buffer.read()
        if chunk:
            if cache_file:
                cache_file.write(chunk)
            yield chunk
        buffer.truncate(0)
        buffer.seek(0)

    try:
        if filetype == 'lpf':
            yield from write_and_yield(
                '{"@context":"https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld","type":"FeatureCollection"'
            )

            citation = getattr(obj, "citation_csl", None)
            if citation:
                yield from write_and_yield(',"citation":' + json.dumps(citation))

            licence_text = (
                "Unless specified otherwise, all content created for or uploaded to the World Historical Gazetteer — "
                "including editorial content, documentation, images, and contributed datasets and collections — "
                "is licensed under a Creative Commons Attribution-NonCommercial 4.0 International License. "
                "Externally hosted datasets and content that are linked to by WHG remain under the copyrights "
                "and licenses specified by their original contributors."
            )
            yield from write_and_yield(',"license":' + json.dumps(licence_text))
            yield from write_and_yield(',"features":[')

            first = True
            if obj_type == "dataset":
                qs = obj.places.all()
            elif obj_type == "collection" and obj.collection_class == "dataset":
                yield from write_and_yield(
                    '],"error":{"message":"Dataset collections may not be downloaded. Please download each constituent dataset individually."}}'
                )
                return
            elif obj_type == "collection" and obj.collection_class == "place":
                qs = obj.places.all()
            else:
                yield from write_and_yield(
                    '],"error":{"message":"LPF export by streaming is only supported for datasets and place collections."}}'
                )
                return

            for place in qs.iterator():
                if not first:
                    yield from write_and_yield(',')
                else:
                    first = False

                feature = PlaceFeatureSerializer(place, context={"request": request}).data
                yield from write_and_yield(json.dumps(feature))

            yield from write_and_yield(']')
            yield from write_and_yield('}')

        else:  # TSV format
            # LP-TSV headers based on the specification
            headers = [
                "id",
                "title",
                "title_source",
                "title_uri",
                "ccodes",
                "matches",
                "names",
                "types",
                "aat_types",
                "parent_name",
                "parent_id",
                "lon",
                "lat",
                "geowkt",
                "geo_source",
                "geo_id",
                "start",
                "end",
            ]
            yield from write_and_yield('\t'.join(headers) + '\n')

            if obj_type == "dataset":
                qs = obj.places.all()
            elif obj_type == "collection" and obj.collection_class == "dataset":
                yield from write_and_yield(
                    'Error: Dataset collections may not be downloaded. Please download each constituent dataset individually.\n'
                )
                return
            elif obj_type == "collection" and obj.collection_class == "place":
                qs = obj.places.all()
            else:
                yield from write_and_yield(
                    'Error: TSV export by streaming is only supported for datasets and place collections.\n'
                )
                return

            for place in qs.iterator():
                # Serialize the place to get structured data
                feature = PlaceFeatureSerializer(place, context={"request": request}).data

                # Extract primary geometry (representative point)
                lon, lat = '', ''
                geowkt = ''
                geo_source = ''
                geo_id = ''

                if feature.get('geoms'):
                    primary_geom = feature['geoms'][0] if feature['geoms'] else None
                    if primary_geom:
                        geom_data = primary_geom.get('geom')
                        if geom_data:
                            # Extract representative coordinates
                            geom_type = geom_data.get('type', '')
                            coords = geom_data.get('coordinates', [])

                            if geom_type == 'Point' and coords:
                                lon, lat = str(coords[0]), str(coords[1])
                            elif geom_type in ['LineString', 'MultiPoint'] and coords:
                                lon, lat = str(coords[0][0]), str(coords[0][1])
                            elif geom_type == 'Polygon' and coords:
                                lon, lat = str(coords[0][0][0]), str(coords[0][0][1])
                            elif geom_type == 'MultiPolygon' and coords:
                                lon, lat = str(coords[0][0][0][0]), str(coords[0][0][0][1])

                            # Convert to WKT with size management
                            try:
                                from django.contrib.gis.geos import GEOSGeometry
                                geos_geom = GEOSGeometry(json.dumps(geom_data))

                                # For complex geometries, use simplified version or representative point
                                if geom_type in ['Polygon', 'MultiPolygon', 'LineString', 'MultiLineString']:
                                    # First try the full WKT
                                    full_wkt = geos_geom.wkt

                                    if len(full_wkt) > MAX_WKT_LENGTH:
                                        # Try simplifying the geometry
                                        try:
                                            simplified = geos_geom.simplify(
                                                tolerance=SIMPLIFY_TOLERANCE,
                                                preserve_topology=True
                                            )
                                            simplified_wkt = simplified.wkt

                                            if len(simplified_wkt) <= MAX_WKT_LENGTH:
                                                geowkt = simplified_wkt
                                            else:
                                                # Still too large, use centroid/representative point
                                                if hasattr(geos_geom, 'point_on_surface'):
                                                    geowkt = geos_geom.point_on_surface.wkt
                                                else:
                                                    geowkt = geos_geom.centroid.wkt
                                        except:
                                            # Simplification failed, use representative point
                                            geowkt = geos_geom.centroid.wkt
                                    else:
                                        geowkt = full_wkt
                                else:
                                    # Simple geometries - use as is
                                    geowkt = geos_geom.wkt

                            except Exception as e:
                                # Fallback: use the lon/lat point if available
                                if lon and lat:
                                    geowkt = f"POINT({lon} {lat})"
                                else:
                                    geowkt = ''

                        geo_source = primary_geom.get('src', '')
                        if primary_geom.get('citation'):
                            citation_data = primary_geom['citation']
                            if isinstance(citation_data, dict):
                                geo_id = citation_data.get('id', '')
                            else:
                                geo_id = str(citation_data)

                # Extract temporal coverage
                start, end = '', ''
                if feature.get('whens'):
                    for when in feature['whens']:
                        timespans = when.get('timespans', [])
                        if timespans:
                            timespan = timespans[0]
                            if timespan.get('start'):
                                start_val = timespan['start'].get('in', '')
                                if start_val and not start:  # Take first available
                                    start = str(start_val)
                            if timespan.get('end'):
                                end_val = timespan['end'].get('in', '')
                                if end_val and not end:  # Take first available
                                    end = str(end_val)
                        if start and end:
                            break

                # Extract names - semicolon separated with language tags
                names_list = []
                if feature.get('names'):
                    for name in feature['names']:
                        name_str = name.get('toponym', '')
                        if name_str:
                            lang = name.get('lang', '')
                            if lang:
                                names_list.append(f"{name_str}@{lang}")
                            else:
                                names_list.append(name_str)
                names = ';'.join(names_list)

                # Extract types - semicolon separated
                types_list = []
                aat_types_list = []
                if feature.get('types'):
                    for type_obj in feature['types']:
                        type_label = type_obj.get('label', '')
                        if type_label:
                            types_list.append(type_label)
                        # Extract AAT identifiers if present
                        if type_obj.get('identifier'):
                            aat_id = type_obj['identifier']
                            if 'vocab.getty.edu/aat' in str(aat_id):
                                aat_types_list.append(str(aat_id))
                types = ';'.join(types_list)
                aat_types = ';'.join(aat_types_list)

                # Extract matches (closeMatch, exactMatch links)
                matches_list = []
                if feature.get('links'):
                    for link in feature['links']:
                        link_type = link.get('type', '')
                        if link_type in ['closeMatch', 'exactMatch']:
                            identifier = link.get('identifier', '')
                            if identifier:
                                matches_list.append(str(identifier))
                matches = ';'.join(matches_list)

                # Extract title source and URI
                title_source = ''
                title_uri = ''
                if feature.get('links'):
                    for link in feature['links']:
                        if link.get('type') == 'primaryTopicOf':
                            title_uri = link.get('identifier', '')
                            break

                # Parent information (if available from related)
                parent_name = ''
                parent_id = ''
                if feature.get('related'):
                    for rel in feature['related']:
                        rel_type = rel.get('relationType', '')
                        if rel_type in ['gvp:broaderPartitive', 'broader', 'partOf']:
                            parent_name = rel.get('label', '')
                            parent_id = rel.get('relationTo', '')
                            break

                # Build the row
                row = [
                    str(feature.get('id', '')),
                    feature.get('title', ''),
                    title_source,
                    title_uri,
                    ';'.join(str(c) for c in feature.get('ccodes', [])),
                    matches,
                    names,
                    types,
                    aat_types,
                    parent_name,
                    parent_id,
                    lon,
                    lat,
                    geowkt,
                    geo_source,
                    geo_id,
                    start,
                    end,
                ]

                # Escape tabs, newlines, and carriage returns in field values
                row = [
                    field.replace('\t', ' ').replace('\n', ' ').replace('\r', '').strip()
                    for field in row
                ]

                yield from write_and_yield('\t'.join(row) + '\n')

        # Finish gzip stream properly
        gzip_file.close()
        buffer.seek(0)
        trailer = buffer.read()
        if trailer:
            if cache_file:
                cache_file.write(trailer)
            yield trailer

    finally:
        if cache_file:
            cache_file.close()
            if os.path.exists(cache_filepath + '.tmp'):
                os.rename(cache_filepath + '.tmp', cache_filepath)


@shared_task(bind=True)
def build_cache(self, obj_type, obj_id, filetype='lpf'):
    """
    Celery task to build LPF or TSV cache file in background.
    Preserves logging, atomic writes, revocation handling, and cleanup.
    """
    cache_path = FileCache.get_cache_path(obj_type, obj_id, filetype)
    logger.info(f"Starting cache build for {filetype.upper()} {obj_type}:{obj_id}")

    # Store task ID for potential cancellation
    FileCache.store_build_task_id(obj_type, obj_id, self.request.id, filetype)

    try:
        config = TYPE_MAP.get(obj_type)
        if not config:
            return f"Unsupported object type: {obj_type}"

        # Get the object
        obj = config["model"].objects.get(pk=obj_id)

        # Build the cache file atomically
        with open(cache_path + '.tmp', 'wb', encoding='utf-8') as cache_file:
            for chunk in stream_live(obj_type, obj, None, filetype=filetype, cache_filepath=None):
                cache_file.write(chunk)

                # Check if task has been revoked
                if self.is_aborted():
                    cache_file.close()
                    if os.path.exists(cache_path + '.tmp'):
                        os.remove(cache_path + '.tmp')
                    return f"Build task for {filetype.upper()} {obj_type}:{obj_id} was cancelled"

        # Atomically rename to final location
        os.rename(cache_path + '.tmp', cache_path)
        logger.info(f"Finished cache build for {filetype.upper()} {obj_type}:{obj_id}")

        return f"Successfully built {filetype.upper()} cache for {obj_type}:{obj_id}"

    except Exception as e:
        if os.path.exists(cache_path + '.tmp'):
            os.remove(cache_path + '.tmp')
        return f"Failed to build {filetype.upper()} cache for {obj_type}:{obj_id}: {str(e)}"

    finally:
        FileCache.release_build_lock(obj_type, obj_id, filetype)
        FileCache.clear_build_task_id(obj_type, obj_id, filetype)


# Signal-triggered cache management
def invalidate_and_rebuild_cache(obj_type, obj_id, filetype='lpf', force=False):
    """
    Signal-triggered function to invalidate and rebuild cache for LPF or TSV.
    """
    result = {
        'cancelled_existing': False,
        'deleted_cache': False,
        'started_rebuild': False,
        'throttled': False,
        'deferred': False,
        'message': ''
    }

    # Throttling unless forced
    if not force and FileCache.should_throttle_rebuild(obj_type, obj_id, filetype=filetype):
        FileCache.mark_pending_rebuild(obj_type, obj_id, filetype=filetype)

        last_time = float(redis_client.get(
            FileCache.get_last_rebuild_key(obj_type, obj_id, filetype=filetype)
        ).decode())
        delay_seconds = max(300 - (time.time() - last_time) + 10, 10)

        deferred_rebuild.apply_async(
            args=[obj_type, obj_id, filetype],
            countdown=int(delay_seconds)
        )

        result.update({'throttled': True, 'deferred': True,
                       'message': f"Rebuild deferred for {obj_type}:{obj_id} ({filetype}) in {int(delay_seconds)}s"})
        return result

    # Cancel current build
    if FileCache.cancel_current_build(obj_type, obj_id, filetype=filetype):
        result['cancelled_existing'] = True

    # Delete existing cache
    if FileCache.delete_cache(obj_type, obj_id, filetype=filetype):
        result['deleted_cache'] = True

    # Clear pending rebuild
    FileCache.clear_pending_rebuild(obj_type, obj_id, filetype=filetype)

    # Record rebuild time
    FileCache.record_rebuild_time(obj_type, obj_id, filetype=filetype)

    # Start new build if we can acquire lock
    if FileCache.acquire_build_lock(obj_type, obj_id, filetype=filetype):
        task = build_cache.delay(obj_type, obj_id, filetype=filetype)
        result.update({'started_rebuild': True,
                       'message': f"Started rebuild for {obj_type}:{obj_id} ({filetype}) (task: {task.id})"})
    else:
        result['message'] = f"Could not acquire lock for {obj_type}:{obj_id} ({filetype})"

    return result


@shared_task
def deferred_rebuild(obj_type, obj_id, filetype='lpf'):
    """
    Celery task for deferred rebuilds of LPF or TSV caches.
    """
    if FileCache.has_pending_rebuild(obj_type, obj_id, filetype=filetype):
        if not FileCache.should_throttle_rebuild(obj_type, obj_id, filetype=filetype):
            result = invalidate_and_rebuild_cache(obj_type, obj_id, filetype=filetype, force=True)
            return f"Deferred rebuild completed for {obj_type}:{obj_id} ({filetype}): {result['message']}"
        else:
            # Still throttled, try again in 1 minute
            deferred_rebuild.apply_async(args=[obj_type, obj_id, filetype], countdown=60)
            return f"Deferred rebuild rescheduled for {obj_type}:{obj_id} ({filetype})"
    else:
        return f"No pending rebuild for {obj_type}:{obj_id} ({filetype}) - skipping"


# Optional: Management command or API endpoint to pre-build caches
def prebuild_cache(obj_type, obj_id, filetype='lpf', force_rebuild=False):
    """
    Helper function to trigger LPF or TSV cache building.
    """
    if force_rebuild:
        return invalidate_and_rebuild_cache(obj_type, obj_id, filetype=filetype, force=True)['started_rebuild']

    if not FileCache.is_cached(obj_type, obj_id, filetype=filetype) and \
            not FileCache.is_building(obj_type, obj_id, filetype=filetype):
        if FileCache.acquire_build_lock(obj_type, obj_id, filetype=filetype):
            build_cache.delay(obj_type, obj_id, filetype=filetype)
            return True
    return False
