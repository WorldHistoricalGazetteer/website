# validation/tasks.py
import os
import json
import logging
import time

from celery import Celery
from celery import shared_task
from celery.result import AsyncResult
from datetime import timedelta, datetime
from itertools import chain

from jsonschema import Draft7Validator, ValidationError
from django.http import JsonResponse
from django.utils import timezone
from django.conf import settings
import redis

from shapely.geometry import shape
from shapely.geometry import mapping as shapely_mapping
from shapely.validation import make_valid

from validation.coordinates import LON_LIMIT, LAT_LIMIT
from validation.create_dataset import save_dataset

logger = logging.getLogger('validation')


def get_redis_client():
    return redis.StrictRedis.from_url(settings.CELERY_BROKER_URL)


def task_count(task_name='validation.tasks.validate_feature_batch'):
    app = Celery('whg')
    i = app.control.inspect()

    def count_tasks_of_type(task_data, task_type):
        if task_data:
            return len([
                task for task in chain.from_iterable(task_data.values())
                if task.get("name") == task_type
            ])
        return 0

    task_count = 0
    task_count += count_tasks_of_type(i.active(), task_name)
    task_count += count_tasks_of_type(i.reserved(), task_name)
    return task_count


def traverse_path(data, path):
    """
    Traverse the nested data structure according to the given path.
    
    :param data: The nested data structure (dict or list)
    :param path: List or deque of keys/indices to traverse
    :return: The target element or None if path is invalid
    """
    current = data
    for key in path:
        if isinstance(current, dict):
            current = current.get(key, None)
        elif isinstance(current, list):
            if isinstance(key, int) and 0 <= key < len(current):
                current = current[key]
            else:
                return None
        else:
            return None
    return current


def fix_feature(featureCollection, e, namespaces):
    """
    Traverse the featureCollection according to the error_path and attempt fixes.
    """
    fixes = []
    try:
        # logger.debug(f'Attempting fix of: {featureCollection}')

        path_list = list(e.absolute_path)
        current_element = traverse_path(featureCollection, path_list[:-1])
        if current_element is None:
            logger.error("Failed to traverse path. The path might be invalid.")
            return featureCollection, fixes

        last_key = path_list[-1]
        invalid_value = e.instance
        feature_id = featureCollection["features"][0].get("@id", "- no @id -")

        # Attempt to convert integers to strings where necessary
        if e.validator == 'type' and e.validator_value == 'string':
            if isinstance(invalid_value, int):
                current_element[last_key] = str(invalid_value)
                fix_description = f"Converted integer '{invalid_value}' to string '{current_element[last_key]}'"
                fixes.append({
                    "feature_id": feature_id,
                    "path": ".".join(map(str, path_list)),
                    "fix": current_element[last_key],
                    "description": fix_description
                })
                logger.debug(fix_description)
            else:
                logger.debug("Invalid value is not an integer or does not require conversion.")
        else:
            logger.debug(
                f"Validator or validator_value does not match type check: validator={e.validator}, validator_value={e.validator_value}")

        # Attempt to fix missing timespans
        if e.validator == 'required' and isinstance(e.validator_value, list):
            if any(ref == 'timespans' for ref in e.validator_value):
                logger.debug(f'Attempting "timespans" fix... ({current_element})')
                when = current_element.get('when', None)
                start = when.get('start', None)
                end = when.get('end', None)
                if start or end:
                    timespan = {}
                    if start:
                        timespan['start'] = start
                    if end:
                        timespan['end'] = end
                    when['timespans'] = [timespan]
                    when.pop('start', None)
                    when.pop('end', None)
                    fix_description = f"Created '{current_element['when']}' from start='{start}' and end='{end}'"
                    fixes.append({
                        "feature_id": feature_id,
                        "path": ".".join(map(str, path_list)),
                        "fix": current_element['when'],
                        "description": fix_description
                    })
                    logger.debug(fix_description)
                else:
                    logger.debug(f"... failed: no appropriate start or end values found.")

        # Attempt to fix ids/urls by either removal, expansion from namespaces, or prepending a dummy namespace
        if isinstance(invalid_value, str) and isinstance(e.validator_value, list):
            ref_list = [ref.get('$ref') for ref in e.validator_value]

            def insert_anon():
                new_value = f"anon:{invalid_value}"
                current_element[last_key] = new_value
                fix_description = f"Fixed @id value: '{invalid_value}' to '{new_value}'"
                fixes.append({
                    "feature_id": feature_id,
                    "path": ".".join(map(str, path_list)),
                    "fix": current_element[last_key],
                    "description": fix_description
                })
                logger.debug(fix_description)

            if '#/definitions/patterns/definitions/validURL' in ref_list or '#/definitions/patterns/definitions/namespaceTerm' in ref_list or '#/definitions/patterns/definitions/namespaceTermNarrow' in ref_list:
                if invalid_value == "":
                    # Remove the element if invalid_value is an empty string
                    del current_element[last_key]
                    fix_description = f"Removed empty @id field from element"
                    fixes.append({
                        "feature_id": feature_id,
                        "path": ".".join(map(str, path_list)),
                        "description": fix_description
                    })
                    logger.debug(fix_description)
                else:
                    if namespaces:
                        # Replace any namespaces which were defined in @context of uploaded jsonld file
                        for prefix, namespace_uri in namespaces.items():
                            if invalid_value.startswith(prefix):
                                new_value = invalid_value.replace(f'{prefix}:', namespace_uri, 1)
                                current_element[last_key] = new_value
                                fix_description = f"Substituted prefix '{prefix}' with '{namespace_uri}' in '{invalid_value}'"
                                fixes.append({
                                    "feature_id": feature_id,
                                    "path": ".".join(map(str, path_list)),
                                    "fix": current_element[last_key],
                                    "description": fix_description
                                })
                                logger.debug(fix_description)
                                break
                        else:
                            insert_anon()
                    else:
                        insert_anon()

    except Exception as e:
        raise

    return featureCollection, fixes


@shared_task
def clean_tmp_files(directory='/var/tmp', age_in_seconds=10800):  # 10800 = 3 hours
    """Delete files older than `age_in_seconds` in `directory`."""
    now = time.time()
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath) and os.stat(filepath).st_mtime < (now - age_in_seconds):
            os.remove(filepath)


@shared_task
def cleanup(task_id):
    redis_client = get_redis_client()
    revoke_all_subtasks(redis_client, task_id)
    redis_client.delete(f"{task_id}_errors")
    redis_client.delete(f"{task_id}_fixes")
    redis_client.delete(f"{task_id}_metadata")
    redis_client.delete(task_id)
    logger.debug(f"Cleanup completed for task {task_id}.")


def get_task_status(request, task_id):
    current_time = timezone.now()
    redis_client = get_redis_client()
    status = redis_client.hgetall(task_id)
    if not status:
        return JsonResponse({"status": "not_found", "message": "Task ID not found"}, status=404)
    status = {k.decode('utf-8'): v.decode('utf-8') for k, v in status.items()}

    # Calculate remaining features
    total_features = int(status.get('total_features', 0))
    queued_features = int(status.get('queued_features', 0))
    processed_features = total_features - queued_features

    # Calculate remaining queue
    if processed_features == 0:
        queued_batches = int(status.get('queued_batches', 0))
        status['remaining_queue'] = task_count() - queued_batches
        # Queueing - reset start time        
        redis_client.hset(task_id, 'start_time', current_time.isoformat())

    # Estimate remaining time
    start_time = datetime.fromisoformat(
        status.get('mapdata_start_time') or
        status.get('insert_start_time', status.get('start_time'))
    )
    last_update_time_str = status.get('last_update', status.get('start_time'))
    last_update_time = datetime.fromisoformat(last_update_time_str)

    if queued_features == 0:
        estimated_remaining_time = 0
    elif processed_features > 0:
        elapsed_time = (current_time - start_time).total_seconds()
        average_time_per_feature = elapsed_time / processed_features
        estimated_remaining_time = average_time_per_feature * queued_features
    else:
        estimated_remaining_time = None

    status['time_since_last_update'] = str((current_time - last_update_time).total_seconds())
    status['estimated_remaining_time'] = str(
        timedelta(seconds=estimated_remaining_time)) if estimated_remaining_time is not None else "queueing"

    # Add fixes and errors if they exist
    if not status.get('insert_start_time'): # Insertion necessitates incremental removal of fixes from redis array
        status['fixes'] = [fix.decode('utf-8') for fix in redis_client.lrange(f"{task_id}_fixes", 0, -1)]
    status['errors'] = [error.decode('utf-8') for error in redis_client.lrange(f"{task_id}_errors", 0, -1)]

    # Check if task is no longer in progress
    if status.get('status') != 'in_progress':
        # revoke scheduled default cleanup task
        cleanup_task_id = status.get('cleanup_task_id')
        if cleanup_task_id:
            cleanup_task_result = AsyncResult(cleanup_task_id)
            cleanup_task_result.revoke(terminate=True)
            logger.debug(f"Revoked scheduled cleanup task {cleanup_task_id} for task {task_id}.")
            redis_client.hdel(task_id, 'cleanup_task_id')
            del status['cleanup_task_id']
        cleanup(task_id)

    return JsonResponse({
        "status": "success",
        "task_status": status
    })


def validate_geometry(geometry):
    """
    Validate and fix a single geometry using Shapely.
    Apply buffer(0) first, then make_valid if necessary.

    :param geometry: A dictionary representing a GeoJSON geometry
    :return: Tuple of (geometry, fixed, valid, reason), where `reason` describes the
             failure when `valid` is False (and is None otherwise)
    """
    fixed = False
    valid = False
    reason = None

    geometry_type = geometry.get('type', None)
    geometry_coordinates = geometry.get('coordinates', None)
    def _round_coords(obj, ndigits):
        if isinstance(obj, float):
            return round(obj, ndigits)
        if isinstance(obj, int):
            return obj
        if isinstance(obj, (list, tuple)):
            # Convert tuples to lists (Shapely outputs tuples)
            return [_round_coords(i, ndigits) for i in obj]
        return obj

    ndigits = getattr(settings, 'VALIDATION_COORD_DECIMALS', 6)

    if geometry_type and geometry_coordinates is not None:
        try:
            # Convert GeoJSON to Shapely geometry
            geom = shape({
                'type': geometry.get('type'),
                'coordinates': geometry_coordinates
            })

            # Check if the geometry is valid
            if not geom.is_valid:
                logger.debug(f"Geometry is invalid: {geom}")

                # First Tier Fix: Attempt to fix the geometry using buffer(0)
                fixed_geom = geom.buffer(0)
                if fixed_geom.is_valid:
                    geojson_fixed = shapely_mapping(fixed_geom)
                    geometry['type'] = geojson_fixed.get('type')
                    geometry['coordinates'] = _round_coords(geojson_fixed.get('coordinates'), ndigits)
                    logger.debug(f"Fixed invalid geometry with buffer(0).")
                    fixed = True
                    valid = True
                else:
                    logger.debug("Buffer(0) did not fix the geometry. Attempting make_valid...")

                    # Second Tier Fix: Attempt to fix the geometry using make_valid
                    fixed_geom = make_valid(geom)
                    if fixed_geom.is_valid:
                        geojson_fixed = shapely_mapping(fixed_geom)
                        geometry['type'] = geojson_fixed.get('type')
                        geometry['coordinates'] = _round_coords(geojson_fixed.get('coordinates'), ndigits)
                        logger.debug(f"Fixed invalid geometry with make_valid.")
                        fixed = True
                        valid = True
                    else:
                        logger.error("Failed to fix geometry with make_valid.")
                        reason = ("Geometry is invalid (self-intersecting or malformed) and "
                                  "could not be repaired.")
            else:
                # Geometry is valid; ensure coordinates are normalized/rounded
                try:
                    geojson_ok = shapely_mapping(geom)
                    geometry['type'] = geojson_ok.get('type')
                    geometry['coordinates'] = _round_coords(geojson_ok.get('coordinates'), ndigits)
                    logger.debug(f"Geometry passed validation.")
                    valid = True
                except Exception:
                    logger.debug(f"Geometry passed validation (could not remap).")
                    valid = True

        except Exception as e:
            logger.error(f"Error processing geometry: {e}")
            reason = f"Geometry could not be read: {e}"

        if valid:
            # Range check. A coordinate outside these bounds is not a geometry problem
            # Shapely can see — the shape is perfectly well-formed, it is just nowhere on
            # Earth. Left unchecked it reaches the map (and the index) as a wild outlier.
            try:
                minx, miny, maxx, maxy = shape({
                    'type': geometry.get('type'),
                    'coordinates': geometry.get('coordinates'),
                }).bounds
                if (abs(minx) > LON_LIMIT or abs(maxx) > LON_LIMIT
                        or abs(miny) > LAT_LIMIT or abs(maxy) > LAT_LIMIT):
                    valid = False
                    reason = (f"Coordinates ({minx:.10g}, {miny:.10g}) to ({maxx:.10g}, {maxy:.10g}) "
                              f"are outside the valid range (longitude +/-{LON_LIMIT:.10g}, "
                              f"latitude +/-{LAT_LIMIT:.10g}).")
            except Exception as e:
                logger.debug(f"Could not range-check geometry bounds: {e}")
    else:
        logger.error(f"Error: geometry lacks either type or coordinates.")
        valid = False
        reason = "Geometry lacks either `type` or `coordinates`."

    return geometry, fixed, valid, reason


def validate_feature_geometry(feature):
    """
    Validate the geometry of a GeoJSON feature using Shapely.
    Fix invalid geometries using a two-tier approach for single geometries or geometries in a GeometryCollection.

    :param feature: A GeoJSON feature with geometry to validate
    :return: Tuple of (feature, fixed, valid, reason), where fixed is a boolean indicating if a fix was
             applied, valid is a boolean indicating if the geometry is valid after fixing, and reason
             describes the failure when it is not.
    """
    fixed = False
    valid = False
    reason = None

    if 'geometry' in feature:
        # Extract geometry from the feature
        geometry = feature.get('geometry', None)

        if geometry is None:
            logger.debug("Feature has no geometry (null).")
            valid = True  # null geometries are valid
        elif isinstance(geometry, dict):
            geometry_type = geometry.get('type', None)

            if geometry_type == 'GeometryCollection':
                geometries = geometry.get('geometries', [])
                filtered_geometries = []
                all_valid = True  # Assume all geometries are valid initially

                for i, geom in enumerate(geometries):
                    if geom is None:
                        logger.debug(f"Skipping null geometry at index {i}.")
                        continue

                    logger.debug(f"Validating geometry {i} in GeometryCollection.")
                    geom, geom_fixed, geom_valid, geom_reason = validate_geometry(geom)
                    if geom_fixed:
                        fixed = True
                    if not geom_valid:
                        all_valid = False
                        if reason is None:
                            reason = f"Geometry {i} of the collection: {geom_reason}"
                    filtered_geometries.append(geom)  # Collect valid geometries

                # Update the feature with filtered geometries
                feature['geometry']['geometries'] = filtered_geometries
                valid = all_valid  # Set valid to True only if all geometries are valid
            elif geometry_type:
                # Handle single geometries
                geometry, fixed, valid, reason = validate_geometry(geometry)
                feature['geometry'] = geometry
            else:
                logger.debug("Feature geometry lacks `type`.")
                reason = "Feature geometry lacks `type`."

        else:
            logger.error("Invalid geometry format in feature.")
            reason = "Feature geometry is not an object."

    # No need to handle absence of `geometry` or other errors as this will be done by JSON Schema validation
    return feature, fixed, valid, reason


def revoke_all_subtasks(redis_client, task_id):
    subtasks = [subtask.decode('utf-8') for subtask in redis_client.lrange(f"{task_id}_subtasks", 0, -1)]

    for sub_task_id in subtasks:
        try:
            task_result = AsyncResult(sub_task_id)
            task_result.revoke(terminate=True)
            logger.debug(f"Sub-task {sub_task_id} has been cancelled.")
        except Exception as e:
            logger.error(f"Failed to cancel sub-task {sub_task_id}: {e}")

    # Cleanup Redis record of subtasks
    redis_client.delete(f"{task_id}_subtasks")
    logger.debug(f"Redis list '{task_id}_subtasks' has been deleted.")


@shared_task(bind=True)
def validate_feature_batch(self, feature_batch, schema, task_id, namespaces=None):
    """
    Validate a batch of features and manage subtasks.
    
    :param self: The Celery task instance.
    :param feature_batch: List of GeoJSON features to validate.
    :param schema: JSON schema for validation.
    :param task_id: ID of the parent task.
    """

    validator = Draft7Validator(schema)
    redis_client = get_redis_client()

    # Store the current task ID as a subtask (do not duplicate recording which is done by the scheduler).
    sub_task_id = self.request.id
    logger.debug(f"Subtask {sub_task_id} registered locally for parent {task_id}")

    # If a batch reference was passed as a string, support either Redis key references (redis:<key>)
    # or a filesystem path (legacy). Load the JSON into memory and remember which backing store to clean up.
    temp_batch_file = None
    redis_batch_key = None
    if isinstance(feature_batch, str):
        temp_ref = feature_batch
        try:
            if temp_ref.startswith('redis:'):
                # Load batch from Redis
                redis_batch_key = temp_ref.split(':', 1)[1]
                t_load_start = time.time()
                raw = redis_client.get(redis_batch_key)
                if raw is None:
                    raise Exception(f"Redis key '{redis_batch_key}' not found or expired")
                # raw may be bytes
                if isinstance(raw, bytes):
                    raw = raw.decode('utf-8')
                feature_batch = json.loads(raw)
                t_load = time.time() - t_load_start
                logger.info(f"Sub-task {sub_task_id}: loaded {len(feature_batch)} features from redis key {redis_batch_key} in {t_load:.2f}s")
            else:
                # Treat as filepath
                temp_batch_file = temp_ref
                t_load_start = time.time()
                with open(temp_batch_file, 'r', encoding='utf-8') as bf:
                    feature_batch = json.load(bf)
                t_load = time.time() - t_load_start
                logger.info(f"Sub-task {sub_task_id}: loaded {len(feature_batch)} features from {temp_batch_file} in {t_load:.2f}s")
        except Exception as e:
            logger.error(f"Sub-task {sub_task_id}: failed to load batch {temp_ref}: {e}")
            # Record an error and exit early
            try:
                redis_client.rpush(f"{task_id}_errors", json.dumps({
                    "feature_id": "--batch-load--",
                    "path": "batch_load",
                    "description": str(e)
                }))
                redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
            except Exception:
                pass
            return

    # record subtask start and ensure we always clean up temp files
    try:
        # record subtask start and ensure we always clean up temp files
        try:
            redis_client.hset(task_id, mapping={f'subtask_{sub_task_id}_start': timezone.now().isoformat()})
        except Exception:
            pass

        logger.info(f"Sub-task {sub_task_id} started: validating batch size {len(feature_batch)} for parent {task_id}")

        LOG_EVERY = getattr(settings, 'VALIDATION_LOG_EVERY', 50)

        # Main processing loop (existing logic follows)
        processed_in_batch = 0
        batch_start_time = time.time()

        for feature in feature_batch:
            stopValidation = False
            fixAttempts = 0

            processed_in_batch += 1
            feature_id = feature.get('@id', feature.get('id', '-- no @id --'))
            feature_start = time.time()
            logger.debug(f"Subtask {sub_task_id}: processing feature {processed_in_batch}/{len(feature_batch)} id={feature_id}")

            feature, fixed, valid, geometry_reason = validate_feature_geometry(feature)
            geom_time = time.time() - feature_start
            logger.debug(f"Subtask {sub_task_id}: geometry validation for feature id={feature_id} -> valid={valid}, fixed={fixed} (took {geom_time:.2f}s)")
            # update Redis heartbeat
            try:
                redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
            except Exception:
                pass

            if not valid:
                redis_client.rpush(f"{task_id}_errors", json.dumps({
                    "feature_id": feature.get("@id", "-- no @id --"),
                    "path": "features.feature.geometry",
                    "description": geometry_reason or "Geometry failed validation and could not be fixed."
                }))
                redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
            if fixed:
                redis_client.rpush(f"{task_id}_fixes", json.dumps({
                    "feature_id": feature.get("@id", "-- no @id --"),
                    "path": "features.feature.geometry",
                    "fix": feature['geometry'],
                    "description": "Geometry fixed."
                }))
                redis_client.hset(task_id, 'last_update', timezone.now().isoformat())

            featureCollection = {
                "type": "FeatureCollection",
                "features": [feature]
            }

            while not stopValidation:
                try:
                    # logger.debug(f'Validating feature: {feature}')
                    # Log immediately before schema validation to catch hangs
                    logger.debug(f"Subtask {sub_task_id}: starting schema validation for feature id={feature_id} ({processed_in_batch}/{len(feature_batch)})")
                    validate_start = time.time()
                    validator.validate(featureCollection)
                    validate_elapsed = time.time() - validate_start
                    stopValidation = True
                    logger.debug(f"Subtask {sub_task_id}: validated feature id={feature_id} (schema) in {validate_elapsed:.2f}s")
                    # Warn if validation was unusually slow
                    SLOW_VALIDATE_WARN = getattr(settings, 'VALIDATION_SLOW_VALIDATE_WARN', 5.0)
                    if validate_elapsed > SLOW_VALIDATE_WARN:
                        logger.warning(f"Subtask {sub_task_id}: slow schema validation for feature id={feature_id} took {validate_elapsed:.1f}s")
                except ValidationError as e:
                    # logger.debug(f'ValidationError: {e}')
                    error_path = " -> ".join([str(p) for p in e.absolute_path])
                    detailed_error = parse_validation_error(e)
                    full_error = f"Validation error at {error_path}: {detailed_error}"
                    # logger.debug(full_error)
                    json_error = json.dumps({
                        "feature_id": feature.get("@id", "-- no @id --"),
                        "path": error_path,
                        "description": detailed_error
                    })
                    if fixAttempts < settings.VALIDATION_MAXFIXATTEMPTS:
                        try:
                            featureCollection, fixes = fix_feature({
                                "type": "FeatureCollection",
                                "features": [feature]
                            }, e, namespaces)
                            fixAttempts += 1

                            if fixes:
                                for fix in fixes:  # Iterate over the list of fixes
                                    try:
                                        redis_client.rpush(f"{task_id}_fixes", json.dumps(fix))
                                    except Exception as e:
                                        logger.error(f"Failed to push fix to Redis: {e}")
                            else:
                                # No fixes applied; no point in revalidating
                                logger.error(full_error)
                                redis_client.rpush(f"{task_id}_errors", json_error)
                                redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
                                stopValidation = True

                        except Exception as fix_error:
                            logger.error(f"Failed to fix feature: {fix_error}")
                            logger.error(full_error)
                            redis_client.rpush(f"{task_id}_errors", json_error)
                            redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
                            stopValidation = True
                    else:
                        logger.error(full_error)
                        redis_client.rpush(f"{task_id}_errors", json_error)
                        redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
                        stopValidation = True
                except Exception as e:
                    logger.error(f"Unexpected error during validation: {e}")
                    redis_client.rpush(f"{task_id}_errors", str(e))
                    redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
                    stopValidation = True

                if stopValidation:
                    try:
                        new_q = redis_client.hincrby(task_id, 'queued_features', -1)
                        redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
                        logger.debug(f"Subtask {sub_task_id}: decremented queued_features, new value: {new_q}")
                    except Exception as e:
                        logger.error(f"Error updating Redis status: {e}")

                # NB: Cannot keep tally of errors within this task because it may be running multiple times concurrently
                errors = [error.decode('utf-8') for error in redis_client.lrange(f"{task_id}_errors", 0, -1)]
                if len(errors) > settings.VALIDATION_MAX_ERRORS:
                    task_status = redis_client.hgetall(f"{task_id}_metadata")
                    task_status = {k.decode('utf-8'): v.decode('utf-8') for k, v in task_status.items()}

                    # Clean up files
                    delimited_filepath = task_status.get('delimited_filepath', '')
                    if delimited_filepath and os.path.exists(delimited_filepath):
                        os.remove(delimited_filepath)
                    file_path = task_status.get('file_path', '')
                    if file_path and os.path.exists(file_path):
                        os.remove(file_path)

                    revoke_all_subtasks(redis_client, task_id)
                    redis_client.hset(task_id, mapping={
                        'status': 'aborted',
                        'end_time': timezone.now().isoformat(),
                        'time_remaining': 0
                    })

                    logger.debug(
                        f"More than {settings.VALIDATION_MAX_ERRORS} errors found: aborting validation of feature batch.")
                    return

                    # Add a delay to each iteration for testing UI
                time.sleep(settings.VALIDATION_TEST_DELAY)

            # Periodic progress log for the subtask
            if processed_in_batch % LOG_EVERY == 0 or processed_in_batch == len(feature_batch):
                elapsed = time.time() - batch_start_time
                try:
                    task_status = redis_client.hgetall(task_id)
                    queued_features = int(task_status.get(b'queued_features', b'0')) if task_status else 0
                except Exception:
                    queued_features = 0
                logger.info(f"Subtask {sub_task_id}: processed {processed_in_batch}/{len(feature_batch)} features in {elapsed:.1f}s; parent queued_features={queued_features}")

        # End of try main processing
        total_elapsed = time.time() - batch_start_time
        logger.info(f"Subtask {sub_task_id} completed: processed {processed_in_batch} features in {total_elapsed:.1f}s")
        try:
            redis_client.hset(task_id, mapping={f'subtask_{sub_task_id}_end': timezone.now().isoformat()})
        except Exception:
            pass

        try:
            task_status = redis_client.hgetall(task_id)
            task_status = {k.decode('utf-8'): v.decode('utf-8') for k, v in task_status.items()}

            all_queued = task_status.get('all_queued', '')
            queued_features = int(task_status.get('queued_features', 0))
            start_time_str = task_status.get('start_time', '')

            if start_time_str:
                start_time = datetime.fromisoformat(start_time_str)
            else:
                start_time = timezone.now()
            end_time = timezone.now()
            elapsed_time = (end_time - start_time).total_seconds()

            if all_queued == 'true' and queued_features == 0:

                if redis_client.llen(f"{task_id}_errors") == 0:
                    logger.debug(f"Saving Dataset: {task_status.get('label', '(missing label)')}")
                    save_dataset(task_id)

                redis_client.hset(task_id, mapping={
                    'status': 'complete',
                    'end_time': end_time.isoformat(),
                    'time_taken': elapsed_time,
                    'time_remaining': 0
                })
                logger.debug(f'Task {task_id} completed successfully.')

                # Cleanup Redis record of subtasks
                redis_client.delete(f"{task_id}_subtasks")
                logger.debug(f"Redis list '{task_id}_subtasks' has been deleted.")

        except Exception as e:
            logger.error(f"Error checking or updating task status: {e}")
    finally:
        # Ensure temp file is deleted
        if temp_batch_file and os.path.exists(temp_batch_file):
            try:
                os.remove(temp_batch_file)
                logger.debug(f"Temporary batch file {temp_batch_file} deleted.")
            except Exception as e:
                logger.error(f"Error deleting temp batch file {temp_batch_file}: {e}")
        if redis_batch_key:
            try:
                redis_client.delete(redis_batch_key)
                logger.debug(f"Temporary redis batch key {redis_batch_key} deleted.")
            except Exception as e:
                logger.error(f"Error deleting temp redis batch key {redis_batch_key}: {e}")


def parse_validation_error(e: ValidationError) -> str:
    """Return a concise, human-readable description of a jsonschema ValidationError.

    Includes the validator, validator_value, a short message and the failing instance value.
    """
    try:
        parts = []
        if hasattr(e, 'validator') and e.validator:
            parts.append(f"validator={e.validator}")
        if hasattr(e, 'validator_value') and e.validator_value is not None:
            parts.append(f"validator_value={e.validator_value}")
        if hasattr(e, 'message'):
            parts.append(f"message={e.message}")
        # include problematic instance snippet
        try:
            inst = e.instance
            # stringify small values
            if isinstance(inst, (str, int, float, bool)):
                parts.append(f"instance={inst}")
            else:
                parts.append(f"instance_type={type(inst).__name__}")
        except Exception:
            pass
        return "; ".join(parts)
    except Exception as ex:
        return str(getattr(e, 'message', str(e)))

