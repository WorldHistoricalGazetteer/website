# validation/tasks.py
import os
import json
import logging
import time

from celery import Celery
from celery import shared_task
from celery.result import AsyncResult
from datetime import timedelta
from itertools import chain

from jsonschema import Draft7Validator, ValidationError
from django.http import JsonResponse
from django.utils import timezone
from django.conf import settings
import redis

from shapely.geometry import shape
from shapely.validation import make_valid
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
                        "description": fix_description
                    })
                    logger.debug(fix_description)
                else:
                    logger.debug(f"... failed: no appropriate start or end values found.")

        # Attempt to fix ids/urls by either removal, expansion from namespaces, or prepending a dummy namespace
        if isinstance(invalid_value, str) and isinstance(e.validator_value, list):
            ref_list = [ref.get('$ref') for ref in e.validator_value]

            if '#/definitions/patterns/definitions/validURL' in ref_list or '#/definitions/patterns/definitions/namespaceTerm' in ref_list:
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
                    # Replace any namespaces which were defined in @context of uploaded jsonld file
                    for prefix, namespace_uri in namespaces.items():
                        if invalid_value.startswith(prefix):
                            new_value = invalid_value.replace(prefix, namespace_uri, 1)
                            current_element[last_key] = new_value
                            fix_description = f"Substituted prefix '{prefix}' with '{namespace_uri}' in '{invalid_value}'"
                            fixes.append({
                                "feature_id": feature_id,
                                "path": ".".join(map(str, path_list)),
                                "description": fix_description
                            })
                            logger.debug(fix_description)
                            break
                    # Otherwise, prepend a dummy namespace if invalid_value is not empty
                    else:
                        new_value = f"anon:{invalid_value}"
                        current_element[last_key] = new_value
                        fix_description = f"Fixed @id value: '{invalid_value}' to '{new_value}'"
                        fixes.append({
                            "feature_id": feature_id,
                            "path": ".".join(map(str, path_list)),
                            "description": fix_description
                        })
                        logger.debug(fix_description)

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
    start_time = timezone.datetime.fromisoformat(status.get('insert_start_time', status.get('start_time')))
    last_update_time_str = status.get('last_update', status.get('start_time'))
    last_update_time = timezone.datetime.fromisoformat(last_update_time_str)

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
    :return: Tuple of (geometry, fixed, valid)
    """
    fixed = False
    valid = False

    geometry_type = geometry.get('type', None)
    geometry_coordinates = geometry.get('coordinates', None)
    if geometry_type and geometry_coordinates:
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
                    geometry['coordinates'] = fixed_geom.geometry['coordinates']
                    logger.debug(f"Fixed invalid geometry with buffer(0).")
                    fixed = True
                    valid = True
                else:
                    logger.debug("Buffer(0) did not fix the geometry. Attempting make_valid...")

                    # Second Tier Fix: Attempt to fix the geometry using make_valid
                    fixed_geom = make_valid(geom)
                    if fixed_geom.is_valid:
                        geometry['coordinates'] = fixed_geom.geometry['coordinates']
                        logger.debug(f"Fixed invalid geometry with make_valid.")
                        fixed = True
                        valid = True
                    else:
                        logger.error("Failed to fix geometry with make_valid.")
            else:
                logger.debug(f"Geometry passed validation.")
                valid = True

        except Exception as e:
            logger.error(f"Error processing geometry: {e}")
    else:
        logger.error(f"Error: geometry lacks either type or coordinates.")
        valid = False

    return geometry, fixed, valid


def validate_feature_geometry(feature):
    """
    Validate the geometry of a GeoJSON feature using Shapely.
    Fix invalid geometries using a two-tier approach for single geometries or geometries in a GeometryCollection.

    :param feature: A GeoJSON feature with geometry to validate
    :return: Tuple of (feature, fixed, valid), where fixed is a boolean indicating if a fix was applied,
             and valid is a boolean indicating if the geometry is valid after fixing.
    """
    fixed = False
    valid = False

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
                    geom, geom_fixed, geom_valid = validate_geometry(geom)
                    if geom_fixed:
                        fixed = True
                    if not geom_valid:
                        all_valid = False
                    filtered_geometries.append(geom)  # Collect valid geometries

                # Update the feature with filtered geometries
                feature['geometry']['geometries'] = filtered_geometries
                valid = all_valid  # Set valid to True only if all geometries are valid
            elif geometry_type:
                # Handle single geometries
                geometry, fixed, valid = validate_geometry(geometry)
                feature['geometry'] = geometry
            else:
                logger.debug("Feature geometry lacks `type`.")

        else:
            logger.error("Invalid geometry format in feature.")

    # No need to handle absence of `geometry` or other errors as this will be done by JSON Schema validation
    return feature, fixed, valid


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

    # Store the current task ID as a subtask
    sub_task_id = self.request.id
    redis_client.rpush(f"{task_id}_subtasks", sub_task_id)
    redis_client.hincrby(task_id, 'queued_batches', 1)

    for feature in feature_batch:
        stopValidation = False
        fixAttempts = 0

        feature, fixed, valid = validate_feature_geometry(feature)
        if not valid:
            redis_client.rpush(f"{task_id}_errors", json.dumps({
                "feature_id": feature.get("@id", "-- no @id --"),
                "path": "features.feature.geometry",
                "description": "Geometry failed validation and could not be fixed."
            }))
            redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
        if fixed:
            redis_client.rpush(f"{task_id}_fixes", json.dumps({
                "feature_id": feature.get("@id", "-- no @id --"),
                "path": "features.feature.geometry",
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
                validator.validate(featureCollection)
                stopValidation = True
                # logger.debug(f'Validated feature: {feature}')
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
                    redis_client.hincrby(task_id, 'queued_features', -1)
                    redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
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

    try:
        task_status = redis_client.hgetall(task_id)
        task_status = {k.decode('utf-8'): v.decode('utf-8') for k, v in task_status.items()}

        all_queued = task_status.get('all_queued', '')
        queued_features = int(task_status.get('queued_features', 0))
        start_time_str = task_status.get('start_time', '')

        if start_time_str:
            start_time = timezone.datetime.fromisoformat(start_time_str)
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


def parse_validation_error(error: ValidationError) -> str:
    schema_path = ".".join([str(p) for p in error.schema_path])
    instance_path = ".".join([str(p) for p in error.absolute_path])
    error_message = error.message
    error_value = '...' if error.validator == "required" else error.instance

    formatted_error = (
        f"Error Type: {error.validator} ({error.validator_value})\n"
        f"Schema Path: {schema_path}\n"
        f"Instance Path: {instance_path}\n"
        f"Invalid Value: {error_value}\n"
        f"Message: {error_message}"
    )
    return formatted_error
