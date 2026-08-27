# validation/views.py
import os
import pandas as pd
import json
import codecs
import logging
import subprocess
import uuid
import ijson
import zipfile
import tempfile
import shutil
import time
from django.conf import settings
from django.http import JsonResponse
from django.utils import timezone

from validation.create_dataset import read_json_features_in_batches
from validation.tasks import validate_feature_batch, cleanup
import redis
from validation.tLPF_mappings import tLPF_mappings
from validation.coordinates import resolve_lonlat, check_coordinate_pair, is_blank
from shapely import wkt
from shapely.geometry import mapping as shapely_mapping

logger = logging.getLogger('validation')


def get_redis_client():
    return redis.StrictRedis.from_url(settings.CELERY_BROKER_URL)


# def get_memory_size(obj):
#     """Estimate the memory size of an object."""
#     return sys.getsizeof(obj) + sum(sys.getsizeof(v) for v in obj.values() if isinstance(obj, dict))

def json_feature_count(file_path):
    """ Count the number of features in a JSON FeatureCollection file."""
    try:
        with open(file_path, 'r') as file:
            feature_count = sum(1 for _ in ijson.items(file, 'features.item'))
            return feature_count
    except (IOError, ValueError) as e:
        logger.error(f"Error counting features in JSON file: {e}")
        raise


def get_file_info(file_path):
    info = {}

    try:
        # Get MIME type
        mime_type_result = subprocess.run(
            ['file', '--mime-type', '-b', file_path],
            capture_output=True,
            text=True,
            check=True
        )
        info['mime_type'] = mime_type_result.stdout.strip()

        # Get MIME encoding
        mime_encoding_result = subprocess.run(
            ['file', '--mime-encoding', '-b', file_path],
            capture_output=True,
            text=True,
            check=True
        )
        info['mime_encoding'] = mime_encoding_result.stdout.strip()

        logger.debug(f"File info: {info}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running file command: {e}")
        return {'mime_type': None, 'mime_encoding': None}
    except Exception as e:
        logger.error(f"Unexpected error while getting file information: {e}")
        return {'mime_type': None, 'mime_encoding': None}

    return info


def parse_to_LPF(delimited_filepath, ext):
    """
    Convert a delimited upload to LPF.

    Returns (lpf_file_path, feature_count, separator, header, coordinate_report), where
    `coordinate_report` carries the `lon`/`lat` repairs and rejections found while
    converting, plus a tally of rows that ended up with no geometry at all. Those used to
    be discarded in silence — see place#212 — so they are collected here and pushed to the
    validation report as soon as the task has an id.
    """
    try:
        _, ext = os.path.splitext(delimited_filepath)
        ext = ext.lower()
        separator = ',' if ext == '.csv' else '\t' if ext == '.tsv' else ''

        # Detect delimiter in CSV/TSV files
        if ext in ['.csv', '.tsv']:
            with open(delimited_filepath, 'r', encoding='utf-8') as f:
                first_line = f.readline()
                if '\t' in first_line:
                    separator = '\t'
                    logger.debug(f"Detected TSV format in '{delimited_filepath}'.")
                elif ',' in first_line:
                    separator = ','
                    logger.debug(f"Detected CSV format in '{delimited_filepath}'.")
                elif first_line == '':
                    message = f"Empty first line in '{delimited_filepath}'. The first line should contain the LPF field names (e.g. `id`, `title` etc.)."
                    logger.error(message)
                    raise ValueError(message)
        else:
            separator = '' # Do not use `None` as it is not serializable for storage in Redis

        lpf_file_path = delimited_filepath.replace(ext, '.jsonld')
        # Deletion is managed by validation.tasks.clean_tmp_files, triggered by beat_schedule in celery.py
        logger.debug(f"Processing [separator: {separator}] file '{delimited_filepath}'.")

        converters = {key: mapping['converter'] for key, mapping in tLPF_mappings.items()}
        header = ""

        def get_df_reader():
            nonlocal header
            configuration = {
                'nrows': settings.VALIDATION_CHUNK_ROWS,
                'header': 0,
                'true_values': ['true', 'True'],
                'false_values': ['false', 'False'],
                'na_values': ['NA', 'NaN'],
                'na_filter': False
            }
            try:  # read_excel does not support chunk-size, so implementation requires line-reading
                skiprows_set = set()  # Keep track of the rows to skip, excluding the header (0th row)
                skiprows_start = 1  # Start reading after the header

                while True:
                    if separator:
                        if skiprows_start == 1:
                            logger.debug(f"Reading from CSV (separator: '{separator}').")
                        df_chunk = pd.read_csv(delimited_filepath, skiprows=lambda x: x != 0 and x in skiprows_set,
                                               **configuration, sep=separator, encoding='utf-8', skipinitialspace=True)
                        if header == "":  # Capture the header only once, from the first chunk
                            header = ";".join(df_chunk.columns.tolist()) or ""
                            logger.debug(f"Header: '{header}'.")
                    else:
                        if skiprows_start == 1:
                            logger.debug(f"Reading from Excel.")
                            first_row = pd.read_excel(delimited_filepath, nrows=1, header=None, sheet_name=0, convert_float=False)
                            first_row_values = first_row.iloc[0].tolist()

                            if all(pd.isna(val) for val in first_row_values):
                                message = f"Empty first row in Excel file '{delimited_filepath}'. The first row should contain the LPF field names."
                                logger.error(message)
                                raise ValueError(message)
                            elif not any(isinstance(val, str) and val.strip() for val in first_row_values):
                                message = f"Invalid labels in the first row of Excel file '{delimited_filepath}'. The first row should contain the LPF field names."
                                logger.error(message)
                                raise ValueError(message)
                            else:
                                logger.debug(f"Header: {first_row_values}")

                        df_chunk = pd.read_excel(delimited_filepath, skiprows=lambda x: x != 0 and x in skiprows_set,
                                                 **configuration, sheet_name=0, convert_float=False)
                    skiprows_set.update(range(skiprows_start, skiprows_start + settings.VALIDATION_CHUNK_ROWS))
                    skiprows_start += settings.VALIDATION_CHUNK_ROWS
                    if df_chunk.empty:
                        break
                    yield df_chunk

            except pd.errors.EmptyDataError:
                logger.warning("Empty chunk encountered; stopping.")
            except UnicodeDecodeError as e:
                logger.error(f"Encoding error detected: {e}")
                raise
            except pd.errors.ParserError as e:
                logger.error(f"Parsing error detected: {e}")
                raise
            except Exception as e:
                logger.error(f"Error reading file {delimited_filepath}: {e}")
                raise

        def assign_nested_value(d, keys, value):
            """
            Assigns a value to a nested dictionary, creating any intermediate keys as necessary.
            Initializes the nested structure based on the type of the final value.
            """

            for i, key in enumerate(keys[:-1]):
                next_key = keys[i + 1]

                if isinstance(d, dict) and key not in d:
                    if isinstance(next_key, int):
                        d[key] = [{}] * (next_key + 1)
                    else:
                        d[key] = {}

                d = d[key]

            final_key = keys[-1]
            if isinstance(d, list) and final_key >= len(d):
                d.extend([{}])
            d[final_key] = value

        # Coordinate parsing/repair report (place#212). Lists are capped so that a
        # wholesale mangling of a large file cannot exhaust memory, but the tallies are not.
        max_reported = getattr(settings, 'VALIDATION_MAX_ERRORS', 100)
        coordinate_report = {
            'errors': [],
            'fixes': [],
            'error_count': 0,
            'fix_count': 0,
            'rows_without_geometry': 0,
        }

        counter_key = {'errors': 'error_count', 'fixes': 'fix_count'}

        def report(bucket, feature_id, description):
            coordinate_report[counter_key[bucket]] += 1
            if len(coordinate_report[bucket]) < max_reported:
                coordinate_report[bucket].append({
                    'feature_id': feature_id,
                    'path': 'features.feature.geometry',
                    'description': description,
                })

        with open(lpf_file_path, 'w') as lpf_file:
            first_line = True  # To handle commas between records in feature array
            feature_count = 0

            # Write the opening of the JSON FeatureCollection
            lpf_file.write('{\n"type": "FeatureCollection",\n"features": [\n')
            logger.debug(f"Started writing output to '{lpf_file_path}'.")

            for chunk in get_df_reader():

                for _, record in chunk.iterrows():
                    record = record.to_dict()  # Convert row to a dictionary
                    logger.debug(f"Processing record #{feature_count}: '{record}'.")

                    feature_id = str(record.get('id', '')).strip() or f"row {feature_count + 1}"

                    # Resolve `lon`/`lat` as a pair before the converters run. `float()`
                    # failing here used to leave `coordinates` unassigned and the feature
                    # with a null geometry, reported to the contributor as valid.
                    if 'lon' in record or 'lat' in record:
                        ccodes = [c.strip() for c in str(record.get('ccodes') or '').split(';') if c.strip()]
                        lon, lat, repairs, coord_errors = resolve_lonlat(
                            record.get('lon'), record.get('lat'), ccodes=ccodes)
                        # A `geowkt` column supersedes lon/lat entirely, so its problems
                        # are not the contributor's to fix.
                        if is_blank(record.get('geowkt')):
                            for description in repairs:
                                report('fixes', feature_id, description)
                            for description in coord_errors:
                                report('errors', feature_id, description)
                        record['lon'] = lon
                        record['lat'] = lat

                    # Apply converters: NB these cannot be applied during pd.read_excel due to the unhashable lists they produce
                    for key, converter in converters.items():
                        if key in record:
                            record[key] = converter(record[key])

                    # Create the nested JSON structure
                    lpf_feature = {'type': 'Feature'}
                    for key, mapping in tLPF_mappings.items():
                        if key in record:
                            value = record[key]
                            if isinstance(value, list) or pd.notna(value):
                                nested_keys = [int(key) if key.isdigit() else key for key in mapping['lpf'].split('.')]
                                assign_nested_value(lpf_feature, nested_keys, value)

                                # Append additional names or types to the main record if needed
                    if 'names' in lpf_feature and isinstance(lpf_feature['names'],
                                                             list) and 'additional_names' in lpf_feature:
                        lpf_feature['names'].extend(lpf_feature.pop('additional_names'))
                    if 'types' in lpf_feature and isinstance(lpf_feature['types'],
                                                             list) and 'additional_types' in lpf_feature:
                        lpf_feature['types'].extend(lpf_feature.pop('additional_types'))

                    # Create `title` from names.0.toponym
                    if 'properties' not in lpf_feature:
                        lpf_feature['properties'] = {}
                    lpf_feature['properties']['title'] = lpf_feature['names'][0]['toponym']

                    if 'geometry' in lpf_feature and 'coordinates' in lpf_feature['geometry']:
                        lpf_feature['geometry']['type'] = 'Point'
                        # Belt and braces: never hand a malformed position downstream to
                        # GEOSGeometry, which would fail on insert long after validation.
                        position_error = check_coordinate_pair(lpf_feature['geometry']['coordinates'])
                        if position_error:
                            report('errors', feature_id, position_error)
                            lpf_feature['geometry'] = None
                    else:
                        lpf_feature['geometry'] = None

                    # Replace any existing geometry with geometry contained in any `geowkt`
                    if 'geowkt' in lpf_feature:
                        logger.debug(f"Converting WKT geometry to GeoJSON for record #{feature_count}.")
                        geometry = wkt.loads(lpf_feature['geowkt'])
                        geojson_geometry = shapely_mapping(geometry)
                        lpf_feature['geometry'] = geojson_geometry
                        lpf_feature.pop('geowkt')

                    if lpf_feature.get('geometry') is None:
                        coordinate_report['rows_without_geometry'] += 1

                    logger.debug(f"Processed record #{feature_count}: '{lpf_feature}'.")

                    # Write the JSON object to the file
                    if not first_line:
                        lpf_file.write(',\n')
                    else:
                        first_line = False

                    json.dump(lpf_feature, lpf_file, ensure_ascii=False)
                    feature_count += 1

            # Write the closing of the JSON FeatureCollection
            lpf_file.write('\n]}\n')

        logger.debug(f"fLPF file '{delimited_filepath}' converted to LPF and written to '{lpf_file_path}'.")
        logger.debug(f"Returning additional values: feature_count: {feature_count}, separator: {separator}, header: {header}.")
        logger.info(f"Coordinate report for '{delimited_filepath}': "
                    f"{coordinate_report['fix_count']} repaired, {coordinate_report['error_count']} rejected, "
                    f"{coordinate_report['rows_without_geometry']}/{feature_count} rows without geometry.")
        return lpf_file_path, feature_count, separator, header, coordinate_report

    except Exception as e:
        logger.error(f"Error processing file {delimited_filepath}: {e}")
        raise


def validate_file(request, dataset_metadata):
    dataset_metadata = {
        key: (
            ";".join(value) if isinstance(value, list) else (value if value is not None else '')
        )
        for key, value in dataset_metadata.items()
    }

    logger.debug(f"Validating file with form data: {dataset_metadata}")

    uploaded_filepath = dataset_metadata.get('uploaded_filepath')
    uploaded_filename = dataset_metadata.get('uploaded_filename')

    try:
        with codecs.open(settings.LPF_SCHEMA_PATH, 'r', 'utf8') as schema_file:
            schema = json.load(schema_file)
        with codecs.open(settings.LPF_CONTEXT_PATH, 'r', 'utf8') as context_file:
            context = json.load(context_file)
    except (IOError, json.JSONDecodeError) as e:
        message = f"Error reading schema or context file: {e}"
        logger.error(message)
        return JsonResponse({"status": "failed", "message": message}, status=500)

    file_info = get_file_info(uploaded_filepath)

    if file_info['mime_type'] is None:
        message = "Unable to determine the content type of the file."
        logger.info(message)
        # Downloaded files may not have a MIME type
        # return JsonResponse({"status": "failed", "message": message}, status=500)
    elif file_info['mime_type'] not in settings.VALIDATION_SUPPORTED_TYPES:
        message = f"The detected content type (<b>{file_info['mime_type']}</b>) is not supported."
        logger.error(message)
        return JsonResponse({"status": "failed", "message": message}, status=500)

    # Preliminary utf-8 encoding test: further validation is performed when reading non-JSON files
    allowed_encodings = settings.VALIDATION_ALLOWED_ENCODINGS + [
        'binary']  # 'binary' required for spreadsheets when using Unix file command
    if file_info['mime_encoding'] is None:
        message = "Unable to determine the encoding type of the file."
        logger.error(message)
        # Downloaded files may not have a MIME encoding
        # return JsonResponse({"status": "failed", "message": message}, status=500)
    elif file_info['mime_encoding'] not in allowed_encodings:
        message = f"The detected encoding type (<b>{file_info['mime_encoding']}</b>) is not supported. Please ensure that the file is encoded as UTF or ASCII."
        logger.error(message)
        return JsonResponse({"status": "failed", "message": message}, status=500)

    # Convert delimited files to LPF JSON
    _, ext = os.path.splitext(uploaded_filepath)
    ext = ext.lower().lstrip('.')
    namespaces = None
    schema_org_metadata = None
    coordinate_report = None
    try:
        # Handle .zip uploads by extracting the first suitable json/geojson/jsonld member
        if ext == 'zip':
            logger.debug(f"Received zip file for validation: {uploaded_filepath}")
            try:
                with zipfile.ZipFile(uploaded_filepath, 'r') as zf:
                    # Find candidate members (primary preference order)
                    candidates = [name for name in zf.namelist() if name.lower().endswith(('.geojson', '.json', '.jsonld'))]
                    if not candidates:
                        message = "Zip archive does not contain a supported JSON/GeoJSON/JSON-LD file."
                        logger.error(message)
                        return JsonResponse({"status": "failed", "message": message}, status=500)

                    # Prefer geojson over json/jsonld by sorting
                    candidates.sort(key=lambda n: (not n.lower().endswith('.geojson'), n))
                    chosen = candidates[0]
                    logger.info(f"Extracting member '{chosen}' from uploaded zip for validation")

                    # Extract chosen member to a temporary file
                    extracted_temp = tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(chosen)[1], dir='/var/tmp')
                    with zf.open(chosen) as member, open(extracted_temp.name, 'wb') as out_f:
                        shutil.copyfileobj(member, out_f)

                    # Replace uploaded filepath with extracted file for downstream processing
                    dataset_metadata['jsonld_filepath'] = extracted_temp.name
                    dataset_metadata['delimited_filepath'] = ''
                    ext = 'json'
                    # Remove original uploaded zip to free disk space (best-effort)
                    try:
                        if os.path.exists(uploaded_filepath):
                            os.remove(uploaded_filepath)
                            logger.debug(f"Removed original uploaded zip file {uploaded_filepath} after extraction")
                    except Exception as e:
                        logger.warning(f"Could not remove uploaded zip file {uploaded_filepath}: {e}")
            except zipfile.BadZipFile as e:
                message = f"Uploaded file is not a valid zip archive: {e}"
                logger.error(message)
                return JsonResponse({"status": "failed", "message": message}, status=500)

        if 'json' in ext:  # mime type is not a reliable determinant of JSON
            dataset_metadata["format"] = 'json'
            dataset_metadata["delimited_filepath"] = ''
            # If a zip was extracted above, dataset_metadata['jsonld_filepath'] may already be set
            if not dataset_metadata.get('jsonld_filepath'):
                dataset_metadata["jsonld_filepath"] = uploaded_filepath
            dataset_metadata["feature_count"] = json_feature_count(dataset_metadata["jsonld_filepath"])
            logger.debug(f'JSON contains {dataset_metadata.get("feature_count")} features.')

            # Extract any local namespace definitions (these will be expanded during validation)
            json_path = dataset_metadata.get('jsonld_filepath') or uploaded_filepath
            namespaces = extract_context_namespaces(json_path)

            # Extract any metadata from JSON file
            dataset_metadata['creator'], dataset_metadata['title'], dataset_metadata['description'], \
                dataset_metadata['webpage'], dataset_metadata['citation'] = extract_dataset_metadata(json_path)
            logger.debug(f'Metadata extracted from JSON: {schema_org_metadata}')
        else:
            dataset_metadata["format"] = ext
            dataset_metadata["delimited_filepath"] = uploaded_filepath
            dataset_metadata["jsonld_filepath"], dataset_metadata["feature_count"], dataset_metadata["separator"], \
                dataset_metadata["header"], coordinate_report = parse_to_LPF(dataset_metadata["delimited_filepath"], ext)
    except Exception as e:
        message = f"Error converting delimited text to LPF: {e}"
        logger.error(message)
        return JsonResponse({"status": "failed", "message": message}, status=500)

    # Initialise Redis client
    redis_client = get_redis_client()
    logger.debug('Redis client initialised.')
    task_id = f"validation_task_{uuid.uuid4()}"

    # Schedule the cleanup task
    cleanup_task_result = cleanup.apply_async((task_id,), countdown=settings.VALIDATION_TIMEOUT)
    cleanup_task_id = cleanup_task_result.id

    # Store task details in Redis
    redis_client.hset(task_id, mapping={
        'status': 'in_progress',
        'start_time': timezone.now().isoformat(),
        'all_queued': 'false',
        'total_features': dataset_metadata.get('feature_count'),
        'uploaded_filename': uploaded_filename,
        'label': dataset_metadata.get('label'),
        'cleanup_task_id': cleanup_task_id,
    })
    redis_client.hset(f"{task_id}_metadata", mapping=dataset_metadata)
    logger.debug(f"Dataset metadata saved to redis: {dataset_metadata}")

    # Coordinate repairs and rejections found while converting the delimited file. They are
    # pushed here rather than in `parse_to_LPF` because the task id does not exist until now.
    # Rejections land in `{task_id}_errors`, which both blocks the dataset save and shows the
    # contributor exactly which cell was at fault (place#212).
    if coordinate_report:
        for bucket in ('fixes', 'errors'):
            for entry in coordinate_report[bucket]:
                redis_client.rpush(f"{task_id}_{bucket}", json.dumps(entry))
        for bucket, reported, total in (('fixes', len(coordinate_report['fixes']), coordinate_report['fix_count']),
                                        ('errors', len(coordinate_report['errors']), coordinate_report['error_count'])):
            if total > reported:
                redis_client.rpush(f"{task_id}_{bucket}", json.dumps({
                    'feature_id': '-- summary --',
                    'path': 'features.feature.geometry',
                    'description': f"{total - reported} further coordinate "
                                   f"{'repairs' if bucket == 'fixes' else 'problems'} are not listed here.",
                }))
        redis_client.hset(task_id, mapping={
            'rows_without_geometry': coordinate_report['rows_without_geometry'],
            'coordinates_repaired': coordinate_report['fix_count'],
            'coordinates_rejected': coordinate_report['error_count'],
        })

    try:
        total_scheduled = 0
        # Process each batch of features as a separate Celery task
        batch_index = 0
        redis_client_local = redis_client
        for feature_batch in read_json_features_in_batches(dataset_metadata["jsonld_filepath"]):
            # The following line could be implemented if the LP Ontology were correct
            # feature_batch = [jsonld.compact(feature, context) for feature in feature_batch]
            try:
                # Store batch JSON in Redis under a per-task key to avoid cross-container file sharing
                batch_key = f"{task_id}:batch:{batch_index}"
                batch_index += 1
                try:
                    redis_client_local.set(batch_key, json.dumps(feature_batch), ex=86400)
                    logger.debug(f"Stored batch in Redis key {batch_key} ({len(feature_batch)} features)")
                except Exception as e:
                    logger.error(f"Failed to store batch in Redis key {batch_key}: {e}")
                    raise

                # Schedule the Celery subtask and pass the Redis key reference (prefixed with 'redis:')
                apply_start = time.time()
                async_result = validate_feature_batch.apply_async((f"redis:{batch_key}", schema, task_id, namespaces))
                apply_elapsed = time.time() - apply_start
                if apply_elapsed > 1.0:
                    logger.warning(f"Scheduling subtask took {apply_elapsed:.2f}s which is unusually long")

                subtask_id = async_result.id
                feature_tally = len(feature_batch)
                # Record subtask id so it can be inspected or revoked later
                try:
                    redis_client_local.rpush(f"{task_id}_subtasks", subtask_id)
                    redis_client_local.hincrby(task_id, 'queued_batches', 1)
                except Exception:
                    logger.warning(f"Could not record subtask id {subtask_id} in redis for task {task_id}")
                # Increment the queued_features counter and heartbeat
                redis_client_local.hincrby(task_id, 'queued_features', feature_tally)
                redis_client_local.hset(task_id, 'last_update', timezone.now().isoformat())
                logger.info(f"Scheduled subtask {subtask_id} for {feature_tally} features (parent task {task_id}); redis_key={batch_key}")
                total_scheduled += 1
            except Exception as e:
                logger.error(f"Failed to schedule validate_feature_batch subtask: {e}")

        # Summary after scheduling
        try:
            subtasks_len = redis_client.llen(f"{task_id}_subtasks")
            queued_batches = int(redis_client.hget(task_id, 'queued_batches') or 0)
            queued_features = int(redis_client.hget(task_id, 'queued_features') or 0)
            logger.info(f"Finished scheduling {total_scheduled} subtasks for task {task_id}; redis reports subtasks_len={subtasks_len}, queued_batches={queued_batches}, queued_features={queued_features}")
        except Exception:
            logger.info(f"Finished scheduling subtasks for task {task_id}")

        redis_client.hset(task_id, 'all_queued', 'true')
        redis_client.hset(task_id, 'last_update', timezone.now().isoformat())

    except Exception as e:
        full_error = f"Batch processing error: {str(e)}"
        logger.error(full_error)
        redis_client.rpush(f"{task_id}_errors", full_error)

        redis_client.hset(task_id, mapping={
            'status': 'failed',
            'end_time': timezone.now().isoformat()
        })

        return JsonResponse({"status": "failed", "message": str(e)}, status=500)

    return JsonResponse({"status": "in_progress", "task_id": task_id})


def _creator_name(node):
    """Name of a schema.org creator node — plain Person/Organization, or a CRediT Role wrapper
    ({"@type": "Role", "roleName": <credit-uri>, "contributor": {...}}) whose person is nested."""
    if not isinstance(node, dict):
        return ''
    if node.get('name'):
        return node['name']
    inner = node.get('contributor') or node.get('creator') or node.get('author')
    if isinstance(inner, dict):
        return inner.get('name', '')
    return ''


def extract_dataset_metadata(file_path):
    dataset_metadata = {
        'creator': '',
        'title': '',
        'description': '',
        'webpage': '',
        'citation': '',
    }

    with open(file_path, 'r') as file:
        # Iterate through 'indexing' object
        indexing_data = ijson.items(file, 'indexing', use_float=True)

        for item in indexing_data:
            if isinstance(item, dict):
                # Extract 'creator' names — accept a single node or a list, plain or CRediT-Role-wrapped;
                # de-duplicate so a person contributing under several CRediT roles is named once.
                if 'creator' in item:
                    creators = item['creator'] if isinstance(item['creator'], list) else [item['creator']]
                    names = []
                    for creator in creators:
                        name = _creator_name(creator)
                        if name and name not in names:
                            names.append(name)
                    if names:
                        dataset_metadata['creator'] = "; ".join(names)

                # Extract 'name' as 'title'
                if 'name' in item:
                    dataset_metadata['title'] = item['name']

                # Extract 'description'
                if 'description' in item:
                    dataset_metadata['description'] = item['description']

                # Extract 'url' as 'webpage'
                if 'url' in item:
                    dataset_metadata['webpage'] = item['url']

                # Extract a ready-made citation string (generated in the browser by Map-your-Data);
                # cap to the Dataset.citation column width.
                if isinstance(item.get('citation'), str):
                    dataset_metadata['citation'] = item['citation'][:2044]

    return (dataset_metadata['creator'], dataset_metadata['title'], dataset_metadata['description'],
            dataset_metadata['webpage'], dataset_metadata['citation'])


def extract_context_namespaces(file_path):
    context_namespaces = {}

    with open(file_path, 'r') as file:
        parser = ijson.items(file, '@context.item', use_float=True)

        for item in parser:
            if isinstance(item, dict):
                context_namespaces.update(item)

    return context_namespaces
