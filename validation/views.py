# validation/views.py
import os
import pandas as pd
import json
import codecs
import logging
import subprocess
import uuid
import ijson
from django.conf import settings
from django.http import JsonResponse
from django.utils import timezone
from pyld import jsonld

from validation.create_dataset import read_json_features_in_batches
from validation.tasks import validate_feature_batch, cleanup
import redis
import sys
from validation.tLPF_mappings import tLPF_mappings
from shapely import wkt
from shapely.geometry import mapping as shapely_mapping
import geojson

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
    try:
        _, ext = os.path.splitext(delimited_filepath)
        ext = ext.lower()
        separator = ',' if ext == '.csv' else '\t' if ext == '.tsv' else ''

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
                        logger.debug(f"Reading from CSV.")
                        df_chunk = pd.read_csv(delimited_filepath, skiprows=lambda x: x != 0 and x in skiprows_set,
                                               **configuration, sep=separator, encoding='utf-8', skipinitialspace=True)
                        if header is None:  # Capture the header only once, from the first chunk
                            header = ";".join(df_chunk.columns.tolist()) or ""
                    else:
                        logger.debug(f"Reading from Excel.")
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

                    if 'geometry' in lpf_feature:
                        lpf_feature['geometry']['type'] = 'Point'
                    else:
                        lpf_feature['geometry'] = None

                    # Replace any existing geometry with geometry contained in any `geowkt`
                    if 'geowkt' in lpf_feature:
                        geometry = wkt.loads(lpf_feature['geowkt'])
                        geojson_geometry = shapely_mapping(geometry)
                        lpf_feature['geometry'] = geojson_geometry
                        lpf_feature.pop('geowkt')

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
        return lpf_file_path, feature_count, separator, header

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
    try:
        if 'json' in ext:  # mime type is not a reliable determinant of JSON
            dataset_metadata["format"] = 'json'
            dataset_metadata["delimited_filepath"] = ''
            dataset_metadata["jsonld_filepath"] = uploaded_filepath
            dataset_metadata["feature_count"] = json_feature_count(dataset_metadata["jsonld_filepath"])
            logger.debug(f'JSON contains {dataset_metadata.get("feature_count")} features.')

            # Extract any local namespace definitions (these will be expanded during validation)
            namespaces = extract_context_namespaces(uploaded_filepath)

            # Extract any metadata from JSON file
            dataset_metadata['creator'], dataset_metadata['title'], dataset_metadata['description'], dataset_metadata['webpage'] = extract_dataset_metadata(uploaded_filepath)
            logger.debug(f'Metadata extracted from JSON: {schema_org_metadata}')
        else:
            dataset_metadata["format"] = ext
            dataset_metadata["delimited_filepath"] = uploaded_filepath
            dataset_metadata["jsonld_filepath"], dataset_metadata["feature_count"], dataset_metadata["separator"], \
                dataset_metadata["header"] = parse_to_LPF(dataset_metadata["delimited_filepath"], ext)
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

    try:
        # Process each batch of features as a separate Celery task
        for feature_batch in read_json_features_in_batches(dataset_metadata["jsonld_filepath"]):
            # The following line could be implemented if the LP Ontology were correct
            # feature_batch = [jsonld.compact(feature, context) for feature in feature_batch]
            validate_feature_batch.delay(feature_batch, schema, task_id, namespaces)
            feature_tally = len(feature_batch)
            redis_client.hincrby(task_id, 'queued_features', feature_tally)
            redis_client.hset(task_id, 'last_update', timezone.now().isoformat())

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


def extract_dataset_metadata(file_path):
    dataset_metadata = {
        'creator': '',
        'title': '',
        'description': '',
        'webpage': ''
    }

    with open(file_path, 'r') as file:
        # Iterate through 'indexing' object
        indexing_data = ijson.items(file, 'indexing', use_float=True)

        for item in indexing_data:
            if isinstance(item, dict):
                # Extract 'creator' names
                if 'creator' in item and isinstance(item['creator'], list):
                    dataset_metadata['creator'] = "; ".join(
                        creator.get('name', '') for creator in item['creator'] if 'name' in creator
                    )

                # Extract 'name' as 'title'
                if 'name' in item:
                    dataset_metadata['title'] = item['name']

                # Extract 'description'
                if 'description' in item:
                    dataset_metadata['description'] = item['description']

                # Extract 'url' as 'webpage'
                if 'url' in item:
                    dataset_metadata['webpage'] = item['url']

    return dataset_metadata['creator'], dataset_metadata['title'], dataset_metadata['description'], dataset_metadata['webpage']


def extract_context_namespaces(file_path):
    context_namespaces = {}

    with open(file_path, 'r') as file:
        parser = ijson.items(file, '@context.item', use_float=True)

        for item in parser:
            if isinstance(item, dict):
                context_namespaces.update(item)

    return context_namespaces
