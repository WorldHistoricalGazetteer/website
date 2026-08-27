import json
import logging
import os
import re
import shutil
import sys
import time

import ijson
import redis
from django.conf import settings
from django.contrib.gis.geos import GEOSGeometry
from django.core.exceptions import ObjectDoesNotExist, ValidationError
from django.db import transaction, IntegrityError, DataError
from django.db.models import Max
from django.urls import reverse
from django.utils import timezone

from datasets.models import Dataset, DatasetFile
from datasets.place_types import aat_id_from_identifier, fclasses_for_aat
from datasets.utils import aliasIt, ccodesFromGeom
from main.models import Log
from places.models import PlaceGeom, PlaceWhen, PlaceLink, PlaceRelated, PlaceDescription, PlaceDepiction, PlaceName, \
    PlaceType, Place

logger = logging.getLogger('validation')


def get_redis_client():
    return redis.StrictRedis.from_url(settings.CELERY_BROKER_URL)


class PrimaryKeyManager:
    """
    Manages primary key values for database models using Redis. Ensures unique
    and sequential primary keys across multiple processes by:
    - Initializing Redis with the next available primary key based on the maximum
      value from the database.
    - Providing atomic increment operations to fetch the next primary key.
    """

    def __init__(self):
        self.redis_client = get_redis_client()

    def get_next_pk(self, model_name):
        """Get the next primary key for the given model."""
        key = f"{model_name}_max_pk"
        # Increment the key and return the previous value
        return self.redis_client.incr(key)

    def initialize_pks(self, model_names):
        """Initialize the primary keys for a list of models."""
        for model_name in model_names:
            key = f"{model_name}_max_pk"

            # Check if the key already exists in Redis
            redis_value = self.redis_client.get(key)
            if redis_value is not None:
                redis_value = int(redis_value)
            else:
                redis_value = None

            # Get the maximum primary key from the database
            max_pk = self.get_max_pk_from_db(model_name)

            # Determine the initial value to set in Redis
            if redis_value is None:
                initial_value = max_pk + 1 if max_pk is not None else 1
                self.redis_client.set(key, initial_value)
            elif max_pk is not None and redis_value <= max_pk:
                # Only update if the Redis value is less than or equal to the max_pk
                self.redis_client.set(key, max_pk + 1)

    def get_max_pk_from_db(self, model_name):
        """Fetch the maximum primary key from the database for a given model."""
        model_class = globals().get(f'Place{model_name}')
        if model_class:
            try:
                # Fetch the maximum value from the database
                max_pk = model_class.objects.aggregate(max_pk=Max('pk'))['max_pk']
                return max_pk
            except Exception as e:
                logger.error(f"Error fetching max primary key from database for model {model_name}: {e}")
                return None
        else:
            logger.error(f"Model class {model_name} not found.")
            return None


def safe_key(value):
    """Create a Redis-safe key by replacing unsafe characters."""
    return re.sub(r'\W+', '_', value)


def sort_fixes(task_id):
    # Sort by feature @id the fixes stored in Redis for the given task_id
    redis_client = get_redis_client()
    while True:
        # Pop an item from the fixes list (blocking pop in case the list is empty)
        item_json = redis_client.lpop(f"{task_id}_fixes")
        if not item_json:
            break  # No more items to process

        # Parse the JSON object
        item = json.loads(item_json)

        feature_id = item.get("feature_id", "-- no @id --")
        path = item.get("path")
        fix = item.get("fix")

        # Create a safe Redis key using the feature_id
        safe_feature_id = safe_key(feature_id)
        new_key = f"{task_id}_fixes_{safe_feature_id}"

        # Create a new Redis array containing the path and fix as a JSON object
        redis_client.rpush(new_key, json.dumps({
            "path": path,
            "fix": fix
        }))


def save_dataset(task_id):
    redis_client = get_redis_client()

    cleanup_paths = []  # Keep track of paths to clean up in case of failure

    try:
        # Retrieve stored form data from Redis
        dataset_metadata = redis_client.hgetall(f"{task_id}_metadata")
        dataset_metadata = {k.decode('utf-8'): v.decode('utf-8') for k, v in dataset_metadata.items()}

        logger.debug(f"Retrieved dataset metadata: {dataset_metadata}")

        uploaded_filename = dataset_metadata.get('uploaded_filename')
        jsonld_filepath = dataset_metadata.get('jsonld_filepath')
        delimited_filepath = dataset_metadata.get('delimited_filepath')

        cleanup_paths.append(jsonld_filepath)
        cleanup_paths.append(delimited_filepath)

        # The contributor's chosen licence, carried through as an SPDX id and
        # resolved here (place#158). Left null when absent or unrecognised —
        # "no licence recorded" is honest and recoverable, whereas guessing one
        # would put terms on the data that nobody granted.
        license_obj = None
        license_spdx = (dataset_metadata.get('license') or '').strip()
        if license_spdx:
            from licensing.models import License
            license_obj = License.objects.filter(spdx_id=license_spdx).first()
            if license_obj is None:
                logger.warning("Dataset upload: unknown licence id %r — leaving "
                               "the dataset unlicensed", license_spdx)

        # Start a transaction to ensure atomicity
        with transaction.atomic():

            # Create Dataset object
            dataset = Dataset.objects.create(
                license=license_obj,
                title=dataset_metadata[
                          'title'] or f"[-Title yet to be added to metadata-] ({dataset_metadata['label']})",
                label=dataset_metadata['label'],
                description=dataset_metadata['description'] or '[-Description yet to be added to metadata-]',
                numrows=dataset_metadata['feature_count'],
                creator=dataset_metadata['creator'],
                source=dataset_metadata['source'],
                contributors=dataset_metadata['contributors'],
                citation=dataset_metadata.get('citation') or None,
                uri_base=dataset_metadata['uri_base'],
                webpage=dataset_metadata['webpage'],
                pdf=dataset_metadata['pdf'],
                owner_id=int(dataset_metadata['owner_id']),
                ds_status='uploaded'
            )

        try:
            logger.debug(f"Trying ds_insert...")
            ds_insert(jsonld_filepath, dataset, task_id)
        except Exception as e:
            dataset.delete()
            raise

        # Log the creation
        Log.objects.create(
            category='dataset',
            logtype='ds_create',
            subtype='place',
            dataset_id=dataset.id,
            user_id=int(dataset_metadata['owner_id'])
        )

        # Define paths and filenames
        username = dataset_metadata.get('username', 'unknown_user')
        user_folder = os.path.join(settings.MEDIA_ROOT, f"user_{username}")

        # Ensure that the user folder exists
        os.makedirs(user_folder, exist_ok=True)

        def get_unique_filename(filename, new_ext=None, max_path_length=100):
            base, ext = os.path.splitext(filename)
            ext = new_ext or ext

            # 1. Calculate the fixed length of the directory path: /app/media/user_<username>/
            fixed_dir_length = len(user_folder) + 1  # +1 for the path separator '/'

            # 2. Calculate the fixed length of the extension and potential suffix: _<counter><ext>
            # Max possible suffix is "_9999<ext>"
            max_suffix_length = 5 + len(ext)  # e.g., "_9999" (5 chars) + ".tsv" (4 chars) = 9

            # 3. Calculate the maximum allowed length for the base filename itself
            max_base_length = max_path_length - fixed_dir_length - max_suffix_length

            # Ensure the required length is positive
            if max_base_length < 0:
                max_base_length = 10
                logger.warning("User folder path is too long; truncating filename severely.")

            # 4. Truncate the base filename if necessary
            if len(base) > max_base_length:
                base = base[len(base) - max_base_length:]

            counter = 1
            new_filename = f"{base}{ext}"

            # Check for uniqueness and append counter if needed
            while os.path.exists(os.path.join(user_folder, new_filename)):
                current_max_len = max_base_length - (len(str(counter)) + 1)
                base = base[:current_max_len]

                new_filename = f"{base}_{counter}{ext}"
                counter += 1

                if counter > 9999:  # Safety break to prevent infinite loop or huge file numbers
                    raise Exception("File naming counter exceeded 9999 attempts.")

            return new_filename

        def create_DatasetFile(file, format=dataset_metadata['format'], delimiter=None, header=""):

            DatasetFile.objects.create(
                dataset_id=dataset,
                file=file,
                rev=1,
                format=format,
                delimiter=delimiter,
                header=header.split(';'),
                numrows=dataset_metadata['feature_count'],
                df_status='uploaded'
            )

        if not delimited_filepath:  # No LPF conversion was done, simply move the uploaded file
            if jsonld_filepath:
                new_filename = get_unique_filename(uploaded_filename)
                destination_path = os.path.join(user_folder, new_filename)
                shutil.move(jsonld_filepath, destination_path)
                cleanup_paths.append(destination_path)
                logger.debug(f"Moved uploaded file to {destination_path}")

                create_DatasetFile(destination_path)
            else:
                logger.warning("No file to move as both jsonld_filepath and delimited_filepath are missing.")
        else:  # Move both files
            if delimited_filepath:
                new_filename = get_unique_filename(uploaded_filename)
                destination_path = os.path.join(user_folder, new_filename)
                shutil.move(delimited_filepath, destination_path)
                cleanup_paths.append(destination_path)
                logger.debug(f"Moved delimited file to {destination_path}")
                create_DatasetFile(destination_path, delimiter=dataset_metadata['separator'],
                                   header=dataset_metadata['header'])
            if jsonld_filepath:
                new_filename_jsonld = get_unique_filename(uploaded_filename, '.jsonld')
                destination_path_jsonld = os.path.join(user_folder, new_filename_jsonld)
                shutil.move(jsonld_filepath, destination_path_jsonld)
                cleanup_paths.append(destination_path_jsonld)
                logger.debug(f"Moved uploaded file to {destination_path_jsonld}")
                create_DatasetFile(destination_path_jsonld, format='json')

        redis_client.delete(f"{task_id}_metadata")
        # Do not use cleanup task yet - user may still be polling `get_task_status` to fetch the following URL
        dataset_places_url = reverse('datasets:ds_places', kwargs={'id': dataset.id})
        redis_client.hset(task_id, 'dataset_places_url', dataset_places_url)
        logger.debug(f"DatasetPlacesView URL: {dataset_places_url}")

        notification = (
            f"*Subject:* New Dataset Created (platform: {settings.ENV_CONTEXT})\n"
            f"*Owner Name:* {dataset.owner.name if dataset.owner.name else dataset.owner.username}\n"
            f"*Username:* {dataset.owner.username}\n"
            f"*Dataset Title:* {dataset.title}\n"
            f"*Dataset Label:* {dataset.label}\n"
            f"*Dataset ID:* {dataset.id}\n"
            f"*Dataset Feature Count:* {dataset.numrows}\n"
            f"----------------------------------------"
        )

        from whgmail.messaging import zulip_notification
        zulip_notification(notification, topic="New Dataset Created")

        return

    except ObjectDoesNotExist as e:
        message = f"Dataset or Log object does not exist: {e}"
    except KeyError as e:
        message = f"Missing expected key in dataset metadata: {e}"
    except (OSError, shutil.Error) as e:
        message = f"File operation error: {e}"
    except Exception as e:
        message = f"Unexpected error occurred: {e}"

    # Cleanup files after failure
    for path in cleanup_paths:
        if os.path.exists(path):
            try:
                os.remove(path)
                logger.debug(f"Cleaned up {path}")
            except Exception as cleanup_err:
                logger.error(f"Failed to clean up {path}: {cleanup_err}")

    logger.error(message)
    redis_client.hset(task_id, 'insertion_error', message)


def ds_insert(jsonld_filepath, ds, task_id):
    places_already_exist = Place.objects.filter(dataset=ds.label).exists()
    if places_already_exist:
        message = f"Database already contains places for dataset '{ds.label}'. Cannot add more."
        logger.error(message)
        raise Exception(message)

    redis_client = get_redis_client()
    redis_client.hset(task_id, 'insert_start_time', timezone.now().isoformat())
    redis_client.hset(task_id, 'queued_features', ds.numrows)

    data_mappings = {
        'PlaceGeoms': ('Geom', 'geometry', lambda feat: [
            PlaceGeom(place=newpl, src_id=newpl.src_id, jsonb=g, geom=GEOSGeometry(json.dumps(g)))
            for g in feat['geometry']['geometries']] if feat['geometry']['type'] == 'GeometryCollection' else
        [PlaceGeom(place=newpl, src_id=newpl.src_id, jsonb=feat['geometry'],
                   geom=GEOSGeometry(json.dumps(feat['geometry'])))]),
        'PlaceWhens': ('When', 'when', lambda feat: [
            PlaceWhen(place=newpl, src_id=newpl.src_id, jsonb=feat['when'], minmax=newpl.minmax)]),
        'PlaceLinks': ('Link', 'links', lambda feat: [
            PlaceLink(place=newpl, src_id=newpl.src_id,
                      jsonb={"type": l['type'], "identifier": aliasIt(l['identifier'].rstrip('/'))})
            for l in feat['links']]),
        'PlaceRelated': ('Related', 'relations', lambda feat: [
            PlaceRelated(place=newpl, src_id=newpl.src_id, jsonb=r)
            for r in feat['relations']]),
        'PlaceDescriptions': ('Description', 'descriptions', lambda feat: [
            PlaceDescription(place=newpl, src_id=newpl.src_id, jsonb=des)
            for des in feat['descriptions']]),
        'PlaceDepictions': ('Depiction', 'depictions', lambda feat: [
            PlaceDepiction(place=newpl, src_id=newpl.src_id, jsonb=dep)
            for dep in feat['depictions']]),
        'PlaceNames': ('Name', 'names', lambda feat: [
            PlaceName(place=newpl, src_id=newpl.src_id, toponym=n['toponym'].split(',')[0].strip(), jsonb=n)
            for n in feat.get('names', []) if 'toponym' in n]),
        # NB each type carries its own fclass. Zipping the type list against the place's
        # de-duplicated fclass set silently dropped types whenever that set was shorter —
        # every type, when the place had no fclasses at all (place#213).
        'PlaceTypes': ('Type', 'types', lambda feat: [
            PlaceType(place=newpl, src_id=newpl.src_id, jsonb=t,
                      aat_id=aat_id_from_identifier(t.get('identifier')),
                      fclass=(fclasses_for_aat(aat_id_from_identifier(t.get('identifier'))) or [''])[0])
            for t in feat.get('types', [])])
    }

    errors = []
    pk_manager = PrimaryKeyManager()
    pk_manager.initialize_pks([model for model, _, _ in data_mappings.values()])

    sort_fixes(task_id)

    def apply_fix(target, fix):
        """Apply a fix to a target object based on the path and fix provided."""
        logger.debug(f"apply_fix: {target}, {fix}")
        if target is None or fix is None:
            return
        path = fix.get('path', '')
        keys = path.split('.')
        if len(keys) > 2:
            keys = keys[2:]  # Ignore initial "features.0."
        else:
            return  # Path is not valid if it has fewer than 3 parts
        value = fix.get('fix')
        logger.debug(f"apply_fix path & value: {keys}, {value}")

        current = target
        for key in keys[:-1]:  # Traverse to the second-last key
            current = current[int(key) if key.isdigit() else key]

        last_key = keys[-1]
        if value is None:
            del current[last_key]
        else:
            current[last_key] = value

        logger.debug(f"Updated target: {target}")

    # Start a transaction to ensure atomicity
    with transaction.atomic():
        try:
            total_inserted = 0
            batch_index = 0
            LOG_EVERY = getattr(settings, 'VALIDATION_LOG_EVERY', 50)
            for feature_batch in read_json_features_in_batches(jsonld_filepath):
                batch_index += 1
                batch_size = len(feature_batch)
                logger.info(f"ds_insert: starting batch {batch_index} with {batch_size} features for dataset {ds.label}")
                batch_start = time.time()
                for feat in feature_batch:
                    logger.debug(f"Inserting feature: {feat}")
                    feature_proc_start = time.time()

                    # Apply fixes if available
                    feature_id = feat.get("@id", "-- no @id --")
                    fixes_key = f"{task_id}_fixes_{safe_key(feature_id)}"
                    # logger.debug(f"Fixes key: {fixes_key}")

                    # Apply fixes if available
                    if redis_client.exists(fixes_key):
                        while redis_client.llen(fixes_key) > 0:
                            fix_json = redis_client.lpop(fixes_key)
                            if fix_json:
                                fix = json.loads(fix_json)
                                apply_fix(feat, fix)
                                logger.debug(f"Feature after applying fix: {feat}")
                    # Only strip parenthetical content if there's exactly one set of parentheses
                    # at the end and there's text before it.
                    # Preserves: "(Spring and Autumn States)", "Name with (parentheses) inside"
                    # Removes: "Paris (France)" -> "Paris"
                    raw_title = feat.get('properties', {}).get('title', '')
                    # Check if there's exactly one '(' and it's followed by ')' at the end
                    if raw_title.count('(') == 1 and raw_title.count(')') == 1 and raw_title.rstrip().endswith(')'):
                        # Check if there's text before the opening parenthesis
                        match = re.match(r'^(.+?)\s*\([^)]*\)$', raw_title)
                        if match:
                            title = match.group(1).strip()
                        else:
                            title = raw_title
                    else:
                        title = raw_title
                    # logger.debug(f'title: {title}')
                    geojson = feat.get('geometry')
                    # logger.debug(f'geojson: {geojson}')
                    ccodes = feat.get('properties', {}).get('ccodes', [])
                    if ccodes is None and geojson:
                        ccodes = ccodesFromGeom(geojson)
                    # logger.debug(f'ccodes: {ccodes}')
                    intervals, minmax = parse_dates(feat)
                    # logger.debug(f"intervals: {intervals}")
                    # logger.debug(f"minmax: {minmax}")
                    fclass_list = get_fclass_list(feat)
                    # logger.debug(f"fclass_list: {fclass_list}")

                    logger.debug(f"New Place from feature: {feat}")

                    logger.info(f"ds_insert: creating Place object for feature {feat.get('@id', '-- no @id --')}")
                    newpl = Place(
                        src_id=feat.get('@id') if ds.uri_base in ['', None] or not feat.get('@id').startswith(
                            ds.uri_base) else feat.get('@id')[len(ds.uri_base):],
                        dataset=ds,
                        title=title,
                        fclasses=fclass_list,
                        ccodes=ccodes,
                        minmax=minmax,
                        timespans=intervals,
                        create_date=timezone.now()
                    )
                    # Save new Place and time the operation to detect stalls
                    try:
                        save_start = time.time()
                        newpl.save()
                        save_elapsed = time.time() - save_start
                        logger.info(f"ds_insert: saved Place {newpl} (save took {save_elapsed:.2f}s)")
                    except Exception as e:
                        logger.error(f"Error saving Place {newpl}: {e}")
                        raise

                    objs = {key: list(filter(None, [item for sublist in map(create_func, [feat]) for item in sublist]))
                            for key, (_, feat_key, create_func) in data_mappings.items()
                            if feat.get(feat_key)}

                    for model, obj_list in [(model, objs[key]) for key, (model, _, _) in data_mappings.items() if
                                            objs.get(key)]:
                        try:
                            model_class = globals()[f'Place{model}']
                            for obj in obj_list:
                                obj.pk = pk_manager.get_next_pk(model_class.__name__)
                                # time each related object save
                                try:
                                    rsave_start = time.time()
                                    obj.save()
                                    rsave_elapsed = time.time() - rsave_start
                                    logger.debug(f"Saved related {model_class.__name__} object (pk={obj.pk}) in {rsave_elapsed:.2f}s")
                                except Exception as e:
                                    logger.error(f"Error saving related object {model_class.__name__} for Place {newpl}: {e}")
                                    raise
                        except IntegrityError as e:
                            errors.append({"field": model, "error": str(e)})
                            raise IntegrityError(f"IntegrityError in database insertion for {model}: {e}")
                        except ValidationError as e:
                            errors.append({"field": model, "error": str(e)})
                            raise ValidationError(f"ValidationError in database insertion for {model}: {e}")
                        except DataError as e:
                            errors.append({"field": model, "error": str(e)})
                            raise DataError(f"Database insertion load for {model} failed on {newpl}: {e}")
                        except Exception as e:
                            errors.append({"field": model, "error": str(e)})
                            raise Exception(f"Unexpected error in database insertion for {model}: {e}")

                    total_inserted += 1
                    # update heartbeat frequently so UI shows progress
                    try:
                        redis_client.hincrby(task_id, 'queued_features', -1)
                        redis_client.hset(task_id, 'last_update', timezone.now().isoformat())
                    except Exception:
                        pass

                    # periodic progress log
                    if total_inserted % LOG_EVERY == 0:
                        elapsed = time.time() - batch_start
                        logger.info(f"ds_insert: inserted {total_inserted} features so far (last batch {batch_index} progress: {total_inserted % batch_size}/{batch_size}) in {elapsed:.1f}s")
                    feature_proc_elapsed = time.time() - feature_proc_start
                    logger.debug(f"Processed and saved feature {feat.get('@id', '-- no @id --')} in {feature_proc_elapsed:.2f}s")

                elapsed = time.time() - batch_start
                logger.info(f"ds_insert: completed batch {batch_index} with {batch_size} features in {elapsed:.2f}s")

        except Exception as e:
            logger.error(f"Failed to insert data into dataset: {e}")
            raise Exception(f"Failed to insert data into dataset: {e}, Errors: {errors}")


def parse_dates(feature):
    paths = [  # valid `when` locations
        ['when'],
        ['geometry', 'when'],
        ['geometries', 'when'],
        ['names', 'when'],
        ['types', 'when'],
        ['relations', 'when'],
        ['relations', 'related', 'when']
    ]

    timespans = []

    def reduce_timespan_to_years(timespan):

        def extract_year(date_str):
            if date_str is None:
                return None
            if isinstance(date_str, (int, float)):
                return int(date_str)
            if not isinstance(date_str, str):
                return None
            match = re.match(r'^-?\d{1,4}', date_str)
            if not match:
                match = re.match(r'^-\d{5,}', date_str)
            return int(match.group(0)) if match else None

        def extract_from_dates(dates):
            return {extract_year(dates.get(key)) for key in ('in', 'earliest', 'latest') if dates.get(key)}

        years = set()
        if 'start' in timespan and timespan['start']:
            years.update(extract_from_dates(timespan['start']))
        if 'end' in timespan and timespan['end']:
            years.update(extract_from_dates(timespan['end']))

        sorted_years = sorted(year for year in years if year is not None)

        return [sorted_years[0], sorted_years[-1]] if sorted_years else None

    def when_timespans(when_obj):
        return when_obj.get('timespans', []) if when_obj else []

    for path in paths:
        obj = feature
        for key in path[:-1]:
            if obj is None:
                break
            obj = obj.get(key, [])
            if isinstance(obj, list):
                for item in obj:
                    if item is not None:
                        timespans.extend(when_timespans(item.get(path[-1])))
                break
        else:
            if obj is not None:
                timespans.extend(when_timespans(obj.get(path[-1])))

    unique_intervals = sorted(
        set(
            tuple(result) for timespan in timespans if (result := reduce_timespan_to_years(timespan)) is not None
        ),
        key=lambda x: (x[0], -x[1])  # Sort by start ascending, then by end descending
    )

    # Merge overlapping and contained intervals
    merged_intervals = []
    for start, end in unique_intervals:
        if merged_intervals:
            last_start, last_end = merged_intervals[-1]
            if start <= last_end:  # Overlapping or contained
                merged_intervals[-1][1] = max(last_end, end)
            else:
                merged_intervals.append([start, end])
        else:
            merged_intervals.append([start, end])

    all_years = [year for interval in merged_intervals for year in interval]
    minmax = [min(all_years), max(all_years)] if all_years else None

    return merged_intervals, minmax


def get_fclass_list(feat):
    # Mappings between GeoNames and Wikidata types
    geo_wd_mapping = {
        'A': ['Q56061', 'Q192611', 'Q102496', 'Q10864048', 'Q1799794', 'Q1149654', 'Q82794', 'Q15642541', 'Q217151'],
        'P': ['Q515', 'Q15310171', 'Q18511725', 'Q98929991', 'Q7930989', 'Q486972', 'Q3957', 'Q532', 'Q178342',
              'Q22698', 'Q2983893', 'Q13221722'],
        'S': ['Q41176', 'Q189004', 'Q168719', 'Q3957', 'Q16917', 'Q515', 'Q811979', 'Q220933', 'Q55488', 'Q13221722',
              'Q47168', 'Q32815', 'Q57821', 'Q23442'],
        'R': ['Q34442', 'Q728937', 'Q55488', 'Q22649', 'Q11053', 'Q41176', 'Q1457376', 'Q1078747', 'Q4119149'],
        'L': ['Q82794', 'Q2542546', 'Q15642541', 'Q131681', 'Q35657', 'Q19836241', 'Q27096235'],
        'T': ['Q8502', 'Q207326', 'Q145694', 'Q650118', 'Q54050', 'Q16917', 'Q11444', 'Q8502', 'Q1170715', 'Q189604',
              'Q24415136', 'Q2329'],
        'H': ['Q8502', 'Q4022', 'Q23397', 'Q12284', 'Q9131', 'Q124482', 'Q13100073', 'Q1232506', 'Q166620', 'Q283',
              'Q26557']
    }

    properties = feat.get('properties', {})

    fclass_list = properties.get('fclasses', []) or []
    fclass_set = set(fclass_list)

    # LPF carries `types` at the FEATURE level, and that is where the delimited-to-LPF
    # conversion puts a row's `aat_types`. Reading `properties.types` — which is what this
    # did — found nothing, so `aat_types` contributed no feature class at all on the live
    # upload path. `properties` is still read for any file that nests them there (place#213).
    types_to_process = list(feat.get('types') or []) + list(properties.get('types') or [])
    for t in types_to_process:
        if not isinstance(t, dict):
            logger.warning(f"Invalid type object encountered: {t}")
            continue
        identifier = t.get('identifier')
        if identifier and identifier.startswith('aat:'):
            aat_id = aat_id_from_identifier(identifier)
            # Every class the concept carries, not just the first. `Type.fclass` returns
            # fclasses[0] of a sorted list, so `cities` (['A', 'P']) derived 'A' and
            # dropped out of populated-place filtering.
            derived = fclasses_for_aat(aat_id)
            if derived:
                fclass_set.update(derived)
            else:
                logger.warning(f"No feature class found for AAT concept {identifier}.")
        elif identifier:
            # Mapping from geo_wd_mapping
            mapped_fclass = next((fclass for fclass, wd_types in geo_wd_mapping.items() if identifier[3:] in wd_types),
                                 None)
            if mapped_fclass:
                fclass_set.add(mapped_fclass)
            else:
                logger.warning(f"Identifier {identifier} not found in geo_wd_mapping.")
        else:
            logger.warning(f"Invalid type object encountered: {t}")

    # Sorted for a deterministic array; order carries no meaning here, and every consumer
    # reads the whole array rather than its first element.
    return sorted(fc for fc in fclass_set if fc)


def get_memory_size(obj):
    """Estimate the memory size of an object."""
    return sys.getsizeof(obj) + sum(sys.getsizeof(v) for v in obj.values() if isinstance(obj, dict))


def read_json_features_in_batches(file_path):
    """
    Streams JSON features from a file and yields batches of complete `Feature` objects
    without loading the entire file into memory.
    """
    try:
        with open(file_path, 'r') as file:
            logger.debug(f'Opened {file_path}...')
            parser = ijson.items(file, 'features.item', use_float=True)
            feature_batch = []
            current_memory_size = 0
            total_features_parsed = 0
            LOG_EVERY = 1000

            logger.debug(f'Parsing batch from {file_path} with parser: {type(parser)}')
            for feature in parser:
                current_memory_size += get_memory_size(feature)
                feature_batch.append(feature)
                total_features_parsed += 1

                if total_features_parsed % LOG_EVERY == 0:
                    logger.info(f"Parsed {total_features_parsed} features so far from {file_path}")

                # Also split batches that become too large by feature count to avoid long-running Celery tasks
                max_features = getattr(settings, 'VALIDATION_MAX_BATCH_FEATURES', 250)
                if current_memory_size >= settings.VALIDATION_BATCH_MEMORY_LIMIT or len(feature_batch) >= max_features:
                    logger.info(f"Yielding batch of {len(feature_batch)} features (approx {current_memory_size} bytes) from {file_path}")
                    # If the batch is larger than max_features, split it into sub-batches
                    while len(feature_batch) > max_features:
                        sub = feature_batch[:max_features]
                        logger.debug(f"Splitting large batch: yielding sub-batch of {len(sub)} features")
                        yield sub
                        feature_batch = feature_batch[max_features:]
                    if feature_batch:
                        logger.debug(f"Yielding remaining batch of {len(feature_batch)} features")
                        yield feature_batch
                    feature_batch = []
                    current_memory_size = 0

            # Yield any remaining features in the last batch
            if feature_batch:
                logger.info(f"Yielding final batch of {len(feature_batch)} features (approx {current_memory_size} bytes) from {file_path}; total parsed: {total_features_parsed}")
                yield feature_batch

    except (IOError, ValueError) as e:
        logger.error(f"Error reading JSON features in batches: {e}")
        raise
