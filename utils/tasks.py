# utils/tasks.py
# Generic Celery tasks and helpers 

from __future__ import absolute_import, unicode_literals
import csv
import simplejson as json
import os
import zipfile
from copy import deepcopy

import pandas as pd
from celery import shared_task
from celery.utils.log import get_task_logger
from celery.result import AsyncResult
from django.conf import settings
from django.core import serializers
from django.contrib.auth import get_user_model
from django.http import HttpResponse

from areas.models import Area
from collection.models import Collection, CollPlace
from datasets.models import Dataset
from datasets.utils import makeNow
from main.models import DownloadFile, Log, Comment
from places.models import Place
from whgmail.messaging import WHGmail

logger = get_task_logger('tasks')
User = get_user_model()


def downloader(request, *args, **kwargs):
    logger.debug(f'downloader() user: {request.user}, request.POST: {request.POST}')

    if request.method == 'POST':
        # Check if the request is an AJAX request
        is_ajax = request.headers.get('x-requested-with') == 'XMLHttpRequest'

        if is_ajax:
            logger.debug('AJAX request detected')
            format = request.POST.get('format', 'json')
            dsid = request.POST.get('dsid')
            collid = request.POST.get('collid')
            user = request.user

            if not dsid and not collid:
                logger.error('No dsid or collid provided')
                return HttpResponse(status=400, content='Missing required parameters.')

            try:
                userid = user.id if user.is_authenticated else 1
                download_task = make_download.delay(
                    userid=userid,
                    username=user.username,
                    dsid=dsid,
                    collid=collid,
                    format=format
                )
                logger.info(f'Task sent to Celery, task_id: {download_task.task_id}')
                return HttpResponse(
                    json.dumps({'task_id': download_task.task_id}),
                    content_type='application/json'
                )
            except Exception as e:
                logger.error(f'Error sending task to Celery: {e}')
                return HttpResponse(status=500, content='Internal Server Error')
        else:
            logger.info(f'Non-AJAX POST request, data: {request.POST}')
            return HttpResponse(status=400, content='Invalid request format.')

    elif request.method == 'GET':
        logger.info(f'GET request, data: {request.GET}')
        return HttpResponse(status=200, content='GET requests are not supported at this endpoint.')

    logger.error('Unsupported HTTP method')
    return HttpResponse(status=405, content='Method Not Allowed')


def generate_zip_filename(data_dump_filename):
    try:
        data_filename = os.path.basename(data_dump_filename)
        zip_filename = os.path.join(settings.MEDIA_ROOT, 'downloads', f'{data_filename}.zip')
        logger.debug(f'Generated zip filename: {zip_filename}')
        return zip_filename
    except Exception as e:
        logger.error(f'Error generating zip filename: {e}')
        raise


def create_downloadfile_record(user, ds, coll, zip_filename):
    try:
        if zip_filename.startswith('/app/'):
            zip_filename = zip_filename[5:] # Remove '/app/' from the beginning of the path
        logger.debug(
            f'Creating DownloadFile record for user: {user}, dataset: {ds}, collection: {coll}, zip_filename: {zip_filename}')
        title = coll.title if coll and not ds else ds.title if ds else None
        DownloadFile.objects.create(
            user=user,
            dataset=ds,
            collection=coll,
            title=title,
            filepath=f'/{zip_filename}',
        )
        logger.info('DownloadFile record created successfully')
    except Exception as e:
        logger.error(f'Error creating DownloadFile record: {e}')
        raise


def filter_and_order(metadata_json, desired_fields):
    try:
        filtered_metadata = {field: metadata_json['fields'].get(field) for field in desired_fields}
        logger.debug(f'Filtered metadata: {filtered_metadata}')
        return filtered_metadata
    except Exception as e:
        logger.error(f'Error filtering metadata: {e}')
        raise


def dataset_to_json(dsid):
    try:
        dataset = Dataset.objects.get(id=dsid)
        dataset_json = json.loads(serializers.serialize('json', [dataset]))[0]
        desired_fields = ['title', 'creator', 'create_date', 'description', 'numrows', 'source', 'webpage']
        filtered_dataset = filter_and_order(dataset_json, desired_fields)
        filtered_dataset.update({
            'model': dataset_json['model'],
            'pk': dataset_json['pk']
        })
        logger.info(f'Dataset JSON: {filtered_dataset}')
        return filtered_dataset
    except Dataset.DoesNotExist as e:
        logger.error(f'Dataset with ID {dsid} does not exist: {e}')
        raise
    except Exception as e:
        logger.error(f'Error converting dataset to JSON: {e}')
        raise


def collection_to_json(collid):
    try:
        coll = Collection.objects.get(id=collid)
        metadata_json = json.loads(serializers.serialize('json', [coll]))[0]
        collection_fields = ['title', 'creator', 'created', 'description', 'keywords', 'rel_keywords', 'numrows',
                             'webpage']
        dataset_fields = ['title', 'creator', 'create_date', 'description', 'numrows', 'source', 'webpage']
        filtered_collection = filter_and_order(metadata_json, collection_fields)

        if coll.collection_class == 'dataset':
            filtered_collection['datasets'] = []
            for ds in coll.datasets.all():
                dataset_json = json.loads(serializers.serialize('json', [ds]))[0]
                filtered_dataset = filter_and_order(dataset_json, dataset_fields)
                filtered_collection['datasets'].append(filtered_dataset)

        filtered_collection.update({
            'model': metadata_json['model'],
            'pk': metadata_json['pk']
        })
        logger.info(f'Collection JSON: {filtered_collection}')
        return filtered_collection
    except Collection.DoesNotExist as e:
        logger.error(f'Collection with ID {collid} does not exist: {e}')
        raise
    except Exception as e:
        logger.error(f'Error converting collection to JSON: {e}')
        raise


def create_zipfile(data_dump_filename, dsid=None, collid=None):
    try:
        today = makeNow()
        metadata = dataset_to_json(dsid) if dsid else collection_to_json(collid)
        pretty_metadata = json.dumps(metadata, indent=1, sort_keys=False)
        dl_class = "Dataset" if dsid else "Collection"
        readme_content = (f'World Historical Gazetteer (WHG)\n{dl_class} Download\n'
                          f'data: {os.path.basename(data_dump_filename)}\n'
                          '********************************\n'
                          'This dataset conforms to the CC-BY 4.0 NC license.\n\n'
                          "This license enables reusers to distribute, remix, adapt, and build upon the material "
                          "in any medium or format for noncommercial purposes only, and only so long as attribution "
                          "is given to the creator. CC BY-NC includes the following elements:\n"
                          "* Attribution — You must give appropriate credit, provide a link to the license, and indicate "
                          "if changes were made.\n"
                          "* NonCommercial — You may not use the material for commercial purposes.\n\n"
                          "***********************************\n"
                          "Metadata:\n" + pretty_metadata)

        with open('README.txt', 'w') as f:
            f.write(readme_content)

        zipname = generate_zip_filename(data_dump_filename)
        with zipfile.ZipFile(zipname, 'w') as zipf:
            zipf.write('README.txt', arcname='README.txt')
            zipf.write(data_dump_filename, arcname=os.path.basename(data_dump_filename))

        os.remove('README.txt')
        os.remove(data_dump_filename)
        logger.info(f'Created zip file: {zipname}')
    except Exception as e:
        logger.error(f'Error creating zip file: {e}')
        raise


@shared_task(name="make_download", bind=True)
def make_download(self, *args, **kwargs):
    import logging
    logger = logging.getLogger('tasks')
    user = User.objects.get(pk=kwargs["userid"])
    collid = kwargs["collid"] or None
    dsid = kwargs["dsid"] or None
    req_format = kwargs["format"]
    logger.debug(f"make_download() userid: {user.id}, dsid: {dsid}, collid: {collid}, format: {req_format}")
    date = makeNow()

    # collection or dataset
    if collid and not dsid:
        coll = Collection.objects.get(id=collid)
        colltitle = coll.title
        collclass = coll.collection_class

        qs = coll.places_all.all()
        total_operations = qs.count()

        req_format = "lpf"

        fn = os.path.join(settings.MEDIA_ROOT, 'downloads', f'{user.id}_{collid}_{date}.json')
        # Ensure directory exists
        os.makedirs(os.path.dirname(fn), exist_ok=True)

        with open(fn, "w", encoding="utf-8") as outfile:
            features = []
            for i, p in enumerate(qs):
                geoms = p.geoms.all()
                if len(geoms) == 1:
                    geometry = geoms[0].jsonb
                else:
                    geometry = {
                        "type": "GeometryCollection",
                        "geometries": [g.jsonb for g in geoms],
                    }
                try:
                    anno = p.traces.filter(collection_id=collid, archived=False).first()
                    coll_place = CollPlace.objects.filter(
                        collection=collid, place=p
                    ).first()

                    annotation = {
                        "place_id": p.id,
                        "sequence": coll_place.sequence if coll_place else None,
                        "note": anno.note if anno else "",
                        "relation": anno.relation if anno else [],
                        "start": anno.start if anno else "",
                        "end": anno.end if anno else "",
                        "created": anno.created.strftime("%Y-%m-%d") if anno else "",
                    }
                except Exception as e:
                    annotation = {}
                    logger.error(f"Error fetching annotation or sequence for place {p.id}: {e}")

                rec = {
                    "type": "Feature",
                    "properties": {
                        "id": p.id,
                        "src_id": p.src_id,
                        "title": p.title,
                        "ccodes": p.ccodes,
                        "annotation": annotation,
                    },
                    "geometry": geometry,
                    "names": [n.jsonb for n in p.names.all()],
                    "types": [t.jsonb for t in p.types.all()],
                    "links": [ln.jsonb for ln in p.links.all()],
                    "whens": [w.jsonb for w in p.whens.all()],
                }
                features.append(rec)
                if (i + 1) % 100 == 0:
                    self.update_state(
                        state="PROGRESS", meta={"current": i + 1, "total": total_operations}
                    )
                    logger.info(f"Task state: PROGRESS, current: {i + 1}, total: {total_operations}")

            features_sorted = sorted(
                (f for f in features if f is not None),  # Filter out None values
                key=lambda x: x["properties"].get("annotation", {}).get("sequence") or float("inf")
            )

            logger.info(f"Download file for {total_operations} places in {colltitle}")
            citation = coll.citation_csl
            result = {
                "type": "FeatureCollection",
                "features": features_sorted,
                "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
                "citation": json.loads(citation) if citation else {},
                "filename": "/" + fn,
            }

            # Write the data as json to fn
            outfile.write(json.dumps(result, indent=2).replace("null", '""'))
        # Create zip with README.txt
        create_zipfile(fn, None, collid)
        # Create DownloadFile record
        zipname = generate_zip_filename(fn)
        create_downloadfile_record(user, None, coll, zipname)

    elif dsid:
        # It's a single dataset
        if collid:
            logger.info(f"Single dataset in collection {dsid}, {collid}")
        else:
            logger.info(f"Solo dataset {dsid}")
        ds = Dataset.objects.get(pk=dsid)
        dslabel = ds.label
        qs = ds.places.all()
        total_operations = qs.count()

        logger.debug(f"tasks.make_download() {{'format': {req_format}, 'ds': {dsid}}}")

        if ds.format == "delimited" and req_format in ["tsv", "delimited"]:
            logger.info("Making an augmented TSV file")

            dsf = ds.file
            dsf.delimiter = dsf.delimiter if not dsf.delimiter == 'n/a' else '\t'
            df = pd.read_csv(
                f"media/{dsf.file.name}",
                delimiter=dsf.delimiter,
                dtype={"id": "str", "aat_types": "str"},
                engine='python'
            )
            logger.debug(f"DataFrame: {df}")

            header = list(df)
            logger.debug(f"Original header: {header}")

            newheader = deepcopy(header)
            newheader = list(
                set(
                    newheader
                    + ["lon", "lat", "matches", "geo_id", "geo_source", "geowkt"]
                )
            )
            logger.debug(f"New header with additional columns: {newheader}")

            fn = os.path.join(settings.MEDIA_ROOT, 'downloads', f'{user.id}_{dslabel}_{date}.tsv')
            # Ensure directory exists
            os.makedirs(os.path.dirname(fn), exist_ok=True)

            logger.debug(f"Output file name: {fn}")

            try:
                with open(fn, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile, delimiter="\t", quotechar="\"", quoting=csv.QUOTE_NONE)
                    writer.writerow(newheader)

                    missing = list(set(newheader) - set(header))
                    logger.debug(f"Missing columns: {missing}")

                    for i, row in df.iterrows():
                        dfrow = df.loc[i, :]
                        try:
                            p = qs.get(src_id=dfrow["id"], dataset=ds.label)
                            logger.debug(f"Processing row index {i}, place ID {p.id}")

                            rowjs = json.loads(dfrow.to_json())
                            newrow = deepcopy(rowjs)
                            for m in missing:
                                newrow[m] = ""

                            links = ";".join(
                                list(set([ln.jsonb["identifier"] for ln in p.links.all()]))
                            )
                            newrow["matches"] = links
                            logger.debug(f"Links for place ID {p.id}: {links}")

                            geoms = p.geoms.all()
                            if geoms.count() > 0:
                                geowkt = newrow.get("geowkt", None)
                                lonlat = (
                                    [newrow.get("lon"), newrow.get("lat")]
                                    if len(set(newrow.keys()) & set(["lon", "lat"])) == 2
                                    else None
                                )
                                if not geowkt and (not lonlat or None in lonlat or lonlat[0] == ""):
                                    g = geoms[0]
                                    newrow["geowkt"] = g.geom.wkt if g.geom else ""
                                    xy = (
                                        g.geom.coords[0]
                                        if g.jsonb["type"] == "MultiPoint"
                                        else g.jsonb["coordinates"]
                                    )
                                    newrow["lon"] = xy[0]
                                    newrow["lat"] = xy[1]
                                    logger.debug(
                                        f"Updated geowkt and coordinates for place ID {p.id}: geowkt={newrow['geowkt']}, lon={newrow['lon']}, lat={newrow['lat']}")

                            index_map = {v: i for i, v in enumerate(newheader)}
                            ordered_row = sorted(
                                newrow.items(), key=lambda pair: index_map[pair[0]]
                            )
                            csvrow = [o[1] for o in ordered_row]
                            writer.writerow(csvrow)

                            if (i + 1) % 100 == 0:
                                try:
                                    self.update_state(
                                        state="PROGRESS",
                                        meta={"current": i + 1, "total": total_operations},
                                    )
                                    logger.info(f"Task state: PROGRESS, current: {i + 1}, total: {total_operations}")

                                    task_id = self.request.id
                                    task_result = AsyncResult(task_id)
                                    logger.info(f"Immediate task state: {task_result.state}, info: {task_result.info}")
                                except Exception as e:
                                    logger.error(f"Error updating task state: {e}")

                        except Exception as e:
                            logger.error(f"Error processing row index {i}: {e}")

            except Exception as e:
                logger.error(f"Error writing to TSV file {fn}: {e}")
                return {"msg": "Error writing TSV file", "error": str(e)}

            # Create zip with README.txt
            try:
                create_zipfile(fn, ds.id, None)  # Single dataset
                zipname = generate_zip_filename(fn)
                create_downloadfile_record(user, ds, None, zipname)
                logger.info(f"Zip file created and download record created successfully.")
            except Exception as e:
                logger.error(f"Error creating zip file or download record: {e}")
                return {"msg": "Error creating zip file or download record", "error": str(e)}

        else:
            logger.info("Building LPF file")

            fn = os.path.join(settings.MEDIA_ROOT, 'downloads', f'{user.id}_{dslabel}_{date}.json')
            # Ensure directory exists
            os.makedirs(os.path.dirname(fn), exist_ok=True)

            with open(fn, "w", encoding="utf-8") as outfile:
                features = []
                for i, p in enumerate(qs):
                    logger.debug(f"Place in make_download(): {p.__dict__}")
                    whens = p.whens.all()
                    if len(whens) > 0:
                        when = p.whens.first().jsonb
                        if "minmax" in when:
                            del when["minmax"]
                    else:
                        when = {}
                    geoms = p.geoms.all()
                    if len(geoms) == 1:
                        geometry = geoms[0].jsonb
                    else:
                        geometry = {
                            "type": "GeometryCollection",
                            "geometries": [g.jsonb for g in geoms],
                        }
                    rec = {
                        "type": "Feature",
                        "@id": ds.uri_base
                               + (str(p.id) if "whgazetteer" in ds.uri_base else p.src_id),
                        "properties": {
                            "pid": p.id,
                            "src_id": p.src_id,
                            "title": p.title,
                            "ccodes": p.ccodes,
                            "comment": [
                                {
                                    "user": c.user.name,
                                    "note": c.note,
                                    "created": c.created.isoformat(),
                                }
                                for c in Comment.objects.filter(place_id=p)
                            ],
                        },
                        "geometry": geometry,
                        "names": [n.jsonb for n in p.names.all()],
                        "types": [t.jsonb for t in p.types.all()],
                        "links": [ln.jsonb for ln in p.links.all()],
                        "when": when,
                    }
                    features.append(rec)
                    if (i + 1) % 100 == 0:
                        self.update_state(
                            state="PROGRESS",
                            meta={"current": i + 1, "total": total_operations},
                        )
                        logger.info(
                            f"Task updated: current iteration is {i + 1}, total operations are {total_operations}")

                logger.info(f"Download file for {total_operations} places")

                citation = ds.citation_csl

                result = {
                    "type": "FeatureCollection",
                    "@context": "https://raw.githubusercontent.com/LinkedPasts/linked-places/master/linkedplaces-context-v1.1.jsonld",
                    "citation": json.loads(citation) if citation else {},
                    "filename": "/" + fn,
                    "description": ds.description,
                    "features": features,
                }

                outfile.write(json.dumps(result, indent=2).replace("null", '""'))
            # Create zip with README.txt
            create_zipfile(fn, ds.id, None)
            # Create DownloadFile record
            zipname = generate_zip_filename(fn)
            create_downloadfile_record(user, ds, None, zipname)

    logger.debug(f"@ Log create: user_id:{user.id}, dsid: {dsid}, collid: {collid}")  # DEBUG
    # Log the download, dataset or collection
    Log.objects.create(
        category="dataset" if dsid else "collection",
        logtype="ds_download" if dsid else "coll_download",
        note={"format": req_format, "name": user.username},
        dataset_id=dsid or None,
        collection_id=collid or None,
        user_id=user.id,
    )

    WHGmail(context={
        'template': 'download_ready',
        'subject': 'WHG download file is ready',
        'to_email': user.email,
        'greeting_name': user.name,
        'title': ds.title if dsid else coll.title,
    })

    self.update_state(state="SUCCESS")
    logger.info("Task state: SUCCESS")
    completed_message = {"msg": f"{req_format} written", "filename": fn}
    return completed_message
