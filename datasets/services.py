import ast
import json
import logging
import re

from django.contrib.auth import get_user_model
from django.contrib.gis.geos import GEOSGeometry
from django.db import transaction
from django.db.models import Q, Count
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django_celery_results.models import TaskResult

from datasets.models import Hit, Dataset
from elastic.es_utils import makeDoc
from places.models import Place, PlaceGeom, PlaceName, PlaceLink, CloseMatch
from whg import settings
from .geometry_utils import _simplify_geometry
from .helpers import link_uri
from .static.hashes.parents import ccodes as cchash
from .tasks import maxID

logger = logging.getLogger(__name__)
User = get_user_model()


def _get_task_details(task_id):
    """Fetches task details and extracts relevant information."""
    task = get_object_or_404(TaskResult, task_id=task_id)
    auth = task.task_name[6:].replace("local", "")
    authname = "Wikidata" if auth == "wd" else "WHG"
    try:
        kwargs = ast.literal_eval(task.task_kwargs.strip('"'))
    except (SyntaxError, ValueError):
        kwargs = {}
    test = kwargs.get("test", "off")
    return task, auth, authname, kwargs, test


def _get_hit_counts(task_id):
    """Calculates hit counts for different review passes in one DB query."""
    # Aggregate count of unreviewed hits grouped by query_pass
    counts = (
        Hit.objects
        .filter(task_id=task_id, reviewed=False)
        .values('query_pass')
        .annotate(count=Count('place_id'))
    )
    count_map = {row['query_pass']: row['count'] for row in counts}
    return tuple(count_map.get(pass_name, 0) for pass_name in ['def', 'pass0', 'pass1', 'pass2', 'pass3'])


def _filter_unreviewed_places(dataset, task_id, passnum, auth):
    """Filters unreviewed place IDs based on the pass number and authority."""
    review_field = None
    if auth in ["whg", "idx"]:
        review_field = "review_whg"
    elif auth.startswith("wd"):
        review_field = "review_wd"
    else:
        review_field = "review_tgn"

    logger.debug(f"🚨 Dataset ID: {dataset.id}, Task ID: {task_id}, Pass Number: {passnum}, Authority: {auth}")
    # Accessioning example: Dataset ID: 1601, Task ID: 6fcade71-5035-4fa5-87bb-0ba7084d6ac0, Pass Number: 0and1, Authority: idx

    if passnum.startswith("pass"):
        try:
            pass_int = int(passnum[4])
        except (IndexError, ValueError):
            pass_int = 10
        cnt_pass = Hit.objects.values("place_id").filter(task_id=task_id, reviewed=False, query_pass=passnum).count()
        current_passnum = passnum if cnt_pass > 0 else f"pass{pass_int + 1}"
        hitplaces = Hit.objects.values("place_id").filter(
            task_id=task_id, reviewed=False, query_pass=current_passnum
        )
        filter_kwargs = {f"{review_field}__in": [0]}
        if passnum == "def":
            filter_kwargs = {f"{review_field}__in": [2]}
        return dataset.places.order_by("id").filter(pk__in=hitplaces, **filter_kwargs), current_passnum
    else:
        hitplaces = Hit.objects.values("place_id").filter(task_id=task_id, reviewed=False)
        filter_kwargs = {f"{review_field}__in": [2] if passnum == "def" else [0, None]}
        return dataset.places.order_by("id").filter(pk__in=hitplaces, **filter_kwargs), passnum


def _get_review_page_and_field(auth):
    """Determines the review page template and the review status field name."""
    if auth in ["whg", "idx"]:
        return "accession.html", "review_whg"
    elif auth.startswith("wd"):
        return "review.html", "review_wd"
    else:
        return "review.html", "review_tgn"  # Default to review.html for other authorities


def _get_place_and_hits(place_id, task_id, auth, passnum):
    """Fetches a place object and its associated hits based on the pass number."""
    place = get_object_or_404(Place, id=place_id)
    if passnum.startswith("pass") and auth not in ["whg", "idx"]:
        raw_hits = Hit.objects.filter(
            place_id=place_id, task_id=task_id, query_pass=passnum).order_by("-authority", "-score")
    elif passnum == "def":
        raw_hits = Hit.objects.filter(
            place_id=place_id, task_id=task_id).order_by("-authority", "-score")
    else:
        raw_hits = Hit.objects.filter(place_id=place_id, task_id=task_id).order_by("-score")
    return place, raw_hits


def _build_dataset_details(raw_hits):
    """Extracts and formats details of datasets associated with the hits."""
    dataset_details = {}
    for hit in raw_hits:
        for source in hit.json.get("sources", []):
            ds_label = source.get("dslabel")
            if ds_label:
                try:
                    dataset = Dataset.objects.get(label=ds_label)
                    dataset_details[dataset.label] = {
                        "title": dataset.title or "N/A",
                        "description": dataset.description or "N/A",
                        "owner": dataset.owner.name or "N/A",
                        "creator": dataset.creator or "N/A"
                    }
                except Dataset.DoesNotExist:
                    dataset_details[ds_label] = {
                        "title": "Dataset not found",
                        "description": f"The dataset with label {ds_label} does not exist.",
                        "owner": "N/A",
                        "creator": "N/A"
                    }

    return dataset_details


def _extract_passes(raw_hits, auth):
    """Extracts unique pass values from the hits, if applicable."""
    if auth in ["whg", "idx"]:
        passes = list(set(item for sublist in [h.json.get("sources", []) for h in raw_hits] for s in sublist for item in
                          [s.get("pass")] if item))
        return passes
    return None


def _get_country_names(place):
    """Converts country codes to names."""
    countries = []
    for r in place.ccodes:
        try:
            countries.append(
                cchash[0][r.upper()]["gnlabel"]
                + " ("
                + cchash[0][r.upper()]["tgnlabel"]
                + ")"
            )
        except KeyError:
            pass
    return countries


def _build_feature_collection(records, raw_hits, is_reconciliation):
    """Creates a GeoJSON FeatureCollection for mapping."""
    features = []

    def combine_and_simplify(geoms):
        """Simplify geometries or wrap in a GeometryCollection if mixed types."""
        if not geoms:
            return None
        types = {g.get("type") for g in geoms}
        if len(types) == 1:
            # Single type, simplify individually or merge as appropriate
            return [_simplify_geometry(g) for g in geoms][0] if len(geoms) == 1 else {
                "type": f"Multi{list(types)[0].title()}",
                "coordinates": [g["coordinates"] for g in geoms]
            }
        return {"type": "GeometryCollection", "geometries": geoms}

    # Green: geometries from submitted dataset and reconciled places
    for record in records:
        geoms = [geom['jsonb'] for geom in record.geoms.all().values('jsonb')]
        geometry = combine_and_simplify(geoms)
        if geometry:
            features.append({
                "type": "Feature",
                "properties": {"record_id": record.id, "ds": "dataset"},
                "geometry": geometry,
                "id": len(features)
            })

    # These (orange) are the geometries from the reconciliation or accession suggestions
    if is_reconciliation:
        for hit in raw_hits:
            for geom in hit.json.get('geoms', []):
                simplified = _simplify_geometry({
                    "type": geom["type"],
                    "coordinates": geom.get("coordinates")
                })
                features.append({
                    "type": "Feature",
                    "properties": {k: v for k, v in geom.items() if k not in ["coordinates", "type"]},
                    "geometry": simplified,
                    "id": len(features)
                })
    else:
        # Accession: bulk-fetch all source Places
        all_pids = [
            s.get('pid')
            for hit in raw_hits
            for s in hit.json.get('sources', [])
            if s.get('pid')
        ]
        place_map = {
            p.id: p for p in Place.objects.filter(id__in=all_pids).prefetch_related('geoms')
        }

        for hit in raw_hits:
            for source in hit.json.get('sources', []):
                pid = source.get('pid')
                if not pid:
                    continue
                place = place_map.get(pid)
                if not place:
                    logger.warning(f"Source Place with pid {pid} not found for hit {hit.id}")
                    continue

                geoms = [geom['jsonb'] for geom in place.geoms.all().values('jsonb')]
                geometry = combine_and_simplify(geoms)
                if geometry:
                    features.append({
                        "type": "Feature",
                        "properties": {"record_id": pid, "hit_id": hit.id, "dslabel": source.get('dslabel')},
                        "geometry": geometry,
                        "id": len(features)
                    })

    return json.dumps({"type": "FeatureCollection", "features": features})


@transaction.atomic
def _process_matching_decisions(request, place, formset, task, auth, authname, kwargs, review_field, ds):
    """
    Optimized: Processes user's matching decisions from the formset.
    - Uses preloaded related objects to avoid N+1 queries.
    - Collects all DB changes and performs bulk operations.
    """
    matches = 0
    matched_for_idx = []
    tid = task.task_id

    # --- Preload related objects into memory ---
    # NOTE: These attributes must be created via prefetch_related() in the calling view
    prefetched_geoms = getattr(place, 'prefetched_geoms', place.geoms.all())
    prefetched_names = getattr(place, 'prefetched_names', place.names.all())
    prefetched_links = getattr(place, 'prefetched_links', place.links.all())

    geoms_task_ids = {g.task_id for g in prefetched_geoms}
    names_task_ids = {n.task_id for n in prefetched_names}
    existing_toponyms = {n.toponym for n in prefetched_names}
    links_task_ids = {l.task_id for l in prefetched_links}
    existing_authids = {l.jsonb.get('identifier') for l in prefetched_links if l.jsonb.get('identifier')}

    # --- Collect changes ---
    hits_to_update = []
    geoms_to_create = []
    names_to_create = []
    links_to_create = []
    total_new_links = 0

    for form in formset:
        if not form.is_valid():
            continue

        cleaned_data = form.cleaned_data
        hit_id = form.instance.id
        match_type = cleaned_data.get("match")
        hit_json = cleaned_data.get("json", {})

        if hit_id and match_type not in ["none"]:
            matches += 1

            # --- Handle authority-specific logic ---
            if auth in ["wdlocal", "wd", "tgn"]:
                has_geom = "geoms" in hit_json and hit_json["geoms"]
                has_names = ("variants" in hit_json and hit_json["variants"]) or (
                        "names" in hit_json and hit_json["names"]
                )

                # --- Geometries ---
                if kwargs.get("aug_geoms") == 'on' and has_geom and tid not in geoms_task_ids:
                    geom_data = hit_json["geoms"][0]
                    gobj = GEOSGeometry(json.dumps(geom_data))
                    geoms_to_create.append(PlaceGeom(
                        place=place,
                        task_id=tid,
                        src_id=place.src_id,
                        geom=gobj,
                        reviewer=request.user,
                        jsonb={
                            "type": geom_data["type"],
                            "citation": {"id": f"{auth}:{hit_json.get('authrecord_id', '')}", "label": authname},
                            "coordinates": geom_data["coordinates"]
                        }
                    ))
                    geoms_task_ids.add(tid)  # Prevent duplicates in same batch

                # --- Names ---
                if kwargs.get("aug_names") == 'on' and has_names and tid not in names_task_ids:
                    names_to_add = []
                    if hit_json.get('dataset') == 'wikidata':
                        names_to_add.extend([n.split('@')[0] for n in hit_json.get('variants', [])])
                    elif hit_json.get('dataset') == 'geonames':
                        names_to_add.extend(hit_json.get('variants', []))
                    elif hit_json.get('names'):
                        names_to_add.extend(hit_json.get('names'))

                    for name in set(names_to_add):
                        if name not in existing_toponyms:
                            names_to_create.append(PlaceName(
                                place=place,
                                task_id=tid,
                                src_id=place.src_id,
                                toponym=name,
                                jsonb={
                                    "toponym": name,
                                    "citations": [
                                        {"id": f"{auth}:{hit_json.get('authrecord_id', '')}", "label": authname}]
                                }
                            ))
                            existing_toponyms.add(name)  # Prevent duplicates in same batch
                    names_task_ids.add(tid)  # Mark this task as processed

                # --- Primary link ---
                if tid not in links_task_ids:
                    link_identifier = link_uri(
                        task.task_name,
                        hit_json.get('authrecord_id') if auth != "whg" else hit_json.get("place_id")
                    )
                    links_to_create.append(PlaceLink(
                        place=place,
                        task_id=tid,
                        src_id=place.src_id,
                        reviewer=request.user,
                        jsonb={"type": match_type, "identifier": link_identifier}
                    ))
                    links_task_ids.add(tid)
                    existing_authids.add(link_identifier)
                    total_new_links += 1

                # --- Additional links ---
                for l in hit_json.get("links", []):
                    authid_match = re.search(r": ?(.*?)$", l)
                    authid = authid_match.group(1) if authid_match else None
                    link_clean = l.strip()
                    if authid and link_clean not in existing_authids:
                        links_to_create.append(PlaceLink(
                            place=place,
                            task_id=tid,
                            src_id=place.src_id,
                            reviewer=request.user,
                            jsonb={"type": match_type, "identifier": link_clean}
                        ))
                        existing_authids.add(link_clean)
                        total_new_links += 1

            # --- align_idx logic ---
            elif task.task_name == "align_idx":
                links_count = len(hit_json.get("links", [])) if "links" in hit_json else 0
                matched_for_idx.append({
                    "whg_id": hit_json.get("whg_id"),
                    "pid": hit_json.get("pid"),
                    "score": hit_json.get("score"),
                    "links": links_count,
                })

        # --- Collect hits to mark as reviewed (use formset instance, not a new query) ---
        if hit_id and form.instance:
            form.instance.reviewed = True
            hits_to_update.append(form.instance)

    # --- Bulk operations ---
    if geoms_to_create:
        PlaceGeom.objects.bulk_create(geoms_to_create, batch_size=100)
    if names_to_create:
        PlaceName.objects.bulk_create(names_to_create, batch_size=100)
    if links_to_create:
        PlaceLink.objects.bulk_create(links_to_create, batch_size=100)
    if hits_to_update:
        Hit.objects.bulk_update(hits_to_update, ['reviewed'], batch_size=100)

    # --- Handle indexing for align_idx ---
    update_fields = [review_field]
    if task.task_name == "align_idx":
        if not matched_for_idx:
            logger.debug(f'review()->parent. user: {request.user}, place_post: {place.id}, task: {task}')
            indexMatch(str(place.id), user=request.user, task=task)
            place.indexed = True
            update_fields.append('indexed')
        elif len(matched_for_idx) == 1:
            parent_id = matched_for_idx[0]["pid"]
            indexMatch(str(place.id), hit_pid=parent_id, user=request.user, task=task)
            place.indexed = True
            update_fields.append('indexed')
        elif len(matched_for_idx) > 1:
            indexMultiMatch(place.id, matched_for_idx, user=request.user, task=task)
            place.indexed = True
            update_fields.append('indexed')

    # --- Update place review status (single save) ---
    setattr(place, review_field, 1)
    if 'indexed' in update_fields:
        place.indexed = True
    place.save(update_fields=update_fields)

    # --- Update dataset totals ---
    if total_new_links > 0:
        from django.db.models import F
        unique_place_ids = len({l.place_id for l in links_to_create})
        Dataset.objects.filter(pk=ds.pk).update(
            numlinked=F('numlinked') + unique_place_ids,
            total_links=F('total_links') + total_new_links
        )

    # --- Update dataset status ---
    status_changed = False
    new_status = None

    if ds.unindexed == 0 and ds.ds_status != "indexed":
        new_status = "indexed"
        status_changed = True
    elif auth == "wd" and ds.recon_status.get("wdlocal") == 0 and ds.ds_status != "wd-complete":
        new_status = "wd-complete"
        status_changed = True

    if status_changed:
        ds.ds_status = new_status
        ds.save(update_fields=['ds_status'])


def indexMatch(pid, hit_pid=None, user=None, task=None):
    """
    Indexes a db record upon a single hit match in align_idx review.
    If hit_pid is provided, creates as child of that parent.
    Otherwise, creates as new parent.
    """
    logger = logging.getLogger('accession')

    logger.debug(f'indexMatch(): user: {user}; task: {str(task)}')
    logger.debug(f'indexMatch(): pid {str(pid)}; hit_pid: {str(hit_pid)}')

    es = settings.ES_CONN
    idx = settings.ES_WHG

    # Ensure valid Place object
    try:
        place = get_object_or_404(Place, id=int(pid))
    except ValueError:
        logger.error(f"Invalid 'pid': {pid} is not a valid integer.")
        return
    except Exception as e:
        logger.error(f"Error fetching Place with id={pid}: {e}")
        return

    # Query Elasticsearch for place_id
    try:
        q_place = {"query": {"bool": {"must": [{"match": {"place_id": pid}}]}}}
        res = es.search(index=idx, body=q_place)
        logger.debug(f'Elasticsearch query for place_id={pid}: {res}')
    except Exception as e:
        logger.error(f"Elasticsearch query failed for place_id={pid}: {e}")
        return

    is_already_indexed = res['hits']['total']['value'] > 0

    if is_already_indexed:
        # Place already indexed - shouldn't happen in normal accession flow
        logger.warning(f'Place {pid} already indexed. Skipping.')
        return

    # Place is not yet indexed - need to index it
    if hit_pid:
        # ✅ LINK TO EXISTING PARENT
        try:
            # Find the parent place in ES
            q_hit = {"query": {"bool": {"must": [{"match": {"place_id": hit_pid}}]}}}
            res_hit = es.search(index=idx, body=q_hit)

            if not res_hit['hits']['hits']:
                logger.error(f"No index entry found for hit_pid={hit_pid}. Cannot create child.")
                return

            hit = res_hit['hits']['hits'][0]
            hit_source = hit['_source']

            # Get the whg_id of the parent (may itself be a child, so get its parent)
            if hit_source['relation']['name'] == 'parent':
                parent_whgid = hit['_id']
            else:
                parent_whgid = hit_source['relation']['parent']

            logger.debug(f"Parent WHG ID: {parent_whgid}")

            # Create new doc as child
            new_doc = makeDoc(place)
            new_doc['relation'] = {"name": "child", "parent": parent_whgid}

            # Index the child with place.id as the ES document id
            es.index(index=idx, id=str(pid), routing=1, body=json.dumps(new_doc))
            logger.info(f"Place {pid} indexed as child of parent {parent_whgid}.")

            # ✅ Update parent's children array
            try:
                update_parent_script = {
                    "script": {
                        "source": "if (!ctx._source.children.contains(params.child_id)) { ctx._source.children.add(params.child_id) }",
                        "lang": "painless",
                        "params": {
                            "child_id": str(pid)
                        }
                    }
                }
                es.update(index=idx, id=parent_whgid, body=update_parent_script)
                logger.info(f"Added {pid} to parent {parent_whgid}'s children array")
            except Exception as e:
                logger.error(f"Error updating parent {parent_whgid} children array: {e}")

            # ✅ Create CloseMatch record in database
            if user and task:
                update_close_matches(int(pid), int(hit_pid), user, task)
                logger.info(f"Created CloseMatch between {pid} and {hit_pid}")
            else:
                logger.warning(f"Cannot create CloseMatch for {pid} and {hit_pid}: missing user or task")

            # ✅ Mark place as indexed
            place.indexed = True
            place.save()

        except Exception as e:
            logger.error(f"Error linking Place {pid} to parent {hit_pid}: {e}", exc_info=True)
    else:
        # ✅ CREATE NEW PARENT
        logger.debug(f'Place {pid} - no hit_pid. Creating as new parent.')
        try:
            new_doc = makeDoc(place)
            new_doc['relation'] = {"name": "parent"}
            new_doc['whg_id'] = maxID(es, idx) + 1
            new_doc['children'] = []

            es.index(index=idx, id=str(new_doc['whg_id']), body=json.dumps(new_doc))
            place.indexed = True
            place.save()
            logger.info(f'Place {pid} indexed as new parent with whg_id {new_doc["whg_id"]}.')
        except Exception as e:
            logger.error(f"Failed to index Place {pid} as parent: {e}", exc_info=True)


def indexMultiMatch(pid, matchlist, user, task):
    """
      from datasets.views.review()
      indexes a db record given multiple hit matches in align_idx review
      a LOT has to happen (see _notes/accession-pseudocode.txt):
        - pick a single 'winner' among the matched hits (max score)
        - make new record its child
        - demote all non-winners in index from parent to child
          - whg_id and children[] ids (if any) added to winner
          - name variants added to winner's searchy[] and suggest.item[] lists
    """
    from elasticsearch8 import RequestError
    es = settings.ES_CONN
    idx = settings.ES_WHG
    place = Place.objects.get(id=pid)
    from elastic.es_utils import makeDoc
    new_obj = makeDoc(place)

    # bins for new values going to winner
    addnames = []
    addkids = [str(pid)]  # pid will also be the new record's _id

    # max score is winner
    winner = max(matchlist, key=lambda x: x['score'])  # 14158663
    winner_place_id = winner['pid']  # This is the place_id of the winner
    # this is multimatch so there is at least one demoted (list of whg_ids)
    demoted = [str(i['whg_id']) for i in matchlist if not (i['whg_id'] == winner['whg_id'])]  # ['14090523']

    # complete doc for new record
    new_obj['relation'] = {"name": "child", "parent": winner['whg_id']}
    # copy its toponyms into addnames[] for adding to winner later
    for n in new_obj['names']:
        addnames.append(n['toponym'])
    if place.title not in addnames:
        addnames.append(place.title)

    # New relationships for CloseMatch records
    new_relationships = [(pid, winner_place_id)]  # Add the initial relationship

    # generate script used to update winner w/kids and names
    # from new record and any kids of 'other' matched parents
    def q_updatewinner(addkids, addnames):
        return {"script": {
            "source": """ctx._source.children.addAll(params.newkids);
          ctx._source.searchy.addAll(params.names);
          """,
            "lang": "painless",
            "params": {
                "newkids": addkids,
                "names": addnames}
        }}

    # index the new record as child of winner
    try:
        es.index(index=idx, id=str(pid), routing=1, body=json.dumps(new_obj))
    except RequestError as rq:
        logger.exception(f'Error indexing new record {pid} as child of {winner["whg_id"]}: {rq.error} {rq.info}')

    # demote others
    for _id in demoted:
        # get index record stuff, to be altered then re-indexed
        # ES won't allow altering parent/child relations directly
        q_demote = {"query": {"bool": {"must": [{"match": {"whg_id": _id}}]}}}
        res = es.search(body=q_demote, index=idx)
        srcd = res['hits']['hits'][0]['_source']
        # add names in suggest to names[]
        sugs = srcd['searchy']
        for sug in sugs:
            addnames.append(sug)
        addnames = list(set(addnames))
        # _id of demoted (a whg_id) belongs in winner's children[]
        addkids.append(str(srcd['place_id']))

        haskids = len(srcd['children']) > 0
        # if demoted record has kids, add to addkids[] list
        # for 'adoption' by topdog later
        if haskids:
            morekids = srcd['children']
            for kid in morekids:
                addkids.append(str(kid))

        # update the 'winner' parent
        q = q_updatewinner(list(set(addkids)), list(set(addnames)))  # ensure only unique
        try:
            es.update(index=idx, id=winner['whg_id'], body=q)
        except RequestError as rq:
            logger.exception(f'Error updating winner {winner["whg_id"]}: {rq.error} {rq.info}')

        from copy import deepcopy
        newsrcd = deepcopy(srcd)
        # update it to reflect demotion
        newsrcd['relation'] = {"name": "child", "parent": winner['whg_id']}
        newsrcd['children'] = []
        if 'whg_id' in newsrcd:
            newsrcd.pop('whg_id')

        # zap the demoted, reindex with same _id and modified doc (newsrcd)
        try:
            es.delete(index=idx, id=_id)
            es.index(index=idx, id=_id, body=newsrcd, routing=1)
        except RequestError as rq:
            logger.exception(f'Error reindexing demoted record {_id}: {rq.error} {rq.info}')

        # re-assign parent for kids of all/any demoted parents
        if len(addkids) > 0:
            for kid in addkids:
                q_adopt = {"script": {
                    "source": "ctx._source.relation.parent = params.new_parent; ",
                    "lang": "painless",
                    "params": {"new_parent": winner['whg_id']}
                },
                    "query": {"match": {"place_id": kid}}}
                es.update_by_query(index=idx, body=q_adopt, conflicts='proceed')

                # Add new relationships for CloseMatch
                kid_place_id = kid  # Since addkids contains place_id values
                new_relationships.append((int(kid_place_id), int(winner_place_id)))

    # Create new CloseMatch records
    for place_a, place_b in new_relationships:
        update_close_matches(place_a, place_b, user, task)


def recon_complete(ds):
    ds.ds_status = "wd-complete"
    ds.save()
    # status_emailer(ds, "wd")
    # print("sent status email")


# kwargs = json.loads(task.task_kwargs.replace("'", '"'))
def write_wd_pass0(request, tid):
    """
      write_wd_pass0(taskid)
      called from dataset_detail>status tab
      accepts all pass0 wikidata matches, writes geoms and links
    """
    task = get_object_or_404(TaskResult, task_id=tid)
    try:
        kwargs = ast.literal_eval(task.task_kwargs.strip('"'))
    except (ValueError, SyntaxError) as e:
        logger.exception(f"Error evaluating task_kwargs: {e}")
        # return  # Handle the error appropriately

    # kwargs_str = task.task_kwargs.replace("'", '"')
    # kwargs = json.loads(kwargs_str)
    # kwargs = ast.literal_eval(task.task_kwargs)
    referer = request.META.get('HTTP_REFERER') + '#reconciliation'
    auth = task.task_name[6:].replace('local', '')
    # ds = get_object_or_404(Dataset, pk=kwargs['ds'])
    ds = Dataset.objects.get(id=kwargs['ds'])

    referer = request.META.get('HTTP_REFERER') + '#reconciliation'
    auth = task.task_name[6:].replace('local', '')

    # get unreviewed pass0 hits
    hits = Hit.objects.filter(
        task_id=tid,
        query_pass='pass0',
        reviewed=False
    )
    # print('writing '+str(len(hits))+' pass0 matched records for', ds.label)
    for h in hits:
        hasGeom = 'geoms' in h.json and len(h.json['geoms']) > 0
        hasLinks = 'links' in h.json and len(h.json['links']) > 0
        place = h.place  # object
        # existing for the place
        authids = place.links.all().values_list('jsonb__identifier', flat=True)
        authname = 'Wikidata' if h.authority == 'wd' else 'GeoNames'
        # GEOMS
        # confirm another user hasn't just done this...
        if hasGeom and kwargs['aug_geoms'] == 'on' \
                and tid not in place.geoms.all().values_list('task_id', flat=True):
            for g in h.json['geoms']:
                pg = PlaceGeom.objects.create(
                    place=place,
                    task_id=tid,
                    src_id=place.src_id,
                    geom=GEOSGeometry(json.dumps({"type": g['type'], "coordinates": g['coordinates']})),
                    reviewer=request.user,
                    jsonb={
                        "type": g['type'],
                        "citation": {"id": auth + ':' + h.authrecord_id, "label": authname},
                        "coordinates": g['coordinates']
                    }
                )

        # LINKS
        link_counter = 0
        # add PlaceLink record for wikidata hit if not already there
        if 'wd:' + h.authrecord_id not in authids:
            link_counter += 1
            link = PlaceLink.objects.create(
                place=place,
                task_id=tid,
                src_id=place.src_id,
                reviewer=request.user,
                jsonb={
                    "type": "closeMatch",
                    "identifier": link_uri(task.task_name, h.authrecord_id)
                }
            )

        # create link for each wikidata concordance, if any
        if hasLinks:
            # authids=place.links.all().values_list(
            # 'jsonb__identifier',flat=True)
            for l in h.json['links']:
                link_counter += 1
                authid = re.search(":?(.*?)$", l).group(1)
                # TODO: same no-dupe logic in review()
                # don't write duplicates
                if authid not in authids:
                    link = PlaceLink.objects.create(
                        place=place,
                        task_id=tid,
                        src_id=place.src_id,
                        reviewer=request.user,
                        jsonb={
                            "type": "closeMatch",
                            "identifier": authid
                        }
                    )

        # only matters if autocomplete was the last review action
        if auth in ["wd"] and ds.recon_status["wdlocal"] == 0:
            recon_complete(ds)

        # update dataset totals for metadata page
        ds.numlinked = len(set(PlaceLink.objects.filter(place_id__in=ds.placeids).values_list('place_id', flat=True)))
        if ds.total_links is None:
            ds.total_links = link_counter
        else:
            ds.total_links += link_counter
        ds.save()

        # flag hit as reviewed
        h.reviewed = True
        h.save()

        # flag place as reviewed
        place.review_wd = 1
        place.save()

    return HttpResponseRedirect(referer)


@transaction.atomic
def update_close_matches(new_child_id, parent_place_id, user, task):
    """
    Optimized version: Uses get_or_create which does everything in one query
    """
    # Normalize the tuple
    place_a_id, place_b_id = sorted([new_child_id, parent_place_id])

    try:
        # Ensure valid instances (no DB query needed for these checks)
        assert isinstance(user, User), f'user is not a User instance: {type(user)}'
        assert isinstance(task, TaskResult), f'task is not a TaskResult instance: {type(task)}'

        # Use get_or_create - single query that creates only if doesn't exist
        close_match, created = CloseMatch.objects.get_or_create(
            place_a_id=place_a_id,
            place_b_id=place_b_id,
            defaults={
                'created_by': user,
                'task': task,
                'basis': 'reviewed'
            }
        )

        if created:
            logger.debug(f'Created CloseMatch: {place_a_id} <-> {place_b_id}')
        else:
            logger.debug(f'CloseMatch already exists for {place_a_id} <-> {place_b_id}')

    except Place.DoesNotExist as e:
        logger.error(
            f'Link attempt failed: One or both Place records are missing. '
            f'IDs: {place_a_id}, {place_b_id}. Error: {e}'
        )
    except Exception as e:
        logger.exception(
            f'Error creating CloseMatch: {e}; '
            f'new_child_id={new_child_id}, parent_place_id={parent_place_id}, '
            f'user={user}, task={task}'
        )
