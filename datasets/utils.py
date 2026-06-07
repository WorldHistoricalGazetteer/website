# datasets/utils.py
"""
Dataset utilities that require Django models.
Import these ONLY from views, tasks, and management commands.

Lightweight utilities without Django dependencies are in core_utils.py.
Geometry utilities requiring GEOS are in geometry_utils.py.
"""
import logging
import pprint
import re

import simplejson as json
from django.conf import settings
from django.http import FileResponse, JsonResponse, HttpResponse, Http404
from django.shortcuts import get_object_or_404
from django.views.generic import View

from datasets.models import Dataset, Hit, DatasetFile
from datasets.static.hashes import aliases as al

# Import from core_utils (no circular dependency risk)
from .core_utils import (
    HitRecord,
    PlaceMapper,
    makeNow,
    elapsed,
    makeCoords,
    roundy,
    fixName,
    parsedates_tsv,
    parse_wkt,  # Add this
    flatten  # Add this
)

# Import from geometry_utils (requires GEOS but avoids circular deps)
from .geometry_utils import (
    hullify,
    ccodesFromGeom,
    compute_dataset_bbox
)

# Import from reconciliation_utils
from .reconciliation_utils import (
    getQ,
    post_recon_update,
    aat_lookup,
    classy
)

pp = pprint.PrettyPrinter(indent=1)
from whgmail.messaging import WHGmail

logger = logging.getLogger(__name__)

# Re-export commonly used functions for backward compatibility
__all__ = [
    'HitRecord', 'PlaceMapper', 'makeNow', 'elapsed', 'makeCoords',
    'parse_wkt', 'flatten',  # Add these
    'hullify', 'ccodesFromGeom', 'compute_dataset_bbox',
    'getQ', 'post_recon_update', 'aat_lookup', 'classy',
    'roundy', 'fixName', 'parsedates_tsv',
    'volunteer_offer', 'toggle_volunteers', 'download_file',
    'download_dataset', 'aliasIt', 'UpdateCountsView'
]


def volunteer_offer(request, ds):
    """
    Handle volunteer offer for dataset collaboration.
    Sends emails to both dataset owner and volunteer.

    Args:
        request: HTTP request with user info
        ds: Dataset instance

    Returns:
        str: Success message
    """
    volunteer = request.user
    owner = ds.owner

    # common parameters for both emails
    common_params = {
        'bcc': [settings.DEFAULT_FROM_EDITORIAL],
        'volunteer_username': volunteer.username,
        'volunteer_name': volunteer.name,
        'volunteer_email': volunteer.email,
        'volunteer_greeting': volunteer.name,
        'owner_username': owner.username,
        'owner_name': owner.name,
        'owner_email': owner.email,
        'owner_greeting': owner.name,
        'dataset_title': ds.title,
        'dataset_label': ds.label,
        'dataset_id': ds.id,
        'editor_email': [settings.DEFAULT_FROM_EDITORIAL]
    }

    # send email to dataset owner
    owner_params = common_params.copy()
    owner_params.update({
        'email_type': 'volunteer_offer_owner',
        'subject': 'Volunteer offer for ' + ds.title + ' dataset in WHG',
        'to_email': owner.email,
        'reply_to': volunteer.email,
    })
    WHGmail(context=owner_params)

    # send email to volunteer
    user_params = common_params.copy()
    user_params.update({
        'email_type': 'volunteer_offer_user',
        'subject': 'Volunteer offer for ' + ds.title + ' dataset in WHG received',
        'to_email': volunteer.email,
    })
    WHGmail(context=user_params)

    return 'volunteer offer for ' + ds


def toggle_volunteers(request):
    """
    Toggle volunteer acceptance for a dataset.

    Args:
        request: POST request with is_checked and dataset_id

    Returns:
        JsonResponse: Success status
    """
    if request.method == 'POST':
        is_checked = request.POST.get('is_checked') == 'true'
        dataset_id = request.POST.get('dataset_id')
        dataset = Dataset.objects.get(id=dataset_id)
        dataset.volunteers = is_checked
        dataset.save()
        return JsonResponse({'status': 'success'})


def download_file(request, *args, **kwargs):
    """
    Download the most recent dataset file.

    Args:
        request: HTTP request
        kwargs: Must contain 'id' (dataset ID)

    Returns:
        FileResponse: File download response
    """
    ds = get_object_or_404(Dataset, pk=kwargs['id'])
    if not ds.downloadable:
        return HttpResponse(
            'This dataset is not available for download; '
            'please obtain it from its original source.', status=403)
    fileobj = ds.files.all().order_by('-rev')[0]
    fn = 'media/' + fileobj.file.name
    file_handle = fileobj.file.open()

    # set content type
    content_type = 'text/csv' if fileobj.format == 'delimited' else 'text/json'
    response = FileResponse(file_handle, content_type=content_type)
    response['Content-Disposition'] = 'attachment; filename="' + fileobj.file.name + '"'

    return response


def download_dataset(request, file_id):
    """
    Download a specific dataset file based on file_id.

    Args:
        request: HTTP request
        file_id: DatasetFile ID

    Returns:
        FileResponse: File download response
        HttpResponse: Error response if file not found
    """
    try:
        fileobj = get_object_or_404(DatasetFile, pk=file_id)
        if not fileobj.dataset_id.downloadable:
            return HttpResponse(
                'This dataset is not available for download; '
                'please obtain it from its original source.', status=403)
        fn = 'media/' + fileobj.file.name
        file_handle = fileobj.file.open()

        logger.info('download_dataset: file_id=%s, filename=%s, format=%s',
                    file_id, fileobj.file.name, fileobj.format)

        # Set content type based on the file format
        content_type = 'text/csv' if fileobj.format == 'delimited' else 'application/json'
        response = FileResponse(file_handle, content_type=content_type)

        # Set the Content-Disposition header
        filename = fileobj.file.name.replace("/app/media/", "")
        response['Content-Disposition'] = f'attachment; filename="{filename}"'

        return response

    except Http404:
        logger.error('File not found: file_id=%s', file_id)
        return HttpResponse('File not found', status=404)

    except Exception as e:
        logger.exception('Error downloading file: file_id=%s', file_id)
        return HttpResponse('An error occurred while downloading the file', status=500)


def aliasIt(url):
    """
    Convert URL to aliased identifier format.

    Args:
        url: Full URL to authority record

    Returns:
        str: Aliased identifier (e.g., 'viaf:12345') or original URL
    """
    r1 = re.compile(r"\/(?:.(?!\/))+$")
    id = re.search(r1, url)
    if id:
        id = id.group(0)[1:].replace('cb', '')

    r2 = re.compile(r"bnf|cerl|dbpedia|geonames|d-nb|loc|pleiades|tgn|viaf|wikidata|whg|wikipedia")
    tag = re.search(r2, url)

    if tag and id:
        return al.tags[tag.group(0)]['alias'] + ':' + id
    else:
        return url


class UpdateCountsView(View):
    """
    Returns counts of unreviewed records, per pass and total; also deferred per task.
    """

    @staticmethod
    def get(request):
        """
        Get counts of unreviewed hits for a dataset.

        Args:
            request: GET request with ds_id parameter

        Returns:
            JsonResponse: Dict of task counts
        """
        ds = get_object_or_404(Dataset, id=request.GET.get('ds_id'))

        def defcountfunc(taskname, pids):
            """Count deferred places by task type"""
            if taskname[6:] in ['whg', 'idx']:
                return ds.places.filter(id__in=pids, review_whg=2).count()
            elif taskname[6:].startswith('wd'):
                return ds.places.filter(id__in=pids, review_wd=2).count()
            else:
                return ds.places.filter(id__in=pids, review_tgn=2).count()

        def placecounter(th):
            """Count places by query pass"""
            pcounts = {}
            pcounts['p0'] = th.filter(query_pass='pass0').values('place_id').distinct().count()
            pcounts['p1'] = th.filter(query_pass='pass1').values('place_id').distinct().count()
            pcounts['p2'] = th.filter(query_pass='pass2').values('place_id').distinct().count()
            return pcounts

        updates = {}

        # counts of distinct place ids w/unreviewed hits per task/pass
        for t in ds.tasks.filter(status='SUCCESS'):
            taskhits = Hit.objects.filter(task_id=t.task_id, reviewed=False)
            pcounts = placecounter(taskhits)

            # ids of all unreviewed places
            pids = list(set(taskhits.all().values_list("place_id", flat=True)))
            defcount = defcountfunc(t.task_name, pids)

            updates[t.task_id] = {
                "task": t.task_name,
                "total": len(pids),
                "pass0": pcounts['p0'],
                "pass1": pcounts['p1'],
                "pass2": pcounts['p2'],
                "deferred": defcount
            }

        return JsonResponse(updates, safe=False)