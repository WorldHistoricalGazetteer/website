# datasets.views
# Standard library imports
import ast
import json
import os
import shutil
import sys
import tempfile
from collections import Counter
from pathlib import Path
from shutil import copyfile

# Third-party imports
import numpy as np
import pandas as pd
from celery import current_app as celapp
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.paginator import Paginator
from django.db.models import Count, Prefetch
from django.forms import modelformset_factory
from django.http import (
    HttpResponseRedirect,
    HttpResponseNotFound
)
from django.shortcuts import redirect, render
from django.test import Client
from django.urls import reverse as django_reverse
from django.utils.datastructures import MultiValueDictKeyError
from django.utils.text import slugify
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.views.generic import (
    CreateView,
    ListView,
    UpdateView,
    DeleteView,
    DetailView
)
from shapely import wkt

# from accounts.views import logger
# Local application imports
from areas.models import Area
from collection.models import Collection
from elastic.es_utils import (
    removePlacesFromIndex,
    removeDatasetFromIndex
)
from main.models import Comment
from places.models import *
from utils.regions_countries import get_regions_countries
from validation.views import validate_file
from .forms import (
    HitModelForm,
    DatasetDetailModelForm,
    DatasetUploadForm,
    DatasetCreateEmptyModelForm
)
from .models import DatasetUser
from .services import _get_task_details, _get_hit_counts, _filter_unreviewed_places, _get_review_page_and_field, \
    _get_place_and_hits, _build_dataset_details, _extract_passes, _get_country_names, _build_feature_collection, \
    _process_matching_decisions
from .tasks import *
from .utils import *

es = settings.ES_CONN
User = get_user_model()

logger = logging.getLogger(__name__)

# known MIME types for supported file formats
MIME_TYPE_MAPPING = {
    'application/json': 'json',
    'text/csv': 'csv',
    'text/tab-separated-values': 'tsv',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
    'application/vnd.oasis.opendocument.spreadsheet': 'ods'
}


class DatasetValidate(CreateView):
    logger = logging.getLogger('validation')

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    template_name = 'datasets/dataset_validate.html'
    form_class = DatasetUploadForm

    def get(self, request, *args, **kwargs):
        response = super().get(request, *args, **kwargs)
        response['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response['Pragma'] = 'no-cache'
        response['Expires'] = '0'
        return response

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['task_id'] = ''
        return context

    def form_invalid(self, form):
        return self.render_to_response(self.get_context_data(form=form))

    def form_valid(self, form):
        uploaded_file = None
        uploaded_filepath = None
        try:
            user = self.request.user

            uploaded_file = form.cleaned_data.get('file')
            if uploaded_file is None:
                return self.handle_invalid_form(form, "No file uploaded.")

            uploaded_filepath = self.save_file_temporarily(uploaded_file)
            self.logger.debug(f'File saved to `{uploaded_filepath}`')

            validation_response = validate_file(self.request, {
                'title': form.cleaned_data.get('title'),
                'label': form.cleaned_data.get('label') or self.generate_unique_label(uploaded_file),
                'description': form.cleaned_data.get('description'),
                'creator': form.cleaned_data.get('creator'),
                'source': form.cleaned_data.get('source'),
                'contributors': form.cleaned_data.get('contributors'),
                'uri_base': form.cleaned_data.get('uri_base') or 'https://whgazetteer.org/api/db/?id=',
                'webpage': form.cleaned_data.get('webpage'),
                'pdf': form.cleaned_data.get('pdf'),
                'owner_id': user.id,
                'username': user.username,
                'uploaded_filepath': uploaded_filepath,
                'uploaded_filename': uploaded_file.name
            })

            if not validation_response:
                self.cleanup_uploaded_file(uploaded_filepath)
                return self.handle_invalid_form(form, "No response from validation service.")

            if isinstance(validation_response, JsonResponse):
                response_data = validation_response.content.decode('utf-8')
                response_data = json.loads(response_data)
                status = response_data.get("status")

                if status == "in_progress":
                    task_id = response_data.get("task_id")
                    context = self.get_context_data(form=form)
                    context['task_id'] = task_id
                    return self.render_to_response(context)

                elif status == "failed":
                    message = response_data.get("message", "Unknown error")
                    messages.error(self.request, f"Validation failed: {message}")
                    return self.form_invalid(form)

                else:
                    messages.error(self.request, "Unknown validation status received.")
                    self.cleanup_uploaded_file(uploaded_filepath)
                    return self.form_invalid(form)

        except Exception as e:
            self.logger.error(f"Error during file validation: {str(e)}", exc_info=True)
            self.cleanup_uploaded_file(uploaded_filepath)
            return self.handle_invalid_form(form,
                                            f"Sorry, there was an error while processing the uploaded file: {str(e)}")

        # finally:
        # return redirect('some_success_url') # <<< Dataset browse

    def handle_invalid_form(self, form, message):
        messages.error(self.request, message)
        return self.form_invalid(form)

    def save_file_temporarily(self, uploaded_file):
        original_file_name = uploaded_file.name
        _, file_extension = os.path.splitext(original_file_name)

        # Create a temporary file with the same extension
        with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as temp_file:
            # Deletion is managed by validation.tasks.clean_tmp_files, triggered by beat_schedule in celery.py
            temp_file_path = temp_file.name
            try:
                # If the file is stored on disk (TemporaryUploadedFile)
                if hasattr(uploaded_file, 'temporary_file_path'):
                    shutil.copy(uploaded_file.temporary_file_path(), temp_file_path)
                else:
                    # If the file is in memory (InMemoryUploadedFile)
                    for chunk in uploaded_file.chunks():
                        temp_file.write(chunk)
            except Exception as e:
                # Ensure file is removed in case of error
                self.cleanup_uploaded_file(temp_file_path)
                raise e

        return temp_file_path

    def cleanup_uploaded_file(self, uploaded_filepath):
        if uploaded_filepath is not None and os.path.exists(uploaded_filepath):
            os.remove(uploaded_filepath)

    def generate_unique_label(self, uploaded_file):
        min_length = 3
        max_length = 20

        # Initialise label from the file name
        base_label = slugify(os.path.splitext(uploaded_file.name)[0])

        # Helper function to ensure the label does not exceed max length
        def adjust_label_length(label):
            if len(label) > max_length:
                return label[-max_length:]
            return label

        # Adjust the base label length
        label = adjust_label_length(base_label)

        # Check if the label already exists and ensure uniqueness
        if Dataset.objects.filter(label=label).exists():
            # If the label already exists, append the user's surname
            label = f"{label}_{slugify(self.request.user.surname)}"
            label = adjust_label_length(label)  # Adjust length after appending surname

            count = 1
            # If the new label still exists, append "_v" and a number
            while Dataset.objects.filter(label=label).exists():
                label = f"{base_label}_v{count}"
                label = adjust_label_length(label)  # Adjust length after appending version
                count += 1

        return label


class VolunteeringView(ListView):
    template_name = 'datasets/volunteering.html'
    model = Dataset

    def get_queryset(self):
        qs = super().get_queryset()
        return qs.filter(volunteers_text__isnull=False).order_by('-create_date')

    def get_context_data(self, *args, **kwargs):
        context = super(VolunteeringView, self).get_context_data(*args, **kwargs)
        context['dataset_list'] = context.pop('object_list')

        return context


# TODO: in use?
class DatasetGalleryView(ListView):
    redirect_field_name = 'redirect_to'

    context_object_name = 'datasets'
    template_name = 'datasets/ds_gallery.html'
    model = Dataset

    def get_queryset(self):
        qs = super().get_queryset()
        # return qs.filter(public=True).order_by('title')
        # TODO: remove this exclude
        return qs.filter(public=True).exclude(owner_id=119).order_by('title')

    def get_context_data(self, *args, **kwargs):
        context = super(DatasetGalleryView, self).get_context_data(*args, **kwargs)

        context['active_tab'] = self.kwargs.get('gallery_type',
                                                'datasets')  # datasets|collections: default to 'datasets' if not provided

        context['num_datasets'] = Dataset.objects.filter(public=True).count()
        context['num_collections'] = Collection.objects.filter(public=True).count()

        context['dropdown_data'] = get_regions_countries()

        country_feature_collection = {
            'type': 'FeatureCollection',
            'features': []
        }

        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'admins']).exists() else False
        return context


# check Celery process is running before initiating reconciliation task
def celeryUp():
    response = celapp.control.ping(timeout=1.0)
    return len(response) > 0


def review(request, dsid, tid, passnum):
    """
      GET   returns review.html for Wikidata, or accession.html for accessioning
      POST  for each record that got hits, process user matching decisions
      NB.   'passnum' is always a string: ['pass*' | 'def' | '0and1' (for idx)]
    """
    pid = request.GET.get("pid")
    ds = get_object_or_404(Dataset, id=dsid)
    task, auth, authname, kwargs, test = _get_task_details(tid)
    cnt_pass_def, cnt_pass0, cnt_pass1, cnt_pass2, cnt_pass3 = _get_hit_counts(tid)
    record_list, current_passnum = _filter_unreviewed_places(ds, tid, passnum, auth)
    review_page, review_field = _get_review_page_and_field(auth)

    nohit_context = {
        "nohits": True,
        "ds_id": dsid,
        "ds_label": ds.label,
        "dataset_details": {},
        "task_id": tid,
        "authority": task.task_name[6:8] if auth == "wdlocal" else task.task_name[6:],
        "deferred": True if current_passnum == "def" else False,
        "passnum": current_passnum,
        "mbtoken": False,
        "already": False,
        "test": test,
        "feature_collection": {},
        "hit_list": None,
        "passes": None,
        "records": None,
        "countries": None,
        "page": None,
        "aug_geoms": kwargs["aug_geoms"],
        "count_pass0": cnt_pass0,
        "count_pass1": cnt_pass1,
        "count_pass2": cnt_pass2,
        "count_pass3": cnt_pass3,
        "formset": None,
    }

    if not record_list.exists():
        return render(request, f"datasets/{review_page}", context=nohit_context)

    if pid:
        try:
            place_obj = Place.objects.get(id=pid)
            page = list(record_list).index(place_obj) + 1
        except (Place.DoesNotExist, ValueError):
            return render(request, f"datasets/{review_page}", context=nohit_context)
    else:
        page = request.GET.get("page", 1)

    paginator = Paginator(record_list, 1)
    records = paginator.get_page(page)

    if not records:
        return render(request, f"datasets/{review_page}", context=nohit_context)

    try:
        place = records[0]
    except IndexError:
        return render(request, f"datasets/{review_page}", context=nohit_context)

    # GET request - no transaction needed
    if request.method == "GET":
        place = Place.objects.prefetch_related(
            Prefetch('geoms', queryset=PlaceGeom.objects.only('id', 'task_id', 'jsonb'), to_attr='prefetched_geoms'),
            Prefetch('names', queryset=PlaceName.objects.only('id', 'task_id', 'toponym'), to_attr='prefetched_names'),
            Prefetch('links', queryset=PlaceLink.objects.only('id', 'task_id', 'jsonb'), to_attr='prefetched_links')
        ).get(id=place.id)

        _, raw_hits = _get_place_and_hits(place.id, tid, auth, current_passnum)
        dataset_details = _build_dataset_details(raw_hits)
        passes = _extract_passes(raw_hits, auth)
        countries = _get_country_names(place)
        feature_collection = _build_feature_collection(place, raw_hits)

        HitFormset = modelformset_factory(
            Hit,
            fields=("id", "authority", "authrecord_id", "query_pass", "score", "json", "match"),
            form=HitModelForm,
            extra=0,
        )
        formset = HitFormset(request.POST or None, queryset=raw_hits)

        context = {
            "ds_id": dsid,
            "ds_label": ds.label,
            "task_id": tid,
            "hit_list": raw_hits,
            "dataset_details": dataset_details,
            "passes": passes,
            "authority": task.task_name[6:8] if auth == "wdlocal" else task.task_name[6:],
            "records": records,
            "countries": countries,
            "passnum": passnum,
            "page": page,
            "aug_geoms": kwargs["aug_geoms"],
            "count_pass0": cnt_pass0,
            "count_pass1": cnt_pass1,
            "count_pass2": cnt_pass2,
            "count_pass3": cnt_pass3,
            "deferred": True if passnum == "def" else False,
            "test": test,
            "formset": formset,
            "feature_collection": feature_collection,
            "already": False,
            "mbtoken": False,
            "nohits": False,
        }
        return render(request, "datasets/" + review_page, context=context)

    # POST request - wrap in transaction for select_for_update
    with transaction.atomic():
        # Lock the row during POST
        place = Place.objects.select_for_update().prefetch_related(
            Prefetch('geoms', queryset=PlaceGeom.objects.only('id', 'task_id', 'jsonb'), to_attr='prefetched_geoms'),
            Prefetch('names', queryset=PlaceName.objects.only('id', 'task_id', 'toponym'), to_attr='prefetched_names'),
            Prefetch('links', queryset=PlaceLink.objects.only('id', 'task_id', 'jsonb'), to_attr='prefetched_links')
        ).get(id=place.id)

        review_status = getattr(place, review_field)

        if review_status == 1:
            messages.success(
                request, ("Last record (" + place.title + ") reviewed by another")
            )
            return redirect(
                "/datasets/" + str(dsid) + "/review/" + task.task_id + "/" + passnum
            )

        _, raw_hits = _get_place_and_hits(place.id, tid, auth, current_passnum)

        HitFormset = modelformset_factory(
            Hit,
            fields=("id", "authority", "authrecord_id", "query_pass", "score", "json", "match"),
            form=HitModelForm,
            extra=0,
        )
        formset = HitFormset(request.POST, queryset=raw_hits)

        if formset.is_valid():
            _process_matching_decisions(request, place, formset, task, auth, authname, kwargs, review_field, ds)
            return redirect(
                f"/datasets/{dsid}/review/{tid}/{current_passnum}?page={int(page)}"
            )
        else:
            logger.debug(f'formset is NOT valid. errors: {formset.errors} data: {formset.data}')
            # If formset is invalid, we still need to render the page
            # Fall through to render with errors

    # This should rarely be reached (only if POST formset is invalid)
    return redirect(f"/datasets/{dsid}/review/{tid}/{current_passnum}?page={int(page)}")


def ds_recon(request, pk):
    """
      ds_recon()
      initiates & monitors Celery tasks against Elasticsearch indexes
      i.e. align_[wdlocal | wd | builder] in tasks.py
      url: datasets/{ds.id}/reconcile ('ds_reconcile'; from ds_addtask.html)
      params: pk (dataset id), auth, region, userarea, geom, scope
      each align_{auth} task runs matching es_lookup_{auth}() and writes Hit instances
    """
    ds = get_object_or_404(Dataset, id=pk)
    # TODO: handle multipolygons from "#area_load" and "#area_draw"
    user = request.user
    context = {"dataset": ds.title}
    if request.method == 'POST' and request.POST:

        # return

        test = 'on' if 'test' in request.POST else 'off'  # for idx only
        auth = request.POST['recon']
        scope_geom = request.POST.get('scope_geom', False)
        aug_geoms = request.POST.get('accept_geoms', False)
        aug_names = request.POST.get('accept_names', False)
        geonames = request.POST.get('no_geonames', False)
        # print('ds_recon() aug_geoms', aug_geoms)
        # print('ds_recon() aug_names', aug_names)
        # print('ds_recon() geonames', geonames)  # on/off

        language = request.LANGUAGE_CODE
        if auth == 'idx' and ds.public == False and test == 'off':
            messages.add_message(request, messages.ERROR, """Dataset must be public before indexing!""")
            return redirect('/datasets/' + str(ds.id) + '/addtask')
        # previous successful task of this type?
        #   wdlocal? archive previous, scope = unreviewed
        #   idx? scope = unindexed
        # collection_id = request.POST.get('collection_id')
        previous = ds.tasks.filter(task_name='align_' + auth, status='SUCCESS')
        prior = request.POST.get('prior', 'na')
        if previous.count() > 0:  # there is a previous task
            if auth == 'idx':
                scope = "unindexed"
            else:  # wdlocal
                # get its id and archive it
                tid = previous.first().task_id
                task_archive(tid, prior)
                scope = 'unreviewed'
        else:
            # no existing task, submit all rows
            # print('ds_recon(): no previous, submitting all')
            scope = 'all'

        # which task? wdlocal, idx, builder. wdgn
        func = eval('align_' + auth)

        # Get bounds with proper defaults
        region = request.POST.get('region', '0')  # Use .get() with default
        userarea = request.POST.get('userarea', '0')

        # Ensure empty strings are treated as '0'
        region = region if region else '0'
        userarea = userarea if userarea else '0'

        bounds = {
            "type": ["region" if region != "0" else "userarea"],
            "id": [region if region != "0" else userarea]
        }

        # check Celery service
        # if not celeryUp():
        #     print('Celery is down :^(')
        #     emailer('Celery is down :^(',
        #             'if not celeryUp() -- look into it!',
        #             'whg@kgeographer.org',
        #             ['karlg@kgeographer.org'])
        #     messages.add_message(request, messages.INFO, """Sorry! The WHG reconciliation service appears to be down.
        # The system administrator has been notified.""")
        #     return redirect('/datasets/' + str(ds.id) + '/reconcile')

        # 4ce548e4-b765-4f04-aaf6-2824da89f385

        # initiate celery/redis task
        # needs positional and declared ds.id; don't know why
        try:
            result = func.delay(
                ds.id,
                ds=ds.id,
                dslabel=ds.label,
                owner=ds.owner.id,
                user=user.id,
                bounds=bounds,
                aug_geoms=aug_geoms,  # accept geoms
                aug_names=aug_names,  # accept names
                scope=scope,  # all/unreviewed
                scope_geom=scope_geom,  # all/geom_free
                geonames=geonames,  # on/off
                lang=language,
                test=test,  # for idx only
            )
            messages.add_message(request, messages.INFO,
                                 "<span class='text-danger'>Your reconciliation task is under way.</span><br/>When complete, you will receive an email and if successful, results will appear below (you may have to refresh screen). <br/>In the meantime, you can navigate elsewhere.")
            return redirect('/datasets/' + str(ds.id) + '/reconcile')
        except:
            logger.exception(f"Failed to start task align_{auth} for dataset {ds.id}", exc_info=True)
            messages.add_message(request, messages.INFO,
                                 "Sorry! Reconciliation services appear to be down. The system administrator has been notified.<br/>" + str(
                                     sys.exc_info()))
            # emailer('WHG recon task failed',
            #         'a reconciliation task has failed for dataset #' + str(ds.id) + ', w/error: \n' + str(
            #             sys.exc_info()) + '\n\n',
            #         'whg@kgeographer.org',
            #         'karl@kgeographer.org')

            return redirect('/datasets/' + str(ds.id) + '/reconcile')


def task_delete(request, tid, scope="task"):
    """
    Initiate background deletion of reconciliation task results
    Returns immediately with task ID for status polling
    """
    try:
        tr = TaskResult.objects.get(task_id=tid)
    except TaskResult.DoesNotExist:
        return JsonResponse({
            'status': 'error',
            'message': f'Task with ID {tid} does not exist'
        }, status=404)

    # Extract dataset ID
    dsid = int(tr.task_args[2:-3])

    # Mark task as ARCHIVED so recon_status no longer includes it
    if scope == 'task':
        tr.status = 'ARCHIVED'
        tr.save()

    # Start background deletion task
    from datasets.tasks import delete_reconciliation_task
    deletion_task = delete_reconciliation_task.delay(
        tid=tid,
        scope=scope,
        dsid=dsid,
        user_id=request.user.id
    )

    return JsonResponse({
        'status': 'started',
        'deletion_task_id': deletion_task.id,
        'message': 'Task deletion started in background',
        'redirect_url': f'/datasets/{dsid}/reconcile'
    })


def task_archive(tid, prior):
    """
      task_archive(tid, scope, prior)
      delete hits
      if prior = 'zap: delete geoms and links added by review
      reset Place.review_{auth} to null
      set task status to 'ARCHIVED'
    """
    hits = Hit.objects.all().filter(task_id=tid)
    tr = get_object_or_404(TaskResult, task_id=tid)
    dsid = tr.task_args[1:-1]
    auth = tr.task_name[6:]
    places = Place.objects.filter(id__in=[h.place_id for h in hits])

    # reset Place.review_{auth} to null
    for p in places:
        p.defer_comments.delete()
        if auth in ['whg', 'idx'] and p.review_whg != 1:
            p.review_whg = None
        elif auth.startswith('wd') and p.review_wd != 1:
            p.review_wd = None
        elif auth == 'tgn' and p.review_tgn != 1:
            p.review_tgn = None
        p.save()

    # zap hits
    hits.delete()
    if prior == 'na':
        tr.delete()
    else:
        # flag task as ARCHIVED
        tr.status = 'ARCHIVED'
        tr.save()
        # zap prior links/geoms if requested
        if prior == 'zap':
            PlaceLink.objects.all().filter(task_id=tid).delete()
            PlaceGeom.objects.all().filter(task_id=tid).delete()


def collab_add(request, dsid, v):
    """
      add collaborator to dataset in role
    """
    try:
        email = request.POST['email']
        role = request.POST['role']
        uid = get_object_or_404(User, email=email).id
    except MultiValueDictKeyError:
        # Handle missing 'email' or 'role' key in POST data
        messages.add_message(
            request, messages.INFO, "The email or role field is missing from the form. Please check and try again.")
        return redirect('/datasets/' + str(dsid) + '/collab') if v == '1' else HttpResponseRedirect(
            request.META.get('HTTP_REFERER'))
    except User.DoesNotExist:
        # Handle case where user with the provided email does not exist
        messages.add_message(
            request, messages.INFO,
            f"Please check email address: we don't have '{request.POST.get('email', 'unknown')}'")
        return redirect('/datasets/' + str(dsid) + '/collab') if v == '1' else HttpResponseRedirect(
            request.META.get('HTTP_REFERER'))
    except:
        messages.add_message(
            request, messages.INFO, "An error occurred while processing the request. Please try again.")
        if not v:
            return redirect('/datasets/' + str(dsid) + '/collab')
        else:
            return HttpResponseRedirect(request.META.get('HTTP_REFERER'))

    # Create the dataset-user relationship
    DatasetUser.objects.create(user_id_id=uid, dataset_id_id=dsid, role=role)

    # Redirect based on the value of 'v'
    return redirect('/datasets/' + str(dsid) + '/collab') if v == '1' else HttpResponseRedirect(
        request.META.get('HTTP_REFERER'))


def collab_delete(request, uid, dsid, v):
    """
      collab_delete(uid, dsid)
      remove collaborator from dataset
    """
    get_object_or_404(DatasetUser, user_id_id=uid, dataset_id_id=dsid).delete()
    if v == '1':
        return redirect('/datasets/' + str(dsid) + '/collab')
    else:
        return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


def dataset_file_delete(ds):
    """
      dataset_file_delete(ds)
      delete all uploaded files for a dataset
    """
    dsf_list = ds.files.all()
    for f in dsf_list:
        ffn = 'media/' + f.file.name
        if os.path.exists(ffn) and f.file.name != 'dummy_file.txt':
            os.remove(ffn)
        else:
            logger.debug(f'file {ffn} not found')


def update_rels_tsv(pobj, row):
    """
      update_rels_tsv(pobj, row) refactored 26 Nov 2022 (backup below)
      updates objects related to a Place (pobj)
      make new child objects of pobj: names, types, whens, related, descriptions
      for geoms and links, add from row if not there
      row is a pandas dict
    """
    header = list(row.keys())
    # print('update_rels_tsv(): pobj, row, header', pobj, row, header)
    src_id = row['id']
    title = row['title']
    # TODO: leading parens problematic for search on title
    title = re.sub('^\(.*?\)', '', title).strip()
    title_source = row['title_source']
    title_uri = row['title_uri'] if 'title_uri' in header else ''
    variants = [x.strip() for x in row['variants'].split(';')] \
        if 'variants' in header and row['variants'] not in ['', 'None', None] else []

    types = [x.strip() for x in row['types'].split(';')] \
        if 'types' in header and str(row['types']) not in ['', 'None', None] else []

    aat_types = [x.strip() for x in row['aat_types'].split(';')] \
        if 'aat_types' in header and str(row['aat_types']) not in ['', 'None', None] else []

    parent_name = row['parent_name'] if 'parent_name' in header else ''

    parent_id = row['parent_id'] if 'parent_id' in header else ''

    # empty lon and lat are None
    coords = makeCoords(row['lon'], row['lat']) \
        if 'lon' in header and 'lat' in header and row['lon'] else []
    try:
        matches = [x.strip() for x in row['matches'].split(';')] \
            if 'matches' in header and row['matches'] else []
    except:
        logger.exception(f'error on matches: {row["matches"]}', sys.exc_info())

    description = row['description'] \
        if row['description'] else ''

    # lists for associated objects
    objs = {"PlaceName": [], "PlaceType": [], "PlaceGeom": [], "PlaceWhen": [],
            "PlaceLink": [], "PlaceRelated": [], "PlaceDescription": []}

    # title as a PlaceName
    objs['PlaceName'].append(
        PlaceName(
            place=pobj,
            src_id=src_id,
            toponym=title,
            jsonb={"toponym": title, "citation": {"id": title_uri, "label": title_source}}
        ))

    # add variants as PlaceNames, if any
    if len(variants) > 0:
        for v in variants:
            haslang = re.search("@(.*)$", v.strip())
            new_name = PlaceName(
                place=pobj,
                src_id=src_id,
                toponym=v.strip(),
                jsonb={"toponym": v.strip(), "citation": {"id": "", "label": title_source}}
            )
            if haslang:
                new_name.jsonb['lang'] = haslang.group(1)
            objs['PlaceName'].append(new_name)
    #
    # PlaceType()
    if len(types) > 0:
        for i, t in enumerate(types):
            fclass_list = []
            # i always 0 in tsv
            aatnum = 'aat:' + aat_types[i] if len(aat_types) >= len(types) else None
            # get fclass(es) to add to Place (pobj)
            if aatnum and int(aatnum[4:]) in Type.objects.values_list('aat_id', flat=True):
                fc = get_object_or_404(Type, aat_id=int(aatnum[4:])).fclass
                fclass_list.append(fc)
            objs['PlaceType'].append(
                PlaceType(
                    place=pobj,
                    src_id=src_id,
                    jsonb={"identifier": aatnum,
                           "sourceLabel": t,
                           "label": aat_lookup(int(aatnum[4:])) if aatnum != 'aat:' else ''
                           }
                ))
        pobj.fclasses = fclass_list
        pobj.save()

    #
    # PlaceGeom()
    # TODO: test no existing identical geometry
    if len(coords) > 0:
        geom = {"type": "Point",
                "coordinates": coords,
                "geowkt": 'POINT(' + str(coords[0]) + ' ' + str(coords[1]) + ')'}
    elif 'geowkt' in header and row['geowkt'] not in ['', None]:  # some rows no geom
        geom = parse_wkt(row['geowkt'])
    else:
        geom = None
    # TODO:
    # if pobj is existing place, add geom only if it's new
    # if pobj is new place and row has geom, always add it
    if geom:
        def trunc4(val):
            return round(val, 4)

        new_coords = list(map(trunc4, list(geom['coordinates'])))

        # if no geoms, add this one
        if pobj.geoms.count() == 0:
            objs['PlaceGeom'].append(
                PlaceGeom(
                    place=pobj,
                    src_id=src_id,
                    jsonb=geom,
                    geom=GEOSGeometry(json.dumps(geom))
                ))
        # otherwise only add if coords don't match
        elif pobj.geoms.count() > 0:
            try:
                for g in pobj.geoms.all():
                    if list(map(trunc4, g.jsonb['coordinates'])) != new_coords:
                        objs['PlaceGeom'].append(
                            PlaceGeom(
                                place=pobj,
                                src_id=src_id,
                                jsonb=geom,
                                geom=GEOSGeometry(json.dumps(geom))
                            ))
            except:
                logger.exception(f'failed on {pobj}', sys.exc_info())

    # PlaceLink() - all are closeMatch
    # Pandas turns nulls into NaN strings, 'nan'
    if len(matches) > 0:
        # any existing? only add new
        exist_links = list(pobj.links.all().values_list('jsonb__identifier', flat=True))
        if len(set(matches) - set(exist_links)) > 0:
            # one or more new matches; add 'em
            for m in matches:
                objs['PlaceLink'].append(
                    PlaceLink(
                        place=pobj,
                        src_id=src_id,
                        jsonb={"type": "closeMatch", "identifier": m}
                    ))
    # print('objs after matches', objs)

    #
    # PlaceRelated()
    if parent_name != '':
        objs['PlaceRelated'].append(
            PlaceRelated(
                place=pobj,
                src_id=src_id,
                jsonb={
                    "relationType": "gvp:broaderPartitive",
                    "relationTo": parent_id,
                    "label": parent_name}
            ))
    # print('objs after related', objs)

    # PlaceWhen()
    # timespans[{start{}, end{}}], periods[{name,id}], label, duration
    objs['PlaceWhen'].append(
        PlaceWhen(
            place=pobj,
            src_id=src_id,
            jsonb={
                "timespans": [{
                    "start": {"earliest": pobj.minmax[0]},
                    "end": {"latest": pobj.minmax[1]}}]
            }
        ))
    # print('objs after when', objs)

    #
    # PlaceDescription()
    # @id, value, lang
    if description != '':
        objs['PlaceDescription'].append(
            PlaceDescription(
                place=pobj,
                src_id=src_id,
                jsonb={
                    "@id": "", "value": description, "lang": ""
                }
            ))

    # TODO: update place.fclasses, place.minmax, place.timespans

    # bulk_create(Class, batch_size=n) for each
    PlaceName.objects.bulk_create(objs['PlaceName'], batch_size=10000)
    logger.info('names done')
    PlaceType.objects.bulk_create(objs['PlaceType'], batch_size=10000)
    logger.info('types done')
    PlaceGeom.objects.bulk_create(objs['PlaceGeom'], batch_size=10000)
    logger.info('geoms done')
    PlaceLink.objects.bulk_create(objs['PlaceLink'], batch_size=10000)
    logger.info('links done')
    PlaceRelated.objects.bulk_create(objs['PlaceRelated'], batch_size=10000)
    logger.info('related done')
    PlaceWhen.objects.bulk_create(objs['PlaceWhen'], batch_size=10000)
    logger.info('whens done')
    PlaceDescription.objects.bulk_create(objs['PlaceDescription'], batch_size=10000)
    logger.info('descriptions done')


def ds_update(request):
    """
      ds_update() refactored 26 Nov 2022 (backup below)
      perform updates to database and index, given ds_compare() results
      params: dsid, format, keepg, keepl, compare_data (json string)
    """
    if request.method == 'POST':
        dsid = request.POST['dsid']
        ds = get_object_or_404(Dataset, id=dsid)
        file_format = request.POST['format']

        # keep previous recon/review results?
        keepg = request.POST['keepg']
        keepl = request.POST['keepl']

        # comparison returned by ds_compare
        compare_data = json.loads(request.POST['compare_data'])
        compare_result = compare_data['compare_result']

        # tempfn has .tsv or .jsonld extension from validation step
        tempfn = compare_data['tempfn']
        filename_new = compare_data['filename_new']
        dsfobj_cur = ds.files.all().order_by('-rev')[0]
        rev_num = dsfobj_cur.rev

        # rename file if already exists in user area
        if Path('media/' + filename_new).exists():
            fn = os.path.splitext(filename_new)
            # filename_new=filename_new[:-4]+'_'+tempfn[-11:-4]+filename_new[-4:]
            filename_new = fn[0] + '_' + tempfn[-11:-4] + fn[1]

        # user said go...copy tempfn to media/{user} folder
        filepath = 'media/' + filename_new
        copyfile(tempfn, filepath)

        # and create new DatasetFile; increment rev
        DatasetFile.objects.create(
            dataset_id=ds,
            file=filename_new,
            rev=rev_num + 1,
            format=file_format,
            upload_date=datetime.date.today(),
            header=compare_result['header_new'],
            numrows=compare_result['count_new']
        )

        # reopen new file as panda dataframe bdf
        if file_format == 'delimited':
            try:
                bdf = pd.read_csv(filepath, delimiter='\t')

                # replace pandas NaN with None
                bdf = bdf.replace({np.nan: ''})
                # bdf = bdf.replace({np.nan: None})
                # force data types
                bdf = bdf.astype({"id": str, "ccodes": str, "types": str, "aat_types": str})
            except:
                raise

            # CURRENT PLACES
            ds_places = ds.places.all()
            # pids of missing src_ids
            rows_delete = list(ds_places.filter(src_id__in=compare_result['rows_del']).values_list('id', flat=True))

            # CASCADE includes links & geoms
            try:
                ds_places.filter(id__in=rows_delete).delete()
            except:
                raise

            # for use below
            def delete_related(pid):
                # option to keep prior links and geoms matches; remove the rest
                if not keepg:
                    # keep no geoms
                    PlaceGeom.objects.filter(place_id=pid).delete()
                else:
                    # leave results of prior matches
                    PlaceGeom.objects.filter(place_id=pid, task_id__isnull=True).delete()
                if not keepl:
                    # keep no links
                    PlaceLink.objects.filter(place_id=pid).delete()
                else:
                    # leave results of prior matches
                    PlaceLink.objects.filter(place_id=pid, task_id__isnull=True).delete()
                PlaceName.objects.filter(place_id=pid).delete()
                PlaceType.objects.filter(place_id=pid).delete()
                PlaceWhen.objects.filter(place_id=pid).delete()
                PlaceRelated.objects.filter(place_id=pid).delete()
                PlaceDescription.objects.filter(place_id=pid).delete()

            # counts for report
            count_new, count_replaced, count_redo = [0, 0, 0]
            # pids for index operations
            rows_add = []
            idx_delete = []

            place_fields = {'id', 'title', 'ccodes', 'start', 'end', 'attestation_year'}
            alldiffs = []
            # bdfx=bdf.iloc[1:]
            # for index, row in bdfx.iterrows():
            for index, row in bdf.iterrows():
                # row=bdf.iloc[1]
                # new row as dict
                row = row.to_dict()

                start = int(row['start']) if 'start' in row else int(row['attestation_year']) \
                    if ('attestation_year' in row) else None
                end = int(row['end']) if 'end' in row and str(row['end']) != 'nan' else start
                minmax_new = [start, end] if start else [None]

                # extract coords from upload file
                row_coords = makeCoords(row['lon'], row['lat']) \
                    if row['lon'] and row['lat'] else None
                if row['geowkt']:
                    gtype = wkt.loads(row['geowkt']).type
                    if 'Multi' not in gtype:
                        row_coords = [list(u) for u in wkt.loads(row['geowkt']).coords]
                    else:
                        row_coords = [list(u) for u in wkt.loads(row['geowkt']).xy]
                # all columns in mew file
                header = list(bdf.keys())
                # row_mapper = [{k: row[k]} for k in header]
                row_mapper = {
                    'src_id': row['id'],
                    'title': row['title'],
                    'minmax': minmax_new,
                    'title_source': row['title_source'] if 'title_source' in header else '',
                    'title_uri': row['title_uri'] if 'title_uri' in header else '',
                    'ccodes': row['ccodes'].split(';') if 'ccodes' in header and row['ccodes'] else [],
                    'matches': row['matches'].split(';') if 'matches' in header and row['matches'] else [],
                    'variants': row['variants'].split(';') if 'variants' in header and row['variants'] else [],
                    'types': row['types'].split(';') if 'types' in header and row['types'] else [],
                    'aat_types': row['aat_types'].split(';') if 'aat_types' in header and row['aat_types'] else [],
                    'parent_name': row['parent_name'] if 'parent_name' in header else '',
                    'parent_id': row['parent_id'] if 'parent_id' in header else '',
                    'geo_source': row['geo_source'] if 'geo_source' in header else '',
                    'geo_id': row['geo_id'] if 'geo_id' in header else '',
                    'description': row['description'] if 'description' in header else '',
                    'coords': row_coords or [],
                }

                try:
                    # is there corresponding current Place?
                    p = ds_places.get(src_id=row['id'])
                    # fetch existing API record
                    c = Client()
                    from datasets.utils import PlaceMapper
                    try:
                        # result = c.get('/api/place_compare/' + str(6873911) + '/')
                        result = c.get('/api/place_compare/' + str(p.id) + '/')
                        pobj = result.json()
                        pobj = {key: val for key, val in sorted(pobj.items(), key=lambda ele: ele[0])}
                    except:
                        logger.exception(f'pobj failed {p.id}', sys.exc_info())

                    # build object for comparison
                    # TODO: build separate serializer(s) for this? performance?
                    p_mapper = PlaceMapper(
                        pobj['id'],
                        pobj['src_id'],
                        pobj['title'],
                    )

                    # id,title,title_source,title_uri,ccodes,matches,variants,types,aat_types,
                    # parent_name,parent_id,geo_source,geo_id,description
                    # add key:value pairs to consider
                    p_mapper['minmax'] = pobj['minmax']
                    title_name = next(n for n in pobj['names'] if n['toponym'] == pobj['title']) or None
                    p_mapper['title_source'] = title_name['citation']['label'] if \
                        'citation' in title_name and 'label' in title_name['citation'] else ''
                    p_mapper['title_id'] = title_name['citation']['id'] if \
                        'citation' in title_name and 'id' in title_name['citation'] else ''
                    p_mapper['ccodes'] = pobj['ccodes'] or []
                    p_mapper['types'] = [t['sourceLabel'] for t in pobj['types']] or []
                    p_mapper['aat_types'] = [t['identifier'][4:] for t in pobj['types']] or []
                    p_mapper['variants'] = [n['toponym'] for n in pobj['names'] if n['toponym'] != pobj['title']] or []
                    p_mapper['coords'] = [g['coordinates'] for g in pobj['geoms']] or []

                    p_mapper['geo_sources'] = [g['citation']['label'] for g in pobj['geoms'] \
                                               if 'citation' in g and 'label' in g['citation']] or []
                    p_mapper['geo_ids'] = [g['citation']['id'] for g in pobj['geoms'] \
                                           if 'citation' in g and 'id' in g['citation']] or []

                    p_mapper['links'] = [l['identifier'] for l in pobj['links']] or []
                    p_mapper['related'] = [r['label'] for r in pobj['related']]
                    p_mapper['related_id'] = [r['identifier'] for r in pobj['related']]
                    p_mapper['descriptions'] = [d['value'] for d in pobj['related']]

                    # diff incoming (row_mapper) & database (p_mapper)
                    # meaningful = title, variants, aat_types, links/matches, coords
                    diffs = []

                    # [:8] not meaningful (don't affect reconciliation)
                    diffs.append(
                        row_mapper['title_source'] == p_mapper['title_source'] if row_mapper['title_source'] else True)
                    diffs.append(row_mapper['title_uri'] == p_mapper['title_id'] if row_mapper['title_uri'] else True)
                    diffs.append(
                        row_mapper['parent_name'] in p_mapper['related'] if row_mapper['parent_name'] else True)
                    diffs.append(row_mapper['parent_id'] in p_mapper['related_id'] if row_mapper['parent_id'] else True)
                    diffs.append(
                        row_mapper['geo_source'] in p_mapper['geo_sources'] if row_mapper['geo_source'] != '' else True)
                    diffs.append(row_mapper['geo_id'] in p_mapper['geo_ids'] if row_mapper['geo_id'] != '' else True)
                    diffs.append(
                        row_mapper['description'] in p_mapper['descriptions'] if row_mapper['description'] else True)
                    diffs.append(row_mapper['minmax'] == p_mapper['minmax'])
                    diffs.append(sorted(row_mapper['types']) == sorted(p_mapper['types']))

                    # [9:] meaningful
                    diffs.append(row_mapper['title'] == p_mapper['title'])
                    diffs.append(sorted(row_mapper['variants']) == sorted(p_mapper['variants']))
                    diffs.append(sorted(row_mapper['aat_types']) == sorted(p_mapper['aat_types']))
                    diffs.append(sorted(row_mapper['matches']) == sorted(p_mapper['links']))
                    diffs.append(sorted(row_mapper['ccodes']) == sorted(p_mapper['ccodes']))
                    if row_mapper['coords'] != []:
                        diffs.append(row_mapper['coords'] == p_mapper['coords'])

                    alldiffs.append({'title': row_mapper['title'], 'diffs': diffs})

                    # update Place record in all cases
                    count_replaced += 1
                    p.title = row_mapper['title']
                    p.ccodes = row_mapper['ccodes']
                    p.minmax = minmax_new
                    p.timespans = [minmax_new]

                    if False in diffs:
                        # there was SOME change(s) -> add to delete-from-index list
                        # (will be reindexed after re-reconciling)
                        idx_delete.append(p.id)
                    if False not in diffs[9:]:
                        # no meaningful changes
                        # replace related, preserving geoms & links if keepg, keepl
                        # leave review_wd and flag status intact
                        delete_related(p)
                        update_rels_tsv(p, row)
                    else:
                        # meaningful change(s) exist
                        count_redo += 1
                        # replace related, including geoms and links
                        keepg, keepl = [False, False]
                        delete_related(p)
                        update_rels_tsv(p, row)

                        # (re)set Place.review_wd & Place.flag (needs reconciliation)
                        p.review_wd = None
                        p.flag = True

                        # meaningful change, so
                        # add to list for index deletion
                        if p.id not in idx_delete:
                            idx_delete.append(p.id)

                    p.save()
                except:
                    # no corresponding Place, create new one
                    count_new += 1
                    newpl = Place.objects.create(
                        src_id=row['id'],
                        title=re.sub('\(.*?\)', '', row['title']),
                        ccodes=[] if str(row['ccodes']) == 'nan' else row['ccodes'].replace(' ', '').split(';'),
                        dataset=ds,
                        minmax=minmax_new,
                        timespans=[minmax_new],
                        # flax for reconciling
                        flag=True
                    )
                    newpl.save()
                    pobj = newpl
                    rows_add.append(pobj.id)
                    # add related rcords (PlaceName, PlaceType, etc.)
                    update_rels_tsv(pobj, row)
                # except:
                #   print('update failed on ', row)
                #   print('error', sys.exc_info())

            # update numrows
            ds.numrows = ds.places.count()
            ds.save()

            # initiate a result object
            result = {"status": "updated", "format": file_format,
                      "update_count": count_replaced, "redo_count": count_redo,
                      "new_count": count_new, "deleted_count": len(rows_delete),
                      "newfile": filepath}

            #
            if compare_data['count_indexed'] > 0:
                result["indexed"] = True

                # surgically remove as req.
                # rows_delete(gone from db) + idx_delete(rows with meaningful change)
                idx_delete = rows_delete + idx_delete
                if len(idx_delete) > 0:
                    es = settings.ES_CONN
                    idx = settings.ES_WHG
                    removePlacesFromIndex(es, idx, idx_delete)
            else:
                logger.info('not indexed')

            # write log entry
            Log.objects.create(
                # category, logtype, "timestamp", subtype, note, dataset_id, user_id
                category='dataset',
                logtype='ds_update',
                note=json.dumps(compare_result),
                dataset_id=dsid,
                # user_id = 1
                user_id=request.user.id
            )
            ds.ds_status = 'updated'
            ds.save()
            # return to update modal
            return JsonResponse(result, safe=False)
        elif file_format == 'lpf':
            logger.info("ds_update for lpf; doesn't get here yet")


class PublicListsView(ListView):
    """
      PublicListView()
      list public datasets and collections
    """
    redirect_field_name = 'redirect_to'

    context_object_name = 'dataset_list'
    template_name = 'datasets/public_list.html'
    model = Dataset

    def get_queryset(self):
        # original qs
        qs = super().get_queryset()
        return qs.filter(public=True).order_by('core', 'title')

    def get_context_data(self, *args, **kwargs):
        context = super(PublicListsView, self).get_context_data(*args, **kwargs)

        # public datasets available as dataset_list
        # public collections
        context['coll_list'] = Collection.objects.filter(status='published').order_by('create_date')
        context['viewable'] = ['uploaded', 'inserted', 'reconciling', 'review_hits', 'reviewed', 'review_whg',
                               'indexed']

        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'admins']).exists() else False
        return context


class DatasetCreateEmptyView(LoginRequiredMixin, CreateView):
    """
      DatasetCreateEmptyView()
      initial create, no file; for remote, typically
    """
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'

    form_class = DatasetCreateEmptyModelForm
    template_name = 'datasets/dataset_create_empty.html'
    success_message = 'empty dataset created'

    def form_invalid(self, form):
        logger.error(f'form invalid: {form.errors.as_data()}')
        context = {'form': form}
        return self.render_to_response(context=context)

    def form_valid(self, form):
        data = form.cleaned_data
        context = {"format": "empty"}
        # context={"format":data['format']}
        user = self.request.user
        # validated -> create Dataset, DatasetFile, Log instances,
        # advance to dataset_detail
        # else present form again with errors
        # if len(result['errors']) == 0:
        context['status'] = 'format_ok'

        # print('validated, no errors')
        # print('validated, no errors; result:', result)
        nolabel = form.cleaned_data["label"] == ''
        # new Dataset record ('owner','id','label','title','description')
        dsobj = form.save(commit=False)
        dsobj.ds_status = 'format_ok'
        dsobj.numrows = 0
        clean_label = form.cleaned_data['label'].replace(' ', '_')
        if not form.cleaned_data['uri_base']:
            dsobj.uri_base = 'https://whgazetteer.org/api/db/?id='

        # links will be counted later on insert
        dsobj.numlinked = 0
        dsobj.total_links = 0
        try:
            dsobj.save()
            ds = Dataset.objects.get(id=dsobj.id)
            label = 'ds_' + str(ds.id)
            # generate a unique label if none was entered
            if dsobj.label == '':
                ds.label = 'ds_' + str(dsobj.id)
                ds.save()
        except:
            # self.args['form'] = form
            return render(self.request, 'datasets/dataset_create.html', self.args)

        #
        # create user directory if necessary
        userdir = r'media/user_' + user.id + '/'
        if not Path(userdir).exists():
            os.makedirs(userdir)

        # build path, and rename file if already exists in user area
        # file_exists = Path(userdir+filename).exists()
        # if not file_exists:
        #   filepath = userdir+filename
        # else:
        #   splitty = filename.split('.')
        #   filename=splitty[0]+'_'+tempfn[-7:]+'.'+splitty[1]
        #   filepath = userdir+filename

        # write log entry
        Log.objects.create(
            # category, logtype, "timestamp", subtype, dataset_id, user_id
            category='dataset',
            logtype='ds_create_empty',
            subtype=data['datatype'],
            dataset_id=dsobj.id,
            user_id=user.id
        )

        # create initial DatasetFile record
        DatasetFile.objects.create(
            dataset_id=dsobj,
            file='dummy_file.txt',
            rev=1,
            format='delimited',
            delimiter='n/a',
            df_status='dummy',
            upload_date=None,
            header=[],
            numrows=0
        )

        # data will be written on load of dataset.html w/dsobj.status = 'format_ok'
        # return redirect('/datasets/'+str(dsobj.id)+'/detail')
        return redirect('/datasets/' + str(dsobj.id) + '/summary')

        # else:
        context['action'] = 'errors'
        # context['format'] = result['format']
        # context['errors'] = parse_errors_lpf(result['errors']) \
        #   if ext == 'json' else parse_errors_tsv(result['errors'])
        # context['columns'] = result['columns'] \
        #   if ext != 'json' else []

        # os.remove(tempfn)

        return self.render_to_response(
            self.get_context_data(
                form=form, context=context
            ))

    def get_context_data(self, *args, **kwargs):
        context = super(DatasetCreateEmptyView, self).get_context_data(*args, **kwargs)
        # context['action'] = 'create'
        return context


class DatasetPublicView(DetailView):
    """
      returns public dataset 'meta' (summary) page
    """
    template_name = 'datasets/ds_meta.html'

    model = Dataset

    def get_context_data(self, **kwargs):
        context = super(DatasetPublicView, self).get_context_data(**kwargs)

        ds = get_object_or_404(Dataset, id=self.kwargs['pk'])
        file = ds.file

        placeset = ds.places.all()

        if file:
            context['current_file'] = file
            context['format'] = file.format
            context['numrows'] = file.numrows
            context['filesize'] = round(file.file.size / 1000000, 1)

            context['links_added'] = PlaceLink.objects.filter(
                place_id__in=placeset, task_id__contains='-').count()
            context['geoms_added'] = PlaceGeom.objects.filter(
                place_id__in=placeset, task_id__contains='-').count()

        return context


# TODO: delete other stuff: disk files; archive??
# class DatasetDeleteView(DeleteView):
#     """
#       loads page for confirm ok on delete
#         - delete dataset, with CASCADE to DatasetFile, places, place_name, etc
#         - also deletes from index if indexed (fails silently if not)
#         - also removes dataset_file records
#     """
#     template_name = 'datasets/dataset_delete.html'
#
#     def delete_complete(self):
#         ds = get_object_or_404(Dataset, pk=self.kwargs.get("id"))
#         dataset_file_delete(ds)
#         if ds.ds_status == 'indexed':
#             pids = list(ds.placeids)
#             removePlacesFromIndex(es, 'whg', pids)
#
#     def get_object(self):
#         id_ = self.kwargs.get("id")
#         ds = get_object_or_404(Dataset, id=id_)
#         return (ds)
#
#     def get_context_data(self, **kwargs):
#         context = super(DatasetDeleteView, self).get_context_data(**kwargs)
#         ds = get_object_or_404(Dataset, id=self.kwargs.get("id"))
#         context['owners'] = ds.owners
#         return context
#
#     def get_success_url(self):
#         self.delete_complete()
#         return reverse('dashboard')
class DatasetDeleteView(DeleteView):
    template_name = 'datasets/dataset_delete.html'
    model = Dataset

    def delete(self, request, *args, **kwargs):
        self.object = self.get_object()
        success_url = self.get_success_url()

        # Custom deletion logic
        dataset_file_delete(self.object)
        if self.object.ds_status == 'indexed':
            pids = list(self.object.placeids)
            removePlacesFromIndex(es, 'whg', pids)

        # Deletes the object
        self.object.delete()
        return HttpResponseRedirect(self.get_success_url())

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['owners'] = self.get_object().owners
        return context

    def get_success_url(self):
        url = django_reverse('dashboard')
        assert isinstance(url, str), f"Expected string, got {type(url)}: {url}"
        return url


def ds_list(request, label):
    """
      fetch places in specified dataset
      utility used for place collections
    """
    qs = Place.objects.all().filter(dataset=label)
    geoms = []
    for p in qs.all():
        feat = {"type": "Feature",
                "properties": {"src_id": p.src_id, "name": p.title},
                "geometry": p.geoms.first().jsonb}
        geoms.append(feat)
    return JsonResponse(geoms, safe=False)


def match_undo(request, ds, tid, pid):
    """
      undo last review match action
      - delete any geoms or links created
      - reset flags for hit.reviewed and place.review_xxx
    """
    from django_celery_results.models import TaskResult
    geom_matches = PlaceGeom.objects.filter(task_id=tid, place_id=pid)
    link_matches = PlaceLink.objects.filter(task_id=tid, place_id=pid)
    geom_matches.delete()
    link_matches.delete()

    # reset place.review_xxx to 0
    tasktype = TaskResult.objects.get(task_id=tid).task_name[6:]
    place = Place.objects.get(pk=pid)
    # remove any defer comments
    place.defer_comments.delete()
    # TODO: variable field name?
    if tasktype.startswith('wd'):
        place.review_wd = 0
    elif tasktype == 'tgn':
        place.review_tgn = 0
    else:
        place.review_whg = 0
    place.save()

    # match task_id, place_id in hits; set reviewed = false
    Hit.objects.filter(task_id=tid, place_id=pid).update(reviewed=False)

    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


class DatasetStatusView(LoginRequiredMixin, UpdateView):
    """
      returns dataset owner summary page
    """
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    form_class = DatasetDetailModelForm
    template_name = 'datasets/ds_status.html'

    def get_object(self):
        return get_object_or_404(Dataset, id=self.kwargs.get("id"))

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)

        ds = self.get_object()
        place_qs = ds.places.all()
        place_ids = list(place_qs.values_list('id', flat=True))
        place_count = len(place_ids)

        def count_review_fields(field_name):
            return {
                "rows": place_count,
                "got_hits": place_qs.exclude(**{f"{field_name}__isnull": True}).count(),
                "reviewed": place_qs.filter(**{field_name: 1}).count(),
                "deferred": place_qs.filter(**{field_name: 2}).count(),
                "remain": place_qs.filter(**{f"{field_name}__in": [0, 2]}).count(),
            }

        context['wdgn_status'] = count_review_fields("review_wd")
        context['idx_status'] = count_review_fields("review_whg")

        # Fetch dataset tasks excluding irrelevant ones
        ds_tasks = ds.tasks.exclude(status__in=['FAILURE', 'ARCHIVED'])
        context['tasks'] = ds_tasks

        task_wdgn = ds_tasks.filter(task_name__startswith='align_wd').order_by('-date_done').first()
        task_idx = ds_tasks.filter(task_name__startswith='align_idx').order_by('-date_done').first()

        context['task_wdgn'] = task_wdgn
        context['task_idx'] = task_idx

        def placecounter(queryset):
            values = queryset.values_list('query_pass', 'place_id').distinct()
            seen = set()
            counter = Counter()
            for qpass, pid in values:
                if (qpass, pid) not in seen:
                    counter[qpass] += 1
                    seen.add((qpass, pid))
            return {
                'p0': counter.get('pass0', 0),
                'p1': counter.get('pass1', 0),
                'p2': counter.get('pass2', 0),
                'p0and1': counter.get('pass0', 0) + counter.get('pass1', 0),
            }

        if task_wdgn:
            hits_wdgn = Hit.objects.filter(task_id=task_wdgn.task_id, reviewed=False)
            context['wdgn_passes'] = placecounter(hits_wdgn)
        else:
            context['wdgn_passes'] = {}

        if task_idx:
            hits_idx = Hit.objects.filter(task_id=task_idx.task_id, reviewed=False)
            context['idx_passes'] = placecounter(hits_idx)
        else:
            context['idx_passes'] = {}

        user = self.request.user
        user_groups = set(user.groups.values_list('name', flat=True))

        context.update({
            'ds': ds,
            'updates': {},
            'is_owner': ds.owners.filter(id=user.id).exists(),
            'is_collaborator': ds.collaborators.filter(id=user.id).exists(),
            'is_admin': 'whg_admins' in user_groups,
            'is_editorial': 'editorial' in user_groups,
            'beta_or_better': bool(user_groups & {'beta', 'admins'}),
        })

        # Aggregated counts across related models
        context['numrows'] = place_count

        def agg_counts(model):
            return model.objects.filter(place_id__in=place_ids).aggregate(
                base=Count('id', filter=Q(task_id=None)),
                added=Count('id', filter=~Q(task_id=None))
            )

        name_counts = agg_counts(PlaceName)
        link_counts = agg_counts(PlaceLink)
        geom_counts = agg_counts(PlaceGeom)

        context.update({
            'num_names': name_counts['base'],
            'names_added': name_counts['added'],
            'num_links': link_counts['base'],
            'links_added': link_counts['added'],
            'num_geoms': geom_counts['base'],
            'geoms_added': geom_counts['added'],
        })

        context['vis_parameters_dict'] = ds.vis_parameters or {
            key: {'tabulate': False, 'temporal_control': 'none', 'trail': False}
            for key in ('seq', 'min', 'max')
        }

        return context


@require_POST
def update_vis_parameters(request, *args, **kwargs):
    """
      vis_parameters on ds_status page
    """
    try:
        ds_id = request.POST.get('ds_id')
        checked = bool(request.POST.get('checked') == 'true')

        if checked:
            vis_parameters = {
                'seq': {'tabulate': False, 'temporal_control': 'none', 'trail': False},
                'min': {'tabulate': 'initial', 'temporal_control': 'filter', 'trail': False},
                'max': {'tabulate': True, 'temporal_control': 'filter', 'trail': False}
            }
        else:
            vis_parameters = {
                'seq': {'tabulate': False, 'temporal_control': 'none', 'trail': False},
                'min': {'tabulate': False, 'temporal_control': 'none', 'trail': False},
                'max': {'tabulate': False, 'temporal_control': 'none', 'trail': False}
            }

        # Update the vis_parameters field of the dataset
        dataset = get_object_or_404(Dataset, pk=ds_id)
        dataset.vis_parameters = vis_parameters
        dataset.save()

        return JsonResponse(
            {'message': 'Visualisation parameters updated successfully', 'vis_parameters': json.dumps(vis_parameters)})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
def update_volunteers_text(request):
    """
      volunteers request on ds_status page
    """
    if request.method == 'POST':
        dataset_id = request.POST.get('dataset_id')
        volunteers_text = request.POST.get('volunteers_text')
        reset = request.POST.get('reset', 'false') == 'true'

        dataset = Dataset.objects.get(id=dataset_id)

        if reset:
            dataset.volunteers_text = None
        else:
            dataset.volunteers_text = volunteers_text

        dataset.save()
        return JsonResponse({'status': 'success'})


class DatasetMetadataView(LoginRequiredMixin, UpdateView):
    """
      returns dataset owner metadata page
      (formerly DatasetSummaryView)
    """
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'

    form_class = DatasetDetailModelForm

    template_name = 'datasets/ds_metadata.html'

    # Dataset has been edited, form submitted
    def form_valid(self, form):
        data = form.cleaned_data
        ds = get_object_or_404(Dataset, pk=self.kwargs.get("id"))
        dsid = ds.id
        user = self.request.user
        file = data['file']
        filerev = ds.files.all().order_by('-rev')[0].rev
        # print('DatasetSummaryView kwargs',self.kwargs)
        if data["file"] == None:
            # no file, updating dataset only
            ds.title = data['title']
            ds.description = data['description']
            ds.uri_base = data['uri_base']
            ds.save()

        return super().form_valid(form)

    def form_invalid(self, form):
        logger.error(f'form invalid: {form.errors.as_data()}')
        return super().form_invalid(form)

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Dataset, id=id_)

    def get_context_data(self, *args, **kwargs):
        context = super(DatasetMetadataView, self).get_context_data(*args, **kwargs)

        # print('DatasetSummaryView get_context_data() kwargs:',self.kwargs)
        # print('DatasetSummaryView get_context_data() request.user',self.request.user)
        id_ = self.kwargs.get("id")
        ds = get_object_or_404(Dataset, id=id_)

        """
          when coming from DatasetCreateView() (file.df_status == format_ok)
          runs ds_insert_tsv() or ds_insert_lpf()
          using most recent dataset file
        """
        file = ds.file

        # build context for rendering ds_status.html
        me = self.request.user
        placeset = ds.places.all()

        context['updates'] = {}
        context['ds'] = ds
        context['collaborators'] = ds.collaborators.all()
        context['owners'] = ds.owners
        context['is_admin'] = True if me.groups.filter(name__in=['whg_admins']).exists() else False
        context['editorial'] = True if me.groups.filter(name__in=['editorial']).exists() else False

        # Filter files to include only those that exist on the filesystem
        context['files'] = [
                               file for file in ds.files.order_by('-id')
                               if os.path.exists(file.file.path)  # Check if the file exists
                           ] or None  # If no files exist, set to None

        # excludes datasets w/o an associated DatasetFile
        if file and hasattr(file, 'file') and os.path.exists(file.file.path):
            context['current_file'] = file
            context['format'] = file.format
            context['numrows'] = file.numrows
            context['filesize'] = round(file.file.size / 1000000, 1)

        # initial (non-task)
        context['num_names'] = PlaceName.objects.filter(place_id__in=placeset).count()
        context['num_links'] = PlaceLink.objects.filter(
            place_id__in=placeset, task_id=None).count()
        context['num_geoms'] = PlaceGeom.objects.filter(
            place_id__in=placeset, task_id=None).count()

        # augmentations (has task_id)
        context['links_added'] = PlaceLink.objects.filter(
            place_id__in=placeset, task_id__contains='-').count()
        context['geoms_added'] = PlaceGeom.objects.filter(
            place_id__in=placeset, task_id__contains='-').count()

        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'admins']).exists() else False

        # print('context from DatasetSummaryView', context)
        return context


class DatasetBrowseView(LoginRequiredMixin, DetailView):
    """
      returns dataset owner's browse table
    """
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'

    model = Dataset
    template_name = 'datasets/ds_browse.html'

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        user = self.request.user
        return '/datasets/' + str(id_) + '/browse'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Dataset, id=id_)

    def get_context_data(self, *args, **kwargs):
        context = super(DatasetBrowseView, self).get_context_data(*args, **kwargs)

        id_ = self.kwargs.get("id")

        ds = get_object_or_404(Dataset, id=id_)
        me = self.request.user
        ds_tasks = [t for t in ds.recon_status]

        context['collaborators'] = ds.collaborators.all()
        context['owners'] = ds.owners
        context['is_admin'] = True if me.groups.filter(name__in=['whg_admins']).exists() else False
        context['updates'] = {}
        context['ds'] = ds
        context['num_places'] = ds.num_places
        context['tgntask'] = 'tgn' in ds_tasks
        context['whgtask'] = len(set(['whg', 'idx']) & set(ds_tasks)) > 0
        context['wdtask'] = len(set(['wd', 'wdlocal']) & set(ds_tasks)) > 0

        return context


class DatasetPlacesView(DetailView):
    """
      returns public dataset browse table
    """
    login_url = "/accounts/login/"
    redirect_field_name = "redirect_to"

    model = Dataset
    template_name = "datasets/ds_places.html"
    unavailable_template_name = "main/503.html"

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Dataset, id=id_)

    def get(self, request, *args, **kwargs):
        ds = self.get_object()

        if ds.num_places > settings.DATASETS_PLACES_LIMIT:
            message = f"Sorry, this dataset cannot be viewed on this page because it has too many places."
            return render(
                request,
                self.unavailable_template_name,
                {"message": message},
                status=503
            )

        return super().get(request, *args, **kwargs)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        id_ = self.kwargs.get("id")
        ds = get_object_or_404(Dataset, id=id_)
        me = self.request.user

        context.update(
            {
                "URL_FRONT": settings.URL_FRONT,
                "ds": ds,
                "is_admin": me.groups.filter(name__in=["whg_admins"]).exists(),
                "loggedin": "true" if not me.is_anonymous else "false",
                "my_collections": Collection.objects.filter(
                    collection_class="place",
                    **(
                        {"owner": me}
                        if not me.groups.filter(name="whg_admins").exists()
                        else {}
                    )
                )
                if not me.is_anonymous
                else None,
            }
        )

        return context


class DatasetReconcileView(LoginRequiredMixin, DetailView):
    """
      returns dataset owner "Linking" tab listing reconciliation tasks
    """
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'

    model = Dataset
    template_name = 'datasets/ds_reconcile.html'

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        user = self.request.user
        return '/datasets/' + str(id_) + '/reconcile'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Dataset, id=id_)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        ds = self.object
        place_qs = ds.places.all()

        def review_status(field_name):
            if field_name == 'review_whg':
                got_hits_qs = place_qs.exclude(review_whg=None)
                logger.debug(f"🚨 Got hits count: {got_hits_qs.count()}")

                distinct_values = got_hits_qs.values_list('review_whg', flat=True).distinct()
                logger.debug(f"🚨 Distinct review_whg values in got_hits: {list(distinct_values)}")

                all_values = list(got_hits_qs.values_list('review_whg', flat=True))
                logger.debug(f"🚨 All review_whg values in got_hits: {all_values[:100]}")

                # --- Additional detailed debug for unreviewed places with no unreviewed hits ---
                unreviewed_places = place_qs.filter(review_whg=0)
                # Use current task_id and passnum if available (pass as args or set here)
                # For now, let's assume you want to check all unreviewed hits for all tasks:
                task_ids = ds.tasks.values_list('task_id', flat=True)
                places_with_unreviewed_hits = Hit.objects.values_list("place_id", flat=True).filter(
                    place__dataset=ds,
                    reviewed=False,
                    task_id__in=task_ids
                ).distinct()
                places_without_hits = unreviewed_places.exclude(pk__in=places_with_unreviewed_hits)
                logger.debug(f"🚨 Unreviewed places without unreviewed hits: {[p.id for p in places_without_hits]}")

            return {
                "rows": place_qs.count(),
                "got_hits": place_qs.exclude(**{field_name: None}).count(),
                "reviewed": place_qs.filter(**{field_name: 1}).count(),
                "deferred": place_qs.filter(**{field_name: 2}).count(),
                "remain": place_qs.filter(**{f"{field_name}__in": [0, 2]}).count(),
            }

        context["wdgn_status"] = review_status("review_wd")
        context["idx_status"] = review_status("review_whg")

        context["is_admin"] = self.request.user.groups.filter(name="whg_admins").exists()
        context["beta_or_better"] = self.request.user.groups.filter(name__in=["beta", "whg_admins"]).exists()

        context["ds"] = ds
        tasks = ds.tasks.filter(status="SUCCESS")

        for task in tasks: # These are structured differently between align and reconcile
            task.result = json.loads(task.result) if task.result else {}
            if 'summary' in task.result: # align tasks
                task.result = task.result['summary']
                task.result['total_hits'] = Hit.objects.filter(place__dataset=ds, task_id=task.task_id).count()
                task.result['elapsed'] = task.result['elapsed_min']
                del task.result['elapsed_min']
            # Convert back to JSON
            task.result = json.dumps(task.result, indent=2, ensure_ascii=False)

        context["tasks"] = tasks

        return context


class DatasetCollabView(LoginRequiredMixin, DetailView):
    """
      returns dataset owner "Collaborators" tab
    """
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'

    model = DatasetUser
    template_name = 'datasets/ds_collab.html'

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        user = self.request.user
        return '/datasets/' + str(id_) + '/collab'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Dataset, id=id_)

    def get_context_data(self, *args, **kwargs):
        context = super(DatasetCollabView, self).get_context_data(*args, **kwargs)

        id_ = self.kwargs.get("id")
        ds = get_object_or_404(Dataset, id=id_)

        # build context for rendering dataset.html
        me = self.request.user

        context['ds'] = ds

        context['is_admin'] = True if me.groups.filter(name__in=['whg_admins']).exists() else False
        context['editorial'] = True if me.groups.filter(name__in=['editorial']).exists() else False
        context['collabs'] = ds.collabs.all()
        context['collaborators'] = ds.collaborators.all()
        context['owners'] = ds.owners

        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'admins']).exists() else False

        return context


class DatasetAddTaskView(LoginRequiredMixin, DetailView):
    """
    View to return the add (reconciliation) task page for a specific dataset.

    This view allows users to view and manage reconciliation tasks related to a dataset,
    including displaying status messages about tasks in progress, user areas, and predefined regions.
    """
    logger = logging.getLogger('reconciliation')

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    model = Dataset
    template_name = 'datasets/ds_addtask.html'

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        self.logger.debug('Redirecting to success URL for dataset ID: %s', id_)
        return f'/datasets/{id_}/log'

    def get_object(self):
        id_ = self.kwargs.get("id")
        dataset = get_object_or_404(Dataset, id=id_)
        self.logger.debug('Retrieved dataset object: %s', dataset)
        return dataset

    def get_context_data(self, *args, **kwargs):
        """
        Populates the context with data needed to render the task page.

        It retrieves user areas, predefined regions, task statuses, and other relevant
        information for the dataset, handling errors gracefully and logging useful
        information for debugging.
        """
        context_start = time.perf_counter()
        context = super().get_context_data(*args, **kwargs)

        total_t0 = time.perf_counter()
        ds_t0 = total_t0
        ds = self.get_object()
        self.logger.debug("Fetched dataset object in %.3fs", time.perf_counter() - ds_t0)

        # Prepare context variables
        me = self.request.user
        area_types = ['ccodes', 'copied', 'drawn']
        is_admin = self.request.user.groups.filter(name__in=['whg_admins']).exists()

        # --- user areas ---
        t0 = time.perf_counter()
        try:
            userareas = Area.objects.filter(
                type__in=area_types,
                **({} if is_admin else {'owner_id': me.id})
            ).values('id', 'title').order_by('-created')
        except Exception as e:
            self.logger.error('Error retrieving user areas: %s', e)
            userareas = []
        self.logger.debug("Retrieved user areas in %.3fs", time.perf_counter() - t0)

        # --- predefined areas ---
        t0 = time.perf_counter()
        try:
            predefined = Area.objects.filter(type='predefined').values('id', 'title')
        except Exception as e:
            self.logger.error('Error retrieving predefined areas: %s', e)
            predefined = []
        self.logger.debug("Retrieved predefined areas in %.3fs", time.perf_counter() - t0)

        # --- task + got_hits section ---
        t0 = time.perf_counter()
        gothits = {}
        for t in ds.tasks.filter(status='SUCCESS', task_name__startswith='align_'):
            try:
                result_data = json.loads(t.result)
                gothits[t.task_id] = int(result_data.get('got_hits', 0))
            except json.JSONDecodeError:
                gothits[t.task_id] = 0
        self.logger.debug("Processed %d tasks in %.3fs", len(gothits), time.perf_counter() - t0)

        # --- prepare status messages ---
        t0 = time.perf_counter()
        self._prepare_status_messages(context, ds, gothits)
        self.logger.debug("Prepared status messages in %.3fs", time.perf_counter() - t0)

        # --- gather remaining context fields ---
        t0 = time.perf_counter()
        context['region_list'] = predefined
        context['area_list'] = userareas
        context['userarea'] = self.request.GET.get('userarea', None)
        context['ds'] = ds
        context['numrows'] = ds.places.count()
        self.logger.debug("numrows computed in %.3fs", time.perf_counter() - t0)
        context['collaborators'] = ds.collabs.all()
        self.logger.debug("collaborators fetched in %.3fs", time.perf_counter() - t0)
        context['owners'] = ds.owners
        self.logger.debug("owners fetched in %.3fs", time.perf_counter() - t0)
        context['remain_to_review'] = {k[6:]: v[0]['total'] for k, v in ds.taskstats.items() if len(v) > 0}
        self.logger.debug("remain_to_review computed in %.3fs", time.perf_counter() - t0)
        context['missing_geoms'] = ds.missing_geoms
        self.logger.debug("missing_geoms computed in %.3fs", time.perf_counter() - t0)
        context['is_admin'] = is_admin
        self.logger.debug("Filled remaining context in %.3fs", time.perf_counter() - t0)

        self.logger.debug("Total get_context_data() time: %.3fs", time.perf_counter() - context_start)
        return context

    def _prepare_status_messages(self, context, ds, gothits):
        """
        Helper method to prepare status messages based on task statistics.

        Args:
            context: The context dictionary for the template.
            ds: The Dataset object being processed.
            gothits: Dictionary containing the number of hits per task.
        """
        # Define status message templates
        msg_unreviewed = ("There is a <span class='strong'>%s</span> task in progress, "
                          "and all %s records that got hits remain unreviewed. "
                          "<span class='text-danger strong'>Starting this new task "
                          "will delete the existing one</span>, with no impact on your dataset.")

        msg_inprogress = ("<p class='mb-1'>There is a <span class='strong'>%s</span> task in progress, "
                          "and %s of the %s records that had hits have been reviewed. "
                          "<span class='text-danger strong'>Starting this new task "
                          "will archive the existing task and submit only unreviewed records.</span> "
                          "If you proceed, you can keep or delete prior match results (links and/or geometry):</p>")

        msg_done = ("All records have been submitted for reconciliation to %s and reviewed. "
                    "To begin the step of accessioning to the WHG index, please <a href='%s'>contact our editorial team.</a>")

        for task_type, stats in ds.taskstats.items():
            auth = task_type[6:]  # Strip 'align_'
            auth_name = 'Wikidata+GeoNames' if auth == 'wdlocal' else 'WHG index'
            if stats:
                tid = stats[0]['tid']
                remaining = stats[0]['total']
                hadhits = gothits.get(tid, 0)
                reviewed = hadhits - remaining

                self.logger.debug("Auth: %s, Task ID: %s, Remaining: %s, Had Hits: %s", auth, tid, remaining, hadhits)

                if remaining == 0 and ds.ds_status != 'updated':
                    context[f'msg_{auth}'] = {
                        'msg': msg_done % (auth_name, "/contact"),
                        'type': 'done'}
                elif remaining < hadhits and ds.ds_status != 'updated':
                    context[f'msg_{auth}'] = {
                        'msg': msg_inprogress % (auth_name, reviewed, hadhits),
                        'type': 'inprogress'}
                else:
                    context[f'msg_{auth}'] = {
                        'msg': msg_unreviewed % (auth_name, hadhits),
                        'type': 'unreviewed'}
            else:
                context[f'msg_{auth}'] = {
                    'msg': "",
                    'type': 'none'}


class DatasetLogView(LoginRequiredMixin, DetailView):
    """
    View to return the 'Log & Comments' tab for a specific dataset.
    """
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    model = Dataset
    template_name = 'datasets/ds_log.html'

    def get_success_url(self):
        """Redirect to the success URL after processing."""
        id_ = self.kwargs.get("id")

        # Log the messages for debugging purposes
        logger.debug(f'Messages: {messages.get_messages(self.request)}')
        return f'/datasets/{id_}/log'

    def get_object(self):
        """Retrieve the dataset object based on the provided ID."""
        id_ = self.kwargs.get("id")
        return get_object_or_404(Dataset, id=id_)

    def get_context_data(self, *args, **kwargs):
        """
        Populates the context with data needed to render the Log & Comments tab.

        Retrieves the dataset's log entries and comments, along with user permissions.
        """
        context = super().get_context_data(*args, **kwargs)

        # Use the get_object method to retrieve the dataset
        ds = self.get_object()

        # Prepare context variables
        context['ds'] = ds
        context['log'] = ds.log.filter(category='dataset').order_by('-timestamp')
        context['comments'] = Comment.objects.filter(place_id__dataset=ds).order_by('-created')
        context['beta_or_better'] = self.request.user.groups.filter(name__in=['beta', 'admins']).exists()

        # Log the retrieved context data for debugging
        logger.debug(f'Context data for DatasetLogView: {context}')

        return context


def dataset_citation(request, id):
    try:
        dataset = Dataset.objects.get(id=id)
        citation_data = json.loads(dataset.citation_csl)
        return JsonResponse(citation_data, safe=False)
    except Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset not found'}, status=404)
