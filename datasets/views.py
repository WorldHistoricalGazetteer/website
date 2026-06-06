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
from django.contrib.auth.decorators import login_required
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.gis.geos import GEOSGeometry
from django.core.cache import caches
from django.core.paginator import Paginator
from django.db.models import Count, Prefetch, Q
from django.forms import modelformset_factory
from django.http import (
    HttpResponseRedirect,
    HttpResponseNotFound,
    JsonResponse, HttpResponseForbidden
)
from django.shortcuts import redirect, render, get_object_or_404
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
from .models import DatasetUser, DatasetFile
from .services import _get_task_details, _get_hit_counts, _filter_unreviewed_places, _get_review_page_and_field, \
    _get_place_and_hits, _build_dataset_details, _extract_passes, _get_country_names, _build_feature_collection, \
    _process_matching_decisions
from .tasks import *
from .utils import *

es = settings.ES_CONN
User = get_user_model()

logger = logging.getLogger(__name__)

# Known MIME types for supported file formats
MIME_TYPE_MAPPING = {
    'application/json': 'json',
    'text/csv': 'csv',
    'text/tab-separated-values': 'tsv',
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
    'application/vnd.oasis.opendocument.spreadsheet': 'ods'
}


# ============================================================================
# HELPER CLASSES AND MIXINS
# ============================================================================

class DatasetContextMixin:
    """
    Mixin to provide common dataset context building logic.
    Reduces duplication across multiple views.
    """

    def get_base_dataset_context(self, ds, user=None):
        """
        Build common context data for dataset views.

        Args:
            ds: Dataset object
            user: User object (optional)

        Returns:
            dict: Common context data
        """
        if user is None:
            user = getattr(self, 'request', None) and self.request.user

        context = {
            'ds': ds,
            'collaborators': ds.collaborators.all(),
            'owners': ds.owners,
            'beta_or_better': self._is_beta_or_better(user),
        }

        if user and not user.is_anonymous:
            context.update({
                'is_owner': ds.owners.filter(id=user.id).exists(),
                'is_collaborator': ds.collaborators.filter(id=user.id).exists(),
                'is_admin': self._is_admin(user),
                'editorial': self._is_editorial(user),
            })

        return context

    def _is_admin(self, user):
        """Check if user is admin."""
        return user.groups.filter(name__in=['whg_admins']).exists()

    def _is_editorial(self, user):
        """Check if user is editorial."""
        return user.groups.filter(name__in=['editorial']).exists()

    def _is_beta_or_better(self, user):
        """Check if user has beta or admin access."""
        if not user or user.is_anonymous:
            return False
        return user.groups.filter(name__in=['beta', 'admins', 'whg_admins']).exists()


class FileHandlingMixin:
    """
    Mixin for common file handling operations.
    Consolidates file upload, validation, and cleanup logic.
    """

    def save_file_temporarily(self, uploaded_file):
        """
        Save uploaded file to temporary location.

        Args:
            uploaded_file: Django UploadedFile object

        Returns:
            str: Path to temporary file
        """
        original_file_name = uploaded_file.name
        _, file_extension = os.path.splitext(original_file_name)

        with tempfile.NamedTemporaryFile(suffix=file_extension, delete=False) as temp_file:
            temp_file_path = temp_file.name
            try:
                if hasattr(uploaded_file, 'temporary_file_path'):
                    shutil.copy(uploaded_file.temporary_file_path(), temp_file_path)
                else:
                    for chunk in uploaded_file.chunks():
                        temp_file.write(chunk)
            except Exception as e:
                self.cleanup_uploaded_file(temp_file_path)
                raise e

        return temp_file_path

    def cleanup_uploaded_file(self, filepath):
        """Remove temporary file if it exists."""
        if filepath and os.path.exists(filepath):
            try:
                os.remove(filepath)
            except OSError as e:
                logger.warning(f"Failed to remove temporary file {filepath}: {e}")

    def generate_unique_label(self, base_text, user=None, min_length=3, max_length=20):
        """
        Generate a unique label for a dataset.

        Args:
            base_text: Base text for the label
            user: User object (optional)
            min_length: Minimum label length
            max_length: Maximum label length

        Returns:
            str: Unique label
        """

        def adjust_label_length(label):
            if len(label) > max_length:
                return label[-max_length:]
            return label

        base_label = slugify(base_text)
        label = adjust_label_length(base_label)

        if Dataset.objects.filter(label=label).exists():
            if user:
                label = f"{label}_{slugify(user.surname)}"
                label = adjust_label_length(label)

            count = 1
            while Dataset.objects.filter(label=label).exists():
                label = f"{base_label}_v{count}"
                label = adjust_label_length(label)
                count += 1

        return label


class ReviewStatusMixin:
    """
    Mixin to calculate review status for different authority types.
    Consolidates duplicate status calculation logic.
    """

    def get_review_status(self, place_qs, field_name):
        """
        Calculate review status for a given review field.

        Args:
            place_qs: QuerySet of Place objects
            field_name: Name of the review field (e.g., 'review_wd', 'review_whg')

        Returns:
            dict: Status counts
        """
        return {
            "rows": place_qs.count(),
            "got_hits": place_qs.exclude(**{f"{field_name}__isnull": True}).count(),
            "reviewed": place_qs.filter(**{field_name: 1}).count(),
            "deferred": place_qs.filter(**{field_name: 2}).count(),
            "remain": place_qs.filter(**{f"{field_name}__in": [0, 2]}).count(),
        }

    def get_all_review_statuses(self, ds):
        """
        Get review statuses for all authority types.

        Args:
            ds: Dataset object

        Returns:
            dict: All review statuses
        """
        place_qs = ds.places.all()
        return {
            'wdgn_status': self.get_review_status(place_qs, 'review_wd'),
            'idx_status': self.get_review_status(place_qs, 'review_whg'),
        }


class AugmentationCountsMixin:
    """
    Mixin to calculate augmentation counts (links, geoms, names added via tasks).
    """

    def get_augmentation_counts(self, place_ids):
        """
        Calculate counts of added names, links, and geoms.

        Args:
            place_ids: List of place IDs

        Returns:
            dict: Counts of augmentations
        """

        def count_by_task(model):
            return model.objects.filter(place_id__in=place_ids).aggregate(
                base=Count('id', filter=Q(task_id=None)),
                added=Count('id', filter=~Q(task_id=None))
            )

        name_counts = count_by_task(PlaceName)
        link_counts = count_by_task(PlaceLink)
        geom_counts = count_by_task(PlaceGeom)

        return {
            'num_names': name_counts['base'],
            'names_added': name_counts['added'],
            'num_links': link_counts['base'],
            'links_added': link_counts['added'],
            'num_geoms': geom_counts['base'],
            'geoms_added': geom_counts['added'],
        }


class TaskPassCounterMixin:
    """
    Mixin to count hits by pass for reconciliation tasks.
    """

    def count_pass_hits(self, hits_qs):
        """
        Count hits by pass number.

        Args:
            hits_qs: QuerySet of Hit objects

        Returns:
            dict: Counts by pass
        """
        values = hits_qs.values_list('query_pass', 'place_id').distinct()
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


# ============================================================================
# VALIDATION VIEW
# ============================================================================

class DatasetValidate(CreateView, FileHandlingMixin):
    """Validate and upload dataset files."""

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
                'label': form.cleaned_data.get('label') or self.generate_unique_label(
                    os.path.splitext(uploaded_file.name)[0], user
                ),
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
                response_data = json.loads(validation_response.content.decode('utf-8'))
                status = response_data.get("status")

                if status == "in_progress":
                    context = self.get_context_data(form=form)
                    context['task_id'] = response_data.get("task_id")
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
            return self.handle_invalid_form(
                form, f"Sorry, there was an error while processing the uploaded file: {str(e)}"
            )

    def handle_invalid_form(self, form, message):
        """Helper to handle invalid forms with error message."""
        messages.error(self.request, message)
        return self.form_invalid(form)


# ============================================================================
# LIST VIEWS
# ============================================================================

class VolunteeringView(ListView):
    """Display datasets requesting volunteers."""

    template_name = 'datasets/volunteering.html'
    model = Dataset

    def get_queryset(self):
        return Dataset.objects.filter(
            volunteers_text__isnull=False
        ).order_by('-create_date')

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context['dataset_list'] = context.pop('object_list')
        return context


class PublicListsView(ListView, DatasetContextMixin):
    """List public datasets and collections."""

    redirect_field_name = 'redirect_to'
    context_object_name = 'dataset_list'
    template_name = 'datasets/public_list.html'
    model = Dataset

    def get_queryset(self):
        return Dataset.objects.filter(public=True).order_by('core', 'title')

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)

        context['coll_list'] = Collection.objects.filter(
            status='published'
        ).order_by('create_date')

        context['viewable'] = [
            'uploaded', 'inserted', 'reconciling', 'review_hits',
            'reviewed', 'review_whg', 'indexed'
        ]
        context['beta_or_better'] = self._is_beta_or_better(self.request.user)

        return context


# ============================================================================
# DATASET DETAIL VIEWS (Using Mixins)
# ============================================================================


class DatasetGalleryView(ListView):
    redirect_field_name = 'redirect_to'

    context_object_name = 'datasets'
    template_name = 'datasets/ds_gallery.html'
    model = Dataset

    def get_queryset(self):
        qs = super().get_queryset()
        return qs.filter(public=True).order_by('title')

    def get_context_data(self, *args, **kwargs):
        context = super(DatasetGalleryView, self).get_context_data(*args, **kwargs)

        context['active_tab'] = self.kwargs.get('gallery_type',
                                                'datasets')  # datasets|collections: default to 'datasets' if not provided

        context['num_datasets'] = Dataset.objects.filter(public=True).count()
        context['num_collections'] = Collection.objects.filter(public=True).count()

        context['dropdown_data'] = get_regions_countries()

        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'admins']).exists() else False
        return context


class DatasetStatusView(LoginRequiredMixin, UpdateView, DatasetContextMixin,
                        ReviewStatusMixin, AugmentationCountsMixin, TaskPassCounterMixin):
    """Dataset owner summary/status page."""

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    form_class = DatasetDetailModelForm
    template_name = 'datasets/ds_status.html'

    def get_object(self):
        return get_object_or_404(Dataset, id=self.kwargs.get("id"))

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        ds = self.get_object()

        # Use mixins for common operations
        context.update(self.get_base_dataset_context(ds, self.request.user))
        context.update(self.get_all_review_statuses(ds))

        place_ids = list(ds.places.values_list('id', flat=True))
        context.update(self.get_augmentation_counts(place_ids))
        context['numrows'] = len(place_ids)

        # Tasks
        ds_tasks = ds.tasks.exclude(status__in=['FAILURE', 'ARCHIVED'])
        context['tasks'] = ds_tasks

        task_wdgn = ds_tasks.filter(task_name__startswith='align_wd').order_by('-date_done').first()
        task_idx = ds_tasks.filter(task_name__startswith='align_idx').order_by('-date_done').first()

        context['task_wdgn'] = task_wdgn
        context['task_idx'] = task_idx

        # Pass counts
        if task_wdgn:
            # Get non-deferred places only for pass counting
            non_deferred_place_ids = ds.places.exclude(review_wd=2).values_list('id', flat=True)
            hits_wdgn = Hit.objects.filter(
                task_id=task_wdgn.task_id,
                reviewed=False,
                place_id__in=non_deferred_place_ids
            )
            context['wdgn_passes'] = self.count_pass_hits(hits_wdgn)
        else:
            context['wdgn_passes'] = {}

        if task_idx:
            # Get non-deferred places only for pass counting
            non_deferred_place_ids = ds.places.exclude(review_whg=2).values_list('id', flat=True)
            hits_idx = Hit.objects.filter(
                task_id=task_idx.task_id,
                reviewed=False,
                place_id__in=non_deferred_place_ids
            )
            context['idx_passes'] = self.count_pass_hits(hits_idx)
        else:
            context['idx_passes'] = {}

        # Vis parameters
        context['vis_parameters_dict'] = ds.vis_parameters or {
            key: {'tabulate': False, 'temporal_control': 'none', 'trail': False}
            for key in ('seq', 'min', 'max')
        }

        context['updates'] = {}

        return context


class DatasetMetadataView(LoginRequiredMixin, UpdateView, DatasetContextMixin, AugmentationCountsMixin):
    """Dataset owner metadata page."""

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    form_class = DatasetDetailModelForm
    template_name = 'datasets/ds_metadata.html'

    def form_valid(self, form):
        data = form.cleaned_data
        ds = self.get_object()

        if data["file"] is None:
            ds.title = data['title']
            ds.description = data['description']
            ds.uri_base = data['uri_base']
            ds.save()

        return super().form_valid(form)

    def form_invalid(self, form):
        logger.error(f'form invalid: {form.errors.as_data()}')
        return super().form_invalid(form)

    def get_object(self):
        return get_object_or_404(Dataset, id=self.kwargs.get("id"))

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        ds = self.get_object()

        # Use mixins
        context.update(self.get_base_dataset_context(ds, self.request.user))

        place_ids = list(ds.places.values_list('id', flat=True))
        context.update(self.get_augmentation_counts(place_ids))

        # Files
        file = ds.file
        context['files'] = [
                               f for f in ds.files.order_by('-id')
                               if os.path.exists(f.file.path)
                           ] or None

        if file and hasattr(file, 'file') and os.path.exists(file.file.path):
            context['current_file'] = file
            context['format'] = file.format
            context['numrows'] = file.numrows
            context['filesize'] = round(file.file.size / 1000000, 1)

        context['updates'] = {}

        # CRediT contributors (Phase 2b)
        from django.contrib.contenttypes.models import ContentType
        from persons.models import Contribution, CreditRole, ContributionDegree
        ct = ContentType.objects.get_for_model(Dataset)
        context['contributions'] = (Contribution.objects
                                     .filter(content_type=ct, object_id=str(ds.id))
                                     .select_related('person').order_by('order'))
        context['credit_roles'] = CreditRole.choices
        context['contribution_degrees'] = ContributionDegree.choices
        context['can_edit_credit'] = (self.request.user.is_staff
                                      or self.request.user in ds.owners)
        context['contrib_base'] = f"/datasets/{ds.id}/contributions"

        return context


class DatasetBrowseView(LoginRequiredMixin, DetailView, DatasetContextMixin):
    """Dataset owner's browse table."""

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    model = Dataset
    template_name = 'datasets/ds_browse.html'

    def get_object(self):
        return get_object_or_404(Dataset, id=self.kwargs.get("id"))

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        ds = self.get_object()

        context.update(self.get_base_dataset_context(ds, self.request.user))

        ds_tasks = [t for t in ds.recon_status]
        context['updates'] = {}
        context['num_places'] = ds.num_places
        context['tgntask'] = 'tgn' in ds_tasks
        context['whgtask'] = len(set(['whg', 'idx']) & set(ds_tasks)) > 0
        context['wdtask'] = len(set(['wd', 'wdlocal']) & set(ds_tasks)) > 0

        return context


class DatasetPlacesView(DetailView, DatasetContextMixin):
    """Public dataset browse table."""

    login_url = "/accounts/login/"
    redirect_field_name = "redirect_to"
    model = Dataset
    template_name = "datasets/ds_places.html"
    unavailable_template_name = "main/503.html"

    def get_object(self):
        return get_object_or_404(Dataset, id=self.kwargs.get("id"))

    def get(self, request, *args, **kwargs):
        ds = self.get_object()

        if ds.num_places > settings.DATASETS_PLACES_LIMIT:
            message = "Sorry, this dataset cannot be viewed on this page because it has too many places."
            return render(request, self.unavailable_template_name, {"message": message}, status=503)

        return super().get(request, *args, **kwargs)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        ds = self.get_object()
        me = self.request.user

        context.update({
            "URL_FRONT": settings.URL_FRONT,
            "ds": ds,
            "is_admin": self._is_admin(me),
            "loggedin": "true" if not me.is_anonymous else "false",
            "my_collections": Collection.objects.filter(
                collection_class="place",
                **({} if self._is_admin(me) else {"owner": me})
            ) if not me.is_anonymous else None,
        })

        return context


class DatasetReconcileView(LoginRequiredMixin, DetailView, ReviewStatusMixin):
    """Dataset owner 'Linking' tab."""

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    model = Dataset
    template_name = 'datasets/ds_reconcile.html'

    def get_object(self):
        return get_object_or_404(Dataset, id=self.kwargs.get("id"))

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        ds = self.object

        # Use mixin for review status
        context.update(self.get_all_review_statuses(ds))

        context["is_admin"] = self.request.user.groups.filter(name="whg_admins").exists()
        context["beta_or_better"] = self.request.user.groups.filter(
            name__in=["beta", "whg_admins"]
        ).exists()
        context["ds"] = ds

        # Process tasks
        tasks = ds.tasks.filter(status="SUCCESS")
        for task in tasks:
            task.result = json.loads(task.result) if task.result else {}
            if 'summary' in task.result:
                task.result = task.result['summary']
                task.result['total_hits'] = Hit.objects.filter(
                    place__dataset=ds, task_id=task.task_id
                ).count()
                task.result['elapsed'] = task.result.get('elapsed_min')
                task.result.pop('elapsed_min', None)
            task.result = json.dumps(task.result, indent=2, ensure_ascii=False)

        context["tasks"] = tasks
        return context


class DatasetCollabView(LoginRequiredMixin, DetailView, DatasetContextMixin):
    """Dataset owner 'Collaborators' tab."""

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    model = DatasetUser
    template_name = 'datasets/ds_collab.html'

    def get_object(self):
        return get_object_or_404(Dataset, id=self.kwargs.get("id"))

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        ds = self.get_object()

        context.update(self.get_base_dataset_context(ds, self.request.user))
        context['collabs'] = ds.collabs.all()

        return context


class DatasetAddTaskView(LoginRequiredMixin, DetailView):
    """Add reconciliation task page."""

    logger = logging.getLogger('reconciliation')
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    model = Dataset
    template_name = 'datasets/ds_addtask.html'

    def get_object(self):
        dataset = get_object_or_404(Dataset, id=self.kwargs.get("id"))
        self.logger.debug('Retrieved dataset object: %s', dataset)
        return dataset

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        ds = self.get_object()
        me = self.request.user
        is_admin = me.groups.filter(name__in=['whg_admins']).exists()

        # User and predefined areas
        area_types = ['ccodes', 'copied', 'drawn']
        userareas = Area.objects.filter(
            type__in=area_types,
            **({} if is_admin else {'owner_id': me.id})
        ).values('id', 'title').order_by('-created')

        predefined = Area.objects.filter(type='predefined').values('id', 'title')

        # Task processing
        gothits = {}
        for t in ds.tasks.filter(status='SUCCESS', task_name__startswith='align_'):
            try:
                result_data = json.loads(t.result)
                gothits[t.task_id] = int(result_data.get('got_hits', 0))
            except json.JSONDecodeError:
                gothits[t.task_id] = 0

        # Status messages
        self._prepare_status_messages(context, ds, gothits)

        # Build context
        context.update({
            'region_list': predefined,
            'area_list': userareas,
            'userarea': self.request.GET.get('userarea'),
            'ds': ds,
            'numrows': ds.places.count(),
            'collaborators': ds.collabs.all(),
            'owners': ds.owners,
            'remain_to_review': {
                k[6:]: v[0]['total']
                for k, v in ds.taskstats.items() if v
            },
            'missing_geoms': ds.missing_geoms,
            'is_admin': is_admin,
        })

        return context

    def _prepare_status_messages(self, context, ds, gothits):
        """Prepare status messages based on task statistics."""
        msg_templates = {
            'unreviewed': (
                "There is a <span class='strong'>%s</span> task in progress, "
                "and all %s records that got hits remain unreviewed. "
                "<span class='text-danger strong'>Starting this new task "
                "will delete the existing one</span>, with no impact on your dataset."
            ),
            'inprogress': (
                "<p class='mb-1'>There is a <span class='strong'>%s</span> task in progress, "
                "and %s of the %s records that had hits have been reviewed. "
                "<span class='text-danger strong'>Starting this new task "
                "will archive the existing task and submit only unreviewed records.</span> "
                "If you proceed, you can keep or delete prior match results (links and/or geometry):</p>"
            ),
            'done': (
                "All records have been submitted for reconciliation to %s and reviewed. "
                "To begin the step of accessioning to the WHG index, please "
                "<a href='%s'>contact our editorial team.</a>"
            )
        }

        for task_type, stats in ds.taskstats.items():
            auth = task_type[6:]
            auth_name = 'Wikidata+GeoNames' if auth == 'wdlocal' else 'WHG index'

            if stats:
                tid = stats[0]['tid']
                remaining = stats[0]['total']
                hadhits = gothits.get(tid, 0)
                reviewed = hadhits - remaining

                if remaining == 0 and ds.ds_status != 'updated':
                    msg_type = 'done'
                    msg = msg_templates['done'] % (auth_name, "/contact")
                elif remaining < hadhits and ds.ds_status != 'updated':
                    msg_type = 'inprogress'
                    msg = msg_templates['inprogress'] % (auth_name, reviewed, hadhits)
                else:
                    msg_type = 'unreviewed'
                    msg = msg_templates['unreviewed'] % (auth_name, hadhits)

                context[f'msg_{auth}'] = {'msg': msg, 'type': msg_type}
            else:
                context[f'msg_{auth}'] = {'msg': "", 'type': 'none'}


class DatasetLogView(LoginRequiredMixin, DetailView, DatasetContextMixin):
    """Dataset 'Log & Comments' tab."""

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    model = Dataset
    template_name = 'datasets/ds_log.html'

    def get_object(self):
        return get_object_or_404(Dataset, id=self.kwargs.get("id"))

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        ds = self.get_object()

        context.update(self.get_base_dataset_context(ds, self.request.user))
        context['log'] = ds.log.filter(category='dataset').order_by('-timestamp')
        context['comments'] = Comment.objects.filter(place_id__dataset=ds).order_by('-created')

        return context


class DatasetPublicView(DetailView):
    """Public dataset metadata page."""

    template_name = 'datasets/ds_meta.html'
    model = Dataset

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        ds = get_object_or_404(Dataset, id=self.kwargs['pk'])
        file = ds.file
        placeset = ds.places.all()

        if file:
            context['current_file'] = file
            context['format'] = file.format
            context['numrows'] = file.numrows
            context['filesize'] = round(file.file.size / 1000000, 1)
            context['links_added'] = PlaceLink.objects.filter(
                place_id__in=placeset, task_id__contains='-'
            ).count()
            context['geoms_added'] = PlaceGeom.objects.filter(
                place_id__in=placeset, task_id__contains='-'
            ).count()

        return context


# ============================================================================
# CREATE/UPDATE/DELETE VIEWS
# ============================================================================

class DatasetCreateEmptyView(LoginRequiredMixin, CreateView, FileHandlingMixin):
    """Create empty dataset (for remote data typically)."""

    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'
    form_class = DatasetCreateEmptyModelForm
    template_name = 'datasets/dataset_create_empty.html'
    success_message = 'empty dataset created'

    def form_invalid(self, form):
        logger.error(f'form invalid: {form.errors.as_data()}')
        return self.render_to_response(context={'form': form})

    def form_valid(self, form):
        user = self.request.user
        dsobj = form.save(commit=False)

        # Set defaults
        dsobj.ds_status = 'format_ok'
        dsobj.numrows = 0
        dsobj.numlinked = 0
        dsobj.total_links = 0

        if not form.cleaned_data['uri_base']:
            dsobj.uri_base = 'https://whgazetteer.org/api/db/?id='

        if not dsobj.label:
            dsobj.label = self.generate_unique_label('dataset', user)
        else:
            dsobj.label = dsobj.label.replace(' ', '_')

        try:
            dsobj.save()
        except Exception as e:
            logger.error(f"Failed to save dataset: {e}")
            return self.render_to_response(context={'form': form})

        # Create user directory if needed
        userdir = Path(f'media/user_{user.id}')
        userdir.mkdir(exist_ok=True)

        # Log creation
        Log.objects.create(
            category='dataset',
            logtype='ds_create_empty',
            subtype=form.cleaned_data['datatype'],
            dataset_id=dsobj.id,
            user_id=user.id
        )

        # Create dummy file record
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

        return redirect(f'/datasets/{dsobj.id}/summary')


class DatasetDeleteView(DeleteView):
    """Delete dataset with cleanup."""

    template_name = 'datasets/dataset_delete.html'
    model = Dataset

    def delete(self, request, *args, **kwargs):
        self.object = self.get_object()

        # Custom deletion logic
        dataset_file_delete(self.object)

        if self.object.ds_status == 'indexed':
            pids = list(self.object.placeids)
            removePlacesFromIndex(es, 'whg', pids)

        self.object.delete()
        return HttpResponseRedirect(self.get_success_url())

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['owners'] = self.get_object().owners
        return context

    def get_success_url(self):
        return django_reverse('dashboard')


# ============================================================================
# FUNCTIONAL VIEWS (Reconciliation, Review, Updates)
# ============================================================================

def review(request, dsid, tid, passnum):
    """
    Handle reconciliation review for Wikidata or WHG index.
    GET: Returns review page
    POST: Processes user matching decisions
    """
    pid = request.GET.get("pid")
    ds = get_object_or_404(Dataset, id=dsid)
    task, auth, authname, kwargs, test = _get_task_details(tid)
    record_list, current_passnum = _filter_unreviewed_places(ds, tid, passnum, auth)
    review_page, review_field = _get_review_page_and_field(auth)

    # ✅ Use Redis cache for hit counts
    review_cache = caches['property_cache']
    cache_key = f"hit_counts:{tid}"
    hit_counts = review_cache.get(cache_key)

    if not hit_counts:
        hit_counts = _get_hit_counts(tid)
        review_cache.set(cache_key, hit_counts, timeout=300)  # 5 minutes

    cnt_pass_def, cnt_pass0, cnt_pass1, cnt_pass2, cnt_pass3 = hit_counts

    is_reconciliation = auth in ["wd", "wdlocal"]

    # Base context for no hits
    nohit_context = {
        "nohits": True,
        "ds_id": dsid,
        "ds_label": ds.label,
        "dataset_details": {},
        "task_id": tid,
        "authority": task.task_name[6:8] if is_reconciliation else task.task_name[6:],
        "deferred": current_passnum == "def",
        "passnum": current_passnum,
        "test": test,
        "aug_geoms": kwargs["aug_geoms"],
        "count_pass0": cnt_pass0,
        "count_pass1": cnt_pass1,
        "count_pass2": cnt_pass2,
        "count_pass3": cnt_pass3,
    }

    if not record_list.exists():
        return render(request, f"datasets/{review_page}", context=nohit_context)

    # Determine page
    if pid:
        try:
            page = record_list.filter(id__lt=pid).count() + 1
        except Exception:
            page = 1
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

    # Prefetch related data
    place = Place.objects.prefetch_related(
        Prefetch('geoms', to_attr='prefetched_geoms'),
        Prefetch('names', to_attr='prefetched_names'),
        Prefetch('links', to_attr='prefetched_links')
    ).get(id=place.id)

    _, raw_hits = _get_place_and_hits(place.id, tid, auth, current_passnum)
    dataset_details = _build_dataset_details(raw_hits)
    passes = _extract_passes(raw_hits, auth)
    countries = _get_country_names(place)
    feature_collection = _build_feature_collection(records, raw_hits, is_reconciliation)

    # Formset
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
        "page": page if request.method == "GET" else str(int(page) - 1),
        "aug_geoms": kwargs["aug_geoms"],
        "count_pass0": cnt_pass0,
        "count_pass1": cnt_pass1,
        "count_pass2": cnt_pass2,
        "count_pass3": cnt_pass3,
        "deferred": passnum == "def",
        "test": test,
        "formset": formset,
        "feature_collection": feature_collection,
        "already": False,
        "mbtoken": False,
        "nohits": False,
    }

    if request.method == "POST":
        place_post = get_object_or_404(Place, pk=place.id)
        review_status = getattr(place_post, review_field)

        if review_status == 1:
            context["already"] = True
            messages.success(
                request, f"Last record ({place_post.title}) reviewed by another"
            )
            return redirect(f"/datasets/{dsid}/review/{tid}/{passnum}")
        elif formset.is_valid():
            _process_matching_decisions(
                request, place_post, formset, task, auth, authname, kwargs, review_field, ds
            )
            return redirect(f"/datasets/{dsid}/review/{tid}/{current_passnum}?page={int(page)}")
        else:
            logger.debug(f'formset invalid. errors: {formset.errors}')

    return render(request, f"datasets/{review_page}", context=context)


def ds_recon(request, pk):
    """
    Initiate Celery reconciliation task.
    Runs align_[wdlocal|wd|idx] against Elasticsearch indexes.
    """
    ds = get_object_or_404(Dataset, id=pk)
    user = request.user

    if request.method != 'POST':
        return redirect(f'/datasets/{ds.id}/reconcile')

    # Extract parameters
    test = 'on' if 'test' in request.POST else 'off'
    auth = request.POST['recon']
    scope_geom = request.POST.get('scope_geom', False)
    aug_geoms = request.POST.get('accept_geoms', False)
    aug_names = request.POST.get('accept_names', False)
    geonames = request.POST.get('no_geonames', False)
    language = request.LANGUAGE_CODE

    # Validation
    if auth == 'idx' and not ds.public and test == 'off':
        messages.error(request, "Dataset must be public before indexing!")
        return redirect(f'/datasets/{ds.id}/addtask')

    # Determine scope
    previous = ds.tasks.filter(task_name=f'align_{auth}', status='SUCCESS')
    prior = request.POST.get('prior', 'na')

    if previous.exists():
        if auth == 'idx':
            scope = "unindexed"
        else:
            tid = previous.first().task_id
            task_archive(tid, prior)
            scope = 'unreviewed'
    else:
        scope = 'all'

    # Bounds
    region = request.POST.get('region', '0') or '0'
    userarea = request.POST.get('userarea', '0') or '0'

    bounds = {
        "type": ["region" if region != "0" else "userarea"],
        "id": [region if region != "0" else userarea]
    }

    # Initiate task
    func = eval(f'align_{auth}')

    try:
        result = func.delay(
            ds.id,
            ds=ds.id,
            dslabel=ds.label,
            owner=ds.owner.id,
            user=user.id,
            bounds=bounds,
            aug_geoms=aug_geoms,
            aug_names=aug_names,
            scope=scope,
            scope_geom=scope_geom,
            geonames=geonames,
            lang=language,
            test=test,
        )
        messages.info(
            request,
            "<span class='text-danger'>Your reconciliation task is under way.</span><br/>"
            "When complete, you will receive an email and if successful, results will appear below "
            "(you may have to refresh screen)."
        )
        return redirect(f'/datasets/{ds.id}/reconcile')
    except Exception as e:
        logger.exception(f"Failed to start align_{auth} for dataset {ds.id}")
        messages.error(
            request,
            f"Sorry! Reconciliation services appear to be down. "
            f"The system administrator has been notified."
        )
        return redirect(f'/datasets/{ds.id}/reconcile')


def task_delete(request, tid, scope="task"):
    """Initiate background deletion of reconciliation task results."""
    try:
        tr = TaskResult.objects.get(task_id=tid)
    except TaskResult.DoesNotExist:
        return JsonResponse({
            'status': 'error',
            'message': f'Task with ID {tid} does not exist'
        }, status=404)

    dsid = int(tr.task_args[2:-3])

    if scope == 'task':
        tr.status = 'ARCHIVED'
        tr.save()

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
    Archive reconciliation task.
    - Delete hits
    - If prior='zap': delete geoms and links added by review
    - Reset Place.review_{auth} to null
    - Set task status to 'ARCHIVED'
    """
    hits = Hit.objects.filter(task_id=tid)
    tr = get_object_or_404(TaskResult, task_id=tid)
    dsid = tr.task_args[1:-1]
    auth = tr.task_name[6:]
    places = Place.objects.filter(id__in=[h.place_id for h in hits])

    # Reset review status
    for p in places:
        p.defer_comments.delete()
        if auth in ['whg', 'idx'] and p.review_whg != 1:
            p.review_whg = None
        elif auth.startswith('wd') and p.review_wd != 1:
            p.review_wd = None
        elif auth == 'tgn' and p.review_tgn != 1:
            p.review_tgn = None
        p.save()

    hits.delete()

    if prior == 'na':
        tr.delete()
    else:
        tr.status = 'ARCHIVED'
        tr.save()
        if prior == 'zap':
            PlaceLink.objects.filter(task_id=tid).delete()
            PlaceGeom.objects.filter(task_id=tid).delete()


def match_undo(request, ds, tid, pid):
    """
    Undo last review match action.
    - Delete any geoms or links created
    - Reset flags for hit.reviewed and place.review_xxx
    """
    PlaceGeom.objects.filter(task_id=tid, place_id=pid).delete()
    PlaceLink.objects.filter(task_id=tid, place_id=pid).delete()

    tasktype = TaskResult.objects.get(task_id=tid).task_name[6:]
    place = Place.objects.get(pk=pid)
    place.defer_comments.delete()

    if tasktype.startswith('wd'):
        place.review_wd = 0
    elif tasktype == 'tgn':
        place.review_tgn = 0
    else:
        place.review_whg = 0
    place.save()

    Hit.objects.filter(task_id=tid, place_id=pid).update(reviewed=False)
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


@login_required
def collab_add(request, dsid, v):
    """Add or update a collaborator on a dataset using their username."""

    if request.method != "POST":
        return HttpResponseForbidden("POST required")

    username = request.POST.get("username")
    role = request.POST.get("role", "member")

    if not username:
        messages.info(request, "Username is required.")
        return HttpResponseRedirect(request.META.get("HTTP_REFERER"))

    dataset = get_object_or_404(Dataset, id=dsid)

    # Permission check
    if not (request.user.is_superuser or request.user in dataset.owners.all()):
        return HttpResponseForbidden("Not allowed")

    try:
        user = User.objects.get(username=username)
    except User.DoesNotExist:
        messages.info(request, f"No user with username '{username}'.")
        return HttpResponseRedirect(request.META.get("HTTP_REFERER"))

    # Check if user is already a collaborator
    collab, created = DatasetUser.objects.get_or_create(
        dataset_id=dataset,
        user_id=user,
        defaults={"role": role},
    )

    if not created:
        # Only update if the role has changed
        if collab.role != role:
            collab.role = role
            collab.save(update_fields=["role"])
            messages.success(request, f"{user.username} role updated to {role}.")
        else:
            messages.info(request, f"{user.username} is already a {role}.")
    else:
        messages.success(request, f"{user.username} added as {role}.")

    # Redirect (v switch preserved)
    return redirect(f"/datasets/{dsid}/collab") if v == "1" else HttpResponseRedirect(request.META.get("HTTP_REFERER"))


def collab_delete(request, uid, dsid, v):
    """Remove collaborator from dataset."""
    get_object_or_404(DatasetUser, user_id_id=uid, dataset_id_id=dsid).delete()
    return redirect(f'/datasets/{dsid}/collab') if v == '1' else HttpResponseRedirect(
        request.META.get('HTTP_REFERER')
    )


def dataset_file_delete(ds):
    """Delete all uploaded files for a dataset."""
    for f in ds.files.all():
        ffn = f'media/{f.file.name}'
        if os.path.exists(ffn) and f.file.name != 'dummy_file.txt':
            try:
                os.remove(ffn)
            except OSError as e:
                logger.warning(f'Failed to remove {ffn}: {e}')
        else:
            logger.debug(f'file {ffn} not found')


def ds_list(request, label):
    """Fetch places in specified dataset (utility for place collections)."""
    qs = Place.objects.filter(dataset=label)
    geoms = [
        {
            "type": "Feature",
            "properties": {"src_id": p.src_id, "name": p.title},
            "geometry": p.geoms.first().jsonb
        }
        for p in qs.all()
    ]
    return JsonResponse(geoms, safe=False)


@require_POST
def update_vis_parameters(request, *args, **kwargs):
    """Update visualization parameters on ds_status page."""
    try:
        ds_id = request.POST.get('ds_id')
        checked = request.POST.get('checked') == 'true'

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

        dataset = get_object_or_404(Dataset, pk=ds_id)
        dataset.vis_parameters = vis_parameters
        dataset.save()

        return JsonResponse({
            'message': 'Visualisation parameters updated successfully',
            'vis_parameters': json.dumps(vis_parameters)
        })
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
def update_volunteers_text(request):
    """Update volunteers request text on ds_status page."""
    if request.method == 'POST':
        dataset_id = request.POST.get('dataset_id')
        volunteers_text = request.POST.get('volunteers_text')
        reset = request.POST.get('reset', 'false') == 'true'

        dataset = get_object_or_404(Dataset, id=dataset_id)
        dataset.volunteers_text = None if reset else volunteers_text
        dataset.save()

        return JsonResponse({'status': 'success'})


def dataset_citation(request, id):
    """Return dataset citation in CSL format."""
    try:
        dataset = get_object_or_404(Dataset, id=id)
        citation_data = json.loads(dataset.citation_csl)
        return JsonResponse(citation_data, safe=False)
    except Dataset.DoesNotExist:
        return JsonResponse({'error': 'Dataset not found'}, status=404)


# ============================================================================
# UPDATE OPERATIONS (TSV)
# ============================================================================

def update_rels_tsv(pobj, row):
    """
    Update objects related to a Place from TSV row.
    Creates new child objects: names, types, whens, related, descriptions.
    For geoms and links, only adds if not already present.
    """
    header = list(row.keys())
    src_id = row['id']

    # Title processing
    title = row['title']
    title = re.sub('^\(.*?\)', '', title).strip()
    title_source = row['title_source']
    title_uri = row.get('title_uri', '')

    # Parse lists from row
    variants = [x.strip() for x in row['variants'].split(';')] \
        if 'variants' in header and row['variants'] not in ['', 'None', None] else []

    types = [x.strip() for x in row['types'].split(';')] \
        if 'types' in header and str(row['types']) not in ['', 'None', None] else []

    aat_types = [x.strip() for x in row['aat_types'].split(';')] \
        if 'aat_types' in header and str(row['aat_types']) not in ['', 'None', None] else []

    parent_name = row.get('parent_name', '')
    parent_id = row.get('parent_id', '')

    coords = makeCoords(row['lon'], row['lat']) \
        if 'lon' in header and 'lat' in header and row['lon'] else []

    try:
        matches = [x.strip() for x in row['matches'].split(';')] \
            if 'matches' in header and row['matches'] else []
    except Exception as e:
        logger.exception(f'error on matches: {row["matches"]}')
        matches = []

    description = row.get('description', '')

    # Collect objects to bulk create
    objs = {
        "PlaceName": [],
        "PlaceType": [],
        "PlaceGeom": [],
        "PlaceWhen": [],
        "PlaceLink": [],
        "PlaceRelated": [],
        "PlaceDescription": []
    }

    # Title as PlaceName
    objs['PlaceName'].append(
        PlaceName(
            place=pobj,
            src_id=src_id,
            toponym=title,
            jsonb={"toponym": title, "citation": {"id": title_uri, "label": title_source}}
        )
    )

    # Variants as PlaceNames
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

    # PlaceTypes
    if types:
        fclass_list = []
        for i, t in enumerate(types):
            aatnum = f'aat:{aat_types[i]}' if len(aat_types) >= len(types) else None
            if aatnum and int(aatnum[4:]) in Type.objects.values_list('aat_id', flat=True):
                fc = get_object_or_404(Type, aat_id=int(aatnum[4:])).fclass
                fclass_list.append(fc)
            objs['PlaceType'].append(
                PlaceType(
                    place=pobj,
                    src_id=src_id,
                    jsonb={
                        "identifier": aatnum,
                        "sourceLabel": t,
                        "label": aat_lookup(int(aatnum[4:])) if aatnum != 'aat:' else ''
                    }
                )
            )
        pobj.fclasses = fclass_list
        pobj.save()

    # PlaceGeom
    if coords:
        geom = {
            "type": "Point",
            "coordinates": coords,
            "geowkt": f'POINT({coords[0]} {coords[1]})'
        }
    elif 'geowkt' in header and row['geowkt'] not in ['', None]:
        geom = parse_wkt(row['geowkt'])
    else:
        geom = None

    if geom:
        def trunc4(val):
            return round(val, 4)

        new_coords = list(map(trunc4, geom['coordinates']))

        if pobj.geoms.count() == 0:
            objs['PlaceGeom'].append(
                PlaceGeom(
                    place=pobj,
                    src_id=src_id,
                    jsonb=geom,
                    geom=GEOSGeometry(json.dumps(geom))
                )
            )
        else:
            try:
                for g in pobj.geoms.all():
                    if list(map(trunc4, g.jsonb['coordinates'])) != new_coords:
                        objs['PlaceGeom'].append(
                            PlaceGeom(
                                place=pobj,
                                src_id=src_id,
                                jsonb=geom,
                                geom=GEOSGeometry(json.dumps(geom))
                            )
                        )
            except Exception as e:
                logger.exception(f'failed on {pobj}')

    # PlaceLink
    if matches:
        exist_links = list(pobj.links.values_list('jsonb__identifier', flat=True))
        new_matches = set(matches) - set(exist_links)
        for m in new_matches:
            objs['PlaceLink'].append(
                PlaceLink(
                    place=pobj,
                    src_id=src_id,
                    jsonb={"type": "closeMatch", "identifier": m}
                )
            )

    # PlaceRelated
    if parent_name:
        objs['PlaceRelated'].append(
            PlaceRelated(
                place=pobj,
                src_id=src_id,
                jsonb={
                    "relationType": "gvp:broaderPartitive",
                    "relationTo": parent_id,
                    "label": parent_name
                }
            )
        )

    # PlaceWhen
    objs['PlaceWhen'].append(
        PlaceWhen(
            place=pobj,
            src_id=src_id,
            jsonb={
                "timespans": [{
                    "start": {"earliest": pobj.minmax[0]},
                    "end": {"latest": pobj.minmax[1]}
                }]
            }
        )
    )

    # PlaceDescription
    if description:
        objs['PlaceDescription'].append(
            PlaceDescription(
                place=pobj,
                src_id=src_id,
                jsonb={"@id": "", "value": description, "lang": ""}
            )
        )

    # Bulk create all objects
    PlaceName.objects.bulk_create(objs['PlaceName'], batch_size=10000)
    PlaceType.objects.bulk_create(objs['PlaceType'], batch_size=10000)
    PlaceGeom.objects.bulk_create(objs['PlaceGeom'], batch_size=10000)
    PlaceLink.objects.bulk_create(objs['PlaceLink'], batch_size=10000)
    PlaceRelated.objects.bulk_create(objs['PlaceRelated'], batch_size=10000)
    PlaceWhen.objects.bulk_create(objs['PlaceWhen'], batch_size=10000)
    PlaceDescription.objects.bulk_create(objs['PlaceDescription'], batch_size=10000)


def ds_update(request):
    """
    Perform updates to database and index based on ds_compare() results.
    Params: dsid, format, keepg, keepl, compare_data (json string)
    """
    if request.method != 'POST':
        return JsonResponse({'error': 'POST required'}, status=400)

    dsid = request.POST['dsid']
    ds = get_object_or_404(Dataset, id=dsid)
    file_format = request.POST['format']

    # Keep previous recon/review results?
    keepg = request.POST['keepg']
    keepl = request.POST['keepl']

    # Comparison returned by ds_compare
    compare_data = json.loads(request.POST['compare_data'])
    compare_result = compare_data['compare_result']
    tempfn = compare_data['tempfn']
    filename_new = compare_data['filename_new']

    dsfobj_cur = ds.files.order_by('-rev').first()
    rev_num = dsfobj_cur.rev

    # Rename file if already exists
    if Path('media/' + filename_new).exists():
        fn = os.path.splitext(filename_new)
        filename_new = fn[0] + '_' + tempfn[-11:-4] + fn[1]

    # Copy temp file to media folder
    filepath = 'media/' + filename_new
    copyfile(tempfn, filepath)

    # Create new DatasetFile
    DatasetFile.objects.create(
        dataset_id=ds,
        file=filename_new,
        rev=rev_num + 1,
        format=file_format,
        upload_date=datetime.date.today(),
        header=compare_result['header_new'],
        numrows=compare_result['count_new']
    )

    # Process updates for delimited format
    if file_format == 'delimited':
        try:
            bdf = pd.read_csv(filepath, delimiter='\t')
            bdf = bdf.replace({np.nan: ''})
            bdf = bdf.astype({"id": str, "ccodes": str, "types": str, "aat_types": str})
        except Exception as e:
            logger.error(f"Failed to read updated file: {e}")
            raise

        # Delete missing rows
        ds_places = ds.places.all()
        rows_delete = list(
            ds_places.filter(src_id__in=compare_result['rows_del']).values_list('id', flat=True)
        )

        try:
            ds_places.filter(id__in=rows_delete).delete()
        except Exception as e:
            logger.error(f"Failed to delete rows: {e}")
            raise

        # Helper to delete related objects
        def delete_related(pid):
            if not keepg:
                PlaceGeom.objects.filter(place_id=pid).delete()
            else:
                PlaceGeom.objects.filter(place_id=pid, task_id__isnull=True).delete()

            if not keepl:
                PlaceLink.objects.filter(place_id=pid).delete()
            else:
                PlaceLink.objects.filter(place_id=pid, task_id__isnull=True).delete()

            PlaceName.objects.filter(place_id=pid).delete()
            PlaceType.objects.filter(place_id=pid).delete()
            PlaceWhen.objects.filter(place_id=pid).delete()
            PlaceRelated.objects.filter(place_id=pid).delete()
            PlaceDescription.objects.filter(place_id=pid).delete()

        # Counters
        count_new, count_replaced, count_redo = 0, 0, 0
        idx_delete = []

        # Process each row
        for index, row in bdf.iterrows():
            row = row.to_dict()

            start = int(row['start']) if 'start' in row else int(row['attestation_year']) \
                if 'attestation_year' in row else None
            end = int(row['end']) if 'end' in row and str(row['end']) != 'nan' else start
            minmax_new = [start, end] if start else [None]

            # Extract coords
            row_coords = makeCoords(row['lon'], row['lat']) \
                if row['lon'] and row['lat'] else None
            if row.get('geowkt'):
                gtype = wkt.loads(row['geowkt']).type
                if 'Multi' not in gtype:
                    row_coords = [list(u) for u in wkt.loads(row['geowkt']).coords]
                else:
                    row_coords = [list(u) for u in wkt.loads(row['geowkt']).xy]

            header = list(bdf.keys())
            row_mapper = {
                'src_id': row['id'],
                'title': row['title'],
                'minmax': minmax_new,
                'title_source': row.get('title_source', ''),
                'title_uri': row.get('title_uri', ''),
                'ccodes': row['ccodes'].split(';') if 'ccodes' in header and row['ccodes'] else [],
                'matches': row['matches'].split(';') if 'matches' in header and row['matches'] else [],
                'variants': row['variants'].split(';') if 'variants' in header and row['variants'] else [],
                'types': row['types'].split(';') if 'types' in header and row['types'] else [],
                'aat_types': row['aat_types'].split(';') if 'aat_types' in header and row['aat_types'] else [],
                'parent_name': row.get('parent_name', ''),
                'parent_id': row.get('parent_id', ''),
                'geo_source': row.get('geo_source', ''),
                'geo_id': row.get('geo_id', ''),
                'description': row.get('description', ''),
                'coords': row_coords or [],
            }

            try:
                # Existing place?
                p = ds_places.get(src_id=row['id'])

                # Fetch existing record via API
                c = Client()
                try:
                    result = c.get(f'/api/place_compare/{p.id}/')
                    pobj = result.json()
                except Exception as e:
                    logger.exception(f'pobj failed {p.id}')
                    continue

                # Build comparison mapper
                from datasets.utils import PlaceMapper
                p_mapper = PlaceMapper(pobj['id'], pobj['src_id'], pobj['title'])
                p_mapper['minmax'] = pobj['minmax']

                title_name = next((n for n in pobj['names'] if n['toponym'] == pobj['title']), None)
                if title_name:
                    p_mapper['title_source'] = title_name.get('citation', {}).get('label', '')
                    p_mapper['title_id'] = title_name.get('citation', {}).get('id', '')

                p_mapper['ccodes'] = pobj.get('ccodes', [])
                p_mapper['types'] = [t['sourceLabel'] for t in pobj.get('types', [])]
                p_mapper['aat_types'] = [t['identifier'][4:] for t in pobj.get('types', [])]
                p_mapper['variants'] = [
                    n['toponym'] for n in pobj['names'] if n['toponym'] != pobj['title']
                ]
                p_mapper['coords'] = [g['coordinates'] for g in pobj.get('geoms', [])]
                p_mapper['links'] = [l['identifier'] for l in pobj.get('links', [])]

                # Compare
                diffs = []
                diffs.append(row_mapper['title_source'] == p_mapper.get('title_source', '') if row_mapper[
                    'title_source'] else True)
                diffs.append(
                    row_mapper['title_uri'] == p_mapper.get('title_id', '') if row_mapper['title_uri'] else True)
                diffs.append(row_mapper['minmax'] == p_mapper['minmax'])
                diffs.append(sorted(row_mapper['types']) == sorted(p_mapper.get('types', [])))
                diffs.append(row_mapper['title'] == p_mapper['title'])
                diffs.append(sorted(row_mapper['variants']) == sorted(p_mapper.get('variants', [])))
                diffs.append(sorted(row_mapper['aat_types']) == sorted(p_mapper.get('aat_types', [])))
                diffs.append(sorted(row_mapper['matches']) == sorted(p_mapper.get('links', [])))
                diffs.append(sorted(row_mapper['ccodes']) == sorted(p_mapper.get('ccodes', [])))
                if row_mapper['coords']:
                    diffs.append(row_mapper['coords'] == p_mapper.get('coords', []))

                # Update place
                count_replaced += 1
                p.title = row_mapper['title']
                p.ccodes = row_mapper['ccodes']
                p.minmax = minmax_new
                p.timespans = [minmax_new]

                if False in diffs:
                    idx_delete.append(p.id)

                # Check if meaningful changes (last few fields)
                if False not in diffs[-5:]:
                    # No meaningful changes
                    delete_related(p.id)
                    update_rels_tsv(p, row)
                else:
                    # Meaningful changes
                    count_redo += 1
                    keepg, keepl = False, False
                    delete_related(p.id)
                    update_rels_tsv(p, row)
                    p.review_wd = None
                    p.flag = True
                    if p.id not in idx_delete:
                        idx_delete.append(p.id)

                p.save()

            except Place.DoesNotExist:
                # New place
                count_new += 1
                newpl = Place.objects.create(
                    src_id=row['id'],
                    title=re.sub('\(.*?\)', '', row['title']),
                    ccodes=[] if str(row['ccodes']) == 'nan' else row['ccodes'].replace(' ', '').split(';'),
                    dataset=ds,
                    minmax=minmax_new,
                    timespans=[minmax_new],
                    flag=True
                )
                newpl.save()
                update_rels_tsv(newpl, row)

        # Update dataset row count
        ds.numrows = ds.places.count()
        ds.save()

        # Build result
        result = {
            "status": "updated",
            "format": file_format,
            "update_count": count_replaced,
            "redo_count": count_redo,
            "new_count": count_new,
            "deleted_count": len(rows_delete),
            "newfile": filepath
        }

        # Handle index updates
        if compare_data['count_indexed'] > 0:
            result["indexed"] = True
            idx_delete = rows_delete + idx_delete
            if idx_delete:
                idx = settings.ES_WHG
                removePlacesFromIndex(es, idx, idx_delete)
        else:
            logger.info('not indexed')

        # Write log
        Log.objects.create(
            category='dataset',
            logtype='ds_update',
            note=json.dumps(compare_result),
            dataset_id=dsid,
            user_id=request.user.id
        )

        ds.ds_status = 'updated'
        ds.save()

        return JsonResponse(result, safe=False)

    elif file_format == 'lpf':
        logger.info("ds_update for lpf; doesn't get here yet")
        return JsonResponse({'error': 'LPF updates not yet implemented'}, status=501)


# ---------------------------------------------------------------------------
# CRediT contributor editing (Phase 2b public widget)
# ---------------------------------------------------------------------------

def _user_can_edit_dataset(user, ds):
    return user.is_staff or user in ds.owners


@login_required
@require_POST
def dataset_contribution_add(request, id):
    """Create a CRediT Contribution for a dataset. Owner/staff only."""
    from persons.contributions import add_contribution
    ds = get_object_or_404(Dataset, id=id)
    if not _user_can_edit_dataset(request.user, ds):
        return HttpResponseForbidden("Not permitted")
    return add_contribution(request, ds)


@login_required
@require_POST
def dataset_contribution_delete(request, id, cid):
    """Delete a CRediT Contribution from a dataset. Owner/staff only."""
    from persons.contributions import delete_contribution
    ds = get_object_or_404(Dataset, id=id)
    if not _user_can_edit_dataset(request.user, ds):
        return HttpResponseForbidden("Not permitted")
    return delete_contribution(request, ds, cid)
