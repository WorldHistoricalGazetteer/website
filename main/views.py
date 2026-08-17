# main.views
from celery.backends.base import DisabledBackend
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
from django.apps import apps
from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Group
from django.core.exceptions import ValidationError
from django.core.mail import BadHeaderError
from django.db.models import Q
from django.db.models.functions import Lower
from django.http import HttpResponse, JsonResponse, HttpResponseRedirect, HttpResponseServerError, Http404
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse_lazy
from django.utils import timezone
from django.utils.html import escape
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.views.generic import ListView, CreateView, UpdateView, DeleteView
from django.views.generic.base import TemplateView

from areas.models import Area
from .forms import CommentModalForm, ContactForm, AnnouncementForm, VolunteerForm, InviteForm

logger = get_task_logger(__name__)
from collection.models import Collection, CollectionGroup
from datasets.models import Dataset
from datasets.tasks import testAdd
from .models import Announcement, Link, DownloadFile, Comment
from places.models import Place
from resources.models import Resource
from whgmail.messaging import WHGmail, zulip_notification

from bootstrap_modal_forms.generic import BSModalCreateView
import json
import random
import requests
import sys
from urllib.parse import urlparse
import urllib.parse

es = settings.ES_CONN

from django.contrib.auth.mixins import LoginRequiredMixin, PermissionRequiredMixin


def health_check(request):
    # TODO: Could be extended to check other aspects of app health, for example:
    # checks = { # These functions would need to be provided
    #     "database": check_database,
    #     "cache": check_cache,
    #     "external_service": check_external_service,
    #     "disk_space": check_disk_space,
    #     "memory_usage": check_memory_usage,
    #     "cpu_usage": check_cpu_usage
    # }
    #
    # status = "healthy"
    # details = {}
    #
    # for check, func in checks.items():
    #     result, message = func()
    #     details[check] = message
    #     if not result:
    #         status = "unhealthy"
    #
    # return JsonResponse({"status": status, "details": details})    

    return JsonResponse({"Status": "healthy"})


def OpenAPIView(request):
    return render(request, 'main/openapi.html', {'schema_url': '/api/schema/'})


def get_task_progress(request, taskid):
    task = AsyncResult(taskid)

    if isinstance(task.backend, DisabledBackend):
        return JsonResponse({
            'state': 'DISABLED',
            'progress': {'current': 0, 'total': 0}
        })

    response_data = {
        'state': task.state,
        'progress': {'current': 0, 'total': 0}
    }

    if isinstance(task.result, dict):
        response_data['progress'] = {
            'current': task.result.get('current', 0),
            'total': task.result.get('total', 0)
        }

    return JsonResponse(response_data)


class AnnouncementListView(ListView):
    model = Announcement
    context_object_name = 'announcements'
    template_name = 'announcements/announcement_list.html'
    queryset = Announcement.objects.filter(active=True).order_by('-created_at')

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['is_whgadmin'] = self.request.user.groups.filter(name='whg_admins').exists()
        return context


class AnnouncementCreateView(LoginRequiredMixin, PermissionRequiredMixin, CreateView):
    model = Announcement
    form_class = AnnouncementForm
    template_name = 'announcements/announcement_form.html'
    success_url = reverse_lazy('announcements-list')
    permission_required = 'main.add_announcement'  # Adjust based on your app's name and permissions


class AnnouncementDeleteView(LoginRequiredMixin, PermissionRequiredMixin, DeleteView):
    model = Announcement
    template_name = 'announcements/announcement_confirm_delete.html'
    success_url = reverse_lazy('announcements-list')
    permission_required = 'main.delete_announcement'  # Adjust based on your app's name and permissions


class AnnouncementUpdateView(LoginRequiredMixin, PermissionRequiredMixin, UpdateView):
    model = Announcement
    form_class = AnnouncementForm
    template_name = 'announcements/announcement_form.html'
    success_url = reverse_lazy('announcements-list')
    permission_required = 'main.change_announcement'  # Adjust based on your app's name and permissions

    def form_valid(self, form):
        if form.is_valid():
            return super().form_valid(form)
        else:
            logger.debug(f'form.errors: {form.errors}')
            return self.form_invalid(form)


# Mixin for checking splash screen pass
class SplashCheckMixin:
    def dispatch(self, request, *args, **kwargs):
        if not request.session.get('passed_splash'):
            return HttpResponseRedirect('/splash')  # Redirect to splash
        return super().dispatch(request, *args, **kwargs)


class Home30a(TemplateView):
    template_name = 'main/home_v30a4.html'

    # def get_template_names(self):
    #   version = self.kwargs.get('version', '30a4')
    #   print(f"get_template_names accessed with version: {version}")
    #   return [f'main/home_v{version}.html']

    def get_context_data(self, *args, **kwargs):
        context = super(Home30a, self).get_context_data(*args, **kwargs)

        carousel_metadata = []
        # ruling out Olaudah; owtrad dataset also an issue
        for dataset_types in [Collection, Dataset]:
            featured = dataset_types.objects.exclude(featured__isnull=True)
            for dataset in featured:
                # print('dataset in views:186', dataset, dataset.id)
                if dataset.id != 50:
                    carousel_metadata.append(dataset.carousel_metadata)
        random.shuffle(carousel_metadata)
        context['carousel_metadata'] = json.dumps(carousel_metadata)

        context['media_url'] = settings.MEDIA_URL
        context['base_dir'] = settings.BASE_DIR
        context['es_whg'] = settings.ES_WHG
        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'admins']).exists() else False
        context['teacher'] = True if self.request.user.groups.filter(
            name__in=['teacher']).exists() else False
        context['count'] = Place.objects.filter(dataset__public=True, dataset__core=True).count()
        context['announcements'] = Announcement.objects.filter(active=True).order_by('-created_at')
        context['count_places'] = Place.objects.filter(Q(dataset__public=True) | Q(dataset__core=True)).count()

        # TODO: REMOVE THE FOLLOWING? ****************************************************
        # Serialize the querysets to JSON
        f_collections = Collection.objects.exclude(featured__isnull=True)
        f_datasets = Dataset.objects.exclude(featured__isnull=True)
        context['featured_coll'] = f_collections
        context['featured_ds'] = f_datasets

        return context


# TODO: what rules? this or the *_list() functions?
# used for dashboard_user() and dataset_list()
def get_objects_for_user(model, user, filter_criteria, is_admin=False, extra_filters=None):
    from django.db.models import Q
    collaborator_objects = model.objects.none()

    # Always apply extra filters if they are provided and the model is Area
    if extra_filters and model == Area:
        objects = model.objects.filter(**extra_filters)
    elif is_admin:
        objects = model.objects.all()
    else:
        # Get the objects owned by the user
        owned_objects = model.objects.filter(**filter_criteria).exclude(title__startswith='(stub)')

        # Get the objects where the user is a collaborator
        if model == Dataset:
            collaborator_objects = Dataset.objects.filter(collabs__user_id=user.id)
        elif model == Collection:
            collaborator_objects = Collection.objects.filter(collabs__user_id=user.id)

        # Combine the querysets
        objects = (owned_objects | collaborator_objects).distinct()

    if model == Area:
        objects = objects.filter(type__in=['ccodes', 'copied', 'drawn']).order_by('-created')

    if is_admin and model == Area and 'type' in filter_criteria:
        objects = objects.exclude(type__in=filter_criteria['type'])
    elif model == Dataset:  # reverse sort, and some dummy datasets need to be filtered
        objects = objects.exclude(Q(title__startswith='(stub)') | Q(numrows__lt=1)).order_by('-create_date')
        # print('Dataset objects count', objects.count())
        # print('Datasets:', objects)
        # objects = objects.annotate(recent_log_timestamp=Max('log__timestamp'))

    return objects


def area_list(request, sort='', order=''):
    filters = request.GET

    is_admin = request.user.groups.filter(name='whg_admins').exists()
    text_fields = ['title', 'description', 'type', 'owner']

    # only user-created areas
    areas = Area.objects.filter(type__in=['ccodes', 'copied', 'drawn'])

    # Sort based on the parameters
    if sort and order:
        if sort == 'owner':
            sort = 'owner__username'
        if sort in text_fields:
            if order == 'desc':
                areas = areas.order_by(Lower(sort).desc())
            else:
                areas = areas.order_by(Lower(sort))
        else:
            sort_param = f'-{sort}' if order == 'desc' else sort
            areas = areas.order_by(sort_param)
    context = {'areas': areas, 'is_admin': is_admin, 'section': 'areas'}

    # Apply filters from request if any
    # type, owner, title
    if filters:
        if 'type' in filters and filters['type'] != 'all':
            areas = areas.filter(type=filters['type'])

        if 'owner' in filters:
            staff_groups = Group.objects.filter(name__in=['whg_admins', 'whg_staff'])
            if filters['owner'] == 'staff':
                areas = areas.filter(owner__groups__in=staff_groups)
            elif filters['owner'] == 'contributors':
                areas = areas.exclude(owner__groups__in=staff_groups)

        if 'title' in filters and filters['title']:
            search_term = filters['title']
            areas = areas.filter(Q(title__icontains=search_term) | Q(description__icontains=search_term))

        context = {
            'areas': areas,
            'is_admin': is_admin,
            'section': 'areas',
            'filtered': True,
            'filters': {
                'type': request.GET.get('type', ''),
                'owner': request.GET.get('owner', ''),
                'title': request.GET.get('title', '')
            }
        }
    return render(request, 'lists/area_list.html', context)


def dataset_list(request, sort='', order=''):
    filters = request.GET

    is_admin = request.user.groups.filter(name='whg_admins').exists()
    datasets = get_objects_for_user(Dataset, request.user, {'owner': request.user}, is_admin)
    text_fields = ['title', 'label', 'status', 'owner']

    # Sort based on the parameters
    if sort == 'last_modified':
        if order == 'desc':
            datasets = datasets.order_by('-create_date')
        else:
            datasets = datasets.order_by('create_date')
    elif sort and order:
        if sort == 'owner':
            sort = 'owner__username'
        if sort in text_fields:
            # Apply Lower function for text fields
            if order == 'desc':
                datasets = datasets.order_by(Lower(sort).desc())
            else:
                datasets = datasets.order_by(Lower(sort))
        else:
            # Standard sorting for non-text fields
            sort_param = f'-{sort}' if order == 'desc' else sort
            datasets = datasets.order_by(sort_param)
    context = {'datasets': datasets, 'is_admin': is_admin, 'section': 'datasets'}

    # ds_status, owner, title
    if filters:
        if 'ds_status' in filters and filters['ds_status'] != 'all':
            if filters['ds_status'] == 'published':
                datasets = datasets.filter(public=True)
            else:
                datasets = datasets.filter(ds_status=filters['ds_status'])

        if 'owner' in filters:
            staff_groups = Group.objects.filter(name__in=['whg_admins', 'whg_staff'])
            if filters['owner'] == 'staff':
                datasets = datasets.filter(owner__groups__in=staff_groups)
            elif filters['owner'] == 'contributors':
                datasets = datasets.exclude(owner__groups__in=staff_groups)

        if 'title' in filters and filters['title']:
            # datasets = datasets.filter(title__icontains=filters['title'])
            search_term = filters['title']
            datasets = datasets.filter(Q(title__icontains=search_term) | Q(owner__username__icontains=search_term))

        context = {
            'datasets': datasets,
            'is_admin': is_admin,
            'section': 'datasets',
            'filtered': True,
            'filters': {
                'ds_status': request.GET.get('ds_status', ''),
                'owner': request.GET.get('owner', ''),
                'title': request.GET.get('title', '')
            }
        }

    return render(request, 'lists/dataset_list.html', context)


def collection_list(request, sort='', order=''):
    filters = request.GET

    is_admin = request.user.groups.filter(name='whg_admins').exists()
    text_fields = ['title', 'type', 'status', 'owner']

    collections = Collection.objects.all()
    # collections = collections.annotate(recent_log_timestamp=Max('log__timestamp')).order_by('recent_log_timestamp')
    #
    # collections = collections.annotate(
    #   count=Case(
    #     When(collection_class='place', then=Count('annos')),
    #     # When(collection_class='dataset', then=Count('datasets__places')),
    #     default=0
    #   )
    # )

    # Sort based on the parameters
    if sort == 'create_date':
        if order == 'desc':
            collections = collections.order_by('-create_date')
        else:
            collections = collections.order_by('create_date')
    elif sort == 'count':
        if order == 'desc':
            collections = collections.order_by('-count')
        else:
            collections = collections.order_by('count')
    elif sort and order:
        if sort == 'owner':
            sort = 'owner__username'
        if sort in text_fields:
            # Apply Lower function for text fields
            if order == 'desc':
                collections = collections.order_by(Lower(sort).desc())
            else:
                collections = collections.order_by(Lower(sort))
        else:
            # Standard sorting for non-text fields
            sort_param = f'-{sort}' if order == 'desc' else sort
            collections = collections.order_by(sort_param)
    context = {'collections': collections, 'is_admin': is_admin, 'section': 'collections'}

    # status, collection_class, owner, title
    if filters:
        if 'status' in filters and filters['status'] != 'all':
            collections = collections.filter(status=filters['status'])

        if 'class' in filters and filters['class'] != 'all':
            collections = collections.filter(collection_class=filters['class'])

        if 'owner' in filters:
            staff_groups = Group.objects.filter(name__in=['whg_admins', 'whg_staff'])
            if filters['owner'] == 'staff':
                collections = collections.filter(owner__groups__in=staff_groups)
            elif filters['owner'] == 'contributors':
                collections = collections.exclude(owner__groups__in=staff_groups)

        if 'title' in filters and filters['title']:
            # collections = collections.filter(title__icontains=filters['title'])
            search_term = filters['title']
            collections = collections.filter(
                Q(title__icontains=search_term) | Q(owner__username__icontains=search_term))

        context = {
            'collections': collections,
            'is_admin': is_admin,
            'section': 'collections',
            'filtered': True,
            'filters': {
                'status': request.GET.get('status', ''),
                'class': request.GET.get('class', ''),
                'owner': request.GET.get('owner', ''),
                'title': request.GET.get('title', '')
            }
        }

    return render(request, 'lists/collection_list.html', context)


def group_list(request, sort='', order=''):
    filters = request.GET

    is_admin = request.user.groups.filter(name='whg_admins').exists()
    text_fields = ['title', 'category', 'owner']

    groups = CollectionGroup.objects.all()

    if sort and order:
        if sort == 'owner':
            sort = 'owner__username'
        if sort in text_fields:
            # Apply Lower function for text fields
            if order == 'desc':
                groups = groups.order_by(Lower(sort).desc())
            else:
                groups = groups.order_by(Lower(sort))
        else:
            sort_param = f'-{sort}' if order == 'desc' else sort
            groups = groups.order_by(sort_param)
    context = {'groups': groups, 'is_admin': is_admin, 'section': 'groups'}

    # type, owner, title
    if filters:
        if 'type' in filters and filters['type'] != 'all':
            groups = groups.filter(type=filters['type'])

        if 'owner' in filters:
            staff_groups = Group.objects.filter(name__in=['whg_admins', 'whg_staff'])
            if filters['owner'] == 'staff':
                groups = groups.filter(owner__groups__in=staff_groups)
            elif filters['owner'] == 'users':
                groups = groups.exclude(owner__groups__in=staff_groups)

        if 'title' in filters and filters['title']:
            groups = groups.filter(title__icontains=filters['title'])

        context = {
            'groups': groups,
            'is_admin': is_admin,
            'section': 'groups',
            'filtered': True,
            'filters': {
                'type': request.GET.get('class', ''),
                'owner': request.GET.get('owner', ''),
                'title': request.GET.get('title', '')
            }
        }

    return render(request, 'lists/group_list.html', context)


# Link out to the external Baserow "Submit Your Dataset to WHG" form.
# The workflow tool itself lives outside this codebase (Baserow workspace);
# we keep a stable internal URL so the external target can change without
# touching templates. Falls back gracefully until the form URL is configured.
def submit_dataset(request):
    url = getattr(settings, 'BASEROW_SUBMIT_FORM_URL', '')
    if url:
        return redirect(url)
    messages.info(request, "Dataset submission is being set up — please check back soon, or contact us in the meantime.")
    return redirect('home')


# Gazetteer Workbench — browser-based, local-first Reconciliation UI.
# BETA / UNPUBLISHED preview (see WorldHistoricalGazetteer/place#111 spec, #112 collaboration).
# Gated to staff/superusers and beta_tester-role users via `can_access_beta`; everyone else gets a
# 404 so the feature's existence is not disclosed while it is unpublished.
@login_required
def reconciliation_view(request):
    if not request.user.can_access_beta:
        raise Http404()
    # CRediT vocab for the in-browser citation builder (single source of truth = persons.CreditRole).
    from persons.models import CreditRole, ContributionDegree
    return render(request, "main/reconciliation.html", {
        'credit_roles': CreditRole.choices,
        'contribution_degrees': ContributionDegree.choices,
    })


# Unified Collaborative Workbench entry — the "New…" doc-type picker (plan-collaborativeCollections
# §10). Beta-gated exactly like reconciliation_view (404 to non-beta; existence not disclosed). It is
# a SEPARATE surface from the public legacy /workbench/ pathways page, which is untouched in v3.3 —
# rewriting that public page to launch this picker is the later P4 convergence step (plan §12.1).
@login_required
def workbench_home(request):
    if not request.user.can_access_beta:
        raise Http404()
    from workbench import doctypes
    from main.labels import label
    # Editors that are actually built and reachable today. New doc-type editor chunks land here as
    # they ship; until then their tiles render as "in development" rather than dead links.
    READY = {'reconciliation': '/reconciliation/',
             'place_collection': '/workbench/place-collection/',
             'itinerary': '/workbench/itinerary/',
             'gazetteer_group': '/workbench/gazetteer-group/'}
    tiles = []
    for dt in doctypes.creatable():
        tiles.append({'key': dt.key, 'label': dt.label, 'url': READY.get(dt.key),
                      'ready': dt.key in READY})
    # v4 placeholders — reserved, creation gated OFF; shown disabled with a "Coming with v4" badge.
    placeholders = [{'key': 'route', 'label': label('route')},
                    {'key': 'network', 'label': label('network')}]
    return render(request, "main/workbench_new.html",
                  {'tiles': tiles, 'placeholders': placeholders})


# Collaborative Workbench editor pages (doc-types #2/#3). Beta-gated exactly like the reconciliation
# tool (404 to non-beta). Local-first: the page ships static; all state lives in the browser until the
# user saves to their account or publishes (both via the beta-gated workbench API). Place Collection
# and Itinerary share one template + one client core (wb-collection-editor); only the copy/bundle
# differ. Documentation buttons point at the v3.3 Collaborative Collections guide.
_COLLECTIONS_DOC = 'https://docs.whgazetteer.org/content/v3-3/collections.html'


# "Edit published…" hub (plan-collaborativeCollections §10 / plan-dataset-checkout §1f) — one place to
# find and re-open any of the user's own published material for editing in the Workbench. Beta-gated.
@login_required
def workbench_published(request):
    if not request.user.can_access_beta:
        raise Http404()
    from django.db.models import Q
    from collection.models import Collection
    from datasets.models import Dataset
    u = request.user
    mine = Q(owner=u) | Q(collabs__user=u)               # owned OR collaborator (Collection.collabs)
    items = []
    for c in Collection.objects.filter(collection_class='place').filter(mine).distinct().order_by('-create_date'):
        items.append({'kind': 'Place Collection', 'kind_key': 'place_collection', 'icon': 'fa-layer-group',
                      'id': c.id, 'title': c.title, 'meta': f'{c.status or "sandbox"}',
                      'editor': '/workbench/place-collection/', 'checkout': True,
                      'browse': f'/collections/{c.id}/browse_pl'})
    for c in Collection.objects.filter(collection_class='dataset').filter(mine).distinct().order_by('-create_date'):
        items.append({'kind': 'Gazetteer Group', 'kind_key': 'gazetteer_group', 'icon': 'fa-database',
                      'id': c.id, 'title': c.title, 'meta': f'{c.datasets.count()} gazetteers · {c.status or "sandbox"}',
                      'editor': '/workbench/gazetteer-group/', 'checkout': True,
                      'browse': f'/collections/{c.id}/browse_ds'})
    for d in (Dataset.objects.filter(Q(owner=u) | Q(collabs__user_id=u))
              .exclude(title__startswith='(stub)').distinct().order_by('-id')):
        # Whole-/subset-dataset editing is staff-only for now (plan-dataset-checkout §1e). Staff get a
        # direct link into the check-out chooser; everyone else gets the record-by-record "Corrections"
        # path on the dataset page.
        items.append({'kind': 'Gazetteer', 'kind_key': 'gazetteer', 'icon': 'fa-map',
                      'id': d.id, 'title': d.title, 'meta': f'{d.numrows or "?"} records · {d.ds_status or ""}',
                      'editor': None, 'checkout': False,
                      'edit_url': (f'/workbench/dataset/?dataset={d.id}' if u.is_staff else None),
                      'browse': f'/datasets/{d.id}/browse'})
    return render(request, 'main/workbench_published.html', {'items': items})


@login_required
def wb_place_collection_view(request):
    if not request.user.can_access_beta:
        raise Http404()
    return render(request, "main/wb_collection_editor.html", {
        'page_title': 'New Place Collection', 'heading': 'New Place Collection',
        'intro': 'Curate a set of WHG places with notes — for teaching, storytelling, or research. '
                 'Everything stays in your browser until you choose to save it to your account or publish it.',
        'meta_heading': 'About this collection', 'member_heading': 'Places',
        'title_placeholder': 'e.g. Cities of the Hanseatic League',
        'search_placeholder': 'Search WHG for a place to add…',
        'publish_word': 'collection', 'sequenced': False,
        'has_map': True, 'n_map': 3, 'n_collab': 4, 'n_publish': 5,
        'doc_url': _COLLECTIONS_DOC + '#building-a-place-collection',
        'bundle_css': 'webpack/wb-place-collection.bundle.css',
        'bundle_js': 'webpack/wb-place-collection.bundle.js',
    })


@login_required
def wb_itinerary_view(request):
    if not request.user.can_access_beta:
        raise Http404()
    return render(request, "main/wb_collection_editor.html", {
        'page_title': 'New Itinerary', 'heading': 'New Itinerary',
        'intro': 'An itinerary is an ordered journey through WHG places — a sequenced Place Collection. '
                 'Add stops, set their order, and publish. Everything stays in your browser until you publish.',
        'meta_heading': 'About this itinerary', 'member_heading': 'Stops',
        'title_placeholder': 'e.g. A Grand Tour of Italy, 1786',
        'search_placeholder': 'Search WHG for a stop to add…',
        'publish_word': 'itinerary', 'sequenced': True,
        'has_map': True, 'n_map': 3, 'n_collab': 4, 'n_publish': 5,
        'doc_url': _COLLECTIONS_DOC + '#building-an-itinerary',
        'bundle_css': 'webpack/wb-itinerary.bundle.css',
        'bundle_js': 'webpack/wb-itinerary.bundle.js',
    })


@login_required
def wb_place_record_view(request):
    # Single-record correction editor (plan §6.1). Beta-gated; reached via record-level check-out.
    if not request.user.can_access_beta:
        raise Http404()
    return render(request, "main/wb_place_record.html", {})


def _suggestion_diff(cur, prop, changed):
    """Build display rows [(label, current, proposed)] for the changed fields of a suggestion, for the
    review queue's side-by-side diff."""
    def flat(lst, k):
        vals = [(i.get(k) or '').strip() for i in (lst or []) if (i.get(k) or '').strip()]
        return ', '.join(vals) if vals else '—'

    def coord(s):
        return f"{s.get('lng')}, {s.get('lat')}" if s.get('lng') is not None else '—'

    def geom(s):
        g = s.get('geometry')
        if not g or not g.get('type'):
            return '—'
        t = g['type']
        if t == 'Point':
            c = g.get('coordinates') or []
            return f"Point ({c[0]:.4f}, {c[1]:.4f})" if len(c) == 2 else 'Point'
        if t == 'GeometryCollection':
            return f"{len(g.get('geometries') or [])} mixed geometries"
        return f"{t} ({len(g.get('coordinates') or [])} parts)"

    labels = {'name': 'Name', 'ccodes': 'Countries', 'names': 'Also-known-as', 'types': 'Types',
              'links': 'Authority links', 'descriptions': 'Descriptions', 'dates': 'Dates',
              'coordinate': 'Coordinate', 'geometry': 'Geometry'}
    rows = []
    for f in changed:
        if f == 'name':
            rows.append((labels[f], cur.get('title') or '—', prop.get('title') or '—'))
        elif f == 'ccodes':
            rows.append((labels[f], ', '.join(cur.get('ccodes') or []) or '—', ', '.join(prop.get('ccodes') or []) or '—'))
        elif f in ('names', 'descriptions'):
            k = 'toponym' if f == 'names' else 'value'
            rows.append((labels[f], flat(cur.get(f), k), flat(prop.get(f), k)))
        elif f == 'types':
            rows.append((labels[f], flat(cur.get('types'), 'label'), flat(prop.get('types'), 'label')))
        elif f == 'links':
            rows.append((labels[f], flat(cur.get('links'), 'identifier'), flat(prop.get('links'), 'identifier')))
        elif f == 'dates':
            cd, pd = cur.get('dates') or {}, prop.get('dates') or {}
            fmt = lambda d: (f"{d.get('start', '')}–{d.get('end', '')}" if (d.get('start') is not None or d.get('end') is not None) else '—')
            rows.append((labels[f], fmt(cd), fmt(pd)))
        elif f == 'coordinate':
            rows.append((labels[f], coord(cur), coord(prop)))
        elif f == 'geometry':
            rows.append((labels[f], geom(cur), geom(prop)))
    return rows


@login_required
def suggestions_review(request):
    """Review queue for community record corrections (plan-record-suggestions §3). Beta-gated. Staff see
    all pending suggestions; a gazetteer owner sees those on their gazetteers. Accept/reject is a POST to
    the workbench review endpoint."""
    if not request.user.can_access_beta:
        raise Http404()
    from workbench import suggestions as S
    from workbench.checkout import record_snapshot
    cards = []
    for s in S.queue_for(request.user).order_by('dataset_id', '-created')[:200]:
        try:
            cur = record_snapshot(s.place)
        except Exception:
            cur = {}
        cards.append({
            'id': s.id, 'record_id': s.place_id, 'record_title': s.place.title,
            'dataset_title': s.dataset.title, 'dataset_id': s.dataset_id,
            'proposer': (getattr(s.proposer, 'name', '') or getattr(s.proposer, 'username', '') or '—') if s.proposer_id else '—',
            'created': s.created, 'rationale': s.rationale, 'changed': s.changed_fields or [],
            'diff': _suggestion_diff(cur, s.proposed_snapshot or {}, s.changed_fields or []),
        })
    return render(request, 'main/suggestions_review.html',
                  {'cards': cards, 'reviewer_is_staff': request.user.is_staff})


# ── Beta snag form (Beta Testing Plan) — account-free intake for testers (no GitHub account needed) ──
# Testers file problems via this on-site form; it persists a BetaSnag and — when GITHUB_SNAG_TOKEN is
# configured — also opens a GitHub issue in the planning repo, carrying the diagnostics session id so
# the report ties to the tester's GlitchTip errors. Beta-gated (testers hold WHG/ORCiD accounts).
def _reporter_credit(user):
    """How a beta reporter is named on the public GitHub issue.

    Beta testers can record a GitHub handle on their Profile; when they have, the report
    is credited to ``@handle`` rather than their name — so a follow-up question mentions
    them and GitHub notifies them (subject to their notification settings). Falls back to
    the account name for the majority who have no GitHub account.
    """
    handle = (getattr(user, 'github_username', '') or '').strip()
    return f'@{handle}' if handle else (getattr(user, 'name', '') or user.username)


def _snag_github_body(snag, user, role):
    def block(label, val):
        return f'\n**{label}**\n{val.strip()}\n' if (val or '').strip() else ''
    return (
        f'**Reported by:** {_reporter_credit(user)} ({role}) · '
        f'**session:** `{snag.session_id or "—"}`\n'
        f'**Page:** {snag.page_url or "—"}\n'
        f'**Severity:** {snag.severity or "—"} · **Feature:** {snag.feature or "—"}\n'
        f'**Browser:** {snag.user_agent or "—"}\n'
        + block('What happened', snag.what)
        + block('Expected', snag.expected)
        + block('Steps', snag.steps)
        + '\n---\n_Filed via the WHG on-site beta snag form. The session id ties this to the reporter’s '
          'GlitchTip diagnostics for the same session._')


def _suggestion_github_body(snag, user, role):
    """Issue body for a beta *suggestion* — lighter than a snag (no severity/steps/
    session/browser diagnostics; just who, which feature, and the idea)."""
    def block(label, val):
        return f'\n**{label}**\n{val.strip()}\n' if (val or '').strip() else ''
    return (
        f'**Suggested by:** {_reporter_credit(user)} ({role})\n'
        f'**Feature:** {snag.feature or "—"}\n'
        + block('Suggestion', snag.what)
        + '\n---\n_Filed via the WHG on-site beta suggestion form._')


def _file_snag_to_github(snag, user):
    """Best-effort: open a GitHub issue for the snag or suggestion. Returns the issue URL, or '' if
    unfiled (no token or API error). Never raises — the record is already saved locally regardless."""
    token = getattr(settings, 'GITHUB_SNAG_TOKEN', '')
    repo = getattr(settings, 'GITHUB_SNAG_REPO', '')
    if not token or not repo:
        return ''
    import requests
    role = 'staff' if user.is_staff else 'beta'
    if snag.kind == 'suggestion':
        title = f'[suggestion] {snag.title}'
        body = _suggestion_github_body(snag, user, role)
        labels = ['suggestion']
    else:
        title = f'[beta] {snag.title}'
        body = _snag_github_body(snag, user, role)
        labels = ['beta-snag'] + ([f'sev:{snag.severity}'] if snag.severity else [])
    try:
        r = requests.post(
            f'https://api.github.com/repos/{repo}/issues',
            headers={'Authorization': f'Bearer {token}', 'Accept': 'application/vnd.github+json'},
            json={'title': title, 'body': body, 'labels': labels},
            timeout=15)
        if r.status_code in (200, 201):
            return r.json().get('html_url', '')
        logger.warning('beta %s: GitHub file failed (%s): %s', snag.kind, r.status_code, r.text[:300])
    except Exception as e:  # noqa: BLE001
        logger.warning('beta %s: GitHub file error: %s', snag.kind, e)
    return ''


@login_required
def beta_snag(request):
    if not request.user.can_access_beta:
        raise Http404()
    from main.models import BetaSnag
    if request.method == 'POST':
        d = request.POST
        title = (d.get('title') or '').strip()
        if not title:
            return render(request, 'main/beta_snag.html',
                          {'error': 'Please give the problem a short title.', 'form': d,
                           'page': d.get('page_url', '')})
        snag = BetaSnag.objects.create(
            reporter=request.user, title=title[:300],
            what=(d.get('what') or '')[:8000], expected=(d.get('expected') or '')[:8000],
            steps=(d.get('steps') or '')[:8000], feature=(d.get('feature') or '')[:80],
            severity=(d.get('severity') or '')[:20], page_url=(d.get('page_url') or '')[:500],
            session_id=(d.get('session_id') or '')[:64], user_agent=(d.get('user_agent') or '')[:300])
        gh_url = _file_snag_to_github(snag, request.user)
        if gh_url:
            snag.github_url = gh_url
            snag.save(update_fields=['github_url'])
        return render(request, 'main/beta_snag.html', {'submitted': snag, 'github_url': gh_url})
    return render(request, 'main/beta_snag.html',
                  {'page': request.GET.get('page', ''), 'session': request.GET.get('session', '')})


@login_required
def beta_suggestion(request):
    """On-site beta *suggestion* form — a lighter sibling of ``beta_snag`` for ideas /
    improvements rather than bugs. Files a ``[suggestion]`` GitHub issue (label
    ``suggestion``) and stores a ``BetaSnag(kind='suggestion')``, without the snag
    form's diagnostic capture (severity / steps / session / browser)."""
    if not request.user.can_access_beta:
        raise Http404()
    from main.models import BetaSnag
    if request.method == 'POST':
        d = request.POST
        title = (d.get('title') or '').strip()
        if not title:
            return render(request, 'main/beta_suggestion.html',
                          {'error': 'Please give your suggestion a short title.', 'form': d})
        suggestion = BetaSnag.objects.create(
            kind='suggestion', reporter=request.user, title=title[:300],
            what=(d.get('what') or '')[:8000], feature=(d.get('feature') or '')[:80],
            page_url=(d.get('page_url') or '')[:500])
        gh_url = _file_snag_to_github(suggestion, request.user)
        if gh_url:
            suggestion.github_url = gh_url
            suggestion.save(update_fields=['github_url'])
        return render(request, 'main/beta_suggestion.html', {'submitted': suggestion, 'github_url': gh_url})
    return render(request, 'main/beta_suggestion.html', {'page': request.GET.get('page', '')})


@login_required
def wb_dataset_view(request):
    # Gazetteer records-correction shell (plan-dataset-checkout §3/§4). Beta + STAFF-gated (whole-/
    # subset-dataset editing is staff-only initially — see workbench.views._staff_dataset_or_403).
    # Reached with ?dataset=<id> (fresh check-out chooser) or ?project=<uuid> (resume a working copy).
    if not (request.user.can_access_beta and request.user.is_staff):
        raise Http404()
    return render(request, "main/wb_dataset.html", {})


@login_required
def wb_gazetteer_group_view(request):
    if not request.user.can_access_beta:
        raise Http404()
    # Reuses the shared editor DOM; its own client (wb-gazetteer-group) fills the member pane with
    # published gazetteers (datasets) rather than places.
    return render(request, "main/wb_collection_editor.html", {
        'page_title': 'New Gazetteer Group', 'heading': 'New Gazetteer Group',
        'intro': 'Aggregate published gazetteers (datasets) into a single group for comparison or '
                 'analysis. Everything stays in your browser until you choose to save or publish.',
        'meta_heading': 'About this group', 'member_heading': 'Gazetteers',
        'title_placeholder': 'e.g. Colonial & modern gazetteers of South Asia',
        'search_placeholder': 'Search published gazetteers to add…',
        'publish_word': 'group', 'sequenced': False,
        'has_map': False, 'n_collab': 3, 'n_publish': 4,
        'doc_url': _COLLECTIONS_DOC + '#building-a-gazetteer-group',
        'bundle_css': 'webpack/wb-gazetteer-group.bundle.css',
        'bundle_js': 'webpack/wb-gazetteer-group.bundle.js',
    })


# ── Development status & roadmap ─────────────────────────────────────────────────────────────────
# A single, plain-language view of what's being built, its stage, and roughly where it's heading —
# so non-developer staff (and eventually the public) can track WHG's concurrent development without
# reading commits. Written to be PUBLIC-READY: no internal infra, hosts, or credentials. Staff-only
# for now via the gate below; to make public, delete the @login_required + is_staff check here and
# remove the `{% if user.is_staff %}` guard around the About-menu link (search: 'beta_status').
#
# Stages: 'beta' = usable now by staff + invited testers, not yet public; 'dev' = being built, not
# yet testable; 'shipped' = live for everyone / changes an existing workflow; 'horizon' = planned
# direction, NOT a dated commitment. Edit BETA_STATUS_UPDATED whenever this list changes.
BETA_STATUS_UPDATED = "11 July 2026"
BETA_STATUS_SECTIONS = [
    {
        "heading": "In development & beta preview",
        "note": "Available to WHG staff and invited beta testers while we refine them — not yet linked for the public.",
        "items": [
            {"name": "Map your Data", "stage": "beta", "version": "3.3",
             "body": "Turn a spreadsheet or list of place names into located, dated, standardised places — "
                     "matched to the World Historical Gazetteer, cleaned up, and ready to map, share, or "
                     "contribute. Import from CSV/TSV/JSON, Excel, or a Google Sheet — or extract place "
                     "names straight from free text you paste or upload; reconcile against WHG (including "
                     "historical periods via PeriodO); add coordinates, dates, and place types; enrich with "
                     "Wikipedia links from matched records; validate; and export or contribute — all in your "
                     "browser, so your data stays on your device."},
            {"name": "Collaborative Workbench", "stage": "beta", "version": "3.3",
             "body": "Work on the same reconciliation project as a team, together and in real time, with "
                     "shared roles and a project owned by your group rather than a single account."},
            {"name": "Browser-first, collaborative Collections", "stage": "beta", "version": "3.3",
             "body": "Build and curate Collections directly in the browser and together as a team, rather "
                     "than through multi-step server workflows — the same local-first model as Map your "
                     "Data. Now in beta: Place Collections, Itineraries (ordered journeys), and Gazetteer "
                     "Groups, each with search-as-you-type place-finding ranked by geographic nearness, "
                     "map previews, add-places-from-text (NER), team collaboration, and publish into WHG's "
                     "existing public collection pages. (Routes and Networks arrive with the v4 model.)"},
            {"name": "Citations, licensing & contributor credit (CRediT)", "stage": "dev", "version": "3.3",
             "body": "Build a proper citation for a dataset, a machine-readable CITATION.cff and schema.org "
                     "record, and credit everyone who contributed by their role — carried with the data when "
                     "it's shared or contributed to WHG."},
            {"name": "Atlas — a map-first interface with dynamic clustering", "stage": "dev", "version": "3.5",
             "body": "A new, exploration-led interface for the gazetteer, in active development. It clusters "
                     "and reveals places adaptively as you pan and zoom, keeping very large gazetteers "
                     "legible at every scale."},
            {"name": "Standardised place types across every source (Getty AAT)", "stage": "dev", "version": "3.5",
             "staff": True,
             "body": "We're giving every place in WHG a standardised place type from the Getty Art & "
                     "Architecture Thesaurus (AAT) — a shared, hierarchical vocabulary — so you can filter "
                     "and browse by the kind of place (city, river, temple, administrative area…) "
                     "consistently, whichever gazetteer a record came from. This powers the search "
                     "experience directly: a hierarchical type filter (choose 'inhabited places' and also "
                     "get its subtypes) and friendly, human-readable type labels, replacing today's raw "
                     "source codes. It's a large, source-by-source effort spanning several months: the "
                     "biggest sources — GeoNames, OpenStreetMap, Wikidata, OpenHistoricalMap, and now the "
                     "Getty Thesaurus of Geographic Names (~3M records) — are typed, but a number of "
                     "smaller and specialist gazetteers still need their type vocabularies mapped to AAT "
                     "before their records become fully filterable by type. Coverage is growing steadily."},
        ],
    },
    {
        "heading": "Recently shipped",
        "note": "Live now for everyone, or a change to how an existing workflow behaves.",
        "items": [
            {"name": "Publish & index independent of reconciliation", "stage": "shipped", "staff": True, "version": "3.2",
             "body": "A dataset can now be made public and searchable regardless of how far its "
                     "reconciliation has progressed, with a clear warning about the trade-offs — removing a "
                     "trap where public datasets could stay invisible in search. (Staff workflow note.)"},
            {"name": "In-house analytics dashboard (Plausible aggregation)", "stage": "shipped", "staff": True, "version": "3.2",
             "body": "A staff view in the admin area that aggregates WHG's self-hosted Plausible sites "
                     "(main, blog, docs) into one dashboard — top-line metrics, visitors over time, and "
                     "breakdowns — including the Map-your-Data usage funnel. (Staff tool.)"},
        ],
    },
    {
        "heading": "On the horizon",
        "note": "The direction we're heading — shared for planning, not as committed release dates.",
        "items": [
            {"name": "Submission tracker", "stage": "horizon", "version": "3.4",
             "body": "A way to follow a dataset submission through its lifecycle — from first "
                     "contribution, through review, to publication — so contributors and editors can "
                     "see where each submission stands and what happens next. It will be built natively "
                     "into WHG rather than relying on a separate external service, keeping it integrated "
                     "with datasets, accounts, and the rest of the platform. We're finalising the "
                     "specification with colleagues; tentatively targeted for v3.4."},
            {"name": "Joining the Open Metadata Exchange (with ISKME)", "stage": "horizon", "version": "3.4",
             "body": "Exploratory work to make WHG a node in the Open Metadata Exchange (OME) — a "
                     "peer-to-peer metadata-sharing network, an ISKME initiative. WHG would feed its "
                     "place/entity metadata into the network and, in a later phase, extend its reconciliation "
                     "service to query the federated metadata, enriching matches with links, alternate "
                     "spellings, and context contributed by peer nodes. Our OME node is packaged and ready; "
                     "we're awaiting the next stage of OME's own development. Targeted for v3.4."},
            {"name": "Lesson plans on ISKME's publishing platform", "stage": "horizon", "version": "3.6",
             "body": "Separately from OME, we plan to author lesson plans that draw on the World Historical "
                     "Gazetteer using ISKME's publishing platform, in time succeeding WHG's current Lesson "
                     "Plans resources. A later effort, expected after v3.5."},
            {"name": "WHG v4 — a graph data model", "stage": "horizon", "version": "4.0",
             "body": "A re-architecture around a graph model (with the PLATO ontology — Place Attestation "
                     "Ontology) to represent places, the relationships between them, and how they change over "
                     "time far more richly than a flat record allows.",
             "link": {"url": "https://github.com/pelagios/place-attestation-ontology",
                      "label": "PLATO on the Pelagios GitHub"}},
        ],
    },
]
_BETA_STAGE_META = {
    "beta": ("Beta preview", "text-bg-warning"),
    "dev": ("In development", "text-bg-secondary"),
    "shipped": ("Live", "text-bg-success"),
    "horizon": ("Exploratory", "text-bg-info"),
}


@login_required  # remove to make public
def beta_status_view(request):
    # Staff-only for now (content is public-ready — see note above).
    if not request.user.is_staff:
        raise Http404()
    # Staff-flagged items are internal notes: shown (badged) to staff, and hidden once the page is
    # ungated for the public. Sections with no visible items are dropped entirely.
    is_staff = request.user.is_staff
    sections = []
    for sec in BETA_STATUS_SECTIONS:
        items = [dict(it, stage_label=_BETA_STAGE_META[it["stage"]][0],
                      stage_class=_BETA_STAGE_META[it["stage"]][1])
                 for it in sec["items"] if is_staff or not it.get("staff")]
        if items:
            sections.append(dict(sec, items=items))
    return render(request, "main/beta_status.html", {
        "sections": sections,
        "app_version": settings.APP_VERSION,
        "updated": BETA_STATUS_UPDATED,
        "release_note": (
            "Map your Data and the browser-first Collections tools are two halves of one new, unified "
            "Workbench — targeted for v3.3 (alongside Citations). They're intended to reach "
            "everyone together, as a single coordinated release, so the current workflow and its "
            "documentation can be refreshed in one step rather than piecemeal."
        ),
    })


# ── WHG Analytics (in-house proxy for Plausible) ────────────────────────────────────────────────
# Staff-only admin tool (dashboard_admin → Tools → "Analytics"). Aggregates all three WHG Plausible
# sites (main / blog / docs) via the Stats API into one page — top-line metrics, a visitors-over-time
# chart, and breakdowns (pages/sources/countries/devices/browsers/OS) — plus the Map-your-Data usage
# funnel (main-site custom events) at the bottom. Replaces both the external Plausible link and the
# CE-absent funnel feature. Multi-site fetches run concurrently.
PLAUSIBLE_PERIODS = [('7d', 'Last 7 days'), ('30d', 'Last 30 days'), ('month', 'This month'),
                     ('6mo', 'Last 6 months'), ('12mo', 'Last 12 months')]
PLAUSIBLE_MAIN = 'whgazetteer.org'
PLAUSIBLE_SITES = [
    ('whgazetteer.org', 'Main', '', '#2563eb'),
    ('blog.whgazetteer.org', 'Blog', 'blog', '#16a34a'),
    ('docs.whgazetteer.org', 'Docs', 'docs', '#d97706'),
]
DONUT_PALETTE = ['#2563eb', '#16a34a', '#d97706', '#9333ea', '#dc2626', '#0891b2', '#64748b']
MYD_FUNNEL = [
    ('MyD: import', 'Imported a dataset'),
    ('MyD: reconcile', 'Ran reconciliation'),
    ('MyD: reconcile result', 'Got candidate matches'),
    ('MyD: export', 'Exported results'),
    ('MyD: contribute', 'Contributed to WHG'),
]
MYD_OTHER = [
    ('MyD: scope applied', 'Applied a scope filter'),
    ('MyD: place type assigned', 'Assigned a place type'),
    ('MyD: tour', 'Took the guided tour'),
    ('MyD: resume', 'Resumed a saved project'),
    ('MyD: contribute blocked', 'Blocked at contribute (validation)'),
    ('MyD: team save', 'Saved a project to the server (collab)'),
    ('MyD: shared', 'Created a read-only share link'),
    ('MyD: shared open', 'Opened a shared project'),
    ('MyD: conflict', 'Hit a merge conflict'),
]


def _plausible_get(path, params, site_id):
    """Call the Plausible Stats API for a specific site. Returns (json, None) or (None, error_str)."""
    key = getattr(settings, 'PLAUSIBLE_API_KEY', None)
    base = getattr(settings, 'PLAUSIBLE_BASE_URL', '')
    if not (key and base and site_id):
        return None, 'Plausible is not configured (PLAUSIBLE_API_KEY / _BASE_URL).'
    try:
        p = {'site_id': site_id}
        p.update(params)
        resp = requests.get(f'{base}/api/v1/stats/{path}', params=p,
                            headers={'Authorization': f'Bearer {key}'}, timeout=15)
        resp.raise_for_status()
        return resp.json(), None
    except requests.RequestException as e:
        return None, f'Plausible Stats API error: {e}'


def _fmt_duration(seconds):
    try:
        s = int(seconds or 0)
    except (TypeError, ValueError):
        return '—'
    return f'{s // 60}m {s % 60:02d}s'


_MONTH_ABBR = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def _fmt_chart_date(iso, interval):
    """Unambiguous UK/US chart-axis label from a Plausible 'YYYY-MM-DD' date.
    Daily → '8 Jun'; monthly → 'Jun 2026'. Falls back to the raw value on any parse error."""
    try:
        y, m, d = iso.split('-')[:3]
        mon = _MONTH_ABBR[int(m)]
        return f'{mon} {y}' if interval == 'month' else f'{int(d)} {mon}'
    except (ValueError, IndexError):
        return iso


_COUNTRY_NAMES = None


def _country_names():
    """{ISO2 code: country name} from WHG's own static/js/parents.js (window.ccode_hash) so the
    Analytics country list matches the names used site-wide. Parsed once, cached; {} on any failure."""
    global _COUNTRY_NAMES
    if _COUNTRY_NAMES is not None:
        return _COUNTRY_NAMES
    _COUNTRY_NAMES = {}
    try:
        import os
        path = os.path.join(settings.STATIC_ROOT, 'js', 'parents.js')
        with open(path, encoding='utf-8') as fh:
            raw = fh.read()
        seg = raw.split('window.ccode_hash', 1)[1]
        start = seg.index('{')
        end = seg.index('window.regions') if 'window.regions' in seg else len(seg)
        obj = seg[start:end].rstrip().rstrip(';').rstrip()
        data = json.loads(obj)
        _COUNTRY_NAMES = {code: (v.get('gnlabel') or code).strip()
                          for code, v in data.items() if code and isinstance(v, dict)}
    except Exception as e:  # noqa: BLE001 — names are cosmetic; degrade to codes
        logger.warning('Analytics country-name parse failed: %s', e)
    return _COUNTRY_NAMES


_M49 = None


def _m49_country_regions():
    """({ISO2: region label}, [(region label, [ISO2, …]), …]) from the repo's UN M49
    hierarchy (``regions/data/m49_regions.json``, the same file ``import_m49`` loads).

    Used to roll contributed-place counts up to the 22 M49 sub-regions, which — unlike
    the ``areas`` app's *predefined* study areas — partition the world cleanly: every
    country has exactly one parent region, so nothing is double-counted. Parsed once,
    cached; ({}, []) on any failure (the coverage panel then just omits the rollup)."""
    global _M49
    if _M49 is not None:
        return _M49
    _M49 = ({}, [])
    try:
        import os
        path = os.path.join(settings.BASE_DIR, 'regions', 'data', 'm49_regions.json')
        with open(path, encoding='utf-8') as fh:
            nodes = json.load(fh)
        by_region = {}
        for node in nodes.values():
            if node.get('level') != 'country' or not node.get('iso_alpha2'):
                continue
            parents = node.get('parents') or []
            parent = nodes.get(parents[0]) if parents else None
            # Antarctica hangs directly off "World" — label it with its own name.
            if not parent or parent.get('level') == 'global':
                label = (node.get('labels') or {}).get('en') or node['m49']
            else:
                label = (parent.get('labels') or {}).get('en') or parent['m49']
            for code in node['iso_alpha2']:
                by_region.setdefault(label, []).append(code)
        country_region = {c: label for label, codes in by_region.items() for c in codes}
        _M49 = (country_region, sorted(by_region.items()))
    except Exception as e:  # noqa: BLE001 — the rollup is a nicety, never fatal
        logger.warning('M49 region parse failed: %s', e)
    return _M49


_COVERAGE_CACHE_KEY = 'analytics:contributed_coverage:v1'


def _contributed_coverage():
    """Geographic coverage of *contributed* datasets — public, non-core, non-authority —
    for the Analytics page, so we can see where community data is thin.

    INTERIM: this reads Postgres, which is the narrower source. The index carries
    country codes for 97.15% of 51.2M places (place#173) against 72.1% of the
    contributed places here, so these figures understate real coverage by more than a
    quarter. place#179 tracks moving the panel onto an ES aggregation and adding
    temporal-gap reporting; retire this helper then.

    Not derived from the gazetteer registry's ``h3_coverage``, despite it looking like
    the natural source: every ``whg:*`` registry row currently carries an identical copy
    of the whole-namespace H3 union (631,841 cells apiece), so the registry cannot
    distinguish one contributed dataset's footprint from another's.

    Returns a dict for the template, or None on any error. Cached for 30 minutes — the
    figures move only when a dataset is published, and the page is re-rendered on every
    period switch."""
    from django.core.cache import cache
    cached = cache.get(_COVERAGE_CACHE_KEY)
    if cached is not None:
        return cached

    from django.db import connection
    import math
    try:
        contributed = 'd.public AND d.core IS NOT TRUE AND d.authority IS NOT TRUE'
        with connection.cursor() as cur:
            # (dataset, country) record counts — ~1,800 rows, so the rollups below are
            # done in Python rather than as repeated DISTINCT aggregates in the DB.
            cur.execute(f"""
                SELECT x.dataset, x.cc, count(*)::int
                  FROM (SELECT p.dataset, unnest(p.ccodes) AS cc
                          FROM places p JOIN datasets d ON p.dataset = d.label
                         WHERE {contributed}) x
                 GROUP BY 1, 2
            """)
            pairs = cur.fetchall()
            cur.execute(f"""
                SELECT count(*)::int,
                       count(*) FILTER (
                           WHERE p.ccodes IS NULL OR cardinality(p.ccodes) = 0)::int,
                       count(DISTINCT p.dataset)::int
                  FROM places p JOIN datasets d ON p.dataset = d.label
                 WHERE {contributed}
            """)
            total_records, no_country, datasets_with_places = cur.fetchone()
            # Land area per country, so "thin" can mean thin *for its size* — a raw
            # bottom-N is all micro-territories and says nothing about where to direct
            # effort. ~0.4s over the 208 country polygons.
            cur.execute("""
                SELECT iso, (ST_Area(mpoly::geography) / 1e6)::int
                  FROM countries WHERE mpoly IS NOT NULL
            """)
            areas = {iso: km2 for iso, km2 in cur.fetchall() if iso}

        recs, dsets = {}, {}
        for label, code, n in pairs:
            code = (code or '').strip().upper()
            # Contributed data holds a little junk in ccodes (e.g. a literal '""'),
            # which would otherwise reach the map as an unusable region key.
            if len(code) != 2 or not code.isalpha():
                continue
            recs[code] = recs.get(code, 0) + n
            dsets.setdefault(code, set()).add(label)

        names = _country_names()
        country_region, region_members = _m49_country_regions()

        by_country = [{'code': c, 'name': names.get(c) or c, 'records': n,
                       'datasets': len(dsets.get(c, ())),
                       'region': country_region.get(c, '—')}
                      for c, n in recs.items()]
        by_country.sort(key=lambda r: (-r['records'], r['name']))

        # Log-scaled green fills (distinct from the blue visitor choropleth); countries
        # with no contributed records keep the map's default grey, so gaps read as gaps.
        def _green(t):  # 0→pale, 1→deep, over #dcfce7 … #14532d
            t = max(0.0, min(1.0, t))
            r = round(220 + (20 - 220) * t)
            g = round(252 + (83 - 252) * t)
            b = round(231 + (45 - 231) * t)
            return f'#{r:02x}{g:02x}{b:02x}'
        logs = {c: math.log10(n + 1) for c, n in recs.items() if n > 0}
        maxlog = max(logs.values()) if logs else 1
        colors = {c: _green(lv / maxlog) for c, lv in logs.items()}

        regions = []
        for label, codes in region_members:
            members = [c for c in codes if recs.get(c)]
            regions.append({
                'label': label,
                # A place tagged with several countries counts once per country, so a
                # region total can exceed its distinct-place count — a rounding-level
                # effect here (only 866 of ~413k contributed places carry >1 country).
                'records': sum(recs.get(c, 0) for c in codes),
                'datasets': len({d for c in members for d in dsets.get(c, ())}),
                'countries': len(members),
                'countries_total': len(codes),
                'empty': sorted((names.get(c) or c) for c in codes if not recs.get(c)),
            })
        regions.sort(key=lambda r: r['records'])
        max_region = max([r['records'] for r in regions] or [0]) or 1
        for r in regions:
            r['pct'] = round(100 * r['records'] / max_region, 1)

        # The outreach shortlist: countries big enough for the comparison to mean
        # something (≥ LARGE_KM2), ranked by records per 100,000 km². Ranking on raw
        # counts instead would just list micro-territories.
        LARGE_KM2 = 100_000
        sparse = []
        for code, km2 in areas.items():
            if km2 < LARGE_KM2 or code not in country_region:
                continue
            n = recs.get(code, 0)
            sparse.append({'code': code, 'name': names.get(code) or code,
                           'records': n, 'datasets': len(dsets.get(code, ())),
                           'region': country_region.get(code, '—'), 'km2': km2,
                           'density': round(100_000 * n / km2, 1)})
        sparse.sort(key=lambda r: (r['density'], r['name']))

        covered = set(recs)
        no_data = sorted((names.get(c) or c) for c in country_region if c not in covered)
        data = {
            'datasets': Dataset.objects.filter(
                public=True, core=False, authority=False).count(),
            'datasets_with_places': datasets_with_places,
            'records': total_records,
            'no_country': no_country,
            'no_country_pct': round(100 * no_country / total_records, 1) if total_records else 0,
            'countries': len(covered),
            'countries_total': len(country_region) or len(covered),
            'by_country': by_country,
            'country_records': recs,
            'country_colors': colors,
            'regions': regions,
            'top_countries': by_country[:12],
            'sparse_countries': sparse[:15],
            'no_data_countries': no_data,
            'no_data_count': len(no_data),
            'scale_max': max(recs.values()) if recs else 0,
        }
        cache.set(_COVERAGE_CACHE_KEY, data, 1800)
        return data
    except Exception as e:  # noqa: BLE001 — never let coverage break the analytics page
        logger.warning('Contributed-coverage aggregation failed: %s', e)
        return None


def _svg_area(series, w=820, h=190, pad=26):
    """Build SVG polyline/polygon point strings for a visitors-over-time area chart.
    series = [(label, value), …]. Returns None if empty."""
    n = len(series)
    if not n:
        return None
    maxv = max((v for _, v in series), default=0) or 1
    iw, ih = w - 2 * pad, h - 2 * pad
    pts = []
    for i, (_, v) in enumerate(series):
        x = pad + (iw * i / (n - 1) if n > 1 else iw / 2)
        y = pad + ih * (1 - (v / maxv))
        pts.append((round(x, 1), round(y, 1)))
    line = ' '.join(f'{x},{y}' for x, y in pts)
    area = f'{pad},{h - pad} {line} {round(pad + iw, 1)},{h - pad}'
    ticks = []  # a few x-axis date labels
    idxs = sorted(set([0, n // 2, n - 1]))
    for i in idxs:
        ticks.append({'x': pts[i][0], 'label': series[i][0]})
    return {'w': w, 'h': h, 'pad': pad, 'line': line, 'area': area, 'max': maxv, 'ticks': ticks}


def _svg_donut(rows):
    """Segments for an SVG donut. rows = [{label, visitors}]. Returns dict for the template."""
    total = sum(r['visitors'] for r in rows) or 1
    r = 55.0
    circ = 2 * 3.141592653589793 * r
    segs, offset = [], 0.0
    for i, row in enumerate(rows):
        frac = row['visitors'] / total
        dash = frac * circ
        segs.append({'label': row['label'], 'visitors': row['visitors'],
                     'pct': round(frac * 100, 1), 'color': DONUT_PALETTE[i % len(DONUT_PALETTE)],
                     'dash': f'{round(dash, 2)} {round(circ - dash, 2)}', 'offset': round(-offset, 2)})
        offset += dash
    return {'r': r, 'circ': round(circ, 2), 'segs': segs, 'total': total}


@login_required
def plausible_analyser_view(request):
    if not request.user.is_staff:
        raise Http404()
    period = request.GET.get('period', '30d')
    if period not in dict(PLAUSIBLE_PERIODS):
        period = '30d'
    interval = 'month' if period in ('6mo', '12mo') else 'date'

    # Build every Stats API call, then run them concurrently (up to 24 across 3 sites).
    tasks = {}
    for site, _, _, _ in PLAUSIBLE_SITES:
        tasks[('agg', site)] = ('aggregate', {'period': period,
            'metrics': 'visitors,pageviews,views_per_visit,visit_duration,bounce_rate'}, site)
        tasks[('ts', site)] = ('timeseries', {'period': period, 'metrics': 'visitors',
            'interval': interval}, site)
        tasks[('pages', site)] = ('breakdown', {'period': period, 'property': 'event:page',
            'metrics': 'visitors,pageviews', 'limit': 12}, site)
        tasks[('source', site)] = ('breakdown', {'period': period, 'property': 'visit:source',
            'metrics': 'visitors', 'limit': 30}, site)
        tasks[('country', site)] = ('breakdown', {'period': period, 'property': 'visit:country',
            'metrics': 'visitors', 'limit': 150}, site)
        tasks[('device', site)] = ('breakdown', {'period': period, 'property': 'visit:device',
            'metrics': 'visitors', 'limit': 10}, site)
        tasks[('browser', site)] = ('breakdown', {'period': period, 'property': 'visit:browser',
            'metrics': 'visitors', 'limit': 10}, site)
        tasks[('os', site)] = ('breakdown', {'period': period, 'property': 'visit:os',
            'metrics': 'visitors', 'limit': 10}, site)
    tasks[('events', PLAUSIBLE_MAIN)] = ('breakdown', {'period': period, 'property': 'event:name',
        'metrics': 'visitors,events', 'limit': 200}, PLAUSIBLE_MAIN)

    from concurrent.futures import ThreadPoolExecutor
    results, errors = {}, []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futs = {ex.submit(_plausible_get, path, params, site): key
                for key, (path, params, site) in tasks.items()}
        for fut, key in futs.items():
            try:
                results[key] = fut.result()
            except Exception as e:  # noqa: BLE001 - surface any thread error
                results[key] = (None, str(e))
    for res in results.values():
        if res[1]:
            errors.append(res[1])

    def rows_of(key):
        return ((results.get(key) or (None, None))[0] or {}).get('results', [])

    # ── Top-line: combined across sites + per-site cards ──
    def agg_val(site, metric):
        r = ((results.get(('agg', site)) or (None, None))[0] or {}).get('results', {}) or {}
        return (r.get(metric) or {}).get('value', 0) or 0

    per_site = []
    tot_visitors = tot_pageviews = 0
    for site, label, short, color in PLAUSIBLE_SITES:
        v, pv = agg_val(site, 'visitors'), agg_val(site, 'pageviews')
        tot_visitors += v
        tot_pageviews += pv
        per_site.append({'label': label, 'color': color, 'visitors': v, 'pageviews': pv})
    # visitor-weighted quality metrics (main site dominates; simple weighting is fine for an overview)
    def weighted(metric):
        num = sum(agg_val(s, metric) * (agg_val(s, 'visitors') or 0) for s, _, _, _ in PLAUSIBLE_SITES)
        return round(num / tot_visitors, 2) if tot_visitors else 0
    topline = {
        'visitors': tot_visitors, 'pageviews': tot_pageviews,
        'views_per_visit': weighted('views_per_visit'),
        'visit_duration': _fmt_duration(weighted('visit_duration')),
        'bounce_rate': round(weighted('bounce_rate')),
    }

    # ── Visitors over time: sum per date across sites → area chart ──
    ts_by_site = {}
    for site, _, _, _ in PLAUSIBLE_SITES:
        ts_by_site[site] = {row['date']: (row.get('visitors') or 0) for row in rows_of(('ts', site))}
    dates = [row['date'] for row in rows_of(('ts', PLAUSIBLE_MAIN))]
    combined = [(_fmt_chart_date(d, interval), sum(ts_by_site[s].get(d, 0) for s, _, _, _ in PLAUSIBLE_SITES))
                for d in dates]
    chart = _svg_area(combined)

    # ── Merged breakdowns (site-neutral: sum visitors by key) ──
    def merge(kind, key, limit):
        acc = {}
        for site, _, _, _ in PLAUSIBLE_SITES:
            for r in rows_of((kind, site)):
                k = r.get(key) or '(none)'
                acc[k] = acc.get(k, 0) + (r.get('visitors', 0) or 0)
        out = [{'label': k, 'visitors': v} for k, v in acc.items()]
        out.sort(key=lambda x: x['visitors'], reverse=True)
        return out[:limit]

    # Pages differ per site → keep the site as a prefix so they don't collide.
    pages = []
    for site, _, short, _ in PLAUSIBLE_SITES:
        for r in rows_of(('pages', site)):
            name = (f'{short} ' if short else '') + (r.get('page') or '/')
            pages.append({'label': name, 'visitors': r.get('visitors', 0) or 0,
                          'pageviews': r.get('pageviews', 0) or 0})
    pages.sort(key=lambda x: x['visitors'], reverse=True)
    pages = pages[:12]

    # Countries — attach full names (from WHG's own data) for tooltips, and a code→visitors map for
    # the choropleth. Codes come from Plausible as ISO-2 uppercase, matching the map library's regions.
    cnames = _country_names()
    countries_full = merge('country', 'country', 300)
    for row in countries_full:
        row['title'] = cnames.get(row['label'], '')
    country_map = {row['label']: row['visitors'] for row in countries_full if len(row['label']) == 2}
    # Compute the choropleth fill colour per country server-side (direct hex → jsVectorMap sets it
    # verbatim; its own scale/normalise was unreliable). Colour by LOG(visitors) so one hub country
    # doesn't flatten the gradient; raw counts still feed the tooltip + Countries table.
    import math

    def _blue(t):  # 0→light, 1→dark, over #bfdbfe … #1e3a8a
        t = max(0.0, min(1.0, t))
        r = round(191 + (30 - 191) * t); g = round(219 + (58 - 219) * t); b = round(254 + (138 - 254) * t)
        return f'#{r:02x}{g:02x}{b:02x}'
    logs = {code: math.log10(v + 1) for code, v in country_map.items() if v > 0}
    maxlog = max(logs.values()) if logs else 1
    country_colors = {code: _blue(lv / maxlog) for code, lv in logs.items()}

    # ── WHG platform figures: registered users (DB), reach, and index size —
    #    for the admin view and the grant-abstract "Building on a Prototype"
    #    numbers, with a per-period registered-vs-signed-out split. ──
    from django.utils import timezone
    from datetime import timedelta
    from django.db.models import Sum
    from django.contrib.auth import get_user_model
    UserModel = get_user_model()

    # Window start matching the selected Plausible period (approx for DB filters).
    if period == 'month':
        since = timezone.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        _days = {'7d': 7, '30d': 30, '6mo': 182, '12mo': 365}.get(period, 30)
        since = timezone.now() - timedelta(days=_days)

    reg = UserModel.objects.filter(is_active=True)
    users = {
        'total': reg.count(),
        'new_in_period': reg.filter(date_joined__gte=since).count(),
        'active_in_period': reg.filter(last_login__gte=since).count(),
        # Plausible is cookieless, so its visitor count can't identify accounts;
        # it is the whole audience, predominantly signed-out.
        'visitors_in_period': tot_visitors,
    }
    # Distinct countries visitors came from (ISO-2 codes with >0 visitors).
    visitor_countries = len(country_map)

    # Index size ("named entities"): sum of per-authority record counts from the
    # gazetteer registry, kept current by the indexing pipeline's inventory push.
    named_entities_display = None
    source_gazetteers = None
    try:
        from api.models import GazetteerRegistryEntry
        _auth = GazetteerRegistryEntry.objects.filter(entry_class='authority').exclude(id='whg')
        _n = _auth.aggregate(n=Sum('record_count'))['n'] or 0
        named_entities_display = f"{_n:,}" if _n else None
        source_gazetteers = _auth.count()
    except Exception:  # noqa: BLE001 — never let the registry break the analytics page
        pass

    # Geographic coverage of contributed datasets — period-independent (it describes the
    # corpus, not the traffic), so it is computed once and cached.
    coverage = _contributed_coverage()

    sections = [
        {'title': 'Top pages', 'icon': 'fa-file-lines', 'pv': True, 'rows': pages},
        {'title': 'Top sources', 'icon': 'fa-arrow-right-to-bracket', 'rows': merge('source', 'source', 8)},
        {'title': 'Countries', 'icon': 'fa-earth-americas', 'rows': countries_full[:8]},
        {'title': 'Operating systems', 'icon': 'fa-gear', 'rows': merge('os', 'os', 6)},
    ]
    for sec in sections:
        sec['max'] = max([row['visitors'] for row in sec['rows']] or [0]) or 1
    donuts = [
        {'title': 'Devices', 'icon': 'fa-desktop', 'donut': _svg_donut(merge('device', 'device', 6))},
        {'title': 'Browsers', 'icon': 'fa-window-maximize', 'donut': _svg_donut(merge('browser', 'browser', 6))},
    ]

    # ── Map your Data funnel (main-site custom events) ──
    by_name = {r['name']: r for r in rows_of(('events', PLAUSIBLE_MAIN))}

    def mk(name, label):
        r = by_name.get(name) or {}
        return {'name': name, 'label': label,
                'visitors': r.get('visitors', 0) or 0, 'events': r.get('events', 0) or 0}

    funnel = [mk(n, l) for n, l in MYD_FUNNEL]
    base_v = funnel[0]['visitors'] if funnel else 0
    prev_v = None
    for step in funnel:
        step['pct'] = round(100 * step['visitors'] / base_v, 1) if base_v else 0
        step['drop'] = round(100 * (prev_v - step['visitors']) / prev_v, 1) if prev_v else None
        prev_v = step['visitors']
    others = [mk(n, l) for n, l in MYD_OTHER]

    return render(request, 'main/plausible_analyser.html', {
        'period': period, 'periods': PLAUSIBLE_PERIODS,
        'period_label': dict(PLAUSIBLE_PERIODS).get(period, period),
        'users': users, 'visitor_countries': visitor_countries,
        'named_entities_display': named_entities_display,
        'source_gazetteers': source_gazetteers,
        'repo_url': 'https://github.com/WorldHistoricalGazetteer/website',
        'topline': topline, 'per_site': per_site, 'chart': chart,
        'sections': sections, 'donuts': donuts,
        'country_map': country_map, 'country_colors': country_colors,
        'coverage': coverage,
        'funnel': funnel, 'others': others, 'base_v': base_v,
        'error': errors[0] if errors else None,
        'plausible_url': f"{getattr(settings, 'PLAUSIBLE_BASE_URL', '')}/{PLAUSIBLE_MAIN}",
    })


# gets the correct view based on user group
@login_required
def dashboard_redirect(request):
    if request.user.groups.filter(name='whg_admins').exists():
        return redirect('dashboard-admin')
    else:
        return redirect('dashboard-user')


# all-purpose for admins
@login_required
def dashboard_admin_view(request):
    user = request.user
    is_admin = user.groups.filter(name='whg_admins').exists()
    is_leader = user.groups.filter(name='group_leaders').exists()
    django_groups = [group.name for group in user.groups.all()]

    user_datasets_count = Dataset.objects.filter(owner=user.id).count()
    user_collections_count = Collection.objects.filter(owner=user).count()

    # section = request.GET.get('section')
    section = request.GET.get('section', 'datasets')

    #
    datasets = get_objects_for_user(Dataset, request.user, {}, is_admin)
    datasets = datasets.order_by('create_date')

    collections = get_objects_for_user(Collection, request.user, {}, is_admin)
    areas = get_objects_for_user(Area, request.user, {'type': ['predefined', 'country']}, is_admin)
    groups_member = CollectionGroup.objects.filter(members__user=user)
    groups_led = CollectionGroup.objects.filter(owner=user)

    context = {
        'datasets': datasets,
        'collections': collections,
        'areas': areas,
        'has_datasets': user_datasets_count > 0,
        'has_collections': user_collections_count > 0,
        'section': section,
        'django_groups': django_groups,
        'groups_member': groups_member,
        'groups_led': groups_led,
        'is_admin': is_admin,
        'is_leader': is_leader,
    }
    return render(request, 'main/dashboard_admin.html', context)


# for non-admins
@login_required
def dashboard_user_view(request):
    user = request.user
    is_admin = user.groups.filter(name='whg_admins').exists()
    is_leader = user.groups.filter(name='group_leaders').exists()
    django_groups = [group.name for group in user.groups.all()]

    user_datasets_count = Dataset.objects.filter(owner=user.id).count()
    user_collections_count = Collection.objects.filter(owner=user).count()
    user_areas_count = Area.objects.filter(owner=user).count()
    user_resources_count = Resource.objects.filter(owner=user).count()
    user_downloads_count = DownloadFile.objects.filter(user=user).count()

    section = request.GET.get('section')

    datasets = get_objects_for_user(Dataset, request.user, {'owner': user}, False)
    collections = get_objects_for_user(Collection, request.user, {'owner': user}, False)
    areas = get_objects_for_user(Area, request.user, {'owner': user}, False)
    resources = get_objects_for_user(Resource, request.user, {'owner': user}, False)
    downloads = get_objects_for_user(DownloadFile, request.user, {'user': user}, False)
    groups_member = CollectionGroup.objects.filter(members__user=user)
    groups_led = CollectionGroup.objects.filter(owner=user)

    context = {
        'datasets': datasets,
        'collections': collections,
        'areas': areas,
        'resources': resources,
        'downloads': downloads,
        'has_datasets': datasets.count() > 0,
        'has_collections': collections.count() > 0,
        'has_areas': user_areas_count > 0,
        'has_resources': user_resources_count > 0,
        'has_downloads': user_downloads_count > 0,
        'section': section,
        'django_groups': django_groups,
        'groups_member': groups_member,
        'groups_led': groups_led,
        'is_admin': is_admin,
        'is_leader': is_leader,
        'box_titles': ['Datasets', 'Place Collections', 'Dataset Collections', 'Study Areas', 'Groups'],

    }
    return render(request, 'main/dashboard_user.html', context)


# @csrf_exempt
# def home_modal(request):
#   page = request.POST['page']
#   context = {'v1': 'hello there'}
#   url = 'home/' + page + '.html'
#   print('home_modal() url:', url)
#   return render(request, url, context)

# main/views.py
from django.shortcuts import render


def trigger_500_error(request):
    # This will simulate a server error
    raise Exception("Simulated server error")


def server_error_view(request):
    import traceback

    try:
        # Capture request details
        path = request.get_full_path().lstrip('/')
        url = f"{settings.URL_FRONT}{path}"
        method = request.method
        headers = dict(request.headers)
        headers_pretty = json.dumps(headers, indent=2)
        body = request.body.decode('utf-8', errors='replace')
        body_formatted = f"```{body}```" if body else 'None'

        # Capture authenticated user details
        if request.user and request.user.is_authenticated:
            authenticated_user = f'{request.user.username} ({request.user.email})'
        else:
            authenticated_user = 'None'

        # Capture exception details
        exc_type, exc_value, exc_traceback = sys.exc_info()
        exc_type = exc_type.__name__ if exc_type else 'N/A'
        exc_message = str(exc_value) if exc_value else 'N/A'
        tb = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) if exc_traceback else 'N/A'

        # Prepare Zulip message
        message = (
            f"**{exc_type.upper()}: {exc_message.upper()}**\n\n"
            f"**URL:** {url}\n"
            f"**Method:** {method}\n"
            f"**Authenticated User:** {authenticated_user}\n\n"
            f"**Headers:**\n```json\n{headers_pretty}\n```\n\n"
            f"**Body:** {body_formatted}\n\n"
            f"**Traceback:**\n```python\n{tb}\n```"
        )

        zulip_notification(
            message,
            stream="website-errors",
            topic=f"{exc_type}: {url[:50]}"
        )

    except Exception as e:
        # Handle exceptions that occur while sending the message to Zulip (avoid infinite loop!)
        logger.debug(f"Error sending message to Zulip: {e}")

        # Return a user-friendly error page
    context = {  # Rendering of this message is not currently implemented
        'error_message': 'An unexpected error occurred. Our team has been notified and is looking into the issue. Please try again later.'
    }
    try:
        return render(request, "main/500.html", context, status=500)
    except Exception as e:
        # In case rendering the error page fails, return a plain HTTP response
        return HttpResponseServerError('An unexpected error occurred and we were unable to handle it properly.')


def custom_404(request, exception):
    logger.debug(f'404 error request: {request.GET.__dict__}')
    return render(request, 'main/404.html', {}, status=404)


def is_url(url):
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False


"""
  create link associated with instance of various models, so far:
  Collection, CollectionGroup, TraceAnnotation, Place
"""


def create_link(request, *args, **kwargs):
    if request.method == 'POST':
        model = request.POST['model']
        objectid = request.POST['objectid']

        uri = request.POST['uri']
        if not is_url(uri):
            return JsonResponse({'status': 'failed', 'result': 'bad uri'}, safe=False)

        label = request.POST['label']
        link_type = request.POST['link_type']
        # license = request.POST['license']

        # Collection or CollectionGroup
        # from django.apps import apps
        Model = apps.get_model(f"collection.{model}")
        model_str = model.lower() if model == 'Collection' else 'collection_group'
        obj = Model.objects.get(id=objectid)
        gotlink = obj.related_links.filter(uri=uri)
        # gotlink = obj.links.filter(uri=uri)
        status, msg = ['', '']
        # columns in Links table
        # collection_id, collection_group_id, trace_annotation_id, place_id
        if not gotlink:
            try:
                link = Link.objects.create(
                    **{model_str: obj},  # instance identifier
                    uri=uri,
                    label=label,
                    link_type=link_type
                )
                result = {'uri': link.uri, 'label': link.label,
                          'link_type': link.link_type,
                          'link_icon': link.get_link_type_display(),
                          'id': link.id}
                status = "ok"
            except:
                logger.debug(f'failed: {sys.exc_info()}')
                status = "failed"
                result = "Link *not* created...why?"
        else:
            result = 'dupe'
        return JsonResponse({'status': status, 'result': result}, safe=False)


def remove_link(request, *args, **kwargs):
    # print('kwargs', kwargs)
    link = Link.objects.get(id=kwargs['id'])
    # link = CollectionLink.objects.get(id=kwargs['id'])
    link.delete()
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


# TODO on cron in v3?
def statusView(request):
    context = {"status_site": "??",
               "status_database": "??",
               "status_index": "??"}

    # database
    try:
        place = get_object_or_404(Place, id=81011)
        context["status_database"] = "up" if place.title == 'Abydos' else 'error'
    except:
        context["status_database"] = "down"

    # celery recon task — bounded wait so the status page can't hang if a worker
    # is slow/unavailable (place#120: blocking .get() with no timeout returned no
    # HTTP response at all).
    try:
        result = testAdd.delay(8, 8)
        context["status_tasks"] = "up" if result.get(timeout=10) == 16 else 'error'
    except:
        context["status_tasks"] = "down"

    return render(request, "main/status.html", {"context": context})


def volunteer_view(request):
    if request.method == 'POST':
        form = VolunteerForm(request.POST)
        if form.is_valid():
            # Extract the data from the form
            dataset_id = form.cleaned_data.get('dataset_id')
            dataset = Dataset.objects.get(id=dataset_id)

            WHGmail(context={
                'template': 'volunteer_offer_owner',
                'subject': 'WHG Volunteer to Review',
                'to_email': dataset.owner.email,
                'bcc': [settings.DEFAULT_FROM_EDITORIAL],
                'greeting_name': dataset.owner.name,
                'volunteer_greeting': request.user.name,
                'volunteer_username': request.user.username,
                'volunteer_email': request.user.email,
                'message': form.cleaned_data['message'],
                'dataset_title': dataset.title,
                'dataset_id': dataset.id,
            })

            return redirect('/success?return=' + request.GET.get('from', '/'))
    else:
        form = VolunteerForm()

    return render(request, 'volunteer.html', {'form': form})


def contact_modal_view(request):
    # Prepare initial form data
    initial_data = {}
    if request.user.is_authenticated:
        initial_data['from_email'] = request.user.email
        initial_data['name'] = request.user.name
        initial_data['username'] = request.user.username
        initial_data['subject'] = request.GET.get('subject')

    form = ContactForm(request.POST or None, initial=initial_data)

    show_name_field = not request.user.is_authenticated
    show_email_field = not request.user.is_authenticated or not request.user.email

    context = {
        'form': form,
        'show_name_field': show_name_field,
        'show_email_field': show_email_field,
        'TURNSTILE_SITE_KEY': settings.TURNSTILE_SITE_KEY
    }

    if request.method == 'POST':
        # Only check Turnstile for anonymous users
        if not request.user.is_authenticated:
            token = request.POST.get("cf-turnstile-response")
            if not token:
                form.add_error(None, "Please verify that you are human.")
                return render(request, 'main/contact_modal.html', context)

            try:
                resp = requests.post(
                    "https://challenges.cloudflare.com/turnstile/v0/siteverify",
                    data={
                        "secret": settings.TURNSTILE_SECRET_KEY,
                        "response": token,
                        "remoteip": request.META.get("REMOTE_ADDR", ""),
                    },
                    timeout=5,
                )
                result = resp.json()
            except requests.RequestException as e:
                logger.error("Error contacting Turnstile API: %s", e)
                form.add_error(None, "Unable to verify CAPTCHA. Please try again.")
                return render(request, "main/contact_modal.html", context)

            if not result.get("success"):
                form.add_error(None, "CAPTCHA verification failed. Please try again.")
                return render(request, "main/contact_modal.html", context)

        if form.is_valid():
            try:
                name = form.cleaned_data['name']
                username = form.cleaned_data.get('username')
                user_subject = form.cleaned_data['subject']
                user_email = form.cleaned_data['from_email']
                user_message = form.cleaned_data['message']
                page_url = request.POST.get('page_url', 'No page URL provided')
                sent_on = timezone.now().strftime('%Y-%m-%d %H:%M:%S')

                # Prepare reply mailto link
                reply_subject = f"Re: {user_subject}"
                reply_body = (
                    f"\n\n\n--- Original message ---\n"
                    f"From: {name} ({username or 'Unauthenticated User'})\n"
                    f"Email: {user_email}\n"
                    f"Sent on: {sent_on}\n"
                    f"Page URL: {'Home Page' if page_url == '/' else page_url}\n\n"
                    f"{user_message}"
                )
                encoded_subject = urllib.parse.quote(reply_subject)
                encoded_body = urllib.parse.quote(reply_body)
                reply_link = f"mailto:{user_email}?subject={encoded_subject}&body={encoded_body}"

                message = (
                    f"*Subject:* {user_subject}\n"
                    f"*From:* {name} (username: {username or 'N/A - Unauthenticated User'})\n"
                    f"*Email Address:* {user_email}\n"
                    f"*Sent on:* {sent_on}\n"
                    f"*Message:* ```{user_message}```\n"
                    f"*Page URL:* {'Home Page' if page_url == '/' else page_url}\n"
                    f"[🟩 Reply via Email]({reply_link})\n"
                    f"----------------------------------------\n\n"
                )

                zulip_notification(message, stream="website-contact", topic=user_subject)

                messages.success(request, "Your message has been sent successfully.")
                return JsonResponse({'success': True})

            except BadHeaderError:
                return HttpResponse('Invalid header found.')

            except Exception as e:
                logger.exception("Error processing contact form: %s", e)
                messages.error(request, "There was an error sending your message. Please try again later.")
                return JsonResponse({'success': False, 'error': str(e)})

        else:
            logger.debug('form.errors: %s', form.errors)

    # GET request or form errors
    return render(request, 'main/contact_modal.html', context)


def contactSuccessView(request, *args, **kwargs):
    returnurl = request.GET.get('return')
    return HttpResponse(
        '<div style="font-family:sans-serif;margin-top:3rem; width:50%; margin-left:auto; margin-right:auto;"><h4>Thank you for your message! We will reply soon.</h4><p><a href="' + returnurl + '">Return</a><p></div>')


def license_view(request):
    return render(request, 'main/license.html')


def terms_of_use_view(request):
    return render(request, 'main/terms_of_use.html')


def privacy_policy_view(request):
    return render(request, 'main/privacy_policy.html')


# ── Email invitations (place#155) ────────────────────────────────────────────

@login_required
def invite_modal_view(request):
    """Send someone a WHG link, or an invitation to register.

    Loaded into the standard ``data-whg-modal`` dialog. GET renders the form; POST
    sends and returns JSON (which whg-modal.js turns into the confirmation panel) or
    re-renders the form with errors.

    The recipient's address is used to send the message and then discarded — see
    :mod:`main.invitations` for the full set of guarantees.
    """
    from main import invitations

    kind = request.POST.get('kind') or request.GET.get('kind') or 'view'
    if kind not in ('view', 'join'):
        kind = 'view'
    target_url = request.POST.get('target_url') or request.GET.get('url') or ''

    def _context(form):
        return {
            'form': form,
            'kind': kind,
            'target_url': target_url,
            'remaining': invitations.sender_remaining(request.user),
            'daily_limit': invitations.DAILY_LIMIT,
            'can_send': request.user.has_verified_email,
        }

    if request.method != 'POST':
        form = InviteForm(initial={'kind': kind, 'target_url': target_url})
        return render(request, 'main/invite_modal.html', _context(form))

    form = InviteForm(request.POST)
    if form.is_valid():
        try:
            remaining = invitations.send_invitation(
                request,
                kind=form.cleaned_data['kind'],
                to_email=form.cleaned_data['to_email'],
                target_url=form.cleaned_data.get('target_url'),
            )
            return JsonResponse({'success': True, 'remaining': remaining})
        except (invitations.InvitationError, ValidationError) as e:
            form.add_error(None, e.messages[0] if hasattr(e, 'messages') else str(e))

    return render(request, 'main/invite_modal.html', _context(form))


@csrf_exempt
def invite_unsubscribe_view(request, token):
    """"Don't contact me again", from the link (and List-Unsubscribe header) in an
    invitation email.

    The token carries only the signed *hash* of the address, so neither the email nor
    this URL ever contains the address itself. Acting on GET means a mail-scanner
    prefetch could suppress someone who never clicked — but that failure mode is
    "this person receives no invitations", which is the safe direction to fail in.
    ``csrf_exempt`` supports RFC 8058 one-click POST from mail clients.
    """
    from main import invitations

    recipient_hash = invitations.read_unsubscribe_token(token)
    if not recipient_hash:
        return render(request, 'main/invite_unsubscribed.html', {'valid': False}, status=400)

    invitations.suppress(recipient_hash)
    return render(request, 'main/invite_unsubscribed.html', {'valid': True})


class CommentCreateView(BSModalCreateView):
    template_name = 'main/create_comment.html'
    form_class = CommentModalForm
    success_message = 'Success: Comment was created.'
    success_url = reverse_lazy('')

    def form_valid(self, form, **kwargs):
        form.instance.user = self.request.user
        place = get_object_or_404(Place, id=self.kwargs['rec_id'])
        form.instance.place_id = place
        return super(CommentCreateView, self).form_valid(form)

    def get_context_data(self, *args, **kwargs):
        context = super(CommentCreateView, self).get_context_data(*args, **kwargs)
        context['place_id'] = self.kwargs['rec_id']
        return context

    # ** ADDED for referrer redirect
    def get_form_kwargs(self, **kwargs):
        kwargs = super(CommentCreateView, self).get_form_kwargs()
        redirect = self.request.GET.get('next')
        if redirect is not None:
            self.success_url = redirect
        else:
            self.success_url = '/mydata'
        # print('cleaned_data in get_form_kwargs()',form.cleaned_data)
        if redirect:
            if 'initial' in kwargs.keys():
                kwargs['initial'].update({'next': redirect})
            else:
                kwargs['initial'] = {'next': redirect}
        return kwargs
    # ** END


@login_required
@require_POST
def handle_comment(request):
    try:
        comment_text = escape(request.POST.get('commentText'))
        tag = request.POST.get('tag')
        place_id = request.POST.get('placeId')
        delete_id = request.POST.get('deleteId')

        if delete_id:
            # Check that comment's creator is the current request.user
            get_object_or_404(Comment, id=delete_id, user=request.user).delete()

            return JsonResponse({'success': True, 'message': f'Comment #{delete_id} deleted successfully'})

        else:

            place = get_object_or_404(Place, id=place_id)

            comment = Comment.objects.create(user=request.user, note=comment_text, tag=tag, place_id=place)

            comment_data = {
                'id': comment.id,
                'user': comment.user.id,
                'note': comment.note,
                'tag': comment.tag,
                'place_id': comment.place_id.id,
                'created': comment.created.strftime('%Y-%m-%d %H:%M:%S')
            }

            return JsonResponse(
                {'success': True, 'message': f'Comment #{comment.id} created successfully', 'comment': comment_data})

    except Exception as e:
        return JsonResponse({'success': False, 'error': str(e)})
