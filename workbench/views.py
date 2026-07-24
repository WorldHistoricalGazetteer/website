"""
Collaborative Workbench API (place#112), mounted under /reconciliation/.

All endpoints require login + ``user.can_access_beta`` (same gate as the Map-your-Data tool), except
the Phase-0 read-only ``shared/<token>/`` endpoint, whose capability is the unguessable token.

Plain Django function views returning JSON — session auth + CSRF (the client sends ``X-CSRFToken``,
mirroring the existing ``postReconcile`` call in reconciliation.js).
"""
import json
import logging
import re
import time
import uuid

import jwt
import requests
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.http import JsonResponse, Http404
from django.shortcuts import get_object_or_404
from django.utils.text import slugify
from django.views.decorators.http import require_http_methods

from . import doctypes
from .merge import merge_snapshots
from .models import (Team, TeamMember, WorkbenchProject, ProjectSnapshot, RecordSuggestion,
                     ROLE_OWNER, ROLE_EDITOR, ROLE_VIEWER, EDIT_ROLES, TEAM_ROLES,
                     DOC_RECONCILIATION, DOC_TYPES)

logger = logging.getLogger(__name__)
User = get_user_model()

SNAPSHOT_HISTORY = 25  # ProjectSnapshot rows kept per project (merge ancestor + backup)
VALID_ROLES = {r[0] for r in TEAM_ROLES}


# ── helpers ─────────────────────────────────────────────────────────────────
def _beta_required(view):
    """Wrap a view so non-beta users get a 404 (feature existence is not disclosed)."""
    def wrapped(request, *args, **kwargs):
        if not request.user.can_access_beta:
            raise Http404()
        return view(request, *args, **kwargs)
    wrapped.__name__ = view.__name__
    return wrapped


def _body(request):
    try:
        return json.loads(request.body or b'{}')
    except (ValueError, TypeError):
        return {}


def _err(msg, status=400):
    return JsonResponse({'error': msg}, status=status)


def _project_dict(p, role):
    return {
        'id': str(p.id), 'title': p.title, 'team': p.team_id, 'team_title': p.team.title,
        'role': role, 'version': p.version, 'status': p.status, 'doc_type': p.doc_type,
        'updated': p.updated.isoformat(), 'shared': bool(p.public_token),
        'is_personal_team': p.team.is_personal,
        'published_collection': p.published_collection_id,
    }


def prune_snapshots(project):
    """Keep only the most recent SNAPSHOT_HISTORY snapshot rows for a project."""
    keep = list(project.snapshots.values_list('id', flat=True)[:SNAPSHOT_HISTORY])
    project.snapshots.exclude(id__in=keep).delete()


def _find_user(identifier):
    """Resolve an invitee by username (exact) or email.

    `email` is a non-deterministic ``EncryptedTextField``, so ``filter(email=…)`` can NEVER match the
    ciphertext — email invites always failed and the owner had to fall back to the (unfriendly) username
    (snag #153). When the identifier looks like an email we instead decrypt-and-compare: invites are a
    rare, owner-initiated action and the user table is small (~1.3k), so the scan cost is acceptable.
    """
    identifier = (identifier or '').strip()
    if not identifier:
        return None
    u = User.objects.filter(username__iexact=identifier).first()
    if u:
        return u
    if '@' in identifier:
        target = identifier.lower()
        for cand in User.objects.exclude(email__isnull=True).only('id', 'email').iterator():
            try:
                if cand.email and cand.email.strip().lower() == target:
                    return cand
            except Exception:
                continue
    return None


# ── projects ──────────────────────────────────────────────────────────────────
@login_required
@_beta_required
@require_http_methods(['GET', 'POST'])
def projects(request):
    if request.method == 'GET':
        team_ids = TeamMember.objects.filter(user=request.user).values_list('team_id', flat=True)
        qs = WorkbenchProject.objects.filter(team_id__in=list(team_ids)).select_related('team')
        out = [_project_dict(p, p.role_for(request.user)) for p in qs]
        return JsonResponse({'projects': out})

    # POST — create a server project from a client snapshot.
    data = _body(request)
    snapshot = data.get('snapshot')
    if not isinstance(snapshot, dict):
        return _err('snapshot (object) is required')

    # doc_type: default 'reconciliation' (back-compat). Reject unknown types and the v4 placeholders
    # (route/network are reserved but their creation is gated OFF — plan §4.4), then validate the
    # snapshot against the doc-type's schema so a broken/hostile client can't seed a malformed doc.
    doc_type = data.get('doc_type') or DOC_RECONCILIATION
    dt = doctypes.get(doc_type)
    if dt is None:
        return _err(f'unknown doc_type: {doc_type}')
    if not dt.enabled:
        return _err(f'the “{dt.label}” doc-type is not available yet', 403)
    errs = dt.validate(snapshot)
    if errs:
        return _err('; '.join(errs))

    title = (data.get('title') or snapshot.get('title') or snapshot.get('fileName')
             or 'Untitled project')[:300]
    team = _resolve_target_team(request.user, data.get('team'))
    if team is None:
        return _err('you are not an editor of that team', 403)
    with transaction.atomic():
        p = WorkbenchProject.objects.create(
            team=team, title=title, created_by=request.user, snapshot=snapshot, version=1,
            status='draft', doc_type=doc_type)
        ProjectSnapshot.objects.create(project=p, version=1, snapshot=snapshot,
                                       created_by=request.user)
    return JsonResponse({'id': str(p.id), 'version': p.version, 'team': team.id,
                         'doc_type': p.doc_type,
                         'role': ROLE_OWNER if team.is_personal else p.role_for(request.user)},
                        status=201)


def _resolve_target_team(user, team_id):
    """Return the Team to create a project in: an explicit team the user can edit, else personal."""
    if team_id in (None, '', 'personal'):
        return Team.personal_for(user)
    team = Team.objects.filter(id=team_id).first()
    if not team:
        return None
    return team if team.role_for(user) in EDIT_ROLES else None


@login_required
@_beta_required
@require_http_methods(['GET', 'PUT', 'DELETE'])
def project_detail(request, pid):
    p = get_object_or_404(WorkbenchProject.objects.select_related('team'), pk=pid)
    role = p.role_for(request.user)
    if role is None:
        return _err('not a member of this project’s team', 403)

    if request.method == 'GET':
        # can_apply: may THIS user publish this working copy directly to its published source? For a
        # record correction that means edit rights on the record's gazetteer; a non-owner sees the
        # editor's "Submit suggestion" path instead (plan-record-suggestions §3/§4).
        can_apply = True
        if p.doc_type == 'place_record':
            from places.models import Place
            pl = Place.objects.filter(pk=(p.snapshot or {}).get('record_id')).select_related('dataset').first()
            can_apply = bool(pl and pl.dataset.can_edit(request.user))
        return JsonResponse({'id': str(p.id), 'title': p.title, 'team': p.team_id,
                             'team_title': p.team.title, 'team_personal': p.team.is_personal,
                             'role': role, 'version': p.version, 'status': p.status,
                             'doc_type': p.doc_type, 'shared': bool(p.public_token),
                             'published_collection': p.published_collection_id,
                             'can_apply': can_apply, 'snapshot': p.snapshot})

    if request.method == 'DELETE':
        if role != ROLE_OWNER:
            return _err('only an owner can delete a project', 403)
        p.delete()
        return JsonResponse({'ok': True})

    # PUT — optimistic-lock push.
    if role not in EDIT_ROLES:
        return _err('view-only: your role cannot edit this project', 403)
    data = _body(request)
    snapshot = data.get('snapshot')
    base_version = data.get('base_version')
    if not isinstance(snapshot, dict):
        return _err('snapshot (object) is required')
    if not isinstance(base_version, int):
        return _err('base_version (int) is required')
    return _push(request, p, snapshot, base_version)


def _push(request, p, snapshot, base_version):
    with transaction.atomic():
        p = WorkbenchProject.objects.select_for_update().get(pk=p.pk)

        # Fast-forward: client is current.
        if base_version == p.version:
            return _commit(request, p, snapshot, status='ok')

        # Client is stale → attempt a three-way merge against the ancestor snapshot.
        ancestor = p.snapshots.filter(version=base_version).first()
        if ancestor is None:
            # Ancestor pruned/unknown — client must reload and re-apply.
            return JsonResponse({'status': 'stale', 'version': p.version,
                                 'snapshot': p.snapshot}, status=409)

        merged, conflicts = merge_snapshots(ancestor.snapshot, snapshot, p.snapshot)
        if conflicts:
            # Nothing is committed. ``merged`` already carries the client's non-conflicting edits
            # auto-merged with the server value kept for each conflict, so the client can resolve
            # from it (overriding only the conflicts it wants "mine" for) and re-push — its other
            # edits are not lost.
            return JsonResponse({'status': 'conflict', 'version': p.version,
                                 'conflicts': conflicts, 'merged': merged}, status=409)
        return _commit(request, p, merged, status='merged')


def _commit(request, p, snapshot, status):
    title = snapshot.get('fileName') or snapshot.get('title')
    if title:
        p.title = str(title)[:300]
    p.snapshot = snapshot
    p.version += 1
    p.save(update_fields=['snapshot', 'version', 'title', 'updated'])
    ProjectSnapshot.objects.create(project=p, version=p.version, snapshot=snapshot,
                                   created_by=request.user)
    prune_snapshots(p)
    return JsonResponse({'status': status, 'version': p.version,
                         'snapshot': snapshot if status == 'merged' else None})


# ── sharing (Phase 0) ─────────────────────────────────────────────────────────
@login_required
@_beta_required
@require_http_methods(['POST', 'DELETE'])
def project_share(request, pid):
    p = get_object_or_404(WorkbenchProject, pk=pid)
    if p.role_for(request.user) not in EDIT_ROLES:
        return _err('only an editor or owner can share a project', 403)
    if request.method == 'DELETE':
        p.public_token = None
        if p.status == 'shared':
            p.status = 'draft'
        p.save(update_fields=['public_token', 'status', 'updated'])
        return JsonResponse({'ok': True, 'shared': False})
    if not p.public_token:
        p.public_token = uuid.uuid4()
    if p.status == 'draft':
        p.status = 'shared'
    p.save(update_fields=['public_token', 'status', 'updated'])
    url = request.build_absolute_uri(f'/reconciliation/?shared={p.public_token}')
    return JsonResponse({'ok': True, 'shared': True, 'token': str(p.public_token), 'url': url})


@require_http_methods(['GET'])
def shared_snapshot(request, token):
    """Phase-0 read-only fetch. No auth — the token is the capability. Recipients import a *copy*."""
    try:
        uuid.UUID(str(token))
    except (ValueError, TypeError):
        raise Http404()
    p = WorkbenchProject.objects.filter(public_token=token).first()
    if not p:
        raise Http404()
    return JsonResponse({'title': p.title, 'snapshot': p.snapshot, 'version': p.version,
                         'read_only': True})


# ── gazetteer (dataset) search for the Gazetteer Group editor ───────────────────
@login_required
@_beta_required
@require_http_methods(['GET'])
def dataset_search(request):
    """Search public, non-core published gazetteers (``datasets.Dataset``) for the Gazetteer Group
    editor's member picker. Returns a lean ``{datasets:[{id, title, label, description}]}`` (title/
    description match, capped). Beta-gated like the rest of the Workbench."""
    from django.db.models import Q
    from datasets.models import Dataset
    q = (request.GET.get('q') or '').strip()
    qs = Dataset.objects.filter(public=True, core=False)
    if q:
        qs = qs.filter(Q(title__icontains=q) | Q(description__icontains=q))
    qs = qs.order_by('title')[:20]
    out = []
    for d in qs:
        # Centroid of the stored bbox (WGS84 Polygon) → [lng, lat], for colocation ranking on the client.
        c = None
        if getattr(d, 'bbox', None):
            try:
                pt = d.bbox.centroid
                c = [pt.x, pt.y]
            except Exception:
                c = None
        out.append({'id': d.id, 'title': d.title, 'label': d.label,
                    'description': (d.description or '')[:500], 'centroid': c})
    return JsonResponse({'datasets': out})


# ── publish-back (plan §9) ─────────────────────────────────────────────────────
@login_required
@_beta_required
@require_http_methods(['POST'])
def project_publish(request, pid):
    """Publish (or re-publish) a project's snapshot into its canonical model via the doc-type's
    ``publish_target`` (workbench/publish.py). Editor/owner only. Returns the publish summary
    (e.g. ``{collection_id, added, unresolved}``). The published *artefact* then follows the existing
    public-visibility rules — only the authoring tool/route is beta-gated (plan §2.5)."""
    from .publish import PublishError, ConflictError
    p = get_object_or_404(WorkbenchProject.objects.select_related('team'), pk=pid)
    if p.role_for(request.user) not in EDIT_ROLES:
        return _err('only an editor or owner can publish a project', 403)
    dt = doctypes.get(p.doc_type)
    if dt is None or dt.publish_fn is None:
        return _err('this doc-type cannot be published from the Workbench', 400)
    # Direct publish-back to a published gazetteer needs edit rights on THAT gazetteer — not just team
    # membership on the working copy (record check-out is open to any beta user, who may only *suggest*).
    # This is the single enforcement point for "non-owners can suggest but not apply" (plan-record-
    # suggestions §3). Collections/Groups keep their own model-level owner checks in publish.py.
    if p.doc_type == 'place_record':
        from places.models import Place
        place = Place.objects.filter(pk=(p.snapshot or {}).get('record_id')).select_related('dataset').first()
        if not place or not place.dataset.can_edit(request.user):
            return _err('You can suggest a correction to this record, but only its gazetteer’s owner or '
                        'WHG staff can publish it directly. Use “Submit suggestion” instead.', 403)
    elif p.doc_type == 'dataset_edit':
        from datasets.models import Dataset
        ds = Dataset.objects.filter(pk=(p.snapshot or {}).get('dataset_id')).first()
        if not ds or not (request.user.is_staff and ds.can_edit(request.user)):
            return _err('editing a whole gazetteer in the Workbench is limited to staff for now', 403)
    try:
        kwargs = {'sequenced': dt.sequenced} if dt.sequenced else {}
        summary = dt.publish_fn(p, request.user, **kwargs)
    except ConflictError as e:
        return JsonResponse({'error': str(e), 'status': 'conflict'}, status=409)
    except PublishError as e:
        return _err(str(e))
    except Exception as e:  # noqa: BLE001 — surface a clean error, log the detail
        logger.exception('publish failed for project %s: %s', pid, e)
        return _err('publishing failed — please try again.', 500)
    return JsonResponse({'ok': True, 'status': 'published', **summary})


@login_required
@_beta_required
@require_http_methods(['POST'])
def project_checkout(request, pid):
    """Materialise an already-published Place Collection into a new, team-owned Workbench project for
    editing (plan §6). Body: ``{collection_id, team?}``. The published record stays authoritative;
    the new project records ``source_published_id`` + ``base_version`` for the publish-back check."""
    from collection.models import Collection
    from .checkout import checkout_place_collection, checkout_gazetteer_group, CheckoutError
    coll = get_object_or_404(Collection, pk=pid)
    # Only someone with edit rights on this collection may check it out — the SAME predicate the
    # browse page uses to show the button (collection.Collection.can_edit: staff / owner / collaborator).
    # @_beta_required has already 404'd non-beta users; this is the per-object authorisation.
    if not coll.can_edit(request.user):
        return _err('you do not have permission to edit that collection', 403)
    # Place Collection → place_collection editor; Dataset Collection → gazetteer_group editor.
    if coll.collection_class == 'place':
        loader, doc_type = checkout_place_collection, 'place_collection'
    elif coll.collection_class == 'dataset':
        loader, doc_type = checkout_gazetteer_group, 'gazetteer_group'
    else:
        return _err('this kind of collection cannot be checked out yet', 400)
    try:
        snapshot, base_version = loader(coll)
    except CheckoutError as e:
        return _err(str(e))
    team = _resolve_target_team(request.user, _body(request).get('team'))
    if team is None:
        return _err('you are not an editor of that team', 403)
    with transaction.atomic():
        proj = WorkbenchProject.objects.create(
            team=team, title=coll.title[:300], created_by=request.user, snapshot=snapshot,
            version=1, status='draft', doc_type=doc_type,
            published_collection=coll, source_published_id=str(coll.pk), base_version=base_version)
        ProjectSnapshot.objects.create(project=proj, version=1, snapshot=snapshot,
                                       created_by=request.user)
    return JsonResponse({'id': str(proj.id), 'version': proj.version, 'doc_type': proj.doc_type,
                         'team': team.id}, status=201)


@login_required
@_beta_required
@require_http_methods(['POST'])
def project_checkout_place(request, pid):
    """Record-level check-out (plan §6.1 / plan-record-suggestions §3): materialise a single published
    ``places.Place`` into a new team-owned ``WorkbenchProject(doc_type='place_record')``. Open to ANY
    beta user — those WITHOUT edit rights compose a **suggestion** (Submit), owners/staff can Publish
    directly. The direct-apply authorisation is enforced at publish time (see ``project_publish``), so
    widening check-out here doesn't let a non-owner apply their own working copy."""
    from places.models import Place
    from .checkout import checkout_place_record, CheckoutError
    place = get_object_or_404(Place.objects.select_related('dataset'), pk=pid)
    try:
        snapshot, base_version = checkout_place_record(place)
    except CheckoutError as e:
        return _err(str(e))
    team = _resolve_target_team(request.user, _body(request).get('team'))
    if team is None:
        return _err('you are not an editor of that team', 403)
    with transaction.atomic():
        proj = WorkbenchProject.objects.create(
            team=team, title=(place.title or f'place {place.id}')[:300], created_by=request.user,
            snapshot=snapshot, version=1, status='draft', doc_type='place_record',
            source_published_id=str(place.id), base_version=base_version)
        ProjectSnapshot.objects.create(project=proj, version=1, snapshot=snapshot,
                                       created_by=request.user)
    return JsonResponse({'id': str(proj.id), 'version': proj.version, 'doc_type': proj.doc_type,
                         'team': team.id}, status=201)


# ── dataset ("gazetteer") check-out — subset / whole-that-fits (plan-dataset-checkout §3/§4) ─────
def _staff_dataset_or_403(request, pid):
    """Resolve the target dataset and enforce the dataset-checkout gate: beta (already applied by the
    decorator) + **staff** (plan §1e — whole-/subset-dataset editing is higher blast radius than a
    single record, so staff-only initially; widens to owners/team at the v3.3 public release) AND
    ``Dataset.can_edit``. Returns (dataset, None) on success or (None, JsonResponse) on refusal."""
    from datasets.models import Dataset
    ds = get_object_or_404(Dataset, pk=pid)
    if not (request.user.is_staff and ds.can_edit(request.user)):
        return None, _err('editing a whole gazetteer in the Workbench is limited to staff for now', 403)
    return ds, None


@login_required
@_beta_required
@require_http_methods(['GET'])
def dataset_checkout_info(request, pid):
    """Pre-flight for the capacity chooser (plan §1b): record count, per-record byte estimate, and the
    country codes present, so the client can compare against ``navigator.storage.estimate()`` and offer
    whole-dataset vs a filtered subset. Staff-gated like the check-out itself."""
    from .dataset_checkout import dataset_checkout_info as _info
    ds, refusal = _staff_dataset_or_403(request, pid)
    if refusal:
        return refusal
    return JsonResponse(_info(ds))


@login_required
@_beta_required
@require_http_methods(['POST'])
def project_checkout_dataset(request, pid):
    """Check out a bounded set of a gazetteer's records into a new ``dataset_edit`` project (plan §3/§4).
    Body: ``{q?, ccode?, limit?, offset?, team?}``. A filter/limit selects a subset (the workhorse); no
    filter + no limit is the whole-dataset path, allowed only if the dataset fits the server cap. Each
    record carries its own ``base_version`` for per-record optimistic locking at publish-back."""
    from .dataset_checkout import checkout_dataset, DatasetCheckoutError
    ds, refusal = _staff_dataset_or_403(request, pid)
    if refusal:
        return refusal
    data = _body(request)
    try:
        snapshot, base_version = checkout_dataset(
            ds, q=data.get('q'), ccode=data.get('ccode'),
            limit=data.get('limit'), offset=data.get('offset') or 0)
    except DatasetCheckoutError as e:
        return _err(str(e))
    team = _resolve_target_team(request.user, data.get('team'))
    if team is None:
        return _err('you are not an editor of that team', 403)
    loaded = snapshot.get('loaded', 0)
    title = f'{ds.title} — {loaded} record{"s" if loaded != 1 else ""}'
    with transaction.atomic():
        proj = WorkbenchProject.objects.create(
            team=team, title=title[:300], created_by=request.user, snapshot=snapshot,
            version=1, status='draft', doc_type='dataset_edit',
            published_dataset=ds, source_published_id=str(ds.id), base_version=base_version)
        ProjectSnapshot.objects.create(project=proj, version=1, snapshot=snapshot,
                                       created_by=request.user)
    return JsonResponse({'id': str(proj.id), 'version': proj.version, 'doc_type': proj.doc_type,
                         'loaded': loaded, 'total_matched': snapshot.get('total_matched'),
                         'team': team.id}, status=201)


# ── community record corrections — "Suggestions" (plan-record-suggestions) ──────
@login_required
@_beta_required
@require_http_methods(['POST'])
def submit_suggestion(request):
    """Submit a ``place_record`` working-copy project as a pending ``RecordSuggestion`` (plan §3).
    Open to any beta user. Body: ``{project_id, rationale?}``. Deletes the throwaway working copy on
    success. Returns ``{id, changed_fields}``."""
    from . import suggestions as S
    data = _body(request)
    proj = get_object_or_404(WorkbenchProject.objects.select_related('team'), pk=data.get('project_id'))
    if proj.role_for(request.user) is None:
        return _err('not your working copy', 403)
    if proj.doc_type != 'place_record':
        return _err('only a single-record correction can be submitted as a suggestion', 400)
    try:
        sug = S.create_suggestion(proj, request.user, rationale=data.get('rationale') or '')
    except S.SuggestionError as e:
        return _err(str(e))
    proj.delete()                                   # the suggestion is the artefact now
    return JsonResponse({'ok': True, 'id': sug.id, 'changed_fields': sug.changed_fields}, status=201)


@login_required
@_beta_required
@require_http_methods(['POST'])
def review_suggestion(request, sid):
    """Accept or reject a pending suggestion (plan §3). Auth: staff OR the gazetteer's owner. Body:
    ``{decision: 'accept'|'reject', note?}``. Accept applies it via the shared record path + reindex."""
    from . import suggestions as S
    sug = get_object_or_404(RecordSuggestion.objects.select_related('dataset', 'place'), pk=sid)
    if not S.can_review(request.user, sug):
        return _err('only this gazetteer’s owner or WHG staff can review this suggestion', 403)
    decision = (_body(request).get('decision') or '').lower()
    note = _body(request).get('note') or ''
    try:
        if decision == 'accept':
            summary = S.apply_suggestion(sug, request.user)
            return JsonResponse({'ok': True, 'status': 'accepted', **summary})
        if decision == 'reject':
            S.reject_suggestion(sug, request.user, note)
            return JsonResponse({'ok': True, 'status': 'rejected'})
        return _err('decision must be "accept" or "reject"')
    except S.SuggestionSuperseded as e:
        return JsonResponse({'error': str(e), 'status': 'superseded'}, status=409)
    except S.SuggestionError as e:
        return _err(str(e))


@login_required
@_beta_required
@require_http_methods(['POST'])
def withdraw_suggestion(request, sid):
    """The proposer withdraws their own pending suggestion."""
    from . import suggestions as S
    sug = get_object_or_404(RecordSuggestion, pk=sid)
    try:
        S.withdraw_suggestion(sug, request.user)
    except S.SuggestionError as e:
        return _err(str(e), 403)
    return JsonResponse({'ok': True, 'status': 'withdrawn'})


@login_required
@_beta_required
@require_http_methods(['GET'])
def suggestions_for_place(request, pid):
    """Pending-suggestions inset data for one place (plan §1d/§1e): count always; items + review
    controls only for staff / gazetteer owner / the proposer."""
    from places.models import Place
    from . import suggestions as S
    place = get_object_or_404(Place.objects.select_related('dataset'), pk=pid)
    return JsonResponse(S.inset_for_place(place, request.user))


# ── teams ───────────────────────────────────────────────────────────────────
@login_required
@_beta_required
@require_http_methods(['GET', 'POST'])
def teams(request):
    if request.method == 'GET':
        memberships = (TeamMember.objects.filter(user=request.user)
                       .select_related('team').order_by('team__title'))
        out = []
        for m in memberships:
            if m.team.is_personal:
                continue
            out.append({'id': m.team.id, 'title': m.team.title, 'role': m.role,
                        'member_count': m.team.members.count()})
        return JsonResponse({'teams': out})

    data = _body(request)
    title = (data.get('title') or '').strip()
    if not title:
        return _err('title is required')
    slug = Team._unique_slug(slugify(title) or 'team')
    with transaction.atomic():
        team = Team.objects.create(owner=request.user, title=title[:200], slug=slug,
                                   description=(data.get('description') or '')[:3000])
        TeamMember.objects.create(team=team, user=request.user, role=ROLE_OWNER)
    return JsonResponse({'id': team.id, 'title': team.title, 'role': ROLE_OWNER}, status=201)


@login_required
@_beta_required
@require_http_methods(['GET', 'POST'])
def team_members(request, tid):
    team = get_object_or_404(Team, pk=tid)
    my_role = team.role_for(request.user)
    if my_role is None:
        return _err('not a member of this team', 403)

    if request.method == 'GET':
        out = [{'user_id': m.user_id, 'username': m.user.username,
                'name': getattr(m.user, 'name', '') or m.user.username, 'role': m.role}
               for m in team.members.select_related('user')]
        return JsonResponse({'team': team.id, 'title': team.title, 'my_role': my_role,
                             'members': out})

    # POST — invite/add a member (owner only).
    if my_role != ROLE_OWNER:
        return _err('only the team owner can add members', 403)
    data = _body(request)
    role = data.get('role', ROLE_EDITOR)
    if role not in VALID_ROLES:
        return _err('invalid role')
    user = _find_user(data.get('identifier'))
    if not user:
        return _err('no user found with that username or email', 404)
    m, created = TeamMember.objects.get_or_create(team=team, user=user, defaults={'role': role})
    if not created:
        m.role = role
        m.save(update_fields=['role'])
    # Notify the added member by email (issue #143). A properly-signed-up user has a verified email; if
    # they don't, we can't reach them, so surface that to the owner instead of failing silently.
    notified = False
    if getattr(user, 'has_verified_email', False):
        notified = _notify_team_member(request, team, user, m.role, data.get('project_id'))
    return JsonResponse({'ok': True, 'user_id': user.id, 'username': user.username,
                         'name': getattr(user, 'name', '') or user.username, 'role': m.role,
                         'created': created,
                         'has_verified_email': bool(getattr(user, 'has_verified_email', False)),
                         'notified': notified})


def _notify_team_member(request, team, user, role, project_id=None):
    """Email a user that they've been added to a team, linking to the Map-your-Data project. Best-effort:
    returns True if the mail was sent. Never raises (a mail failure must not fail the add)."""
    try:
        from django.urls import reverse
        from whgmail.messaging import WHGmail
        base = reverse('reconciliation')
        # A team invite that carries the project id deep-links straight into it (init() reads ?open=<id>).
        url = request.build_absolute_uri(f'{base}?open={project_id}' if project_id else base)
        inviter = getattr(request.user, 'name', '') or request.user.username
        role_label = dict(TEAM_ROLES).get(role, role)
        return bool(WHGmail(request, {
            'template': 'team_invitation',
            'subject': f'You have been added to the "{team.title}" team on World Historical Gazetteer',
            'user': user,
            'to_email': user.email,
            'team_name': team.title,
            'inviter_name': inviter,
            'member_role': role_label,
            'project_url': url,
        }))
    except Exception:
        logger.exception('team invite email failed (team=%s user=%s)', team.id, user.id)
        return False


@login_required
@_beta_required
@require_http_methods(['DELETE'])
def team_member_detail(request, tid, uid):
    team = get_object_or_404(Team, pk=tid)
    if team.role_for(request.user) != ROLE_OWNER:
        return _err('only the team owner can remove members', 403)
    if int(uid) == team.owner_id:
        return _err('cannot remove the team owner', 400)
    TeamMember.objects.filter(team=team, user_id=uid).delete()
    return JsonResponse({'ok': True})


# ── Google Sheet import proxy (Map-your-Data) ──────────────────────────────────
GSHEET_ID_RE = re.compile(r'docs\.google\.com/spreadsheets/d/([a-zA-Z0-9\-_]+)')
GSHEET_GID_RE = re.compile(r'[#?&]gid=([0-9]+)')


@login_required
@_beta_required
@require_http_methods(['POST'])
def gsheet_proxy(request):
    """Fetch a shared Google Sheet as CSV, server-side, so the browser importer isn't blocked by CORS.
    SSRF-safe: we extract the doc id from the pasted link and only ever fetch the fixed
    ``docs.google.com/spreadsheets/d/<id>/export`` URL — never the user-supplied URL itself."""
    url = (_body(request).get('url') or '').strip()
    m = GSHEET_ID_RE.search(url)
    if not m:
        return _err('That doesn’t look like a Google Sheets link '
                    '(expected docs.google.com/spreadsheets/d/…).')
    doc_id = m.group(1)
    gm = GSHEET_GID_RE.search(url)
    gid = gm.group(1) if gm else '0'
    export = f'https://docs.google.com/spreadsheets/d/{doc_id}/export?format=csv&gid={gid}'
    try:
        r = requests.get(export, timeout=15, allow_redirects=True)
    except requests.RequestException:
        return _err('Could not reach Google Sheets — please try again.', 502)
    # A sheet that isn't link-shared redirects to an HTML sign-in page (still HTTP 200).
    if r.status_code != 200 or 'text/csv' not in r.headers.get('Content-Type', ''):
        return _err('Could not read that sheet — make sure it is shared so that '
                    '“anyone with the link can view”.', 400)
    if len(r.content) > 15 * 1024 * 1024:
        return _err('That sheet is too large to import here (over 15 MB).', 413)
    return JsonResponse({'csv': r.text, 'name': f'gsheet-{doc_id[:8]}.csv'})


# ── Google Doc import proxy (Map-your-Data NER) ────────────────────────────────
GDOC_ID_RE = re.compile(r'docs\.google\.com/document/d/([a-zA-Z0-9\-_]+)')


@login_required
@_beta_required
@require_http_methods(['POST'])
def gdoc_proxy(request):
    """Fetch a shared Google Doc as plain text, server-side (avoids CORS). SSRF-safe: only ever fetch
    the fixed ``docs.google.com/document/d/<id>/export?format=txt`` derived from the pasted link."""
    url = (_body(request).get('url') or '').strip()
    m = GDOC_ID_RE.search(url)
    if not m:
        return _err('That doesn’t look like a Google Docs link '
                    '(expected docs.google.com/document/d/…).')
    doc_id = m.group(1)
    export = f'https://docs.google.com/document/d/{doc_id}/export?format=txt'
    try:
        r = requests.get(export, timeout=15, allow_redirects=True)
    except requests.RequestException:
        return _err('Could not reach Google Docs — please try again.', 502)
    # A doc that isn't link-shared redirects to an HTML sign-in page (still HTTP 200).
    if r.status_code != 200 or 'text/plain' not in r.headers.get('Content-Type', ''):
        return _err('Could not read that document — make sure it is shared so that '
                    '“anyone with the link can view”.', 400)
    if len(r.content) > 10 * 1024 * 1024:
        return _err('That document is too large to import here (over 10 MB).', 413)
    return JsonResponse({'text': r.text, 'name': f'gdoc-{doc_id[:8]}.txt'})


# ── Place-name extraction proxy (Map-your-Data NER) ────────────────────────────
# NB: unlike the rest of Map-your-Data (which stays in the browser), this deliberately sends the
# pasted / extracted text to WHG's on-host spaCy service for named-entity recognition. The UI warns
# the user. The text is forwarded, not stored. Beta-gated.
NER_MAX_CHARS = 200_000

# Gazetteer-assisted recall + collocational disambiguation (prototype). spaCy is trained on modern
# news and misses historical / archaic / non-English toponyms, and can't choose between same-named
# places. So we (1) also pull capitalised n-gram candidates the model didn't tag, (2) reconcile every
# mention against WHG's own place index (reusing the /reconcile matcher), and (3) DISAMBIGUATE by
# geographic mode-seeking — a document's places cluster, so we find the region where the most distinct
# names have a candidate and resolve each name to its nearest candidate there. This uses pure geometry
# (the indexed repr_point), NOT country codes, which are patchy in the index. The chosen match (id +
# coordinates) is returned so the browser can seed a preliminary, located reconciliation. Best-effort:
# if the gateway is slow/unavailable we silently return the spaCy-only result.
_NER_CAND_RE = re.compile(
    r"\b[A-Z][\wÀ-ÿ’'\-]+"
    r"(?:\s+(?:of|the|de|la|le|du|des|upon|on|el|al|von|van)\s+[A-Z][\wÀ-ÿ’'\-]+"
    r"|\s+[A-Z][\wÀ-ÿ’'\-]+){0,3}"
)
_NER_CAND_STOP = {
    'the', 'a', 'an', 'in', 'on', 'at', 'of', 'and', 'or', 'but', 'he', 'she', 'it', 'they', 'we',
    'this', 'that', 'these', 'those', 'later', 'then', 'after', 'before', 'during', 'while', 'when',
    'his', 'her', 'their', 'our', 'its', 'i', 'mr', 'mrs', 'dr', 'st', 'sir', 'lord', 'king', 'queen',
}
NER_RECON_MAX = 25  # cap mentions reconciled against the gateway per request (one call each)


def _ner_context(text, name, width=140):
    i = text.lower().find(name.lower())
    if i < 0:
        return ''
    a, b = max(0, i - width // 2), min(len(text), i + len(name) + width // 2)
    snip = re.sub(r'\s+', ' ', text[a:b]).strip()
    return ('…' if a > 0 else '') + snip + ('…' if b < len(text) else '')


def _ner_candidates(text, already):
    """Capitalised spans NOT already tagged by spaCy → {name: frequency}."""
    seen = {(n or '').lower() for n in already}
    counts = {}
    for m in _NER_CAND_RE.finditer(text):
        s = re.sub(r'\s+', ' ', m.group(0)).strip(" ,.;:’'\"-")
        low = s.lower()
        if not s or low in seen or low in _NER_CAND_STOP or len(s) < 3 or len(s) > 80:
            continue
        counts[s] = counts.get(s, 0) + 1
    return counts


def _median(vals):
    vals = sorted(vals)
    n = len(vals)
    return vals[n // 2] if n % 2 else (vals[n // 2 - 1] + vals[n // 2]) / 2.0


def _haversine_km(a, b):
    from math import radians, sin, cos, asin, sqrt
    (lon1, lat1), (lon2, lat2) = a, b
    h = sin(radians(lat2 - lat1) / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371 * asin(min(1.0, sqrt(h)))


def _ner_name_hit(hit, name):
    """True if `name` is the hit's canonical name OR one of its alternative names (case-insensitive).
    Keeps historical variants (Constantinople → Istanbul) but rejects fuzzy noise (Merchants → …)."""
    nl = name.strip().lower()
    if (hit.get('name') or '').strip().lower() == nl:
        return True
    return any((a or '').strip().lower() == nl for a in (hit.get('alt_names') or []))


def _cluster_locations(hits, thresh_km=40):
    """Greedily group hits by location (points within thresh_km ⇒ same place). Returns
    [(centroid[lng,lat], [hits]), …]; hits with no repr_point are ignored for clustering."""
    clusters = []  # [sum_lng, sum_lat, count, [hits]]
    for h in hits:
        rp = h.get('repr_point')
        if not rp:
            continue
        for cl in clusters:
            if _haversine_km([cl[0] / cl[2], cl[1] / cl[2]], rp) <= thresh_km:
                cl[0] += rp[0]; cl[1] += rp[1]; cl[2] += 1; cl[3].append(h)
                break
        else:
            clusters.append([rp[0], rp[1], 1, [h]])
    return [([cl[0] / cl[2], cl[1] / cl[2]], cl[3]) for cl in clusters]


def _ner_reconcile_disambiguate(mentions, user):
    """mentions: {name: count}. Reconcile the most frequent (capped) against WHG, keep exact name/
    alt-name matches, then DISAMBIGUATE by geographic coherence. A document's places share a country
    and cluster spatially, so we: (1) country-vote to the dominant region; (2) ANCHOR on names that
    resolve to a single location within that region (they pin the area unambiguously); (3) resolve the
    genuinely ambiguous same-country siblings by proximity to that clean anchor cluster. Returns
    {name: match} for gazetteer-matched names."""
    if not mentions:
        return {}
    names = [n for n, _ in sorted(mentions.items(), key=lambda kv: -kv[1])[:NER_RECON_MAX]]
    try:
        from api.reconcile import process_queries
        res = process_queries({f'q{i}': {'query': n} for i, n in enumerate(names)},
                              batch_size=NER_RECON_MAX, user=user)
    except Exception as e:  # gateway down / import issue → degrade to spaCy-only
        logger.warning('NER gazetteer reconcile failed: %s', e)
        return {}

    hits_by_name = {}
    for i, n in enumerate(names):
        exact = [h for h in ((res.get(f'q{i}') or {}).get('result') or []) if _ner_name_hit(h, n)]
        if exact:
            hits_by_name[n] = exact
    if not hits_by_name:
        return {}

    # One representative candidate per DISTINCT location a name resolves to (dedup near-dupes; keep the
    # best-scoring hit per location). We disambiguate by PURE GEOGRAPHY, not ccodes — country codes are
    # patchy in the index, so a country vote mis-fires (a GB place with no ccode loses to a CA sibling).
    locs_by_name = {}
    for n, hits in hits_by_name.items():
        reps = [(pt, max(c_hits, key=lambda h: h.get('score') or 0)) for pt, c_hits in _cluster_locations(hits)]
        if not reps and hits:                                   # no coordinates at all → keep top hit
            reps = [(None, max(hits, key=lambda h: h.get('score') or 0))]
        locs_by_name[n] = reps

    # Mode-seeking: the region (a candidate point) that covers the MOST distinct names within R km — a
    # document's places cluster, so the true region is where the most names have a candidate. Coverage
    # is WEIGHTED BY PROMINENCE (the reconciliation score of each name's nearest candidate), so a
    # cluster containing the prominent original (e.g. Paris, France, score 100) outweighs a tighter
    # cluster of obscure namesakes (Paris, Ohio, score 63). Without this, America's dense "little
    # Europe" town names (Canterbury/Dover/Paris/… all within one Ohio county) out-cluster the real
    # English/French originals, which span a country border. Ties on weight → tighter (smaller spread).
    R = 250.0
    all_pts = [pt for reps in locs_by_name.values() for pt, _ in reps if pt]
    center, best_cov, best_spread = None, -1.0, 1e18
    for cand in all_pts:
        cov = spread = 0.0
        for reps in locs_by_name.values():
            near = [(pt, h) for pt, h in reps if pt and _haversine_km(cand, pt) <= R]
            if near:
                pt, h = min(near, key=lambda ph: _haversine_km(cand, ph[0]))
                cov += (h.get('score') or 0) / 100.0            # prominence-weighted coverage
                spread += _haversine_km(cand, pt)
        if cov > best_cov or (abs(cov - best_cov) < 1e-9 and spread < best_spread):
            center, best_cov, best_spread = cand, cov, spread

    chosen = {}
    for n, reps in locs_by_name.items():
        if center:                                              # nearest candidate to the region centre
            _, best = min(reps, key=lambda pr: _haversine_km(center, pr[0]) if pr[0] else 1e18)
        else:                                                   # no geography anywhere → best relevance
            _, best = max(reps, key=lambda pr: pr[1].get('score') or 0)
        rp = best.get('repr_point') or [None, None]
        chosen[n] = {'id': best.get('id'), 'title': best.get('name'), 'score': round(best.get('score') or 0, 3),
                     'ccodes': best.get('ccodes') or [], 'lng': rp[0], 'lat': rp[1], 'ambiguous': len(reps) > 1}
    return chosen


@login_required
@_beta_required
@require_http_methods(['POST'])
def ner_extract(request):
    text = _body(request).get('text') or ''
    if not text.strip():
        return _err('There’s no text to analyse.')
    text = text[:NER_MAX_CHARS]                      # the service caps too; bound the request here
    base = (getattr(settings, 'NER_URL', '') or '').rstrip('/')
    if not base:
        return _err('Place-name extraction isn’t available right now.', 503)
    try:
        r = requests.post(base + '/extract', json={'text': text}, timeout=60)
    except requests.RequestException:
        return _err('The place-name extractor could not be reached — please try again shortly.', 503)
    if r.status_code != 200:
        return _err('The place-name extractor returned an error — please try again.', 502)
    data = r.json()
    ents = data.get('entities') or []

    # Union of spaCy names + capitalised candidates → reconcile + geo-disambiguate (best-effort).
    mentions = {}
    for e in ents:
        nm = (e.get('name') or '').strip()
        if nm:
            mentions[nm] = max(mentions.get(nm, 0), e.get('count') or 1)
    for nm, c in _ner_candidates(text, list(mentions.keys())).items():
        mentions.setdefault(nm, c)
    matches = _ner_reconcile_disambiguate(mentions, request.user)

    have = {(e.get('name') or '').lower() for e in ents}
    for e in ents:                                   # attach a preliminary match to spaCy entities
        m = matches.get((e.get('name') or '').strip())
        if m:
            e['match'] = m
    added = 0
    for nm, m in matches.items():                    # add gazetteer-confirmed names spaCy missed
        if nm.lower() in have:
            continue
        have.add(nm.lower())
        ents.append({'name': nm, 'label': 'GAZ', 'count': text.count(nm),
                     'context': _ner_context(text, nm), 'match': m})
        added += 1
    ents.sort(key=lambda e: (-(e.get('count') or 0), (e.get('name') or '').lower()))
    data['entities'] = ents
    data['gazetteer_added'] = added
    data['reconciled'] = sum(1 for e in ents if e.get('match'))
    return JsonResponse(data)


# ── Phase-2 real-time collab token ─────────────────────────────────────────────
@login_required
@_beta_required
@require_http_methods(['POST'])
def collab_token(request, pid):
    """Mint a short-lived JWT the client presents to the Hocuspocus WebSocket service (Phase 2,
    place#112). The service verifies it with the shared ``HOCUSPOCUS_SECRET`` and enforces the role
    (viewer → read-only). 501 if the realtime service isn't configured, so the client feature-detects
    and falls back to the Phase-1 REST sync."""
    p = get_object_or_404(WorkbenchProject.objects.select_related('team'), pk=pid)
    role = p.role_for(request.user)
    if role is None:
        return _err('not a member of this project’s team', 403)
    secret = getattr(settings, 'HOCUSPOCUS_SECRET', '')
    if not secret:
        return JsonResponse({'error': 'real-time collaboration is not available'}, status=501)
    now = int(time.time())
    payload = {
        'sub': str(request.user.id),
        'name': getattr(request.user, 'name', '') or request.user.username,
        'project_id': str(p.id),
        'role': role,
        'iat': now,
        'exp': now + 120,  # short TTL: only needs to survive the WS handshake
    }
    token = jwt.encode(payload, secret, algorithm='HS256')
    return JsonResponse({'token': token, 'document': str(p.id), 'role': role})
