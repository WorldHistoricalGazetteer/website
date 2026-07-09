"""
Collaborative Workbench data model (WorldHistoricalGazetteer/place#112).

The "Map your Data" reconciliation Workbench (place#111, /reconciliation/) is a local-first,
single-user tool whose entire state is one ``project`` object persisted to the browser's IndexedDB.
This app makes that object a *shared, synced document* owned by a Team, so groups can collaborate on
an in-progress working copy that only enters the ES index if/when they choose to publish it via the
existing /datasets/validate/ pipeline (decoupling "collaborate" from "publish").

Phase 0 = share a read-only snapshot (``public_token`` + ``/reconciliation/shared/<token>/``).
Phase 1 = Team-owned editable ``WorkbenchProject`` with async optimistic-lock sync (server-held
``snapshot`` + monotonic ``version``; ``ProjectSnapshot`` history is the three-way-merge ancestor).
Phase 2 (future) = Yjs + Hocuspocus real-time co-editing on the same store.
"""
import uuid

from django.conf import settings
from django.db import models
from django.db.models import JSONField
from django.utils.text import slugify

# Roles a Team member can hold on the team's projects.
ROLE_OWNER, ROLE_EDITOR, ROLE_VIEWER = 'owner', 'editor', 'viewer'
TEAM_ROLES = [(ROLE_OWNER, 'Owner'), (ROLE_EDITOR, 'Editor'), (ROLE_VIEWER, 'Viewer')]
EDIT_ROLES = (ROLE_OWNER, ROLE_EDITOR)  # roles allowed to write

STATUS_CHOICES = [('draft', 'Draft'), ('shared', 'Shared'), ('published', 'Published')]

# Workbench doc-types (plan-collaborativeCollections §3.1). One Workbench, many doc-types: the
# reconciliation project (Map your Data) is doc-type #1; the rest share the same envelope (team,
# snapshot, version, share/collab machinery) but carry a doc-type-specific snapshot schema and a
# distinct publish/checkout pair (see workbench/doctypes.py). ``route`` and ``network`` are reserved
# PLACEHOLDERS whose creation is gated OFF in v3.3 (arriving with the v4 graph model) — the values are
# reserved now so no migration is needed when v4 lands.
DOC_RECONCILIATION = 'reconciliation'
DOC_GAZETTEER_GROUP = 'gazetteer_group'
DOC_PLACE_COLLECTION = 'place_collection'
DOC_ITINERARY = 'itinerary'
DOC_PLACE_RECORD = 'place_record'   # single-place correction (created only via record-level check-out)
DOC_DATASET_EDIT = 'dataset_edit'   # whole-/subset-of-a-gazetteer correction (created only via check-out)
DOC_ROUTE = 'route'
DOC_NETWORK = 'network'
DOC_TYPES = [
    (DOC_RECONCILIATION, 'Map your Data (reconciliation)'),
    (DOC_GAZETTEER_GROUP, 'Gazetteer Group'),
    (DOC_PLACE_COLLECTION, 'Place Collection'),
    (DOC_ITINERARY, 'Itinerary'),
    (DOC_PLACE_RECORD, 'Place record correction'),
    (DOC_DATASET_EDIT, 'Gazetteer records correction'),
    (DOC_ROUTE, 'Route'),
    (DOC_NETWORK, 'Network'),
]


class Team(models.Model):
    """A group that co-owns Workbench projects. Every user also gets one hidden *personal* team
    (``is_personal=True``) so a solo, server-saved project needs no explicit team setup."""
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='workbench_teams',
                              on_delete=models.CASCADE)
    title = models.CharField(max_length=200)
    slug = models.SlugField(max_length=220, unique=True)
    description = models.TextField(blank=True, default='')
    is_personal = models.BooleanField(default=False)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'workbench_team'
        ordering = ['title']

    def __str__(self):
        return self.title

    @classmethod
    def _unique_slug(cls, base):
        base = base or 'team'
        slug, i = base, 1
        while cls.objects.filter(slug=slug).exists():
            i += 1
            slug = f'{base}-{i}'
        return slug

    @classmethod
    def personal_for(cls, user):
        """Return (creating if needed) the user's hidden personal team, with them as owner-member."""
        team = cls.objects.filter(owner=user, is_personal=True).first()
        if team:
            return team
        base = slugify(f'my-workbench-{user.username}') or f'my-workbench-{user.pk}'
        team = cls.objects.create(owner=user, title='My workbench',
                                  slug=cls._unique_slug(base), is_personal=True)
        TeamMember.objects.get_or_create(team=team, user=user, defaults={'role': ROLE_OWNER})
        return team

    def role_for(self, user):
        """The user's role on this team, or None if they are not a member."""
        if not user or not getattr(user, 'is_authenticated', False):
            return None
        m = self.members.filter(user=user).first()
        return m.role if m else None


class TeamMember(models.Model):
    team = models.ForeignKey(Team, related_name='members', on_delete=models.CASCADE)
    user = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='workbench_memberships',
                             on_delete=models.CASCADE)
    role = models.CharField(max_length=10, choices=TEAM_ROLES, default=ROLE_EDITOR)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'workbench_team_member'
        unique_together = ('team', 'user')

    def __str__(self):
        return f'{self.user} @ {self.team} ({self.role})'


class WorkbenchProject(models.Model):
    """A server-side, in-progress working copy of a Workbench ``project`` object. Never indexed
    until published (which produces a normal ``datasets.Dataset`` via /datasets/validate/)."""
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    team = models.ForeignKey(Team, related_name='projects', on_delete=models.CASCADE)
    title = models.CharField(max_length=300, default='Untitled project')
    # Which kind of Workbench document this project holds. The snapshot schema is a discriminated
    # union on this value (see workbench/doctypes.py); the envelope (team/version/share) is shared.
    doc_type = models.CharField(max_length=20, choices=DOC_TYPES, default=DOC_RECONCILIATION)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='workbench_projects',
                                   null=True, on_delete=models.SET_NULL)
    created = models.DateTimeField(auto_now_add=True)
    updated = models.DateTimeField(auto_now=True)
    status = models.CharField(max_length=10, choices=STATUS_CHOICES, default='draft')
    # The whole browser ``project`` object (columns, rows, matches, decisions, geom, scope, …).
    snapshot = JSONField(default=dict, blank=True)
    # Monotonic version for optimistic-lock sync; bumped on every accepted write.
    version = models.IntegerField(default=0)
    # Phase-0 read-only capability token (anyone with the link may import a copy).
    public_token = models.UUIDField(null=True, blank=True, unique=True)

    # ── Publish targets (one per doc-type family). A project publishes into exactly one canonical
    # record; the pointer records which, so re-publish (publish-back) updates in place. ``route``/
    # ``network`` pointers are reserved PLACEHOLDERS for v4 (no Route/Network models exist yet).
    published_dataset = models.ForeignKey('datasets.Dataset', null=True, blank=True,
                                          on_delete=models.SET_NULL, related_name='+')
    published_collection = models.ForeignKey('collection.Collection', null=True, blank=True,
                                             on_delete=models.SET_NULL, related_name='+')
    published_route_id = models.IntegerField(null=True, blank=True)      # PLACEHOLDER (v4)
    published_network_id = models.IntegerField(null=True, blank=True)    # PLACEHOLDER (v4)

    # ── Check-out → edit → publish-back (plan §6). When a project is a working copy of an
    # already-published item, ``source_published_id`` records the canonical record it was checked out
    # from and ``base_version`` the server version/hash at checkout, so publish-back can run an
    # optimistic-lock check (did the server copy change under us?). Null for born-in-Workbench drafts.
    source_published_id = models.CharField(max_length=64, null=True, blank=True)
    base_version = models.CharField(max_length=64, null=True, blank=True)

    class Meta:
        db_table = 'workbench_project'
        ordering = ['-updated']

    def __str__(self):
        return f'{self.title} ({self.id})'

    def role_for(self, user):
        return self.team.role_for(user)


class ProjectSnapshot(models.Model):
    """Immutable snapshot of a project at a given version. Serves as the three-way-merge ancestor
    and as backup/versioning. Pruned to the most recent N per project (see views.prune_snapshots)."""
    project = models.ForeignKey(WorkbenchProject, related_name='snapshots', on_delete=models.CASCADE)
    version = models.IntegerField()
    snapshot = JSONField(default=dict, blank=True)
    created = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, on_delete=models.SET_NULL,
                                   related_name='+')

    class Meta:
        db_table = 'workbench_project_snapshot'
        unique_together = ('project', 'version')
        ordering = ['-version']

    def __str__(self):
        return f'{self.project_id} v{self.version}'


class RecordSuggestion(models.Model):
    """A community-proposed **correction to one published gazetteer record** (plan-record-suggestions).

    Any beta/staff user may propose a fix to any record; it is reviewed by the dataset owner (if opted
    in) and/or WHG staff, and applied on accept via the SAME record apply-path the direct editor uses
    (``workbench.publish.apply_record_fields`` + ``_reindex_place``), guarded by the record's
    ``base_version`` at proposal time.

    Deliberately scoped to **error-correction**, not competing/parallel claims (those are v4 Attestations,
    PLATO place#100). But every suggestion is a **permanent, provenance-bearing record** — kept through
    accept/reject/withdraw, never hard-deleted — so the corpus is the forward-compatible seed for v4
    attestations. ``proposed_snapshot`` is the full-LPF record snapshot (``checkout.record_snapshot``)."""
    STATUS_PENDING, STATUS_ACCEPTED, STATUS_REJECTED, STATUS_SUPERSEDED, STATUS_WITHDRAWN = (
        'pending', 'accepted', 'rejected', 'superseded', 'withdrawn')
    STATUS_CHOICES = [(STATUS_PENDING, 'Pending'), (STATUS_ACCEPTED, 'Accepted'),
                      (STATUS_REJECTED, 'Rejected'), (STATUS_SUPERSEDED, 'Superseded'),
                      (STATUS_WITHDRAWN, 'Withdrawn')]

    place = models.ForeignKey('places.Place', related_name='suggestions', on_delete=models.CASCADE)
    # Denormalised so the review queue / opt-in / per-place insets query without joining through Place.
    dataset = models.ForeignKey('datasets.Dataset', related_name='record_suggestions',
                                on_delete=models.CASCADE)
    proposer = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='record_suggestions',
                                 null=True, on_delete=models.SET_NULL)
    proposed_snapshot = JSONField(default=dict)          # full-LPF record snapshot
    base_version = models.CharField(max_length=64)       # record_state_hash at proposal (optimistic lock)
    changed_fields = JSONField(default=list, blank=True)  # ['name','coordinate',…] computed at submit
    rationale = models.TextField(blank=True, default='')  # optional proposer note

    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default=STATUS_PENDING)
    created = models.DateTimeField(auto_now_add=True)
    reviewed_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                    on_delete=models.SET_NULL, related_name='+')
    reviewed_at = models.DateTimeField(null=True, blank=True)
    review_note = models.TextField(blank=True, default='')
    applied_changed = JSONField(default=list, blank=True)  # what actually changed on accept

    class Meta:
        db_table = 'workbench_record_suggestion'
        ordering = ['-created']
        indexes = [
            models.Index(fields=['status', 'dataset']),
            models.Index(fields=['place', 'status']),
        ]

    def __str__(self):
        return f'suggestion #{self.pk} on place {self.place_id} ({self.status})'


class ProjectYDoc(models.Model):
    """Binary Yjs document state for a project's real-time (Hocuspocus) session — Phase 2 (place#112).
    The Node hocuspocus service reads/writes ``state`` (the persistence store); Django owns the schema
    and flattens the doc back into ``WorkbenchProject.snapshot`` so the REST/snapshot path stays
    authoritative for non-realtime clients, export, and publishing."""
    project = models.OneToOneField(WorkbenchProject, related_name='ydoc', on_delete=models.CASCADE,
                                   primary_key=True)
    state = models.BinaryField(null=True, blank=True)
    updated = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'workbench_ydoc'

    def __str__(self):
        return f'ydoc:{self.project_id}'
