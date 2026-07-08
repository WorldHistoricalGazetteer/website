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
    published_dataset = models.ForeignKey('datasets.Dataset', null=True, blank=True,
                                          on_delete=models.SET_NULL, related_name='+')

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
