# models.py
import secrets

from django.conf import settings
from django.db import models
from django.utils import timezone


# APIToken and UserAPIProfile are created lazily when requested by a user.

class APIToken(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="api_token",
    )
    key = models.CharField(max_length=64, unique=True)
    created = models.DateTimeField(auto_now_add=True)
    last_used = models.DateTimeField(null=True, blank=True)

    def regenerate(self):
        self.key = secrets.token_urlsafe(32)
        self.created = timezone.now()
        self.save()

    def __str__(self):
        return f"{self.user} token"


class UserAPIProfile(models.Model):
    user = models.OneToOneField(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="api_profile",
    )
    daily_count = models.IntegerField(default=0)
    daily_reset = models.DateField(default=timezone.now)
    total_count = models.IntegerField(default=0)
    daily_limit = models.IntegerField(default=5000)

    def increment_usage(self, n=1):
        """Charge `n` units against today's allowance. `n` because a caller may spend the quota in
        units larger than one request — Map-your-Data's per-row extraction charges a row at a time
        (place#211), which is what the limit is actually protecting."""
        today = timezone.now().date()
        if self.daily_reset != today:
            self.daily_reset = today
            self.daily_count = 0
        self.daily_count += n
        self.total_count += n
        self.save()

    def remaining_today(self):
        """Units left in today's allowance, or None when the account is UNLIMITED.

        A falsy `daily_limit` means unlimited, not "no allowance". That is the convention
        api/authentication.py already established — `if profile.daily_limit and ...` skips the check
        entirely — and accounts have been set to 0 on purpose. Reading 0 as an exhausted quota would
        silently lock exactly those users out of every metered feature.

        Accounts for a day rollover that has not been written back yet, so a caller checking the
        allowance before spending it sees today's figure rather than yesterday's.
        """
        if not self.daily_limit:
            return None
        used = 0 if self.daily_reset != timezone.now().date() else self.daily_count
        return max(0, self.daily_limit - used)

    def __str__(self):
        return f"{self.user} API profile"


# ---------------------------------------------------------------------------
# Models added for the WHG indexing rebuild
# (see ../indexing/developer/plan-ingestionRebuild.execution.md)
# ---------------------------------------------------------------------------


class ContributorAttestation(models.Model):
    """A user-asserted hard link between two indexed places.

    Canonical store for contributor reconciliation links asserted through
    the platform. This table is populated only by live ``POST /api/links``
    writes; it is *not* the source read by the ingest pipeline's Batch 12
    ``contributor_replay.py``, which replays the legacy corpus from the
    ``place_link`` / ``close_matches`` tables into the Pitt-side SQLite
    hard-link overlay queried by the gateway.

    Live forwarding to the gateway is wired via the ``api/signals.py``
    post_save / post_delete handlers (``crc_post_link`` /
    ``crc_delete_link``, registered in ``ApiConfig.ready()``), but end-to-end
    sync is still pending the gateway-side ``/api/links`` receiver, which does
    not yet exist in the indexing repo. Until that lands, forwarding is a
    best-effort no-op on the receiving side.

    Constraints:
      * ``place_a < place_b`` (canonical ordering, enforced both in code
        and via a CHECK constraint).
      * ``UNIQUE (place_a, place_b, relation_type, user_id)`` so a user
        can't double-attest the same edge.
    """

    RELATION_CHOICES = [
        ("sameAs",      "sameAs"),
        ("exactMatch",  "exactMatch"),
        ("closeMatch",  "closeMatch"),
        ("distinct",    "distinct"),
    ]
    STATUS_CHOICES = [
        ("active",     "active"),     # in the publishable hard-link store
        ("pending",    "pending"),    # in-scope for owner only (Master Plan §7.4)
        ("rejected",   "rejected"),   # editorially rejected; timer resumes
        ("superseded", "superseded"), # replaced by a newer assertion
    ]

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.CASCADE,
        related_name="contributor_attestations",
    )
    # Sub-namespaced WHG dataset id (e.g. "whg:1234"). Used by
    # processing/retention_sweep.py to scope the 11/12-month timers.
    dataset_id = models.CharField(max_length=64, db_index=True)

    place_a = models.CharField(max_length=128, db_index=True)
    place_b = models.CharField(max_length=128, db_index=True)
    relation_type = models.CharField(max_length=16, choices=RELATION_CHOICES)

    asserted_at = models.DateTimeField(default=timezone.now)
    modified_at = models.DateTimeField(auto_now=True)
    justification = models.TextField(null=True, blank=True)

    status = models.CharField(
        max_length=12, choices=STATUS_CHOICES, default="active", db_index=True,
    )
    # Set TRUE for assertions inherited from the v3.2 reconciliation tool
    # (Batch 13b). Propagated through to the SQLite ``source_id`` suffix so
    # the gateway can filter without rejoining DO.
    legacy_v3_2 = models.BooleanField(default=False, db_index=True)

    class Meta:
        constraints = [
            models.CheckConstraint(
                check=models.Q(place_a__lt=models.F("place_b")),
                name="contrib_attest_canonical_order",
            ),
            models.UniqueConstraint(
                fields=["place_a", "place_b", "relation_type", "user"],
                name="contrib_attest_unique_edge_per_user",
            ),
        ]
        indexes = [
            models.Index(fields=["dataset_id", "status"],
                         name="contrib_attest_dataset_status"),
        ]

    def save(self, *args, **kwargs):
        # Enforce canonical ordering at write time so callers can pass
        # either direction; the CHECK constraint catches direct DB writes.
        if self.place_a and self.place_b and self.place_a > self.place_b:
            self.place_a, self.place_b = self.place_b, self.place_a
        return super().save(*args, **kwargs)

    def source_id(self) -> str:
        """Return the ``source_id`` value used in the Pitt SQLite overlay.

        Mirrors ``clustering/harvest/contributor_replay.py::_build_source_id``.
        """
        base = f"contributor:{self.user_id}"
        return f"{base}:legacy_v3_2" if self.legacy_v3_2 else base

    def __str__(self):
        return f"{self.user_id}: {self.place_a} <{self.relation_type}> {self.place_b}"


class GazetteerRegistryEntryQuerySet(models.QuerySet):
    """Adds the authority-visibility gate for embargoed rows (place#162)."""

    def visible_to(self, user):
        """Authority rows visible to ``user`` in registry-discovery surfaces
        (``/api/sources/``, the Gazetteers/Regions offcanvas, Atlas layer
        picker, coverage endpoints).

        ``published`` rows are always visible. ``embargoed`` rows are
        visible to ``can_access_beta`` staff regardless of release date, and
        to everyone once ``embargo_release_at`` has passed — a lazy
        auto-release; ``release_embargoes`` (management command / Celery
        Beat task) durably flips ``status`` to ``published`` once past that
        point so the DB converges even if nothing reads it in the interim.
        Other statuses (draft/submitted/rejected/pending) are untouched here
        — those gate the separate WHG-dataset contributor workflow.
        """
        beta = bool(
            user and getattr(user, 'is_authenticated', False)
            and getattr(user, 'can_access_beta', False)
        )
        visible = models.Q(status='published') | models.Q(
            status='embargoed', embargo_release_at__lte=timezone.now(),
        )
        if beta:
            visible |= models.Q(status='embargoed')
        return self.filter(visible)


class GazetteerRegistryEntry(models.Model):
    """Per-gazetteer / per-WHG-dataset registry row pushed by the ingest
    pipeline's Batch 11 ``processing/push_gazetteer_inventory.py``.

    The push is idempotent: each successful indexing run upserts every
    selected gazetteer's row by ``id``. The Django UI reads this table to
    populate the gazetteer-selection widget and to surface counts /
    coverage / temporal extent in search.

    LOC is intentionally absent — it's a relations-only namespace and never
    appears in the inventory.
    """

    CLASS_CHOICES = [
        ("authority", "authority"),
        ("dataset",   "dataset"),
    ]
    STATUS_CHOICES = [
        ("draft",     "draft"),
        ("submitted", "submitted"),
        ("rejected",  "rejected"),
        ("pending",   "pending"),
        ("embargoed", "embargoed"),
        ("published", "published"),
    ]
    GAZETTEER_TYPE_CHOICES = [
        ("standard",  "Standard"),
        ("itinerary", "Itinerary"),
        ("network",   "Network"),
    ]
    REINGEST_STATUS_CHOICES = [
        ("idle",      "Idle"),
        ("queued",    "Queued"),
        ("running",   "Running"),
        ("completed", "Completed"),
        ("failed",    "Failed"),
    ]

    # Stable id used by the inventory push; matches the namespace for
    # authorities (``"gn"``) and the sub-namespace for WHG datasets
    # (``"whg:1234"``).
    id = models.CharField(max_length=64, primary_key=True)

    name = models.CharField(max_length=255)
    description = models.TextField(null=True, blank=True)
    namespace = models.CharField(max_length=16, db_index=True)
    entry_class = models.CharField(
        max_length=16, choices=CLASS_CHOICES, db_column="class",
        db_index=True,
    )
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name="gazetteer_registry_entries",
    )
    record_count = models.IntegerField(default=0)
    status = models.CharField(
        max_length=12, choices=STATUS_CHOICES, default="published",
        db_index=True,
        help_text=(
            "Authorities only (place#162): set to 'embargoed' to hold a "
            "fully-indexed gazetteer back from anonymous/non-BETA discovery "
            "while it stays usable by can_access_beta staff in the live "
            "Atlas BETA UI — see GazetteerRegistryEntry.objects.visible_to(). "
            "This is a whg3/Django-level visibility gate ONLY: it does not "
            "and cannot restrict direct access to the CRC ES gateway "
            "(/api/search, /api/reconcile), which has no per-namespace "
            "access control of its own. Where that distinction matters, use "
            "the indexing repo's disposable staging ES instead. A push from "
            "the ingest pipeline never sets 'embargoed' itself and never "
            "overwrites it once set — see GazetteerInventoryView._upsert_one."
        ),
    )

    # Either the literal string "global" (for global-coverage authorities)
    # or a list of compacted H3 cell IDs.
    h3_coverage = models.JSONField(default=list)
    # Condensed res-2 rollup of ``h3_coverage`` (≤842 cells global, ~200 KB
    # total across authorities) — small enough to ship to the browser for the
    # Atlas "Hide gazetteers outside Area filter" switch (h3-js intersection).
    # "global" for global-coverage authorities. Pushed by push_gazetteer_inventory.
    h3_coverage_coarse = models.JSONField(default=list)
    # ``[min_start_year, max_end_year]`` with each endpoint optionally null.
    temporal_extent = models.JSONField(default=list)

    # ── Attribution / licence / rights (citations design §3.2) ──────────
    # Source-of-truth = indexing ``AUTHORITIES`` → pushed by Batch 11
    # (``push_gazetteer_inventory.py``) into the inventory payload, applied
    # by ``api/views_indexing.py::GazetteerInventoryView._upsert_one``.
    # These are *push-managed* like the other inventory fields above (NOT in
    # the admin-protected curatorial set), so a push refreshes them from the
    # canonical source. All optional — a source supplies whatever it has
    # (design decision 3: metadata flexibility).
    #
    # Human-readable citation. Historically the citation blob was crammed
    # into ``description``; new pushes populate this and keep ``description``
    # for genuine prose. ``attribution_for()`` prefers this, falling back to
    # ``description`` for rows pushed before the upgrade.
    citation_text = models.TextField(null=True, blank=True)
    # Canonical SPDX licence; resolved from the pushed ``license_spdx`` code.
    license = models.ForeignKey(
        'licensing.License',
        on_delete=models.SET_NULL,
        null=True, blank=True,
        related_name='gazetteer_entries',
    )
    # Override deed URL when the source deviates from the canonical licence
    # deed (left null to use ``license.url``).
    license_url = models.URLField(null=True, blank=True)
    # e.g. "J. Paul Getty Trust", "ISAW".
    rights_holder = models.CharField(max_length=255, null=True, blank=True)
    # Homepage / landing page for the source.
    source_url = models.URLField(null=True, blank=True)
    # Per-place web template for linking an item to the SOURCE's own human page,
    # with ``<id>`` standing in for the place's local id — e.g.
    # ``https://www.geonames.org/<id>`` (place#121). Distinct from ``source_url``
    # (the gazetteer homepage) and from indexing's ``api_item`` (a JSON endpoint).
    # Push-managed for authorities; set from the dataset's own template for WHG
    # datasets. When present, the Atlas popup surfaces a "view at source" link.
    web_item = models.CharField(max_length=500, null=True, blank=True)
    # Optional CRediT-shaped provider credit where the source documents it:
    # ``[{"name": ..., "role": ..., "orcid": ...}]`` (see §3.3 output shape).
    contributors_csl = models.JSONField(default=list, blank=True)

    # ── Download legality + volume (place#136) ──────────────────────────
    # Push-managed like the attribution fields above (NOT admin-curated):
    # they derive from licence + record_count computed at ingest, so a push
    # refreshes them from the canonical indexing ``AUTHORITIES`` config.
    # They govern ONLY whether WHG offers a downloadable copy of the source
    # data — indexing / search / reconciliation are permitted for every
    # authority regardless (those run server-side and never expose raw data).
    #
    # ``redistributable``: the legal determination — may WHG re-host /
    # redistribute a copy of this authority's data? Recorded explicitly per
    # authority (implicit in licence for many: CC0/CC-BY/CC-BY-SA ⇒ yes;
    # UKDS EULA / bespoke non-redistribution ⇒ no).
    redistributable = models.BooleanField(default=True)
    # ``downloadable``: the effective flag the UI acts on —
    # ``redistributable AND record_count <= DOWNLOAD_MAX_RECORDS`` (the volume
    # half prevents a legally-open but very large authority — gn/osm/wd/ohm/gb,
    # millions of records — being offered as a bulk download that could
    # overburden the server). Gates any "Download this dataset" affordance.
    downloadable = models.BooleanField(default=True)
    # Why a download isn't offered, set only when ``downloadable`` is false:
    # ``"licence-restricted"`` or ``"volume-exceeds-cap"``. Null when
    # downloadable (a push clears any stale reason).
    download_blocked_reason = models.CharField(
        max_length=32, null=True, blank=True,
    )

    # Curatorial fields managed via the Django admin only — the inventory
    # push from the indexing pipeline deliberately omits these so it never
    # overwrites staff curation. Defaults must mirror the migration so
    # rows pushed without these fields satisfy NOT NULL on INSERT.
    core = models.BooleanField(default=False, db_index=True)
    # ``no_explore``: hides the entry's tileset-dependent affordances
    # (Explore mode in the Gazetteers offcanvas, in-Atlas polygon hover/click).
    # Renamed from ``tileset_polygon_only`` — the original name was opaque.
    no_explore = models.BooleanField(default=False)
    # ``region_source``: appears as a selectable Source in the Regions
    # offcanvas (the boundary-namespace toggle in the Atlas page). Drives
    # ``available_sources`` in ``search.views.AtlasPageView``.
    region_source = models.BooleanField(default=False, db_index=True)
    # Embargo auto-release (place#162). Only meaningful when
    # ``status == 'embargoed'``. Null = embargoed indefinitely, lifted only
    # by manual admin action. Set = auto-publish once this passes, applied
    # lazily by ``visible_to()`` and converged durably by the
    # ``release_embargoes`` management command / Celery Beat task, so the
    # row's ``status`` itself flips to ``published`` even if nothing reads
    # it in the interim.
    embargo_release_at = models.DateTimeField(null=True, blank=True)
    gazetteer_type = models.CharField(
        max_length=16,
        choices=GAZETTEER_TYPE_CHOICES,
        default="standard",
    )

    # Re-ingest tracking — written by the admin "Re-ingest" action and
    # by ``api.reingest`` polling the Pitt gateway. Only the latest run is
    # tracked; if history is needed later a separate audit table can hold it.
    reingest_status = models.CharField(
        max_length=12,
        choices=REINGEST_STATUS_CHOICES,
        default="idle",
        db_index=True,
    )
    reingest_started_at = models.DateTimeField(null=True, blank=True)
    reingest_finished_at = models.DateTimeField(null=True, blank=True)
    reingest_job_id = models.CharField(max_length=64, null=True, blank=True)
    reingest_message = models.TextField(null=True, blank=True)

    updated_at = models.DateTimeField(auto_now=True)

    objects = GazetteerRegistryEntryQuerySet.as_manager()

    @property
    def is_global(self) -> bool:
        return self.h3_coverage == "global"

    def __str__(self):
        return f"{self.id} ({self.entry_class})"

    @classmethod
    def release_due_embargoes(cls) -> int:
        """Durably converge ``embargo_release_at`` (place#162): flip any
        ``embargoed`` row whose release time has passed to ``published``.

        ``visible_to()`` already treats such rows as visible lazily, so this
        is a convergence step, not the only enforcement — it exists so the
        DB itself reflects the release (e.g. for admin list views/exports
        that don't go through ``visible_to()``). Called by the
        ``release_embargoes`` management command and its Celery Beat
        schedule (``whg/celery.py``). Returns the number of rows released.
        """
        return cls.objects.filter(
            status='embargoed', embargo_release_at__isnull=False,
            embargo_release_at__lte=timezone.now(),
        ).update(status='published')
