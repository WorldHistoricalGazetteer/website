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

    def increment_usage(self):
        today = timezone.now().date()
        if self.daily_reset != today:
            self.daily_reset = today
            self.daily_count = 0
        self.daily_count += 1
        self.total_count += 1
        self.save()

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

    @property
    def is_global(self) -> bool:
        return self.h3_coverage == "global"

    def __str__(self):
        return f"{self.id} ({self.entry_class})"
