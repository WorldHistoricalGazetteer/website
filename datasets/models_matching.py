# datasets.models_matching — match candidates and the contributor feed (place#334 Phase 2, place#338)
#
# Three tables, one per question the feed has to answer:
#
# * ``CandidateGeneration``: one pass of candidate generation over a dataset: what was asked of
#   the gateway (the recipe), against which index and clustering parameters, how far it got.
# * ``MatchCandidate``: one (record, suggested place) pair a generation proposed, with the
#   signals it was scored on and the pieces of the sentence that explains it.
# * ``RecordReviewState``: per record, what the last generation FOUND (``tier``) and, separately,
#   what a person DID (``state``). The two are kept apart so that "the gateway never answered"
#   (``tier='unqueried'``) can never be read as "nothing found" (``tier='none'``): silence is not
#   absence (memory project_entity_timeout_becomes_404).
#
# A suggestion is never a statement: accepting one creates a ContributorAttestation through
# ``api.attestations.create`` (which keeps every visibility and cap rule), and the attestation,
# not this table, is what anyone else sees.
from django.conf import settings
from django.db import models
from django.utils import timezone

TIER_CONFIDENT = "confident"
TIER_AMBIGUOUS = "ambiguous"
TIER_WEAK = "weak"
TIER_NONE = "none"
TIER_UNQUERIED = "unqueried"

CANDIDATE_TIERS = [
    (TIER_CONFIDENT, "Likely the same place"),
    (TIER_AMBIGUOUS, "Which one, if any?"),
    (TIER_WEAK, "Weak suggestion"),
]
RECORD_TIERS = CANDIDATE_TIERS + [
    (TIER_NONE, "Nothing found"),
    # The gateway did not answer (timeout, unreachable, disabled): NOT the same as "none".
    (TIER_UNQUERIED, "Not yet checked"),
]


class CandidateGeneration(models.Model):
    STATUS_CHOICES = [
        ("queued", "queued"),
        ("running", "running"),
        ("paused", "paused"),        # stopped by a gateway outage; resumable from last_place_pk
        ("finished", "finished"),
        ("failed", "failed"),
        ("superseded", "superseded"),  # a newer generation exists for the dataset
    ]

    dataset = models.ForeignKey("datasets.Dataset", on_delete=models.CASCADE,
                                related_name="candidate_generations")
    requested_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                     on_delete=models.SET_NULL, related_name="+")
    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default="queued", db_index=True)
    created = models.DateTimeField(default=timezone.now)
    started = models.DateTimeField(null=True, blank=True)
    finished = models.DateTimeField(null=True, blank=True)
    # Written every batch while running, so a crashed worker is told apart from a live one.
    heartbeat = models.DateTimeField(null=True, blank=True)
    # What the candidates were computed against; a change in either makes them stale (4b).
    index_alias_generation = models.CharField(max_length=64, blank=True, default="")
    clustering_params_version = models.CharField(max_length=32, blank=True, default="")
    clustering_params = models.JSONField(default=dict, blank=True)
    # The retrieval recipe shared by every record of the run (scope chips, size, mode).
    recipe = models.JSONField(default=dict, blank=True)
    # Progress: total, done, answered, unqueried, and one count per record tier.
    counts = models.JSONField(default=dict, blank=True)
    # Resume cursor: records are walked in pk order; everything at or below this is done.
    last_place_pk = models.BigIntegerField(null=True, blank=True)
    # The cap this run was started with (a non-production context must set one).
    record_limit = models.IntegerField(null=True, blank=True)
    error = models.TextField(blank=True, default="")

    class Meta:
        db_table = "match_candidate_generation"
        ordering = ["-id"]

    def __str__(self):
        return f"generation {self.id} of dataset {self.dataset_id} ({self.status})"


class MatchCandidate(models.Model):
    STATUS_CHOICES = [
        ("suggested", "suggested"),
        ("accepted", "accepted"),    # a Link (attestation) was made from it
        ("rejected", "rejected"),    # "not this one"; NOT a `distinct` statement
        ("stale", "stale"),          # the index or the clustering parameters moved on
    ]

    dataset = models.ForeignKey("datasets.Dataset", on_delete=models.CASCADE,
                                related_name="match_candidates")
    place = models.ForeignKey("places.Place", on_delete=models.CASCADE,
                              related_name="match_candidates")
    # The record's gateway id, "whg:<dataset pk>:<src_id>".
    subject_id = models.CharField(max_length=160)
    # The suggested place's gateway id, "gn:…", "wd:…", "whg:<ds>:<src_id>".
    candidate_id = models.CharField(max_length=160)
    candidate_title = models.CharField(max_length=500, blank=True, default="")
    namespace = models.CharField(max_length=32, blank=True, default="", db_index=True)
    # The gateway's relative ranking score and its ABSOLUTE match quality (0-100, place#206).
    # Both nullable: an unmeasured confidence is not a poor one.
    gateway_score = models.FloatField(null=True, blank=True)
    gateway_confidence = models.FloatField(null=True, blank=True)
    # {name, spatial, temporal, type, link, composite, km, type_sanity, ...}; absent = not measured.
    signals = models.JSONField(default=dict, blank=True)
    tier = models.CharField(max_length=12, choices=CANDIDATE_TIERS)
    rank = models.PositiveSmallIntegerField(default=0)
    generation = models.ForeignKey(CandidateGeneration, on_delete=models.CASCADE,
                                   related_name="candidates")
    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default="suggested")
    # The sentence pieces the feed renders; every piece is data from the response.
    explanation = models.JSONField(default=dict, blank=True)
    # A trimmed card of the suggested place as the gateway described it (names, types,
    # ccodes, point, dates), so the feed renders without calling the gateway again.
    card = models.JSONField(default=dict, blank=True)
    created = models.DateTimeField(default=timezone.now)
    decided_at = models.DateTimeField(null=True, blank=True)
    decided_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                   on_delete=models.SET_NULL, related_name="+")

    class Meta:
        db_table = "match_candidate"
        constraints = [
            models.UniqueConstraint(fields=["place", "candidate_id", "generation"],
                                    name="match_candidate_unique_per_generation"),
        ]
        indexes = [
            models.Index(fields=["dataset", "tier", "status"], name="match_cand_ds_tier_status"),
        ]
        ordering = ["place_id", "rank"]

    def __str__(self):
        return f"{self.subject_id} ~ {self.candidate_id} ({self.tier})"


class RecordReviewState(models.Model):
    STATE_CHOICES = [
        ("unreviewed", "unreviewed"),
        ("linked", "linked"),            # at least one Link was made from the feed
        ("no_good_match", "no_good_match"),  # "None of these fits"
        ("later", "later"),              # "Not sure yet": parked, never expires
        ("excluded", "excluded"),        # the contributor chose not to link this record
    ]

    place = models.OneToOneField("places.Place", on_delete=models.CASCADE,
                                 related_name="review_state")
    dataset = models.ForeignKey("datasets.Dataset", on_delete=models.CASCADE,
                                related_name="record_review_states")
    # The generation whose answer ``tier`` reports (None until a generation reached the record).
    generation = models.ForeignKey(CandidateGeneration, null=True, blank=True,
                                   on_delete=models.SET_NULL, related_name="record_states")
    tier = models.CharField(max_length=12, choices=RECORD_TIERS, default=TIER_UNQUERIED)
    state = models.CharField(max_length=16, choices=STATE_CHOICES, default="unreviewed")
    # Why the last attempt went unanswered ("timeout", "unreachable", "disabled", ...).
    last_failure = models.CharField(max_length=32, blank=True, default="")
    attempts = models.PositiveIntegerField(default=0)
    queried_at = models.DateTimeField(null=True, blank=True)
    candidate_count = models.PositiveSmallIntegerField(default=0)
    # Feed ordering within a tier (4c): records lacking geometry first, then by how many
    # records of the dataset the same decision touches.
    has_geometry = models.BooleanField(default=False)
    top_candidate_id = models.CharField(max_length=160, blank=True, default="")
    shared_count = models.PositiveIntegerField(default=0)
    decided_at = models.DateTimeField(null=True, blank=True)
    decided_by = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                   on_delete=models.SET_NULL, related_name="+")
    note = models.TextField(blank=True, default="")

    class Meta:
        db_table = "record_review_state"
        indexes = [
            models.Index(fields=["dataset", "tier", "state"], name="record_review_ds_tier_state"),
        ]

    def __str__(self):
        return f"record {self.place_id}: {self.tier}/{self.state}"
