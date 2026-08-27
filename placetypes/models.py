# placetypes/models.py
"""
The Type model — a local mirror of the AAT place-type hierarchy.

Replaces the original flat lookup table that lived in places.models.
The db_table remains 'types' for full backward compatibility with the
existing database; no data migration is needed.
"""

from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.db import models

from main.choices import FEATURE_CLASSES


class Type(models.Model):
    aat_id = models.IntegerField(unique=True)
    parent_id = models.IntegerField(null=True, blank=True)
    term = models.CharField(max_length=100)
    term_full = models.CharField(max_length=100)
    note = models.TextField(max_length=3000)
    fclasses = ArrayField(
        models.CharField(max_length=1, choices=FEATURE_CLASSES),
        null=True, blank=True,
        help_text="GeoNames feature classes inherited from all AAT ancestor paths.",
    )

    # --- Hierarchy fields (populated by the sync_aat_types command) ---

    # Materialized path: dot-separated ancestor aat_ids from root to self.
    # e.g. "300008347.300008372.300008389" for a city under inhabited places.
    path = models.CharField(max_length=500, blank=True, default='')

    # Depth in the hierarchy (0 = root category).
    depth = models.IntegerField(default=0)

    # Whether this concept is a recognised WHG place type
    # (i.e. it descends from one of the configured AAT entry points
    # and is not in an excluded subtree).
    is_place_type = models.BooleanField(default=True)

    # ------------------------------------------------------------------

    def __str__(self):
        return f"{self.aat_id}:{self.term}"

    @property
    def fclass(self):
        """
        DEPRECATED — the first fclass letter, or None.

        `fclasses` is stored sorted, so this silently truncates: `cities` and `quilombos`
        both carry ['A', 'P'] and this returns 'A' — administrative — which dropped them
        out of populated-place filtering. 583 of the ~59,000 concepts carry more than one
        class. Derivation should read the whole `fclasses` array; see
        `datasets.place_types.fclasses_for_aat` (place#213). Kept only for callers outside
        this repository.
        """
        if self.fclasses:
            return self.fclasses[0]
        return None

    @property
    def aat_identifier(self):
        """Return the prefixed AAT identifier string, e.g. 'aat:300008347'."""
        return f"aat:{self.aat_id}"

    def get_descendants(self, include_self=True):
        """
        Return a queryset of all descendant Types using the materialized path.
        """
        prefix = f"{self.path}." if self.path else f"{self.aat_id}."
        qs = Type.objects.filter(path__startswith=prefix)
        if include_self:
            qs = qs | Type.objects.filter(pk=self.pk)
        return qs

    def get_ancestors(self):
        """Return a queryset of ancestor Types from the materialized path."""
        if not self.path:
            return Type.objects.none()
        ancestor_ids = [int(x) for x in self.path.split('.')[:-1]]
        return Type.objects.filter(aat_id__in=ancestor_ids)

    class Meta:
        managed = True
        db_table = 'types'
        indexes = [
            models.Index(fields=['path']),
            models.Index(fields=['is_place_type']),
            models.Index(fields=['depth', 'is_place_type']),
        ]


class TypeMappingLog(models.Model):
    """
    Audit trail for mapping decisions.

    Each save, remove, or bulk-copy action on the ES types index is
    recorded here so we know who changed what and when.
    """
    ACTION_CHOICES = [
        ('save', 'Save mapping'),
        ('remove', 'Remove mapping'),
    ]

    CONFIDENCE_CHOICES = [
        ('exact', 'Exact match'),
        ('close', 'Close match'),
        ('review', 'Needs review'),
    ]

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        related_name='type_mapping_logs',
    )
    action = models.CharField(max_length=10, choices=ACTION_CHOICES)
    source_vocab = models.CharField(
        max_length=20,
        help_text='Source vocabulary: geonames, wikidata, osm_ohm',
    )
    source_id = models.CharField(
        max_length=120,
        help_text='Source identifier, e.g. P.PPL, Q515, place=city',
    )
    aat_id = models.IntegerField(
        null=True, blank=True,
        help_text='Target AAT concept ID',
    )
    aat_term = models.CharField(
        max_length=200, blank=True, default='',
        help_text='AAT term at the time of this action',
    )
    confidence = models.CharField(
        max_length=10, blank=True, default='',
        choices=CONFIDENCE_CHOICES,
        help_text='Mapping confidence: exact, close, or review',
    )
    note = models.TextField(
        blank=True, default='',
        help_text='Optional context (e.g. bulk copy summary)',
    )
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'type_mapping_log'
        ordering = ['-created']
        indexes = [
            models.Index(fields=['-created']),
            models.Index(fields=['source_vocab', 'source_id']),
        ]

    def __str__(self):
        return (
            f"{self.get_action_display()} {self.source_vocab}:{self.source_id}"
            f" → aat:{self.aat_id} by {self.user} @ {self.created}"
        )


