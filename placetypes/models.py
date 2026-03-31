# placetypes/models.py
"""
The Type model — a local mirror of the AAT place-type hierarchy.

Replaces the original flat lookup table that lived in places.models.
The db_table remains 'types' for full backward compatibility with the
existing database; no data migration is needed.
"""

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
        """Backward-compat: return the first fclass letter, or None."""
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
