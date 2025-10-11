# periods/models.py

from django.contrib.gis.db import models as gis_models
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.db import models


class SpatialEntity(models.Model):
    uri = models.URLField(max_length=1024, unique=True)
    label = models.CharField(max_length=255, blank=True)
    geometry = gis_models.GeometryField(null=True, blank=True, srid=4326)
    bbox = gis_models.PolygonField(null=True, blank=True, srid=4326)  # Bounding box as polygon
    ccodes = ArrayField(
        models.CharField(max_length=2),
        default=list,  # Store an empty list instead of NULL when no codes
        blank=True,
        help_text="2-letter ISO country codes intersecting the geometry."
    )
    gazetteer_source = gis_models.CharField(max_length=255, blank=True)

    class Meta:
        verbose_name_plural = "Spatial Entities"
        db_table = "periods_spatialentity"

    def __str__(self): return self.label or self.uri


class GazetteerTracker(gis_models.Model):
    """Track processed gazetteer files and their commit hashes"""
    filename = gis_models.CharField(max_length=255, unique=True)
    commit_hash = gis_models.CharField(max_length=40)
    last_processed = gis_models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "periods_gazetteer_tracker"

    def __str__(self):
        return f"{self.filename} ({self.commit_hash[:8]})"


class Chrononym(models.Model):
    label = models.CharField(max_length=255)
    languageTag = models.CharField(max_length=20)

    class Meta:
        unique_together = ('label', 'languageTag')
        verbose_name_plural = "Chrononyms"
        db_table = "periods_chrononym"
        indexes = [
            # B-tree index for prefix searches (startsWith)
            models.Index(
                fields=['label'],
                name='chrononym_label_prefix_idx',
                opclasses=['text_pattern_ops']
            ),
            # GIN trigram index for substring searches (contains)
            GinIndex(
                fields=['label'],
                name='chrononym_label_trigram_idx',
                opclasses=['gin_trgm_ops']
            ),
            # Composite index for language-specific searches
            models.Index(
                fields=['languageTag', 'label'],
                name='chrononym_lang_label_idx'
            ),
        ]


    def __str__(self): return f'{self.label} ({self.languageTag})'


class Authority(models.Model):
    id = models.CharField(max_length=7, blank=True, primary_key=True)
    source = models.JSONField(blank=True, null=True)
    type = models.CharField(max_length=9, blank=True)
    editorialNote = models.TextField(blank=True)
    sameAs = models.URLField(max_length=1024, blank=True)

    class Meta:
        verbose_name_plural = "Authorities"
        db_table = "periods_authority"

    def __str__(self):
        return getattr(self, 'label', str(self.pk))


class Period(models.Model):
    id = models.CharField(max_length=11, blank=True, primary_key=True)
    authority = models.ForeignKey(Authority, related_name='periods', on_delete=models.CASCADE)
    type = models.CharField(max_length=6, blank=True)
    language = models.URLField(max_length=1024, blank=True)
    chrononym = models.CharField(max_length=91, blank=True)  # PeriodO label
    editorialNote = models.TextField(blank=True)
    spatialCoverageDescription = models.CharField(max_length=183, blank=True)
    languageTag = models.CharField(max_length=7, blank=True)
    sameAs = models.URLField(max_length=1024, blank=True)
    url = models.URLField(max_length=1024, blank=True)
    note = models.TextField(blank=True)
    broader = models.CharField(max_length=11, blank=True)
    source = models.JSONField(blank=True, null=True)
    narrower = models.JSONField(blank=True, null=True)
    script = models.URLField(max_length=1024, blank=True)
    derivedFrom = models.JSONField(blank=True, null=True)
    spatialCoverage = models.ManyToManyField(SpatialEntity, related_name='periods')
    bbox = gis_models.PolygonField(null=True, blank=True, srid=4326)  # Computed from spatial coverage
    ccodes = ArrayField(  # Aggregate of all linked SpatialEntity ccodes
        models.CharField(max_length=2),
        default=list,
        blank=True,
        help_text="2-letter ISO country codes aggregated from linked SpatialEntities."
    )
    chrononyms = models.ManyToManyField(Chrononym, related_name='periods')  # PeriodO localizedLabels

    class Meta:
        verbose_name_plural = "Periods"
        db_table = "periods_period"

    def __str__(self):
        return getattr(self, 'label', str(self.pk))


class TemporalBound(models.Model):
    period = models.ForeignKey(Period, related_name='bounds', on_delete=models.CASCADE)
    kind = models.CharField(max_length=5, choices=[('start', 'Start'), ('stop', 'Stop')])
    year = models.JSONField(blank=True, null=True)
    label = models.CharField(max_length=91, blank=True)
    latestYear = models.BigIntegerField(null=True, blank=True)
    earliestYear = models.BigIntegerField(null=True, blank=True)

    class Meta:
        verbose_name_plural = "Temporal Bounds"
        db_table = "periods_temporalbound"

    def __str__(self):
        return f"{self.kind.title()} of {self.period}: {self.label or self.earliestYear}"
