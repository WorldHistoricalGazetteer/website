# place.models
from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import GeometryCollection

User = get_user_model()
from django.contrib.gis.db import models as geomodels
from django.contrib.gis.db.models import Extent
from django.contrib.postgres import indexes as django_postgres_indexes
from django.contrib.postgres.fields import ArrayField
from django.db.models import JSONField, Q
from django.db import models

# from datasets.models import Dataset
from datasets.static.hashes.parents import ccodes as cc
from main.choices import FEATURE_CLASSES, STATUS_REVIEW
from django_celery_results.models import TaskResult
from traces.models import TraceAnnotation

import logging

logger = logging.getLogger(__name__)


def yearPadder(y):
    year = str(y).zfill(5) if str(y)[0] == '-' else str(y).zfill(4)
    return year


class Place(models.Model):
    # id is auto-maintained, per Django
    title = models.CharField(max_length=255)
    src_id = models.CharField(max_length=2044, blank=True)
    # note FK is label, not id
    dataset = models.ForeignKey('datasets.Dataset',
                                db_column='dataset',
                                to_field='label',
                                related_name='places',
                                on_delete=models.CASCADE)
    ccodes = ArrayField(models.CharField(max_length=2, null=True), blank=True)
    create_date = models.DateTimeField(null=True, blank=True)
    minmax = ArrayField(models.IntegerField(blank=True, null=True), null=True, blank=True)
    timespans = JSONField(blank=True, null=True)  # for list of lists
    fclasses = ArrayField(models.CharField(max_length=1, choices=FEATURE_CLASSES), null=True, blank=True)
    indexed = models.BooleanField(default=False)
    idx_pub = models.BooleanField(default=False)
    idx_builder = models.BooleanField(default=False)
    flag = models.BooleanField(default=False)  # not in use
    # added Apr 2023, for case of no start/end
    attestation_year = models.IntegerField(null=True, blank=True)

    # 0=hits:unreviewed, 1=hits:reviewed, 2=hits:deferred, null=no hits
    review_wd = models.IntegerField(null=True, choices=STATUS_REVIEW)
    review_tgn = models.IntegerField(null=True, choices=STATUS_REVIEW)
    review_whg = models.IntegerField(null=True, choices=STATUS_REVIEW)

    def __str__(self):
        return '%s:%s' % (self.id, self.title)

    @property
    def authids(self):
        return [i.jsonb['identifier'] for i in self.links.all()]

    @property
    def collections(self):
        """
        Return a queryset of collections that this Place belongs to
        through the CollPlace model.
        """
        from collection.models import CollPlace
        return CollPlace.objects.filter(place=self)

    @property
    def countries(self):
        return [cc[0][x]['gnlabel'] for x in self.ccodes]

    @property
    def defer_comments(self):
        return self.comment_set.filter(tag='defer')

    @property
    def dsid(self):
        return self.dataset.id

    @property
    def extent(self):
        geoms = self.geoms.all()
        if geoms:
            extent = geoms.aggregate(extent=Extent('geom'))
            if extent['extent']:
                return extent['extent']
        return None

    @property
    def geom_count(self):
        return self.geoms.count()

    @property
    def geomtype(self):
        return self.geoms.all()[0].geom.geom_type

    @property
    def hashits_wd(self):
        return self.hit_set.filter(authority__in=['wd', 'wdlocal']).count() > 0

    @property
    def hashits_whg(self):
        return self.hit_set.filter(authority__in=['whg', 'idx']).count() > 0

    @property
    def hashits_tgn(self):
        return self.hit_set.filter(authority='tgn').count() > 0

    @property
    def matches(self):
        close_matches = CloseMatch.objects.filter(
            Q(place_a=self) | Q(place_b=self)
        )
        distinct_places = set(
            [self] + [match.place_a for match in close_matches] + [match.place_b for match in close_matches])
        # First in returned list is the Primary Place, determined by having the most links
        matches = sorted(distinct_places, key=lambda place: (place.links.count(), place.id), reverse=True)
        return matches

    @property
    def public(self):
        return self.dataset.public

    @property
    def repr_geom(self):
        """
        Returns a consolidated GEOS GeometryCollection representing ALL associated geometries.

        This replaces the old logic that only returned the first geometry.
        """
        geoms_qs = self.geoms.all()
        if not geoms_qs:
            return None

        geos_list = [g.geom for g in geoms_qs if g.geom]
        if not geos_list:
            return None

        try:
            return GeometryCollection(geos_list)
        except Exception as e:
            logger.error(f"Failed to create consolidated GeometryCollection for Place {self.id}: {e}")
            return None

    @property
    def repr_point(self):
        g = self.repr_geom

        if not g:
            return None

        try:
            return list(g.centroid.coords)
        except Exception:
            return None

    @property
    def traces(self):
        return TraceAnnotation.objects.filter(place=self.id)

    class Meta:
        managed = True
        db_table = 'places'
        indexes = [
            models.Index(fields=['src_id', 'dataset']),
        ]


# Type has moved to the `placetypes` app. Re-exported here for backward compatibility.
from placetypes.models import Type  # noqa: F401, E402


# NB in LPF spec but seldom used
class Source(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE)
    # TODO: force unique...turn into slug or integer
    src_id = models.CharField(max_length=30, unique=True)  # contributor's id
    uri = models.URLField(null=True, blank=True)
    label = models.CharField(max_length=255)  # short, e.g. title, author
    citation = models.CharField(max_length=500, null=True, blank=True)

    def __str__(self):
        return self.src_id

    class Meta:
        managed = True
        db_table = 'sources'


class PlaceName(models.Model):
    # {toponym, lang, citation{}, when{}}
    place = models.ForeignKey(Place, related_name='names',
                              default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100, default='')  # contributor's identifier
    jsonb = JSONField(blank=True, null=True)
    task_id = models.CharField(max_length=100, blank=True, null=True)

    # written only if task_id
    reviewer = models.ForeignKey(User, null=True, on_delete=models.SET_NULL)

    toponym = models.CharField(max_length=2044)
    name_src = models.ForeignKey(Source, null=True, on_delete=models.SET_NULL)
    create_date = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return self.toponym

    class Meta:
        managed = True
        db_table = 'place_name'
        # unique_together = ('place', 'src_id', 'jsonb',)


class PlaceType(models.Model):
    place = models.ForeignKey(Place, related_name='types',
                              default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100, default='')  # contributor's identifier
    jsonb = JSONField(blank=True, null=True)

    aat_id = models.IntegerField(null=True, blank=True)  # Getty AAT identifier
    fclass = models.CharField(max_length=1, choices=FEATURE_CLASSES)  # geonames feature class

    def __str__(self):
        return (self.jsonb['sourceLabel'] if 'sourceLabel' in self.jsonb else '') + \
            ('; ' + self.jsonb['label'] if 'label' in self.jsonb else '')

    class Meta:
        managed = True
        db_table = 'place_type'


class PlaceGeom(models.Model):
    place = models.ForeignKey(Place, related_name='geoms',
                              default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100, default='')  # contributor's identifier
    jsonb = JSONField(blank=True, null=True)
    task_id = models.CharField(max_length=100, blank=True, null=True)
    geom_src = models.ForeignKey(Source, null=True, db_column='geom_src',
                                 to_field='src_id', on_delete=models.SET_NULL)
    # TODO:
    geom = geomodels.GeometryField(null=True, blank=True, srid=4326)
    s2 = ArrayField(models.CharField(max_length=255, null=True), null=True)
    # informs dataset last_update
    create_date = models.DateTimeField(null=True, auto_now_add=True)
    # written only if task_id
    reviewer = models.ForeignKey(User, null=True, on_delete=models.SET_NULL)

    def save(self, *args, **kwargs):
        if self.task_id and not self.reviewer:
            raise ValueError("Reviewer is required when task_id is provided")
        super().save(*args, **kwargs)

    @property
    def title(self):
        return self.place.title

    # good to have, but not accessible in values_list queries
    @property
    def minmax(self):
        # tsarr=[]; intarr=[]
        # wg = self.jsonb['when']
        from edtf import parse_edtf
        def yearPadder(y):
            # print('y',y)
            year = str(y).zfill(5) if str(y)[0] == '-' else str(y).zfill(4)
            return year if int(y) > -9999 else '-9999'

        def getInt(expr):
            # print('expr',expr)
            return int(parse_edtf(yearPadder(list(expr.values())[0])).get_year())

        # when = pg.jsonb['when'] if 'when' in pg.jsonb else None
        when = self.jsonb['when'] if 'when' in self.jsonb else None
        tsarr = when['timespans'] if when and 'timespans' in when else None
        years = [];
        nullset = set([None]);
        intarr = []
        if when and tsarr:
            # years=[];nullset=set([None]);intarr=[]
            for ts in tsarr:
                start = getInt(ts['start'])
                end = getInt(ts['end']) if 'end' in ts else start
                years += [start, end]
                intarr.append([start, end])
            years = list(set(years) - nullset)
        return [min(years), max(years)] if len(years) > 0 else None

    class Meta:
        managed = True
        db_table = 'place_geom'


class PlaceLink(models.Model):
    place = models.ForeignKey(Place, related_name='links',
                              default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100, default='')  # contributor's identifier
    jsonb = JSONField(blank=True, null=True)
    task_id = models.CharField(max_length=100, blank=True, null=True)

    review_note = models.CharField(max_length=2044, blank=True, null=True)
    black_parent = models.IntegerField(blank=True,
                                       null=True)  # This seems to be a remnant of an earlier implementation referencing the DK Atlas of World History, ed. Black

    # informs dataset last_update
    create_date = models.DateTimeField(null=True, auto_now_add=True)
    # written only if task_id
    reviewer = models.ForeignKey(User, null=True, on_delete=models.SET_NULL)

    def save(self, *args, **kwargs):
        if self.task_id and not self.reviewer:
            raise ValueError("Reviewer is required when task_id is provided")
        super().save(*args, **kwargs)

    class Meta:
        managed = True
        db_table = 'place_link'


class PlaceWhen(models.Model):
    place = models.ForeignKey(Place, related_name='whens',
                              default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100, default='')  # contributor's identifier
    jsonb = JSONField(blank=True, null=True)
    minmax = ArrayField(models.IntegerField(blank=True, null=True), null=True)

    class Meta:
        managed = True
        db_table = 'place_when'


class PlaceRelated(models.Model):
    place = models.ForeignKey(Place, related_name='related',
                              default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100, default='')  # contributor's identifier
    jsonb = JSONField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'place_related'


class PlaceDescription(models.Model):
    place = models.ForeignKey(Place, related_name='descriptions',
                              default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100, default='')  # contributor's identifier
    jsonb = JSONField(blank=True, null=True)
    task_id = models.CharField(max_length=100, blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'place_description'
        indexes = [
            models.Index(fields=['place']),
        ]


class PlaceDepiction(models.Model):
    place = models.ForeignKey(Place, related_name='depictions',
                              default=-1, on_delete=models.CASCADE)
    src_id = models.CharField(max_length=100, default='')  # contributor's identifier
    jsonb = JSONField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = 'place_depiction'


class CloseMatch(models.Model):
    place_a = models.ForeignKey(Place, on_delete=models.CASCADE, related_name="close_match1")
    place_b = models.ForeignKey(Place, on_delete=models.CASCADE, related_name="close_match2")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    task = models.ForeignKey(TaskResult, on_delete=models.CASCADE, related_name="task",
                             null=True, blank=True)
    created_by = models.ForeignKey(User, on_delete=models.CASCADE, related_name="matcher")
    basis = models.CharField(max_length=200,
                             choices=[('authid', 'authority id'), ('reviewed', 'reviewed'), ('imported', 'imported')],
                             null=True, blank=True)

    class Meta:
        managed = True
        db_table = 'close_matches'
        indexes = [
            models.Index(fields=['place_a', 'place_b']),  # Speeds up duplicate checks
            models.Index(fields=['task']),
            models.Index(fields=['created_by']),
        ]


class LegacyUnionRecord(models.Model):
    """
    Frozen snapshot of the legacy Elasticsearch `whg` union index (place#170).

    Each row records what one `whg_id` meant at capture time: the Postgres place
    ids it united, and their titles at that moment.

    Why this exists
    ---------------
    `whg_id` was minted at index time (``maxID + 1``) and lived only in
    Elasticsearch. It is nonetheless in public circulation — carried by Wikidata
    property P13061, and dispensed for years by the portal's Permalink button
    (retired 2026-08-04) — so we are obliged to keep resolving it indefinitely.
    Once the legacy index is retired the mapping is unrecoverable at any price,
    so it is captured here first.

    The upcoming `/entity/locus:<PID>` endpoint reads this table: a request for a
    legacy `1[0-9]{7}` identifier that has no registry entry yet consults this
    snapshot and mints a *frozen* locus — one carrying no recipe, because these
    clusters came from the old pipeline rather than from the browser clusterer,
    and so resolving to their captured membership rather than to a live query.

    Scale (measured on prod, 2026-08-04): the ES index holds 2,134,062 documents,
    but it is a parent/join structure — 2,101,316 parents carrying a `whg_id` and
    32,746 child docs that carry none. Only the parents are union records, so that
    is the row count here. Of those, just 23,709 unite more than one place: 98.87%
    are singletons, and the largest holds 153.

    Invariants
    ----------
    * ``place_ids`` and ``titles`` are POSITIONALLY ALIGNED and always the same
      length. Use :attr:`members` rather than indexing them separately.
    * Append-only. Rows are never edited or deleted: each is a permanent record
      of what an identifier already in the wild was understood to mean.
    """

    whg_id = models.BigIntegerField(primary_key=True)

    # Postgres places.id values united under this whg_id: the ES doc's own
    # `place_id` first, then its `children`.
    place_ids = ArrayField(models.BigIntegerField(), default=list)

    # Titles as they stood at capture, aligned with `place_ids`. Denormalised
    # deliberately: if a Place row is later deleted, the id alone leaves a frozen
    # locus with nothing to show, whereas the captured title still lets it name
    # what used to be there.
    titles = ArrayField(models.TextField(blank=True), default=list)

    captured = models.DateTimeField(auto_now_add=True)

    class Meta:
        managed = True
        db_table = 'legacy_union_records'
        indexes = [
            # Reverse lookup: which legacy identifiers referenced this place?
            django_postgres_indexes.GinIndex(fields=['place_ids']),
        ]

    def __str__(self):
        return f'whg_id {self.whg_id} ({len(self.place_ids)} record(s))'

    @property
    def members(self):
        """[(place_id, title), ...] — the aligned pairs, safe against drift."""
        return list(zip(self.place_ids, self.titles))
