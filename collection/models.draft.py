from collections import Counter
from itertools import chain
import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.db import models as geomodels
from django.contrib.gis.geos import GEOSGeometry
from django.contrib.postgres.fields import ArrayField
from django.core.cache import caches
from django.core.validators import URLValidator
from django.db import models
from django.db.models import JSONField, Q
from django.urls import reverse
from django.utils import timezone
from django_resized import ResizedImageField
from geojson import dumps, loads

from main.choices import (COLLECTIONCLASSES, COLLECTIONGROUP_TYPES, LINKTYPES,
                          STATUS_COLL, TEAMROLES)
from places.models import Place
from utils.carousel_metadata import carousel_metadata
from utils.cluster_geometries import \
    clustered_geometries as calculate_clustered_geometries
from utils.csl_citation_formatter import csl_citation
from utils.feature_collection import feature_collection
from utils.heatmap_geometries import heatmapped_geometries
from utils.hull_geometries import hull_geometries
from utils.mixins import CollectionGeospatialMixin

logger = logging.getLogger(__name__)
User = get_user_model()


# --- Utility Functions ---

def collection_path(instance, filename):
    """Upload path for collection files: MEDIA_ROOT/collections/<coll_id>/<filename>"""
    return f'collections/{instance.id}/{filename}'


def collectiongroup_path(instance, filename):
    """Upload path for collection group files: MEDIA_ROOT/groups/<group_id>/<filename>"""
    return f'groups/{instance.id}/{filename}'


def user_directory_path(instance, filename):
    """Upload path for user files: MEDIA_ROOT/user_<username>/<filename>"""
    return f'user_{instance.owner.id}/{filename}'


def default_vis_parameters():
    """Default visualization parameters for collection display modes."""
    return {
        "max": {"trail": False, "tabulate": False, "temporal_control": "none"},
        "min": {"trail": False, "tabulate": False, "temporal_control": "none"},
        "seq": {"trail": False, "tabulate": False, "temporal_control": "none"}
    }


# --- Through Models ---

class CollDataset(models.Model):
    """Through model linking Collections to Datasets."""
    collection = models.ForeignKey('Collection', on_delete=models.CASCADE)
    dataset = models.ForeignKey('datasets.Dataset', on_delete=models.CASCADE)
    date_added = models.DateTimeField(default=timezone.now, null=True)

    class Meta:
        ordering = ['id']


# --- Main Model ---

class Collection(CollectionGeospatialMixin, models.Model):
    """
    A curated collection of places or datasets.

    Collections can be either 'place' or 'dataset' class, and may belong to
    CollectionGroups for collaborative work or instruction.
    """

    # ===============================================
    # 1. IDENTIFICATION & OWNERSHIP
    # ===============================================
    namespace = models.ForeignKey(
        'Namespace', on_delete=models.SET_NULL, null=True, blank=True, related_name='collections'
    )
    local_id = models.CharField(
        max_length=128, help_text="Local identifier within the namespace, e.g. '036'"
    )
    owner = models.ForeignKey(settings.AUTH_USER_MODEL, related_name='collections', on_delete=models.CASCADE)

    @property
    def identifier(self):
        """Return prefixed identifier, e.g. 'unm49:036'."""
        if self.namespace:
            return f"{self.namespace.prefix}:{self.local_id}"
        return self.local_id

    # ===============================================
    # 2. METADATA & CONTENT
    # ===============================================
    title = models.CharField(null=False, max_length=255)
    description = models.TextField(max_length=3000)
    # Collection type: 'place' or 'dataset'
    collection_class = models.CharField(choices=COLLECTIONCLASSES, max_length=12)

    keywords = ArrayField(models.CharField(max_length=50), null=True, default=list)
    # Per-collection relation keywords (e.g., waypoint, birthplace, battle site)
    rel_keywords = ArrayField(models.CharField(max_length=30), blank=True, null=True, default=list)

    # ===============================================
    # 3. CONTACT & CITATION
    # ===============================================
    creator = models.CharField(null=True, blank=True, max_length=500)
    contact = models.CharField(null=True, blank=True, max_length=500)
    webpage = models.URLField(null=True, blank=True)
    doi = models.BooleanField(default=False, help_text="Indicates if a DOI is associated with this collection")

    @property
    def citation_csl(self):
        """Cached CSL-formatted citation."""
        cached_value = caches['property_cache'].get(f"collection:{self.pk}:citation_csl")
        if cached_value:
            return cached_value

        result = csl_citation(self)
        caches['property_cache'].set(f"collection:{self.pk}:citation_csl", result, timeout=None)
        return result

    # ===============================================
    # 4. STATUS & GROUP WORKFLOW
    # ===============================================
    # Status options: group, sandbox, demo, ready, public
    status = models.CharField(max_length=12, choices=STATUS_COLL, default='sandbox', null=True, blank=True)
    public = models.BooleanField(default=False)
    featured = models.IntegerField(null=True, blank=True)

    create_date = models.DateTimeField(null=True, auto_now_add=True)
    version = models.CharField(null=True, blank=True, max_length=20)
    submit_date = models.DateTimeField(null=True, blank=True)

    group = models.ForeignKey("CollectionGroup", db_column='group', related_name="group",
                              null=True, blank=True, on_delete=models.PROTECT)

    # Nomination workflow (set by group leader)
    nominated = models.BooleanField(default=False)
    nominate_date = models.DateTimeField(null=True, blank=True)

    @property
    def collaborators(self):
        """All users with collection access (members and owners)."""
        team = CollectionUser.objects.filter(collection_id=self.id).values_list('user_id')
        return User.objects.filter(id__in=team)

    @property
    def owners(self):
        """All users with owner role, including primary owner."""
        owner_ids = list(CollectionUser.objects.filter(collection=self, role='owner').values_list('user_id', flat=True))
        owner_ids.append(self.owner.id)
        return User.objects.filter(id__in=owner_ids)

    # ===============================================
    # 5. CONTENT RELATIONSHIPS (M2M)
    # ===============================================
    datasets = models.ManyToManyField(
        'datasets.Dataset', through='collection.CollDataset', related_name='new_datasets', blank=True)
    places = models.ManyToManyField("places.Place", through='CollPlace', blank=True)
    relations = models.ManyToManyField(
        'self', through='CollectionRelation', symmetrical=False, related_name='related_collections', blank=True
    )

    # ===============================================
    # 6. GEOSPATIAL FIELDS (Cached)
    # Fields accessed by CollectionGeospatialMixin
    # ===============================================
    bbox = geomodels.PolygonField(null=True, blank=True, srid=4326)
    unioned_geometries = geomodels.MultiPolygonField(
        null=True, blank=True, srid=4326,
        help_text="Union of all constituent place geometries (points/lines buffered to 1km polygons)"
    )
    unioned_hulls = geomodels.MultiPolygonField(
        null=True, blank=True, srid=4326,
        help_text="Union of convex hulls of constituent place geometries"
    )
    coordinate_density = models.FloatField(null=True, blank=True)

    def invalidate_geometry_cache(self):
        """Clear cached geometries (e.g., after places are added/removed)."""
        self.unioned_geometries = None
        self.unioned_hulls = None
        self.coordinate_density = None
        self.save(update_fields=['unioned_geometries', 'unioned_hulls', 'coordinate_density'])
        logger.info(f"Geometry cache invalidated for collection {self.pk}")

    @property
    def coordinate_density_value(self):
        """
        Calculate coordinate density (coordinates per unit area).
        Cached in database after first calculation.
        """
        if self.coordinate_density is not None:
            return self.coordinate_density

        try:
            clustered_geometries = calculate_clustered_geometries(self, min_clusters=7)

            total_area = 0
            for hull in clustered_geometries['features']:
                geometry = hull['geometry']
                if isinstance(geometry, dict):
                    geojson_obj = loads(dumps(geometry))
                    geometry = GEOSGeometry(str(geojson_obj))
                total_area += geometry.area

            density = clustered_geometries['properties'].get('coordinate_count',
                                                             0) / total_area if total_area > 0 else 0

            self.coordinate_density = density
            self.save()
            return density
        except Exception as e:
            logger.error(f"Error calculating coordinate density for collection {self.pk}: {e}")
            return 0

    # ===============================================
    # 7. DISPLAY & FILES
    # ===============================================
    image_file = ResizedImageField(size=[800, 600], upload_to=collection_path, blank=True, null=True)
    file = models.FileField(upload_to=collection_path, blank=True, null=True)
    vis_parameters = JSONField(default=default_vis_parameters, null=True, blank=True)

    def get_absolute_url(self):
        return reverse('data-collections')

    @property
    def carousel_metadata(self):
        """Cached carousel display metadata."""
        cached_value = caches['property_cache'].get(f"collection:{self.pk}:carousel_metadata")
        if cached_value:
            return cached_value

        result = carousel_metadata(self)
        caches['property_cache'].set(f"collection:{self.pk}:carousel_metadata", result, timeout=None)
        return result

    @property
    def kw_colors(self):
        """Map relation keywords to display colors."""
        colors = ['orange', 'red', 'green', 'blue', 'purple',
                  'red', 'green', 'blue', 'purple']
        return dict(zip(self.rel_keywords, colors))

    # ===============================================
    # 8. DERIVED DATA PROPERTIES (Counts, Lists, Geospatial Outputs)
    # ===============================================

    @property
    def places_all(self):
        """All places in collection (from datasets or directly added)."""
        return Place.objects.filter(
            Q(dataset__in=self.datasets.all()) | Q(id__in=self.places.all().values_list('id'))
        )

    @property
    def places_ds(self):
        """Places from all datasets in this collection."""
        dses = self.datasets.all()
        return Place.objects.filter(dataset__in=dses)

    @property
    def places_thru(self):
        """Places with sequence information from CollPlace through model."""
        seq_places = [{'p': cp.place, 'seq': cp.sequence} for cp in
                      CollPlace.objects.filter(collection=self.id).order_by('sequence')]
        return seq_places

    @property
    def num_places(self):
        """Total number of places in collection."""
        if self.collection_class == "dataset":
            return Place.objects.filter(dataset__in=self.datasets.all()).count()
        else:
            return self.places.all().count()

    @property
    def numrows(self):
        """Alias for num_places (for consistency with Dataset model)."""
        return self.num_places

    @property
    def rowcount(self):
        """Alias for num_places (backward compatibility)."""
        return self.num_places

    @property
    def dl_est(self):
        """Estimated download time based on number of place records (20 seconds per 1000 records)."""
        num_records = self.places_all.count()
        est_time_in_sec = (num_records / 1000) * 20
        minutes, seconds = divmod(est_time_in_sec, 60)

        if minutes < 1:
            return f"{seconds:02.0f} sec"
        elif seconds >= 10:
            return f"{minutes:02.0f} min {seconds:02.0f} sec"
        else:
            return f"{minutes:02.0f} min"

    @property
    def ds_counter(self):
        """Count of places by dataset."""
        dc = self.datasets.all().values_list('label', flat=True)
        dp = self.places.all().values_list('dataset', flat=True)
        all_items = Counter(list(chain(dc, dp)))
        return dict(all_items)

    @property
    def ds_list(self):
        """List of datasets with metadata, varies by collection_class."""
        if self.collection_class == 'dataset':
            dsc = [{"id": d.id, "label": d.label, "extent": d.extent, "bounds": d.bounds, "title": d.title,
                    "dl_est": d.dl_est, "numrows": d.numrows, "modified": d.last_modified_text}
                   for d in self.datasets.all()]
            return list({item['id']: item for item in dsc}.values())
        elif self.collection_class == 'place':
            datasets = set(place.dataset for place in self.places.all())
            dsp = [{"id": d.id, "label": d.label, "title": d.title, "modified": d.last_modified_text}
                   for d in datasets]
            return list({item['id']: item for item in dsp}.values())

    @property
    def last_modified_iso(self):
        """ISO-formatted date of last modification (from logs or creation)."""
        logtypes_to_include = ['create', 'update']
        filtered_logs = self.log.filter(logtype__in=logtypes_to_include)

        if filtered_logs.count() > 0:
            last = filtered_logs.order_by('-timestamp').first().timestamp
        else:
            last = self.create_date

        return last.strftime("%Y-%m-%d")

    # Geospatial Output Properties (using utility functions)
    @property
    def clustered_geometries(self):
        """Geometries clustered for map display."""
        return calculate_clustered_geometries(self)

    @property
    def feature_collection(self):
        """GeoJSON FeatureCollection of all collection geometries."""
        return feature_collection(self)

    @property
    def heatmapped_geometries(self):
        """Geometries processed for heatmap display."""
        return heatmapped_geometries(self)

    @property
    def hull_geometries(self):
        """Convex hull geometries for collection bounds."""
        return hull_geometries(self)

    # ===============================================
    # 9. DJANGO OVERRIDES / META
    # ===============================================
    def __str__(self):
        return '%s' % (self.title)

    class Meta:
        db_table = 'collections'
        indexes = [
            models.Index(fields=['namespace', 'local_id']),
            geomodels.Index(fields=['unioned_geometries']),
            geomodels.Index(fields=['unioned_hulls']),
        ]


# --- Other Collection-Related Models ---

class CollPlace(models.Model):
    """
    Through model for place membership in collections.
    Sequence is managed separately in TraceAnnotation.
    """
    collection = models.ForeignKey(Collection, related_name='annos', on_delete=models.CASCADE)
    place = models.ForeignKey(Place, related_name='annos', on_delete=models.CASCADE)
    sequence = models.IntegerField(null=True, default=0)


class CollectionUser(models.Model):
    """User membership and roles within a collection."""
    collection = models.ForeignKey(Collection, related_name='collabs', default=-1, on_delete=models.CASCADE)
    user = models.ForeignKey(User, related_name='collection_collab', default=-1, on_delete=models.CASCADE)
    role = models.CharField(max_length=20, null=False, choices=TEAMROLES)

    def __str__(self):
        name = self.user.name
        return '<b>' + name + '</b> (' + self.user.username + '); role: ' + self.role + '; ' + self.user.email

    class Meta:
        managed = True
        db_table = 'collection_user'


class CollectionGroup(models.Model):
    """
    Groups for organizing collections (e.g., instructor-led assignments, workshops).
    Collections can belong to multiple groups.
    """
    owner = models.ForeignKey(settings.AUTH_USER_MODEL,
                              related_name='collection_groups', on_delete=models.CASCADE)
    title = models.CharField(null=False, max_length=300)
    description = models.TextField(null=True, max_length=3000)
    type = models.CharField(choices=COLLECTIONGROUP_TYPES, default="class", max_length=8)
    keywords = ArrayField(models.CharField(max_length=50), null=True)
    file = models.FileField(upload_to=collectiongroup_path, blank=True, null=True)
    created = models.DateTimeField(auto_now_add=True)
    start_date = models.DateTimeField(null=True)
    due_date = models.DateTimeField(null=True)

    collections = models.ManyToManyField("collection.Collection", blank=True)

    # Group options
    gallery = models.BooleanField(null=False, default=False)
    gallery_required = models.BooleanField(null=False, default=False)
    collaboration = models.BooleanField(null=False, default=False)
    join_code = models.CharField(null=True, unique=True, max_length=20)

    def __str__(self):
        return self.title

    class Meta:
        managed = True
        db_table = 'collection_group'


class CollectionGroupUser(models.Model):
    """User membership in collection groups."""
    collectiongroup = models.ForeignKey(CollectionGroup, related_name='members',
                                        default=-1, on_delete=models.CASCADE)
    user = models.ForeignKey(User, related_name='members', default=-1, on_delete=models.CASCADE)
    role = models.CharField(max_length=20, null=False, choices=TEAMROLES, default='member')

    def __str__(self):
        return '%s (%s, %s)' % (self.user.email, self.user.id, self.user.name)

    class Meta:
        managed = True
        db_table = 'collection_group_user'


class CollectionLink(models.Model):
    """
    External links associated with collections.
    Note: Consider deprecating in favor of embedded Link model.
    """
    collection = models.ForeignKey(Collection, default=None,
                                   on_delete=models.CASCADE, related_name='links')
    label = models.CharField(null=True, blank=True, max_length=200)
    uri = models.TextField(validators=[URLValidator()])
    link_type = models.CharField(default='webpage', max_length=10, choices=LINKTYPES)
    license = models.CharField(null=True, blank=True, max_length=64)

    def __str__(self):
        cap = self.label[:20] + ('...' if len(self.label) > 20 else '')
        return '%s:%s' % (self.id, cap)

    class Meta:
        managed = True
        db_table = 'collection_link'


class CollectionRelation(models.Model):
    """
    LPF-style relation between collections, modelled after Linked Places Format.
    """
    source = models.ForeignKey(
        'Collection', on_delete=models.CASCADE,
        related_name='relations_from'
    )
    target = models.ForeignKey(
        'Collection', on_delete=models.CASCADE,
        related_name='relations_to'
    )
    relation_type = models.CharField(
        max_length=128,
        help_text="LPF relationType, e.g. 'gvp:broaderPartitive' (is part of), 'gvp:narrowerPartitive' (contains)"
    )
    label = models.CharField(
        max_length=512, null=True, blank=True,
        help_text="Human-readable label, e.g. 'part of Oceania (009)', 'contains Micronesia (057)'"
    )
    when = JSONField(
        null=True, blank=True,
        help_text="JSON structure for LPF-style 'when' object with timespans. Crucial for historical regions."
    )
    citations = JSONField(
        null=True, blank=True,
        help_text="List of citation objects per LPF spec"
    )
    certainty = models.CharField(
        max_length=64, null=True, blank=True,
        help_text="LPF certainty value, e.g. 'certain', 'uncertain'"
    )

    class Meta:
        db_table = 'collection_relation'
        indexes = [
            models.Index(fields=['relation_type']),
        ]
        unique_together = ('source', 'target', 'relation_type')

    def __str__(self):
        return f"{self.source.identifier or self.source_id} → {self.target.identifier or self.target_id} ({self.relation_type})"


class Namespace(models.Model):
    """
    Registry of controlled identifier namespaces used across WHG. # TODO: Use in other models in place of current global JSON object.
    """
    prefix = models.CharField(
        max_length=64, unique=True,
        help_text="Short prefix, e.g. 'whg', 'unm49', 'tgn', 'wikidata'"
    )
    title = models.CharField(max_length=255)
    description = models.TextField(blank=True)
    base_uri = models.URLField(
        null=True, blank=True,
        help_text="Base URI for lookups or linking, e.g. 'https://www.wikidata.org/entity/'"
    )
    authority = models.CharField(
        max_length=255, null=True, blank=True,
        help_text="Responsible organisation or maintainer"
    )
    is_internal = models.BooleanField(
        default=False,
        help_text="True if the namespace is WHG-internal"
    )

    class Meta:
        db_table = "namespace"
        verbose_name = "namespace"
        verbose_name_plural = "namespaces"
        ordering = ["prefix"]

    def __str__(self):
        return self.prefix

## TODO: Move the following into `signals.py` file

"""
Signal handlers for Collection model to maintain geometry cache integrity.
"""
import logging

from django.db.models.signals import post_save, post_delete, m2m_changed
from django.dispatch import receiver

from collection.models import CollPlace, CollDataset

logger = logging.getLogger(__name__)


@receiver([post_save, post_delete], sender=CollPlace)
def invalidate_collection_geometries_on_place_change(sender, instance, **kwargs):
    """
    Invalidate cached geometries when a place is added to or removed from a collection.
    """
    try:
        instance.collection.invalidate_geometry_cache()
    except Exception as e:
        logger.error(f"Error invalidating geometry cache for collection {instance.collection_id}: {e}")


@receiver([post_save, post_delete], sender=CollDataset)
def invalidate_collection_geometries_on_dataset_change(sender, instance, **kwargs):
    """
    Invalidate cached geometries when a dataset is added to or removed from a collection.
    """
    try:
        instance.collection.invalidate_geometry_cache()
    except Exception as e:
        logger.error(f"Error invalidating geometry cache for collection {instance.collection_id}: {e}")


@receiver(m2m_changed, sender='collection.Collection.places.through')
def invalidate_on_places_m2m_change(sender, instance, action, **kwargs):
    """
    Invalidate cached geometries when places M2M relationship changes.
    Triggered on add, remove, or clear actions.
    """
    if action in ['post_add', 'post_remove', 'post_clear']:
        try:
            instance.invalidate_geometry_cache()
        except Exception as e:
            logger.error(f"Error invalidating geometry cache for collection {instance.pk}: {e}")


@receiver(m2m_changed, sender='collection.Collection.datasets.through')
def invalidate_on_datasets_m2m_change(sender, instance, action, **kwargs):
    """
    Invalidate cached geometries when datasets M2M relationship changes.
    Triggered on add, remove, or clear actions.
    """
    if action in ['post_add', 'post_remove', 'post_clear']:
        try:
            instance.invalidate_geometry_cache()
        except Exception as e:
            logger.error(f"Error invalidating geometry cache for collection {instance.pk}: {e}")