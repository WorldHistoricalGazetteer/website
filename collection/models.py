from functools import reduce

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.contrib.postgres.fields import ArrayField
from django.contrib.gis.db import models as geomodels
from django.core.cache import caches
from django.core.validators import URLValidator
from django.db import models
from django.db.models import Q, JSONField, Func, CharField, Exists, OuterRef, Subquery
from django.db.models.functions import Coalesce
from django.urls import reverse
from django.utils import timezone

from datasets.models import Dataset
from main.choices import COLLECTIONCLASSES, LINKTYPES, TEAMROLES, STATUS_COLL, \
    USER_ROLE, COLLECTIONTYPES, COLLECTIONGROUP_TYPES
from places.models import Place, PlaceGeom
from traces.models import TraceAnnotation
from utils.cluster_geometries import clustered_geometries as calculate_clustered_geometries
from utils.csl_citation_formatter import csl_citation
from utils.heatmap_geometries import heatmapped_geometries
from utils.hull_geometries import hull_geometries
from utils.feature_collection import feature_collection
from utils.carousel_metadata import carousel_metadata
# from multiselectfield import MultiSelectField
from django.contrib.gis.geos import GEOSGeometry
# import simplejson as json
# from geojson import Feature
from geojson import loads, dumps

""" for images """
from io import BytesIO
import sys
from PIL import Image
from django.core.files.uploadedfile import InMemoryUploadedFile

""" end """

from django_resized import ResizedImageField

User = get_user_model()


def collection_path(instance, filename):
    # upload to MEDIA_ROOT/collections/<coll id>/<filename>
    return 'collections/{0}/{1}'.format(instance.id, filename)


def collectiongroup_path(instance, filename):
    # upload to MEDIA_ROOT/groups/<collection group id>/<filename>
    return 'groups/{0}/{1}'.format(instance.id, filename)


def user_directory_path(instance, filename):
    # upload to MEDIA_ROOT/user_<username>/<filename>
    return 'user_{0}/{1}'.format(instance.owner.id, filename)


def default_relations():
    return 'locale'.split(', ')


# needed b/c collection place_list filters on it
# ? huh: migration look for these, even though field was deleted
def default_omitted():
    return '{}'


# does nothing until options set in UI
def default_vis_parameters():
    return {
        "max": {"trail": False, "tabulate": False, "temporal_control": "none"},
        "min": {"trail": False, "tabulate": False, "temporal_control": "none"},
        "seq": {"trail": False, "tabulate": False, "temporal_control": "none"}
    }


class CollDataset(models.Model):
    collection = models.ForeignKey('Collection', on_delete=models.CASCADE)
    dataset = models.ForeignKey('datasets.Dataset', on_delete=models.CASCADE)
    date_added = models.DateTimeField(default=timezone.now, null=True)

    class Meta:
        ordering = ['id']


class Collection(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL,
                              related_name='collections', on_delete=models.CASCADE)
    title = models.CharField(null=False, max_length=255)
    description = models.TextField(max_length=3000)
    keywords = ArrayField(models.CharField(max_length=50), null=True, default=list)

    # per-collection relation keyword choices, e.g. waypoint, birthplace, battle site
    # TODO: ?? need default or it errors for some reason
    rel_keywords = ArrayField(models.CharField(max_length=30), blank=True, null=True, default=list)
    # rel_keywords = ArrayField(models.CharField(max_length=30), blank=True, default=default_relations)

    # 3 new fields, 20210619
    creator = models.CharField(null=True, blank=True, max_length=500)
    contact = models.CharField(null=True, blank=True, max_length=500)
    webpage = models.URLField(null=True, blank=True)

    # modified, 20220902: 'place' or 'dataset'; no default
    collection_class = models.CharField(choices=COLLECTIONCLASSES, max_length=12)

    # single representative image
    image_file = ResizedImageField(size=[800, 600], upload_to=collection_path, blank=True, null=True)
    # single pdf file
    file = models.FileField(upload_to=collection_path, blank=True, null=True)

    create_date = models.DateTimeField(null=True, auto_now_add=True)
    version = models.CharField(null=True, blank=True, max_length=20)
    # modified = models.DateTimeField(null=True)

    # group, sandbox, demo, ready, public
    status = models.CharField(max_length=12, choices=STATUS_COLL, default='sandbox', null=True, blank=True)
    featured = models.IntegerField(null=True, blank=True)
    public = models.BooleanField(default=False)
    doi = models.BooleanField(default=False, help_text="Indicates if a DOI is associated with this collection")

    # flag set by group_leader
    nominated = models.BooleanField(default=False)
    nominate_date = models.DateTimeField(null=True, blank=True)

    group = models.ForeignKey("CollectionGroup", db_column='group',
                              related_name="group", null=True, blank=True, on_delete=models.PROTECT)

    # group_leader sees submitted
    # submitted = models.BooleanField(default=False)
    submit_date = models.DateTimeField(null=True, blank=True)

    # writes CollDataset record to collection_colldataset
    datasets = models.ManyToManyField(
        'datasets.Dataset',
        through='collection.CollDataset',
        related_name='new_datasets',
        blank=True)

    # writes CollPlace record to collection_collplace
    places = models.ManyToManyField("places.Place", through='CollPlace', blank=True)

    bbox = geomodels.PolygonField(null=True, blank=True, srid=4326)

    # Visualisation parameters (used in place_collection_browse.html & place_collection_build.html)
    vis_parameters = JSONField(default=default_vis_parameters, null=True, blank=True)

    coordinate_density = models.FloatField(null=True, blank=True)  # for scaling map markers

    @property
    def citation_csl(self):
        cached_value = caches['property_cache'].get(f"collection:{self.pk}:citation_csl")
        if cached_value:
            return cached_value

        result = csl_citation(self)
        caches['property_cache'].set(f"collection:{self.pk}:citation_csl", result, timeout=None)

        return result

    def get_absolute_url(self):
        # return reverse('datasets:dashboard', kwargs={'id': self.id})
        return reverse('data-collections')

    @property
    def carousel_metadata(self):
        cached_value = caches['property_cache'].get(f"collection:{self.pk}:carousel_metadata")
        if cached_value:
            return cached_value

        result = carousel_metadata(self)
        caches['property_cache'].set(f"collection:{self.pk}:carousel_metadata", result, timeout=None)

        return result

    @property
    def clustered_geometries(self):
        return calculate_clustered_geometries(self)

    @property
    def collaborators(self):
        # includes roles: member, owner
        team = CollectionUser.objects.filter(collection_id=self.id).values_list('user_id')
        teamusers = User.objects.filter(id__in=team)
        return teamusers

    @property
    def coordinate_density_value(self):
        if self.coordinate_density is not None:
            return self.coordinate_density

        clustered_geometries = calculate_clustered_geometries(self, min_clusters=7)

        # Calculate the total area
        total_area = 0
        for hull in clustered_geometries['features']:
            geometry = hull['geometry']
            if isinstance(geometry, dict):
                # Convert GeoJSON geometry to WKT
                geojson_obj = loads(dumps(geometry))
                geometry = GEOSGeometry(str(geojson_obj))

            total_area += geometry.area

        density = clustered_geometries['properties'].get('coordinate_count', 0) / total_area if total_area > 0 else 0

        # Store the calculated density
        self.coordinate_density = density
        self.save()

        return density

    # download time estimate
    @property
    def dl_est(self):
        # Get the number of associated Place records
        num_records = self.places_all.count()

        # Calculate the estimated download time in seconds
        # (20 seconds per 1000 records)
        est_time_in_sec = (num_records / 1000) * 20

        # Convert the estimated time to minutes and seconds
        min, sec = divmod(est_time_in_sec, 60)

        # Format the result
        if min < 1:
            result = "%02d sec" % (sec)
        elif sec >= 10:
            result = "%02d min %02d sec" % (min, sec)
        else:
            result = "%02d min" % (min)

        return result

    @property
    def ds_counter(self):
        from collections import Counter
        from itertools import chain
        dc = self.datasets.all().values_list('label', flat=True)
        dp = self.places.all().values_list('dataset', flat=True)
        all = Counter(list(chain(dc, dp)))
        return dict(all)

    @property
    def ds_list(self):
        if self.collection_class == 'dataset':
            dsc = [{"id": d.id, "label": d.label, "extent": d.extent, "bounds": d.bounds, "title": d.title,
                    "dl_est": d.dl_est,
                    "numrows": d.numrows, "modified": d.last_modified_text} for d in self.datasets.all()]
            return list({item['id']: item for item in dsc}.values())
        elif self.collection_class == 'place':
            # Get all distinct datasets associated with all the places in the collection
            datasets = set(place.dataset for place in self.places.all())
            dsp = [{"id": d.id, "label": d.label, "title": d.title,
                    "modified": d.last_modified_text} for d in datasets]
            return list({item['id']: item for item in dsp}.values())

    @property
    def feature_collection(self):
        return feature_collection(self)

    @property
    def heatmapped_geometries(self):
        return heatmapped_geometries(self)

    @property
    def hull_geometries(self):
        return hull_geometries(self)

    @property
    def kw_colors(self):
        colors = ['orange', 'red', 'green', 'blue', 'purple',
                  'red', 'green', 'blue', 'purple']
        return dict(zip(self.rel_keywords, colors))

    # @property
    # def last_modified_iso(self):
    #   # TODO: log entries for collections
    #   return self.create_date.strftime("%Y-%m-%d")
    @property
    def last_modified_iso(self):
        logtypes_to_include = ['create', 'update']
        filtered_logs = self.log.filter(logtype__in=logtypes_to_include)

        if filtered_logs.count() > 0:
            # Get the log with the latest timestamp
            last = filtered_logs.order_by('-timestamp').first().timestamp
        else:
            last = self.create_date

        return last.strftime("%Y-%m-%d")

    @property
    def owners(self):
        owner_ids = list(CollectionUser.objects.filter(collection=self, role='owner').values_list('user_id', flat=True))
        owner_ids.append(self.owner.id)
        owners = User.objects.filter(id__in=owner_ids)
        return owners

    @property
    def places_ds(self):
        dses = self.datasets.all()
        return Place.objects.filter(dataset__in=dses)

    @property
    def places_thru(self):
        seq_places = [{'p': cp.place, 'seq': cp.sequence} for cp in
                      CollPlace.objects.filter(collection=self.id).order_by('sequence')]
        return seq_places

    @property
    def places_all(self):
        all = Place.objects.filter(
            Q(dataset__in=self.datasets.all()) | Q(id__in=self.places.all().values_list('id'))
        )
        return all
        # return all.exclude(id__in=self.omitted)

    @property
    def num_places(self):
        if self.collection_class == "dataset":
            return Place.objects.filter(dataset__in=self.datasets.all()).count()
        else:
            return self.places.all().count()

    @property
    def numrows(self):
        # Added for consistency with Dataset model
        return self.num_places

    @property
    def rowcount(self):
        # Switch to more efficient method but keep property for backward compatibility
        return self.num_places
        # dses = self.datasets.all()
        # ds_counts = [ds.places.count() for ds in dses]
        # return sum(ds_counts) + self.places.count()

    def __str__(self):
        return '%s' % (self.title)
        # return '%s (id: %s)' % (self.title, self.id)

    class Meta:
        db_table = 'collections'


""" 
  records membership of place in collection; sequence is managed in TraceAnnotation
"""


class CollPlace(models.Model):
    collection = models.ForeignKey(Collection, related_name='annos',
                                   on_delete=models.CASCADE)
    place = models.ForeignKey(Place, related_name='annos',
                              on_delete=models.CASCADE)
    sequence = models.IntegerField(null=True, default=0)


class CollectionUser(models.Model):
    collection = models.ForeignKey(Collection, related_name='collabs', default=-1, on_delete=models.CASCADE)
    user = models.ForeignKey(User, related_name='collection_collab',
                             default=-1, on_delete=models.CASCADE)
    role = models.CharField(max_length=20, null=False, choices=TEAMROLES)

    def __str__(self):
        name = self.user.name
        return '<b>' + name + '</b> (' + self.user.username + '); role: ' + self.role + '; ' + self.user.email

    class Meta:
        managed = True
        db_table = 'collection_user'


# used for instructor-led assignments, workshops, etc.
class CollectionGroup(models.Model):
    owner = models.ForeignKey(settings.AUTH_USER_MODEL,
                              related_name='collection_groups', on_delete=models.CASCADE)
    title = models.CharField(null=False, max_length=300)
    description = models.TextField(null=True, max_length=3000)
    type = models.CharField(choices=COLLECTIONGROUP_TYPES, default="class", max_length=8)
    keywords = ArrayField(models.CharField(max_length=50), null=True)
    # e.g. an essay
    file = models.FileField(upload_to=collectiongroup_path, blank=True, null=True)
    created = models.DateTimeField(auto_now_add=True)
    start_date = models.DateTimeField(null=True)
    due_date = models.DateTimeField(null=True)

    # a Collection can belong to >=1 CollectionGroup
    collections = models.ManyToManyField("collection.Collection", blank=True)

    # group options
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
    collectiongroup = models.ForeignKey(CollectionGroup, related_name='members',
                                        default=-1, on_delete=models.CASCADE)
    user = models.ForeignKey(User, related_name='members',
                             default=-1, on_delete=models.CASCADE)
    role = models.CharField(max_length=20, null=False, choices=TEAMROLES, default='member')

    def __str__(self):
        return '%s (%s, %s)' % (self.user.email, self.user.id, self.user.name)

    class Meta:
        managed = True
        db_table = 'collection_group_user'


""" 
  handled in Link model now 
"""


# TODO: decommision; it's embedded!
class CollectionLink(models.Model):
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
