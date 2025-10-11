# datasets.models

from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.contrib.gis.db import models as geomodels
from django.contrib.gis.db.models import Collect, Extent, Aggregate
from django.contrib.gis.db.models.functions import Area
from django.contrib.gis.geos import GeometryCollection, Polygon, GEOSGeometry
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.core.cache import caches
from django.db import models
from django.db.models import JSONField, Exists, OuterRef, Q, Func, CharField, Min, Max
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django.urls import reverse

# from django.shortcuts import get_object_or_404

from django_celery_results.models import TaskResult
from django_resized import ResizedImageField
from elastic.es_utils import escount_ds
from geojson import Feature

from main.choices import *
from places.models import Place, PlaceGeom, PlaceLink
import simplejson as json
from shapely.geometry import box, mapping
from utils.cluster_geometries import (
    clustered_geometries as calculate_clustered_geometries,
)
from utils.csl_citation_formatter import csl_citation
from utils.heatmap_geometries import heatmapped_geometries
from utils.hull_geometries import hull_geometries
from utils.feature_collection import feature_collection
from utils.carousel_metadata import carousel_metadata
from geojson import loads, dumps

User = get_user_model()


# upload to MEDIA_ROOT/user_<username>/<filename>
def user_directory_path(instance, filename):
    return "user_{0}/{1}".format(instance.owner.name, filename)


def dataset_file_path(instance, filename):
    return "user_{0}/{1}".format(instance.dataset_id.owner.username, filename)


def dataset_pdf_path(instance, filename):
    return "user_{0}/{1}".format(instance.owner.username, filename)


def ds_image_path(instance, filename):
    # upload to MEDIA_ROOT/datasets/<id>_<filename>
    return "datasets/{0}_{1}".format(instance.id, filename)


# does nothing until options set in UI
def default_vis_parameters():
    return {
        "max": {"trail": False, "tabulate": False, "temporal_control": "none"},
        "min": {"trail": False, "tabulate": False, "temporal_control": "none"},
        "seq": {"trail": False, "tabulate": False, "temporal_control": "none"},
    }


# owner = models.ForeignKey('auth.User', related_name='snippets', on_delete=models.CASCADE)
class Dataset(models.Model):
    owner = models.ForeignKey(
        settings.AUTH_USER_MODEL, related_name="datasets", on_delete=models.CASCADE
    )
    label = models.CharField(
        max_length=20,
        null=False,
        unique="True",
        blank=True,
        error_messages={
            "unique": "The dataset label entered is already in use, and must be unique. "
                      "Try appending a version # or initials."
        },
    )
    title = models.CharField(max_length=255, null=False)
    description = models.CharField(max_length=2044, null=False)
    webpage = models.URLField(null=True, blank=True)
    create_date = models.DateTimeField(null=True, auto_now_add=True)
    uri_base = models.CharField(max_length=200, null=True, blank=True)
    image_file = ResizedImageField(
        size=[800, 600], upload_to=ds_image_path, blank=True, null=True
    )
    bbox = geomodels.PolygonField(null=True, blank=True, srid=4326)

    ds_status = models.CharField(
        max_length=12, null=True, blank=True, choices=STATUS_DS
    )
    featured = models.IntegerField(null=True, blank=True)  #
    core = models.BooleanField(default=False)  # e.g. tgn, geonames, physical geography
    public = models.BooleanField(default=False)
    doi = models.BooleanField(default=False, help_text="Indicates if a DOI is associated with this dataset")

    coordinate_density = models.FloatField(
        null=True, blank=True
    )  # for scaling map markers
    pdf = models.FileField(
        upload_to=dataset_pdf_path, blank=True, null=True
    )  # essay pdf
    vis_parameters = JSONField(default=default_vis_parameters, null=True, blank=True)
    volunteers = models.BooleanField(default=False, null=True)  # volunteers requested
    volunteers_text = models.CharField(max_length=2044, null=True, blank=True)

    source = models.CharField(max_length=500, null=True, blank=True)
    citation = models.CharField(max_length=2044, null=True, blank=True)  # user-added; if absent, generated in browser

    # Fields to be deprecated following their migration to CSL
    creator = models.CharField(max_length=500, null=True, blank=True)  # NB: Used in API serializer
    contributors = models.CharField(max_length=500, null=True, blank=True)

    # People associated with Dataset creation
    creators_csl = models.ManyToManyField('persons.Person', related_name='datasets_as_creator', blank=True)
    contributors_csl = models.ManyToManyField('persons.Person', related_name='datasets_as_contributor', blank=True)

    # TODO: these are updated in both Dataset & DatasetFile  (??)
    datatype = models.CharField(
        max_length=12, null=False, choices=DATATYPES, default="place"
    )
    numrows = models.IntegerField(null=True, blank=True)

    # these are back-filled
    numlinked = models.IntegerField(null=True, blank=True)
    total_links = models.IntegerField(null=True, blank=True)

    def __str__(self):
        return self.label
        # return '%d: %s' % (self.id, self.label)

    def get_absolute_url(self):
        return reverse("datasets:ds_status", kwargs={"id": self.id})

    @property
    def bounds(self):
        extent = self.extent
        b = box(extent[0], extent[1], extent[2], extent[3])
        feat = Feature(
            geometry=mapping(b),
            properties={"id": self.id, "label": self.label, "title": self.title},
        )
        return feat

    @property
    def extent(self):
        dsgeoms = PlaceGeom.objects.filter(place__dataset=self.label)
        extent = dsgeoms.aggregate(Extent("geom"))["geom__extent"]
        return list(extent) if extent else (0, 0, 1, 1)

    @property
    def carousel_metadata(self):
        cached_value = caches['property_cache'].get(f"dataset:{self.pk}:carousel_metadata")
        if cached_value:
            return cached_value

        result = carousel_metadata(self)
        caches['property_cache'].set(f"dataset:{self.pk}:carousel_metadata", result, timeout=None)

        return result

    # @property
    # def builder_hastask(self):
    #   hastask = False
    #   if self.tasks.filter(task_name='align_collection').count() > 0:
    #     hastask = True
    #   return hastask

    # @property
    # def builder_remaining(self):
    #   remaining = 0
    #   if self.tasks.filter(task_name='align_collection').count() > 0:
    #       remaining = Hit.objects.filter(
    #           task_id=self.tasks.filter(task_name='align_collection')[0].task_id,
    #           reviewed=False
    #       ).values("place_id").distinct().count()
    #   return remaining

    @property
    def convex_hull(self):
        dsgeoms = PlaceGeom.objects.filter(place__dataset=self.label)
        geometry = None
        if dsgeoms.count() > 0:
            geom_list = [GEOSGeometry(dsgeom.geom.wkt) for dsgeom in dsgeoms]
            combined_geom = geom_list[0].convex_hull

            for geom in geom_list[
                        1:
                        ]:  # Union of convex hulls is much faster than union of full geometries
                combined_geom = combined_geom.union(geom.convex_hull)

            geometry = json.loads(combined_geom.convex_hull.geojson)

    @property
    def clustered_geometries(self):
        return calculate_clustered_geometries(self)

    @property
    def citation_csl(self):
        cached_value = caches['property_cache'].get(f"dataset:{self.pk}:citation_csl")
        if cached_value:
            return cached_value

        result = csl_citation(self)
        caches['property_cache'].set(f"dataset:{self.pk}:citation_csl", result, timeout=None)

        return result

    @property
    def collaborators(self):
        ## includes roles: member, owner
        team = DatasetUser.objects.filter(dataset_id_id=self.id).values_list(
            "user_id_id"
        )
        # members of whg_team group are collaborators on all datasets
        # teamusers = User.objects.filter(id__in=team) | User.objects.filter(groups__name='whg_team') | self.owner
        teamusers = User.objects.filter(id__in=team) | User.objects.filter(
            groups__name="whg_team"
        )
        return teamusers

    # dataset time estimates (local on mac laptop)
    # delimited augmented: 20 sec per 1000 records
    # lpf: 20 sec per 1000 records

    @property
    def coordinates_count(self):
        total_coords = 0
        for place in self.places.all():
            for geom in place.geoms.all():
                total_coords += geom.geom.num_coords
        return total_coords

    @property
    def coordinate_density_value(self):
        if self.coordinate_density is not None:
            return self.coordinate_density

        clustered_geometries = calculate_clustered_geometries(self, min_clusters=7)

        # Calculate the total area
        total_area = 0
        for hull in clustered_geometries["features"]:
            geometry = hull["geometry"]
            if isinstance(geometry, dict):
                # Convert GeoJSON geometry to WKT
                geojson_obj = loads(dumps(geometry))
                geometry = GEOSGeometry(str(geojson_obj))

            total_area += geometry.area

        density = (
            clustered_geometries["properties"].get("coordinate_count", 0) / total_area
            if total_area > 0
            else 0
        )

        # Store the calculated density
        self.coordinate_density = density
        self.save()

        return density

    # download time estimate
    @property
    def dl_est(self):
        # Get the number of associated Place records
        num_records = self.places.count()

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

    # @property
    # def dl_est(self):
    #   file = self.files.all().order_by('id')[0]
    #   if file.file:
    #     size = int(file.file.size / 1000000)  # seconds +/-
    #   else:
    #     # substitute record count for *rough* estimate
    #     size = self.places.count() / 1000
    #   min, sec = divmod(size, 60)
    #   if min < 1:
    #     result = "%02d sec" % (sec)
    #   elif sec >= 10:
    #     result = "%02d min %02d sec" % (min, sec)
    #   else:
    #     result = "%02d min" % (min)
    #     # print("est. %02d min %02d sec" % (min, sec))
    #   return result

    @property
    def feature_collection(self):
        return feature_collection(self)

    @property
    def file(self):
        # returns model instance for latest file
        return self.files.order_by("-id").first()

    @property
    def format(self):
        first_file = self.files.first()
        return first_file.format if first_file else None

    # list of dataset geometries
    @property
    def geometries(self):
        g_list = PlaceGeom.objects.filter(place_id__in=self.placeids).values_list(
            "jsonb", flat=True
        )
        return g_list

    @property
    def heatmapped_geometries(self):
        return heatmapped_geometries(self)

    @property
    def hull_geometries(self):
        return hull_geometries(self)

    @property
    def last_modified_iso(self):
        logtypes_to_include = ["ds_create", "ds_recon", "ds_update"]
        filtered_logs = self.log.filter(logtype__in=logtypes_to_include)

        if filtered_logs.count() > 0:
            # Get the log with the latest timestamp
            last = filtered_logs.order_by("-timestamp").first().timestamp
        else:
            last = self.create_date

        return last.strftime("%Y-%m-%d")

    @property
    def last_modified_text(self):
        if self.log.count() > 0:
            last = self.log.all().order_by("-timestamp")[0].timestamp
        else:
            last = self.create_date
        return last.strftime("%d %b %Y")

    # list of dataset links
    @property
    def links(self):
        l_list = PlaceLink.objects.filter(place_id__in=self.placeids).values_list(
            "jsonb", flat=True
        )
        return l_list

    @property
    def minmax(self):
        minmax_values = Place.objects.filter(dataset=self).aggregate(
            # This ignores `None` values, effectively handling temporal sparsity [None, None]
            earliest=Min("minmax__0"),
            latest=Max("minmax__1"),
        )
        earliest = minmax_values["earliest"]
        latest = minmax_values["latest"]
        return [earliest, latest] if earliest and latest else None

    @property
    def missing_geoms(self):
        # Get all Place instances related to this Dataset
        places = self.places.all()

        # Check if any of these Place instances do not have a corresponding PlaceGeom
        for place in places:
            if not place.geoms.exists():
                return True

        # If we've gone through all Place instances and none are missing a PlaceGeom, return False
        return False

    @property
    def num_places(self):
        return Place.objects.filter(dataset=self.label).count()

    @property
    def owners(self):
        du_owner_ids = list(
            self.collabs.filter(role="owner").values_list("user_id_id", flat=True)
        )
        du_owner_ids.append(self.owner.id)
        ds_owners = User.objects.filter(id__in=du_owner_ids)
        return ds_owners

    # list of dataset place_id values
    @property
    def placeids(self):
        return Place.objects.filter(dataset=self.label).values_list("id", flat=True)

    # how many wikidata links?
    @property
    def q_count(self):
        placeids = Place.objects.filter(dataset=self.label).values_list("id", flat=True)
        return PlaceLink.objects.filter(
            place_id__in=placeids, jsonb__icontains="Q"
        ).count()

    @property
    def recon_status(self):
        # Format task_args as a string representation of a tuple
        # because that's how Celery records it now
        args_with_quotes = f'"({self.id},)"'
        tasks = TaskResult.objects.filter(
            task_args=args_with_quotes, task_name__startswith="align", status="SUCCESS"
        )
        # print('tasks', tasks)
        # Calculate the status based on the tasks and hits
        result = {}
        for t in tasks:
            hit_count = (
                Hit.objects.filter(task_id=t.task_id, reviewed=False)
                .values("place_id")
                .distinct()
                .count()
            )
            result[t.task_name[6:]] = hit_count

        return result

    # count of reviewed places
    @property
    def reviewed_places(self):
        result = {}
        result["rev_wd"] = self.places.filter(review_wd=1).count()
        result["rev_tgn"] = self.places.filter(review_tgn=1).count()
        result["rev_whg"] = self.places.filter(review_whg=1).count()
        return result

    # used in ds_compare()
    @property
    def status_idx(self):
        # idx='whg'
        idx = settings.ES_WHG
        submissions = [
            {
                "task_id": t.task_id,
                "date": t.date_done.strftime("%Y-%m-%d %H:%M"),
                "hits_tbr": Hit.objects.filter(
                    task_id=t.task_id, reviewed=False
                ).count(),
            }
            for t in self.tasks.filter(task_name="align_idx").order_by("-date_done")
        ]
        idxcount = escount_ds(idx, self.label)

        result = {"submissions": submissions, "idxcount": idxcount}
        return result

    @property
    def tasks(self):
        args_with_quotes = f'"({self.id},)"'
        return TaskResult.objects.filter(
            task_args=args_with_quotes, task_name__startswith="align"
        )

    # tasks stats
    @property
    def taskstats(self):
        def distinctPlaces(task):
            # counts of distinct place records remaining to review fo reach pass
            p_hits0 = (
                Hit.objects.filter(
                    task_id=t.task_id, query_pass="pass0", reviewed=False
                )
                .values("place_id")
                .distinct()
                .count()
            )
            p_hits1 = (
                Hit.objects.filter(
                    task_id=t.task_id, query_pass="pass1", reviewed=False
                )
                .values("place_id")
                .distinct()
                .count()
            )
            p_hits2 = (
                Hit.objects.filter(
                    task_id=t.task_id, query_pass="pass2", reviewed=False
                )
                .values("place_id")
                .distinct()
                .count()
            )
            p_hits3 = (
                Hit.objects.filter(
                    task_id=t.task_id, query_pass="pass3", reviewed=False
                )
                .values("place_id")
                .distinct()
                .count()
            )
            p_sum = p_hits0 + p_hits1 + p_hits2 + p_hits3

            return {
                "tid": t.task_id,
                # "task":t.task_name,
                "date": t.date_done.strftime("%Y-%m-%d"),
                "total": p_sum,
                "pass0": p_hits0,
                "pass1": p_hits1,
                "pass2": p_hits2,
                "pass3": p_hits3,
            }

        result = {}
        # array for each kind of task
        # task_types = self.tasks.all().values_list("task_name", flat=True)
        task_types = [
            "align_wdlocal",
            "align_tgn",
            "align_idx",
            "align_whg",
            "align_wd",
        ]
        for tt in task_types:
            result[tt] = []
            for t in self.tasks.filter(task_name=tt, status="SUCCESS"):
                result[tt].append(distinctPlaces(t))

        # print(result)
        return result

    @property
    def unindexed(self):
        unidxed = self.places.filter(indexed=False).count()
        return unidxed

    # count of unreviewed hits

    @property
    def unreviewed_hitcount(self):
        unrev = (
            Hit.objects.all()
            .filter(dataset_id=self.id, reviewed=False, authority="wd")
            .count()
        )
        # unrev = Hit.objects.all().filter(dataset_id=self.id, reviewed=False, authority=source).count()
        return unrev

    @property
    def uri(self):
        return settings.URL_FRONT + "datasets/" + str(self.id) + "/places"

    # count of unindexed places
    class Meta:
        managed = True
        db_table = "datasets"
        indexes = [
            models.Index(fields=["id", "label"]),
        ]


# TODO: FK to dataset, not dataset_id
class DatasetFile(models.Model):
    dataset_id = models.ForeignKey(
        Dataset, related_name="files", default=-1, on_delete=models.CASCADE
    )
    rev = models.IntegerField(null=True, blank=True)
    file = models.FileField(upload_to=dataset_file_path)
    # file = models.FileField(upload_to=user_directory_path)
    format = models.CharField(max_length=12, null=False, choices=FORMATS, default="lpf")
    datatype = models.CharField(
        max_length=12, null=False, choices=DATATYPES, default="place"
    )
    delimiter = models.CharField(max_length=5, null=True, blank=True)
    df_status = models.CharField(
        max_length=12, null=True, blank=True, choices=STATUS_FILE
    )
    upload_date = models.DateTimeField(null=True, auto_now_add=True)
    header = ArrayField(models.CharField(max_length=30), null=True, blank=True)
    numrows = models.IntegerField(null=True, blank=True)

    # TODO: generate geotypes, add to file instance
    # geotypes = JSONField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "dataset_file"


class Hit(models.Model):
    # FK to celery_results_task_result.task_id
    place = models.ForeignKey(Place, on_delete=models.CASCADE)
    task_id = models.CharField(max_length=50)
    authority = models.CharField(max_length=12, choices=AUTHORITIES)
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    query_pass = models.CharField(max_length=12, choices=PASSES)
    src_id = models.CharField(max_length=2044)
    score = models.FloatField()

    reviewed = models.BooleanField(default=False)
    matched = models.BooleanField(default=False)
    flag = models.BooleanField(default=False)

    # authority record identifier (could be uri)
    authrecord_id = models.CharField(max_length=255)

    # json response; parse later according to authority
    json = JSONField(blank=True, null=True)
    geom = JSONField(blank=True, null=True)

    def __str__(self):
        return str(self.id)

    class Meta:
        managed = True
        db_table = "hits"


class DatasetUser(models.Model):
    dataset_id = models.ForeignKey(
        Dataset, related_name="collabs", default=-1, on_delete=models.CASCADE
    )
    user_id = models.ForeignKey(
        User, related_name="ds_collab", default=-1, on_delete=models.CASCADE
    )
    role = models.CharField(max_length=20, null=False, choices=TEAMROLES)

    def __str__(self):
        name = self.user_id.name
        return "<b>" + name + "</b> (" + self.role + ")"

    class Meta:
        managed = True
        db_table = "dataset_user"


@receiver(pre_delete, sender=Dataset)
def remove_files(**kwargs):
    ds_instance = kwargs.get("instance")
    files = DatasetFile.objects.filter(dataset_id_id=ds_instance.id)
    files.delete()
