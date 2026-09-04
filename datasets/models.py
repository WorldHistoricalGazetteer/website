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
from django.db.models import JSONField, Exists, OuterRef, Q, Func, CharField, Min, Max, Count
from django.db.models.signals import pre_delete
from django.dispatch import receiver
from django.urls import reverse

# from django.shortcuts import get_object_or_404

from django_celery_results.models import TaskResult
from django_resized import ResizedImageField
from elastic.es_utils import escount_ds
from geojson import Feature
from licensing.models import LICENSE_SOURCE_CHOICES

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
    # Per-place link template to the source's OWN web page, with ``<id>`` for the
    # record's local id — e.g. ``https://example.org/place/<id>`` (place#121).
    # Flows into the gazetteer registry so Atlas Explore popups can offer a
    # "view at source" link for this dataset's places. Distinct from ``webpage``
    # (the dataset homepage) and ``uri_base`` (a plain id prefix, not a template).
    web_item = models.CharField(max_length=500, null=True, blank=True)
    image_file = ResizedImageField(
        size=[800, 600], upload_to=ds_image_path, blank=True, null=True
    )
    bbox = geomodels.PolygonField(null=True, blank=True, srid=4326)

    ds_status = models.CharField(
        max_length=12, null=True, blank=True, choices=STATUS_DS
    )
    featured = models.IntegerField(null=True, blank=True)  #
    core = models.BooleanField(default=False)  # e.g. tgn, geonames, physical geography
    authority = models.BooleanField(default=False)  # eligible as an authority source for new ES indexes
    public = models.BooleanField(default=False)
    # Whether the dataset may be downloaded (any format/path). Set False for
    # very large bulk/authority datasets (e.g. TGN-derived) that should be
    # obtained from their upstream source instead of via a WHG export — those
    # exports are impractically large and can never complete a live stream.
    downloadable = models.BooleanField(
        default=True,
        help_text="Uncheck to disable all downloads of this dataset "
                  "(too large / obtain from upstream source instead).",
    )
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

    # Source licence (the data's own rights). WHG's curation/aggregation licence
    # is asserted separately via settings.WHG_OVERLAY_LICENSE.
    license = models.ForeignKey(
        'licensing.License', null=True, blank=True,
        on_delete=models.PROTECT, related_name='datasets',
    )
    rights_statement = models.TextField(
        null=True, blank=True,
        help_text="Free-text rights, for custom licences or extra conditions.",
    )
    # Provenance of ``license`` — see licensing.models.LICENSE_SOURCE_CHOICES.
    # NULL alongside a set licence means the provenance was never captured;
    # NULL alongside a null licence simply means no licence is recorded.
    license_source = models.CharField(
        max_length=32, null=True, blank=True,
        choices=LICENSE_SOURCE_CHOICES,
        help_text="How this licence came to be recorded.",
    )

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

    # Owner opt-in to the community record-correction review loop (workbench.RecordSuggestion,
    # plan-record-suggestions §1c). When True the owner is surfaced pending suggestions on their
    # gazetteer (queue + digest); when False, WHG staff remain the review backstop so nothing is lost.
    accept_suggestions = models.BooleanField(default=False)

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
        places_without_geom = self.places.annotate(
            has_geom=Exists(PlaceGeom.objects.filter(place=OuterRef('pk')))
        ).filter(has_geom=False).exists()
        return places_without_geom

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

    def can_edit(self, user):
        """True if ``user`` may edit this gazetteer: WHG staff or an owner/co-owner. Matches the
        established datasets._user_can_edit_dataset gate; single source of truth for the "Correct
        this record" (record-level check-out) affordance and its endpoint."""
        if not user or not getattr(user, "is_authenticated", False):
            return False
        return bool(user.is_staff or self.owners.filter(id=user.id).exists())

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
        """
        Returns per-task counts of distinct unreviewed places grouped by query_pass,
        along with task metadata (name and date_done).

        This implementation performs:
          • 1 query on Hit (aggregated counts)
          • 1 small query on ds.tasks (for metadata)
        and avoids any per-task subqueries.
        """
        # --- Step 1: aggregate unreviewed Hit counts per task_id ---
        hit_stats = (
            Hit.objects.filter(dataset=self, reviewed=False)
            .values("task_id")
            .annotate(
                pass0=Count("place_id", filter=Q(query_pass="pass0"), distinct=True),
                pass1=Count("place_id", filter=Q(query_pass="pass1"), distinct=True),
                pass2=Count("place_id", filter=Q(query_pass="pass2"), distinct=True),
                pass3=Count("place_id", filter=Q(query_pass="pass3"), distinct=True),
            )
        )

        # --- Step 2: build a lookup of successful tasks for metadata ---
        tasks = self.tasks.filter(status="SUCCESS").values("task_id", "task_name", "date_done")
        task_meta = {t["task_id"]: t for t in tasks}

        # --- Step 3: merge aggregated counts with metadata ---
        result = {tt: [] for tt in [
            "align_wdlocal", "align_tgn", "align_idx", "align_whg", "align_wd"
        ]}

        for row in hit_stats:
            tid = row["task_id"]
            meta = task_meta.get(tid)
            if not meta:
                continue  # skip stray hits not linked to a known task

            task_name = meta["task_name"]
            total = row["pass0"] + row["pass1"] + row["pass2"] + row["pass3"]

            result[task_name].append({
                "tid": tid,
                "date": meta["date_done"].strftime("%Y-%m-%d") if meta["date_done"] else None,
                "total": total,
                "pass0": row["pass0"],
                "pass1": row["pass1"],
                "pass2": row["pass2"],
                "pass3": row["pass3"],
            })

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
        indexes = [
            models.Index(fields=['task_id', 'reviewed', 'query_pass'], name='hit_review_lookup'),
            models.Index(fields=['place_id', 'task_id'], name='hit_place_task'),
            models.Index(fields=['task_id', 'reviewed'], name='hit_task_reviewed'),
        ]


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
