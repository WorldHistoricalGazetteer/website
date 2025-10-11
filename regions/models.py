from django.contrib.gis.db import models as gis_models
from django.contrib.gis.gdal import SpatialReference, CoordTransform
from django.contrib.gis.geos import MultiPolygon
from django.contrib.postgres.fields import ArrayField
from django.db import models

class Region(models.Model):
    """
    Any M49 entry: global, region, sub-region, intermediate, or country. TODO: Generalise to other coding systems
    """
    m49 = models.CharField(max_length=10, unique=True)
    level = models.CharField(max_length=20)  # global, region, sub-region, intermediate, country

    parents = models.ManyToManyField(
        "self", symmetrical=False, related_name="children", blank=True
    )

    members = models.ManyToManyField(
        "self", symmetrical=False, related_name="member_of", blank=True
    )

    iso_alpha2 = ArrayField(models.CharField(max_length=2), blank=True, default=list)
    iso_alpha3 = ArrayField(models.CharField(max_length=3), blank=True, default=list)

    # GeoDjango fields
    geom = gis_models.MultiPolygonField(null=True, blank=True, srid=4326)
    hull = gis_models.MultiPolygonField(null=True, blank=True, srid=4326)

    # Optional metadata
    flag_url = models.URLField(blank=True, null=True)
    wikidata = models.CharField(max_length=50, blank=True, null=True)
    wikipedia = models.CharField(max_length=100, blank=True, null=True)
    currency = models.CharField(max_length=20, blank=True, null=True)
    default_language = ArrayField(models.CharField(max_length=5), blank=True, default=list)
    population = models.BigIntegerField(blank=True, null=True)
    population_date = ArrayField(models.DateField(), blank=True, default=list)

    def __str__(self):
        # get the English label if available
        label = self.labels.filter(lang='en').first()
        label_en = label.name if label else "(no label)"
        return f"{label_en} ({self.m49})"

    @property
    def labels_dict(self):
        """Return all labels as a {lang: name} dict"""
        return {l.lang: l.name for l in self.labels.all()}

    def get_hull_buffered(self, buffer_km: float) -> MultiPolygon | None:
        """
        Returns a buffered version of the hull in meters, using an Azimuthal Equidistant projection
        centered on the hull's centroid.

        :param buffer_km: buffer distance in kilometres
        """
        if not self.hull:
            return None

        # Compute centroid
        centroid = self.hull.centroid
        lat, lon = centroid.y, centroid.x

        # Define Azimuthal Equidistant projection centered on centroid
        aeqd_proj = SpatialReference(f"+proj=aeqd +lat_0={lat} +lon_0={lon} +units=m +datum=WGS84")

        # Transform hull to local projection
        original_srs = self.hull.srs or SpatialReference('EPSG:4326')
        transform_to_local = CoordTransform(original_srs, aeqd_proj)
        local_hull = self.hull.clone()
        local_hull.transform(transform_to_local)

        # Buffer in meters
        buffer_m = buffer_km * 1000
        buffered_local = local_hull.buffer(buffer_m)

        # Transform back to WGS84
        transform_back = CoordTransform(aeqd_proj, original_srs)
        buffered_local.transform(transform_back)

        # Ensure MultiPolygon
        if buffered_local.geom_type == "Polygon":
            buffered_local = MultiPolygon(buffered_local)

        return buffered_local


class RegionLabel(models.Model):
    """
    Multilingual labels for a Region, with optional qualifier.
    """
    region = models.ForeignKey(
        "Region", on_delete=models.CASCADE, related_name="labels"
    )
    lang = models.CharField(
        max_length=3,  # ISO 639-1 or 639-3 code (2–3 letters)
        help_text="Base ISO language code"
    )
    variant = models.CharField(
        max_length=20,
        blank=True,
        help_text="Optional language variant (e.g., 'tarask', 'Hant')"
    )
    qualifier = models.CharField(
        max_length=20,
        blank=True,
        help_text="Optional qualifier for the label (e.g., 'UN', 'official', 'old', 'short')"
    )
    name = models.CharField(max_length=255)

    class Meta:
        unique_together = ("region", "lang", "variant", "qualifier")

    def __str__(self):
        if self.qualifier:
            return f"{self.name} [{self.lang}:{self.qualifier}]"
        return f"{self.name} [{self.lang}]"
