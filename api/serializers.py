# api.serializers.py
import re
from html import escape

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.urls import reverse

from collection.models import Collection

User = get_user_model()

from django.contrib.gis.geos import GEOSGeometry, Point, Polygon, MultiPolygon, LineString, MultiLineString
from rest_framework import serializers
from django.core import serializers as coreserializers
from rest_framework_gis.serializers import GeoFeatureModelSerializer, GeometrySerializerMethodField
from datasets.models import Dataset
from areas.models import Area
from main.choices import DATATYPES
from places.models import *
from traces.models import TraceAnnotation

import json, geojson
from shapely.geometry import shape

# TODO: these are updated in both Dataset & DatasetFile  (??)
datatype = models.CharField(max_length=12, null=False, choices=DATATYPES,
                            default='place')
numrows = models.IntegerField(null=True, blank=True)

# these are back-filled
numlinked = models.IntegerField(null=True, blank=True)
total_links = models.IntegerField(null=True, blank=True)


class ErrorResponseSerializer(serializers.Serializer):
    error = serializers.CharField()


# ***
# IN USE Apr 2020
# ***
class DatasetSerializer(serializers.HyperlinkedModelSerializer):
    # don't list all places in a dataset API record
    owner = serializers.ReadOnlyField(source='owner.name')
    contributors = serializers.CharField(allow_null=True, allow_blank=True)

    place_count = serializers.SerializerMethodField('get_count')

    def get_count(self, ds):
        return ds.places.count()

    class Meta:
        model = Dataset
        fields = ('id', 'place_count', 'owner', 'label', 'title', 'description', 'datatype',
                  'ds_status', 'create_date', 'public', 'core', 'creator', 'webpage',
                  'contributors')
        extra_kwargs = {
            'created_by': {'read_only': True}}


class GallerySerializer(serializers.HyperlinkedModelSerializer):
    type = serializers.SerializerMethodField()
    icon = serializers.SerializerMethodField()
    label = serializers.SerializerMethodField()
    ds_or_c_id = serializers.SerializerMethodField()
    url = serializers.SerializerMethodField()

    def __init__(self, *args, **kwargs):
        model = kwargs.pop('model', None)
        if model:
            self.Meta.model = model
        super().__init__(*args, **kwargs)

    def get_type(self, obj):
        if self.Meta.model == Collection:
            return f"{obj.collection_class}_collection"
        else:
            return 'dataset'

    def get_icon(self, obj):
        if self.Meta.model == Collection:
            if obj.collection_class == 'dataset':
                return 'fa-layer-group'
            elif obj.collection_class == 'place':
                return 'fa-map-pin'
        else:
            return 'fa-globe-americas'

    def get_label(self, obj):
        if self.Meta.model == Collection:
            if obj.collection_class == 'dataset':
                return 'Dataset Collection'
            elif obj.collection_class == 'place':
                return 'Place Collection'
        else:
            return 'Dataset'

    def get_ds_or_c_id(self, obj):
        return obj.id

    def get_url(self, obj):
        if self.Meta.model == Collection:
            if obj.collection_class == 'dataset':
                return reverse('collection:ds-collection-browse', args=[obj.id])
            elif obj.collection_class == 'place':
                return reverse('collection:place-collection-browse', args=[obj.id])
        else:
            return reverse('datasets:ds_places', args=[obj.id])

    class Meta:
        model = Dataset  # May be overridden by __init__ and changed to Collection
        fields = (
            'title',
            'image_file',
            'description',
            'owner',
            'type',
            'icon',
            'label',
            'featured',
            'ds_or_c_id',
            'webpage',
            'url',
            'citation_csl',
        )


class UserSerializer(serializers.HyperlinkedModelSerializer):
    datasets = serializers.HyperlinkedRelatedField(
        # many=True, view_name='dataset-detail', read_only=True)
        many=True, view_name='ds_status', read_only=True)

    class Meta:
        model = User
        fields = ['id', 'name', 'email', 'url', 'datasets']
        # fields = ('id','url', 'username', 'email', 'groups', 'datasets')


class TraceAnnotationSerializer(serializers.ModelSerializer):
    class Meta:
        model = TraceAnnotation
        fields = ('id', 'src_id', 'collection', 'anno_type', 'motivation',
                  'when', 'sequence', 'creator', 'created')


class PlaceDepictionSerializer(serializers.ModelSerializer):
    # json: @id, title, license
    identifier = serializers.ReadOnlyField(source='jsonb.@id')
    title = serializers.ReadOnlyField(source='jsonb.title')
    license = serializers.ReadOnlyField(source='jsonb.license')

    class Meta:
        model = PlaceDepiction
        fields = ('identifier', 'title', 'license')


class PlaceDescriptionSerializer(serializers.ModelSerializer):
    # json: @id, value, lang
    identifier = serializers.ReadOnlyField(source='jsonb.id')
    value = serializers.ReadOnlyField(source='jsonb.value')
    lang = serializers.ReadOnlyField(source='jsonb.lang')

    class Meta:
        model = PlaceDescription
        fields = ('identifier', 'value', 'lang')


class PlaceWhenSerializer(serializers.ModelSerializer):
    # json: timespans, periods, label, duration
    timespans = serializers.ReadOnlyField(source='jsonb.timespans')
    periods = serializers.ReadOnlyField(source='jsonb.periods')
    label = serializers.ReadOnlyField(source='jsonb.label')
    duration = serializers.ReadOnlyField(source='jsonb.duration')

    class Meta:
        model = PlaceWhen
        fields = ('timespans', 'periods', 'label', 'duration')


class PlaceRelatedSerializer(serializers.ModelSerializer):
    # json: relation_type, relation_to, label, when, citation, certainty
    relation_type = serializers.ReadOnlyField(source='jsonb.relationType')
    relation_to = serializers.ReadOnlyField(source='jsonb.relationTo')
    label = serializers.ReadOnlyField(source='jsonb.label')
    when = serializers.ReadOnlyField(source='jsonb.when')
    citation = serializers.ReadOnlyField(source='jsonb.citation')
    certainty = serializers.ReadOnlyField(source='jsonb.certainty')

    class Meta:
        model = PlaceRelated
        fields = ('relation_type', 'relation_to', 'label', 'when',
                  'citation', 'certainty')


class PlaceLinkSerializer(serializers.ModelSerializer):
    # json: type, identifier
    aug = serializers.SerializerMethodField('augmented')

    def augmented(self, obj):
        return True if obj.task_id is not None else False

    type = serializers.ReadOnlyField(source='jsonb.type')
    identifier = serializers.ReadOnlyField(source='jsonb.identifier')

    class Meta:
        model = PlaceLink
        fields = ('type', 'identifier', 'aug')


# returns single (first) geom for a place
class PlaceGeomSerializer(serializers.ModelSerializer):
    # json: type, geowkt, coordinates, when{}
    # title = serializers.ReadOnlyField(source='title')
    ds = serializers.SerializerMethodField()
    geom = serializers.ReadOnlyField(source="jsonb")  # pass through as-is

    def get_ds(self, obj):
        return obj.place.dataset.id

    # title = serializers.SerializerMethodField()
    # def get_title(self, obj):
    # return obj.place.title

    type = serializers.ReadOnlyField(source='jsonb.type')
    geowkt = serializers.ReadOnlyField(source='jsonb.geowkt')
    coordinates = serializers.ReadOnlyField(source='jsonb.coordinates')
    citation = serializers.ReadOnlyField(source='jsonb.citation')
    when = serializers.ReadOnlyField(source='jsonb.when')
    certainty = serializers.ReadOnlyField(source='jsonb.certainty')

    class Meta:
        model = PlaceGeom
        fields = ('place_id', 'src_id', 'type', 'geowkt', 'coordinates',
                  'geom_src', 'citation', 'when', 'title', 'ds',
                  'certainty', 'geom')


# return list of normalized coordinates for a place
class PlaceGeomsSerializer(serializers.ModelSerializer):
    # ds = serializers.SerializerMethodField()
    # def get_ds(self, obj):
    #   return obj.place.dataset.id

    type = serializers.ReadOnlyField(source='jsonb.type')
    geowkt = serializers.ReadOnlyField(source='jsonb.geowkt')
    coordinates = serializers.ReadOnlyField(source='jsonb.coordinates')
    citation = serializers.ReadOnlyField(source='jsonb.citation')

    # citation = serializers.ReadOnlyField(source='jsonb.citation') or None
    # when = serializers.ReadOnlyField(source='jsonb.when')
    # certainty = serializers.ReadOnlyField(source='jsonb.certainty')

    class Meta:
        model = PlaceGeom
        fields = ('place_id', 'src_id', 'type', 'geowkt', 'coordinates',
                  'citation'
                  # , 'ds'
                  # 'title', 'geom_src', 'when', 'certainty'
                  )


class PlaceGeoFeatureSerializer(GeoFeatureModelSerializer):
    ds = serializers.SerializerMethodField()
    title = serializers.CharField(source='place.title')  # To include the 'title' attribute

    def get_ds(self, obj):
        return obj.place.dataset.id

    type = serializers.ReadOnlyField(source='jsonb.type')
    coordinates = serializers.ReadOnlyField(source='jsonb.coordinates')
    when = serializers.ReadOnlyField(source='jsonb.when', default=None)
    citation = serializers.ReadOnlyField(source='jsonb.citation', default=None)
    certainty = serializers.ReadOnlyField(source='jsonb.certainty', default=None)

    class Meta:
        model = PlaceGeom
        geo_field = 'geom'
        fields = ('ds', 'title', 'place_id', 'type', 'coordinates', 'geom_src', 'citation', 'when', 'certainty')

    def to_representation(self, instance):
        data = super().to_representation(instance)
        # Remove 'coordinates' from the 'properties' dictionary
        if 'coordinates' in data.get('properties', {}):
            data['properties'].pop('coordinates')

        return data


class PlaceTypeSerializer(serializers.ModelSerializer):
    # json: identifier, label, sourceLabel OR sourceLabels[{}], when{}
    identifier = serializers.ReadOnlyField(source='jsonb.identifier')
    label = serializers.ReadOnlyField(source='jsonb.label')
    sourceLabel = serializers.ReadOnlyField(source='jsonb.sourceLabel')
    sourceLabels = serializers.ReadOnlyField(source='jsonb.sourceLabels')
    when = serializers.ReadOnlyField(source='jsonb.when')
    gn_class = serializers.ReadOnlyField(source='fclass')

    class Meta:
        model = PlaceType
        fields = ('label', 'sourceLabel', 'sourceLabels', 'when', 'identifier', 'gn_class')


class PlaceNameSerializer(serializers.ModelSerializer):
    # jsonb: toponym, citation{}
    toponym = serializers.ReadOnlyField(source='jsonb.toponym')
    citations = serializers.ReadOnlyField(source='jsonb.citations')
    # id = serializers.ReadOnlyField(source='jsonb.citation["id"]')
    # label = serializers.ReadOnlyField(source='jsonb.citation.label')
    when = serializers.ReadOnlyField(source='jsonb.when')

    class Meta:
        model = PlaceName
        fields = ('toponym', 'when', 'citations')


"""
    direct representation of normalized records in database
    used in multiple views
"""


class PlaceSerializer(serializers.ModelSerializer):
    dataset = serializers.ReadOnlyField(source='dataset.title')
    dataset_id = serializers.ReadOnlyField(source='dataset.id')
    names = PlaceNameSerializer(many=True, read_only=True)
    types = PlaceTypeSerializer(many=True, read_only=True)
    geoms = PlaceGeomSerializer(many=True, read_only=True)
    extent = serializers.ReadOnlyField()
    links = PlaceLinkSerializer(many=True, read_only=True)
    related = PlaceRelatedSerializer(many=True, read_only=True)
    whens = PlaceWhenSerializer(many=True, read_only=True)
    descriptions = PlaceDescriptionSerializer(many=True, read_only=True)
    depictions = PlaceDepictionSerializer(many=True, read_only=True)

    # cid param passed only from place collection browse screen
    traces = serializers.SerializerMethodField('trace_anno')

    def trace_anno(self, place):
        cid = self.context.get("cid")
        if cid is not None:
            return json.loads(
                coreserializers.serialize("json", TraceAnnotation.objects.filter(
                    place=place.id, collection=cid, archived=False)))
        else:
            return json.loads('[]')

    # traces = serializers.SerializerMethodField('trace_anno')
    # def trace_anno(self, place):
    #   cid = self.context["query_params"]['cid']
    #   return json.loads(
    #     coreserializers.serialize("json", TraceAnnotation.objects.filter(
    #       place=place.id, collection=cid, archived=False)))
    #
    geo = serializers.SerializerMethodField('has_geom')

    def has_geom(self, place):
        return '<i class="fa fa-globe"></i>' if place.geom_count > 0 else "-"

    class Meta:
        model = Place
        fields = ('url', 'id', 'title', 'src_id', 'dataset', 'dataset_id', 'ccodes', 'fclasses',
                  'names', 'types', 'geoms', 'extent', 'links', 'related', 'whens',
                  'descriptions', 'depictions', 'geo', 'minmax', 'traces'
                  )


"""
    partial direct representation of normalized place-related records in database
    used for dataset updates
"""


class PlaceCompareSerializer(serializers.ModelSerializer):
    dataset = serializers.ReadOnlyField(source='dataset.title')
    names = PlaceNameSerializer(many=True, read_only=True)
    types = PlaceTypeSerializer(many=True, read_only=True)
    geoms = PlaceGeomsSerializer(many=True, read_only=True)
    links = PlaceLinkSerializer(many=True, read_only=True)
    related = PlaceRelatedSerializer(many=True, read_only=True)
    whens = PlaceWhenSerializer(many=True, read_only=True)

    class Meta:
        model = Place
        fields = ('url', 'id', 'title', 'src_id', 'dataset', 'ccodes', 'fclasses',
                  'names', 'types', 'geoms', 'links', 'related', 'whens',
                  'minmax')


# returns default and computed columns for owner ds browse table (ds_browse.html)
# TODO: used by both PlaceTableViewSet and PlaceTableCollViewSet - PROBLEM?
class PlaceTableSerializer(serializers.ModelSerializer):
    # print('hit PlaceTableSerializer()')
    dataset = DatasetSerializer()
    ds = serializers.SerializerMethodField()

    def get_ds(self, place):
        cell_value = '<a class="pop-link pop-dataset" data-id=' + str(place.dataset.id) + \
                     ' data-toggle="popover" title="Dataset Profile" data-content=""' + \
                     ' tabindex="0" rel="clickover">' + place.dataset.label + '</a>'
        return cell_value

    id = serializers.SerializerMethodField()

    def get_id(self, place):
        # link to place record
        uri_base = place.dataset.uri_base
        uri_prefix = uri_base if 'whg' not in uri_base else settings.URL_FRONT + '/api/db/?id='
        if place.dataset.public or place.dataset.core:
            pid = '<a href="' + uri_prefix + str(place.id) + '" target="_blank">' + str(place.id) + '</a>'
        else:
            pid = place.id
        return pid

    geo = serializers.SerializerMethodField()

    def get_geo(self, place):
        if place.geom_count > 0:
            gtype = place.geoms.all()[0].jsonb['type'].lower()
            fn = "point" if 'point' in gtype else "polygon" if 'poly' in gtype else "linestring"
            return '<img src="/static/images/geo_' + fn + '.svg" width=12/>'
        else:
            return '&mdash;'

    chk = serializers.SerializerMethodField()

    def get_chk(self, place):
        return '<input type="checkbox" name="addme" class="table-chk" data-id="' + str(place.id) + '"/>'

    revwd = serializers.SerializerMethodField('rev_wd')

    def rev_wd(self, place):
        tasks_wd = place.dataset.tasks.filter(task_name='align_wdlocal', status='SUCCESS')
        if place.review_wd == 1:
            val = '<i class="fa fa-check"></i>'
        elif not place.hashits_wd:
            val = '<i>no hits</i>'
        elif place.review_wd == 0:
            val = '&#9744;'
        elif place.flag == True:
            val = 'altered'
        else:
            # direct link to deferred record
            val = '<a href="/datasets/' + str(place.dataset.id) + '/review/' + \
                  tasks_wd[0].task_id + '/def?pid=' + str(place.id) + '"><i>deferred</i></a>' \
                if len(tasks_wd) > 0 else '<i>deferred</i>'
        return val

    revwhg = serializers.SerializerMethodField('rev_whg')

    def rev_whg(self, place):
        tasks_whg = place.dataset.tasks.filter(task_name='align_idx', status='SUCCESS')
        if place.review_whg == 1:
            val = '<i class="fa fa-check"></i>'
        elif not place.hashits_whg:
            val = '<i>no hits</i>'
        elif place.review_whg == 0:
            val = '&#9744;'
        else:
            # direct link to deferred record
            val = '<a href="/datasets/' + str(place.dataset.id) + '/review/' + \
                  tasks_whg[0].task_id + '/def?pid=' + str(place.id) + '"><i>deferred</i></a>' \
                if len(tasks_whg) > 0 else '<i>deferred</i>'
        return val

    revtgn = serializers.SerializerMethodField('rev_tgn')

    def rev_tgn(self, place):
        if place.review_tgn == 1:
            val = '<i class="fa fa-check"></i>'
        elif not place.hashits_tgn:
            val = '<i>no hits</i>'
        elif place.review_tgn == 0:
            val = '&#9744;'
        else:
            val = '<i>deferred</i>'
        return val

    class Meta:
        model = Place
        fields = ('url', 'id', 'title', 'src_id', 'fclasses',
                  'ccodes', 'geo', 'minmax',
                  'revwhg', 'revwd', 'revtgn',
                  'review_whg', 'review_wd', 'review_tgn'
                  , 'ds', 'dataset', 'dataset_id', 'chk',
                  'annos')


""" used by: api.views.GeoJSONViewSet() """


class FeatureSerializer(GeoFeatureModelSerializer):
    geom = GeometrySerializerMethodField()

    def get_geom(self, obj):
        # print('obj',obj.__dict__)
        s = json.dumps(obj.jsonb)
        g1 = geojson.loads(s)
        g2 = shape(g1)
        djgeo = GEOSGeometry(g2.wkt)
        # print('geom', djgeo.geojson)
        return GEOSGeometry(g2.wkt)

    #
    title = serializers.SerializerMethodField('get_title')

    def get_title(self, obj):
        return obj.place.title

    class Meta:
        model = PlaceGeom
        geo_field = 'geom'
        id_field = False
        # fields = ('place_id','src_id','title')
        fields = ('place_id', 'src_id', 'title', 'geom')


""" uses: AreaViewset()"""


class AreaSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Area
        fields = ('title', 'type', 'geojson')


# TODO: what is this???
class SearchDatabaseSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Place
        # depth = 1
        fields = ('url', 'type', 'properties', 'geometry', 'names', 'types', 'links'
                  , 'related', 'whens', 'descriptions', 'depictions', 'minmax'
                  )


class LPFSerializer(serializers.Serializer):
    names = PlaceNameSerializer(source="placename_set", many=True, read_only=True)
    types = PlaceTypeSerializer(many=True, read_only=True)
    links = PlaceLinkSerializer(many=True, read_only=True)
    related = PlaceRelatedSerializer(many=True, read_only=True)
    whens = PlaceWhenSerializer(many=True, read_only=True)
    descriptions = PlaceDescriptionSerializer(many=True, read_only=True)
    depictions = PlaceDepictionSerializer(many=True, read_only=True)

    # custom fields for LPF transform
    type = serializers.SerializerMethodField('get_type')

    def get_type(self, place) -> str:
        return "Feature"

    uri = serializers.SerializerMethodField('get_uri')

    def get_uri(self, place) -> str:
        return "https://whgazetteer.org/api/place/" + str(place.id)

    properties = serializers.SerializerMethodField('get_properties')

    def get_properties(self, place) -> dict:
        props = {
            "place_id": place.id,
            "dataset_label": place.dataset.label,
            "dataset_title": place.dataset.title,
            "dataset_uri": place.dataset.uri,
            "src_id": place.src_id,
            "title": place.title,
            "ccodes": place.ccodes,
            "fclasses": place.fclasses,
            "minmax": place.minmax,
            "timespans": place.timespans
        }
        return props

    geometry = serializers.SerializerMethodField('get_geometry')

    def get_geometry(self, place) -> dict:
        geometries_data = PlaceGeomSerializer(place.geoms.all(), many=True).data
        if len(geometries_data) == 1:
            return geometries_data[0]

        return {
            "type": "GeometryCollection",
            "geometries": geometries_data
        }

    class Meta:
        model = Place
        fields = ('uri', 'type', 'properties', 'geometry', 'names', 'types', 'links'
                  , 'related', 'whens', 'descriptions', 'depictions', 'minmax'
                  )
