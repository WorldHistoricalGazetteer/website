# heatmapped_geometries.py

from django.contrib.gis.geos import GEOSGeometry, Point
from geojson import Feature, Point
from places.models import PlaceGeom
import numpy as np
import simplejson as json
from json.encoder import INFINITY

def heatmapped_geometries(caller, user=None):
    
    # Detect the class of the caller
    caller_class = type(caller)
    caller_class = caller_class.__name__   
    
    heatmapped_geometries = {
        "type": "FeatureCollection",
        "mode": "heatmap",
        "features": [],
    }
    
    max_area = float('-inf')
    
    def add_geometry(geometry):
        zero_dimension_factor = 0.146 # Seems to give a good balance between point and other geometries
        point_area = 1000 * 3.14159 * (zero_dimension_factor / 2) ** 2 # A = πr^2

        area = None
        centroid = None

        # Calculate area and centroid based on geometry type
        if geometry.geom_type == 'Point':
            centroid = geometry
            area = point_area
        elif geometry.geom_type == 'MultiPoint':
            centroid = geometry.centroid
            area = sum([point_area for point in geometry])
        elif geometry.geom_type in ['LineString', 'MultiLineString']:
            centroid = geometry.centroid
            area = geometry.length * zero_dimension_factor
        elif geometry.geom_type in ['Polygon', 'MultiPolygon']:
            centroid = geometry.centroid
            area = geometry.area
        
        nonlocal max_area
        max_area = max(area, max_area)

        heatmapped_geometries['features'].append(Feature(
            geometry=Point(coordinates=(centroid.x, centroid.y) if centroid else None),
            properties={'area': area, 'original_type': geometry.geom_type}
        ))
    
    if caller_class == 'Dataset':
        dsgeoms = PlaceGeom.objects.filter(place__dataset=caller)
        if not dsgeoms:
            return heatmapped_geometries
        for dsgeom in dsgeoms:
            add_geometry(dsgeom.geom)

    else: # caller_class == 'Collection'
        # place#310: a viewer is named ⇒ withhold members of datasets outside their circle
        places = caller.visible_places(user) if user is not None else caller.places_all
        if not places:
            return heatmapped_geometries
        for place in places:
            for placegeom in place.geoms.all():
                add_geometry(placegeom.geom)
                
    # Normalize area values in the collected features
    for feature in heatmapped_geometries['features']:
        area = feature['properties']['area']
        normalized_area = area / max_area if max_area != 0 else 0.5
        feature['properties']['area'] = normalized_area

    return heatmapped_geometries
