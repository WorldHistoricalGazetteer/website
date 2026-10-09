# hull_geometries.py

from django.contrib.gis.geos import GEOSGeometry
from geojson import Feature
from places.models import PlaceGeom
import numpy as np
import simplejson as json

def hull_geometries(caller, user=None):
    
    # Detect the class of the caller
    caller_class = type(caller)
    caller_class = caller_class.__name__   
    
    hull_geometries = {
        "type": "FeatureCollection",
        "mode": "convexhull",
        "features": [],
    }
    geometry = None
    geom_list = None
    
    if caller_class == 'Dataset':
        dsgeoms = PlaceGeom.objects.filter(place__dataset=caller.label)
        if dsgeoms.count() > 0:
            geom_list = [GEOSGeometry(dsgeom.geom.wkt) for dsgeom in dsgeoms]
    else: # caller_class == 'Collection'
        # place#310: a viewer is named ⇒ withhold members of datasets outside their circle
        places = caller.visible_places(user) if user is not None else caller.places_all
        if places.count() > 0:
            geom_list = [GEOSGeometry(geom.geom.wkt) for place in places for geom in place.geoms.all()]
            
    if geom_list:
        combined_geom = geom_list[0].convex_hull
            
        for geom in geom_list[1:]: # Union of convex hulls is much faster than union of full geometries
            combined_geom = combined_geom.union(geom.convex_hull)
            
        geometry = json.loads(combined_geom.convex_hull.geojson)
        hull_geometries['features'].append(Feature(geometry=geometry))

    return hull_geometries
