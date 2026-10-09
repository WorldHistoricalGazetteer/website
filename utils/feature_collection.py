# feature_collection.py

from django.contrib.gis.geos import GEOSGeometry
from geojson import Feature
from places.models import PlaceGeom
import numpy as np
import simplejson as json

def feature_collection(caller, user=None):
    
    # Detect the class of the caller
    caller_class = type(caller)
    caller_class = caller_class.__name__   
    
    feature_collection = {
        "type": "FeatureCollection",
        "mode": "default",
        "features": [],
    }
    geom_list = None
    
    if caller_class == 'Dataset':
        geom_list = PlaceGeom.objects.filter(place_id__in=caller.placeids).values_list('jsonb', flat=True)
    else: # caller_class == 'Collection'
        # place#310: a viewer is named ⇒ withhold members of datasets outside their circle
        places = caller.visible_places(user) if user is not None else caller.places_all
        if places.count() > 0:
            geom_list = [geom.jsonb for place in places for geom in place.geoms.all()]
            
    if geom_list:
        for geometry in geom_list:
            feature_collection['features'].append(Feature(geometry=geometry))

    return feature_collection
