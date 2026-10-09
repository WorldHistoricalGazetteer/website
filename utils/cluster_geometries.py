# cluster_geometries.py

from django.contrib.gis.geos import GEOSGeometry, Point, MultiPoint
from geojson import Feature, Point
from places.models import PlaceGeom
import numpy as np
import simplejson as json
# from sklearn.cluster import AgglomerativeClustering
# from sklearn.metrics import calinski_harabasz_score
from sklearn.cluster import KMeans

def clustered_geometries(caller, min_clusters=7, max_clusters=10, user=None):    
    # Detect the class of the caller
    caller_class = type(caller)
    caller_class = caller_class.__name__   
    
    clustered_geometries = {
        "type": "FeatureCollection",
        "mode": "clusterhull",
        "features": [],
        "properties": {},
    }

    def flatten_coordinates(coord): # Required to cope with nested GeometryCollections and MultiGeometries
        flattened = []
        for element in coord:
            if isinstance(element, tuple):
                flattened.extend(flatten_coordinates(element))
            else:
                flattened.append(element)
        return flattened
    
    coordinates = []
    
    if caller_class == 'Dataset':
        dsgeoms = PlaceGeom.objects.filter(place__dataset=caller)
        if not dsgeoms:
            return clustered_geometries
        for geom in dsgeoms:
            if geom.geom:  # Check if geom.geom is not None
                coords = GEOSGeometry(geom.geom).tuple
                coordinates.extend(flatten_coordinates(coords))
    else: # caller_class == 'Collection'
        # place#310: a viewer is named ⇒ withhold members of datasets outside their circle
        places = caller.visible_places(user) if user is not None else caller.places_all
        if not places:
            return clustered_geometries
        for place in places:
            for geom in place.geoms.all():
                if geom.geom:  # Check if geom.geom is not None
                    coords = GEOSGeometry(geom.geom).tuple
                    coordinates.extend(flatten_coordinates(coords))
                    
    if len(coordinates) == 0:
        return clustered_geometries
    
    # Add the total coordinate count to properties
    clustered_geometries["properties"]["coordinate_count"] = len(coordinates) // 2
    
            
    # Reshape the flat list into coordinate tuples
    coordinates = np.array([coordinates[i:i+2] for i in range(0, len(coordinates), 2)])
    
    # # Perform Agglomerative Clustering
    # calinski_scores = []
    #
    # for n_clusters in range(2, max_clusters + 1):
    #     clusterer = AgglomerativeClustering(n_clusters=n_clusters)
    #     labels = clusterer.fit_predict(coordinates)
    #     calinski_score = calinski_harabasz_score(coordinates, labels)
    #     calinski_scores.append(calinski_score)
    #
    # # Find the "elbow" point using the first derivative
    # deltas = np.diff(calinski_scores)
    # elbow_index = np.argmax(deltas < np.mean(deltas) / 2) + 1
    # optimal_clusters = elbow_index + 1  # Add 1 because of 0-based indexing
    #
    # optimal_clusters = max(optimal_clusters, min_clusters)
    #
    # print('optimal_clusters', optimal_clusters)
    #
    # # Perform clustering with the optimal number of clusters
    # clusterer = AgglomerativeClustering(n_clusters=optimal_clusters)
    # labels = clusterer.fit_predict(coordinates)
    
    # Perform KMeans clustering with a fixed number of clusters
    kmeans = KMeans(n_clusters=min(len(coordinates), max_clusters))
    labels = kmeans.fit_predict(coordinates)
    
    # Keep track of which labels have been processed
    processed_labels = set()
    
    # Create features using convex hull for clusters with more than 2 members
    for i, coord in enumerate(coordinates):
        current_label = labels[i]
        
        # Check if the label has been processed
        if current_label not in processed_labels:
            members_indices = np.where(labels == current_label)[0]
    
            # Check if there are more than 2 members in the cluster
            if len(members_indices) > 2:
                # Create convex hull polygon using Django's convex_hull method
                cluster_points = [GEOSGeometry(f"POINT ({coord[0]} {coord[1]})") for coord in coordinates[members_indices]]
                multipoint_geom = MultiPoint(cluster_points)
                convex_hull = multipoint_geom.convex_hull
                clustered_geometries['features'].append(Feature(geometry=json.loads(GEOSGeometry(convex_hull).geojson) ))
            else:
                # Create points for clusters with 1 or 2 members
                for member_index in members_indices:
                    clustered_geometries['features'].append(Feature( geometry=Point(coordinates=coordinates[member_index].tolist()) ))
    
            processed_labels.add(current_label)

    print('clustered_geometries', clustered_geometries)
    return clustered_geometries
