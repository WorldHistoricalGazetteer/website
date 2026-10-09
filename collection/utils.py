from django.contrib.gis.geos import Polygon, MultiPolygon

from collection.models import Collection


def compute_collection_bbox(collection, withheld_dataset_pks=()):
    """The collection's bounding box; with ``withheld_dataset_pks`` (place#310),
    members from those datasets are left out, so a viewer who may not see them
    is not shown where they are."""
    withheld = set(withheld_dataset_pks or ())
    if collection.collection_class == "place":
        places = collection.places.all()
        if withheld:
            places = places.exclude(dataset__id__in=withheld)
        bboxes = [Polygon.from_bbox(place.extent) for place in places if place.extent]
    elif collection.collection_class == "dataset":
        datasets = collection.datasets.all()
        if withheld:
            datasets = datasets.exclude(id__in=withheld)
        bboxes = [dataset.bbox for dataset in datasets if dataset.bbox]
    else:
        bboxes = []

    if bboxes:
        combined_bbox = MultiPolygon(bboxes)
        return Polygon.from_bbox(combined_bbox.extent)
    else:
        return None