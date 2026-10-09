# api/querysets.py
from django.db.models import Prefetch

from api.dataset_access import visible_datasets_q, visible_places_q
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset
from periods.models import Period, TemporalBound
from places.models import Place


def area_owner_queryset(authenticated_user):
    """
    Returns the queryset of Areas visible to a particular user.
    """
    return Area.objects.filter(owner=authenticated_user)


def collection_owner_or_public_queryset(authenticated_user):
    """
    Returns the queryset of Collections visible to a particular user.
    """
    return Collection.objects.filter(public=True) | Collection.objects.filter(owner=authenticated_user)


def dataset_owner_or_public_queryset(authenticated_user):
    """
    Returns the queryset of Datasets visible to a particular user.
    """
    # place#310: the one rule — public (and not embargoed), or inside the circle
    # (owner, co-owner, collaborator, staff) — rather than public-or-owner.
    return Dataset.objects.filter(visible_datasets_q(authenticated_user))


def period_public_queryset(user):
    """
    Return periods queryset. All periods are public.
    """
    return (
        Period.objects
        .select_related('authority')
        .prefetch_related(
            'chrononyms',
            'spatialCoverage',
            Prefetch('bounds', queryset=TemporalBound.objects.order_by('kind', 'earliestYear'))
        )
    )


def place_feature_queryset(authenticated_user):
    """
    Full queryset for PlaceFeatureSerializer.
    Prefetches all related objects needed for feature export.
    """
    # place#310: a place of a dataset outside the user's circle is not in the
    # queryset at all, so every consumer (entity API, reconcile EXTEND, exports)
    # answers "not found" for it. One join, no per-row query.
    return (
        Place.objects
        .filter(visible_places_q(authenticated_user))
        .select_related("dataset")
        .prefetch_related(
            "names",
            "types",
            "geoms",
            "links",
            "related",
            "whens",
            "descriptions",
            "depictions",
        )
    )


def place_preview_queryset(authenticated_user):
    """
    Lean queryset for PlacePreviewSerializer.
    Prefetches only the relations required by preview fields.
    """
    return (
        Place.objects
        .filter(visible_places_q(authenticated_user))  # place#310
        .select_related("dataset")
        .prefetch_related(
            "names",
            "types",
            "whens",
        )
    )
