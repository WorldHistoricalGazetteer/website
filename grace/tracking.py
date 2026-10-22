"""Start tracking a dataset that has already been submitted to WHG.

``TrackedDataset`` is deliberately not a ``datasets.Dataset`` (see its
docstring), and its only structural link to one is the Register entry
``whg:NNNN``. That entry is pushed by the indexing pipeline, so a dataset
uploaded last week has none yet. Without this module the only way to review
such a dataset was to retype its title into GRACE by hand and leave nothing to
say which upload it was.

So the link is recorded two ways, and the second is what makes it work before
the Register catches up:

* ``registry`` — set whenever ``whg:NNNN`` exists. This is the real link and
  everything read-through hangs off it.
* a marker line at the head of ``notes``, ``[whg-dataset:NNNN]``. It lets this
  module find the record again (so tracking twice cannot duplicate), lets
  ``TrackedDataset.whg_dataset_id`` read reconciliation status before there is
  a Register row, and is upgraded to a proper link on the next call once the
  entry appears.
"""
from api.models import GazetteerRegistryEntry

from .admin_links import user_label
from .models import TrackedDataset


def marker(dataset_id):
    return f"[whg-dataset:{dataset_id}]"


def tracked_for(dataset_id):
    """The TrackedDataset already standing for this WHG dataset, if any."""
    found = TrackedDataset.objects.filter(registry_id=f"whg:{dataset_id}").first()
    return found or TrackedDataset.objects.filter(
        notes__contains=marker(dataset_id)).first()


def _registry_entry(dataset_id):
    return GazetteerRegistryEntry.objects.filter(pk=f"whg:{dataset_id}").first()


def fill_from_dataset(tracked, dataset, added_by=None):
    """Populate blank fields of ``tracked`` from the submitted ``dataset``.

    Fills, never overwrites: the form may already carry something a person
    typed. Does not save. Only facts GRACE cannot read through are copied —
    the title and a count are stand-ins for a prospect, and the Register wins
    wherever it has an answer.
    """
    if not tracked.title:
        tracked.title = dataset.title
    if tracked.registry_id is None:
        tracked.registry = _registry_entry(dataset.pk)
    if tracked.expected_record_count is None and dataset.numrows:
        tracked.expected_record_count = dataset.numrows
    if tracked.on_radar_since is None and dataset.create_date:
        tracked.on_radar_since = dataset.create_date.date()
    if tracked.added_by_id is None and added_by is not None:
        tracked.added_by = added_by
    if marker(dataset.pk) not in (tracked.notes or ""):
        contributor = user_label(dataset.owner) if dataset.owner_id else "unknown"
        head = (f"{marker(dataset.pk)} Submitted to WHG as ‘{dataset.label}’ "
                f"by {contributor}.")
        tracked.notes = f"{head}\n{tracked.notes}".strip()
    return tracked


def track_dataset(dataset, added_by=None):
    """Create — or find and tidy — the TrackedDataset for ``dataset``.

    Returns ``(tracked, created)``. Idempotent: an existing record is returned
    as it is, except that a missing Register link is filled in if the entry
    has since appeared.
    """
    existing = tracked_for(dataset.pk)
    if existing is not None:
        if existing.registry_id is None:
            entry = _registry_entry(dataset.pk)
            if entry is not None:
                existing.registry = entry
                existing.save(update_fields=["registry", "updated_at"])
        return existing, False
    tracked = fill_from_dataset(TrackedDataset(), dataset, added_by=added_by)
    tracked.save()
    return tracked, True
