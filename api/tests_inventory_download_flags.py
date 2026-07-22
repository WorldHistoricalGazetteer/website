"""Download-legality + volume flags in the gazetteer inventory push (place#136).

Exercises ``GazetteerInventoryView._upsert_one`` directly (no HTTP / token
auth needed) to cover how the push persists ``redistributable`` /
``downloadable`` / ``download_blocked_reason`` — including that ``false`` is a
real fact (not treated as absent) and that ``download_blocked_reason`` is
cleared once a source becomes downloadable again.
"""
from django.test import TestCase

from api.models import GazetteerRegistryEntry
from api.views_indexing import GazetteerInventoryView


class InventoryDownloadFlagTests(TestCase):

    def _upsert(self, entry_id="gn", **extra):
        entry = {"id": entry_id, "namespace": entry_id,
                 "name": entry_id.upper(), **extra}
        ok, err = GazetteerInventoryView()._upsert_one(entry)
        self.assertTrue(ok, err)
        return GazetteerRegistryEntry.objects.get(id=entry_id)

    def test_defaults_true_when_push_omits_flags(self):
        # A not-yet-upgraded push (no flags) must leave a downloadable row.
        row = self._upsert()
        self.assertTrue(row.redistributable)
        self.assertTrue(row.downloadable)
        self.assertIsNone(row.download_blocked_reason)

    def test_licence_restricted_stored(self):
        row = self._upsert(
            entry_id="kain_par",
            redistributable=False,
            downloadable=False,
            download_blocked_reason="licence-restricted",
        )
        self.assertFalse(row.redistributable)
        self.assertFalse(row.downloadable)
        self.assertEqual(row.download_blocked_reason, "licence-restricted")

    def test_volume_cap_keeps_redistributable_but_not_downloadable(self):
        # Legally open but too large: redistributable stays true, download is
        # blocked purely on volume grounds.
        row = self._upsert(
            entry_id="osm",
            redistributable=True,
            downloadable=False,
            download_blocked_reason="volume-exceeds-cap",
        )
        self.assertTrue(row.redistributable)
        self.assertFalse(row.downloadable)
        self.assertEqual(row.download_blocked_reason, "volume-exceeds-cap")

    def test_downloadable_false_not_treated_as_absent(self):
        # Regression guard: a bare ``false`` must overwrite a prior ``true``.
        self._upsert(entry_id="vob", downloadable=True, redistributable=True)
        row = self._upsert(entry_id="vob", downloadable=False,
                           redistributable=False,
                           download_blocked_reason="licence-restricted")
        self.assertFalse(row.downloadable)
        self.assertFalse(row.redistributable)

    def test_reason_cleared_when_becomes_downloadable(self):
        # Once downloadable flips back to true the push omits the reason key;
        # we must clear any stale reason rather than preserve it.
        self._upsert(entry_id="vob", downloadable=False, redistributable=True,
                     download_blocked_reason="volume-exceeds-cap")
        row = self._upsert(entry_id="vob", downloadable=True,
                           redistributable=True)
        self.assertTrue(row.downloadable)
        self.assertIsNone(row.download_blocked_reason)
