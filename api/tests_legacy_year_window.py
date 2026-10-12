"""place#327 — the legacy arm must apply a year window whenever `start` or `end` is given.

It used to need `temporal: true` as well, and ignored an `end` with no `start`, so a direct caller
sending only years got gateway candidates filtered by date merged with legacy ones that were not.
"""
from datetime import datetime

from django.test import SimpleTestCase

from api.reconcile_helpers import build_es_query


def _windows(params):
    """The `timespans` range clauses in the built query, bare or inside the `undated` wrapper."""
    out = []
    for clause in build_es_query(dict({"qstr": "Gayhurst"}, **params))["query"]["bool"]["must"]:
        if "range" in clause and "timespans" in clause["range"]:
            out.append(("bare", clause["range"]["timespans"]))
        should = clause.get("bool", {}).get("should") or []
        for s in should:
            if "range" in s and "timespans" in s["range"]:
                out.append(("undated", s["range"]["timespans"]))
    return out


class LegacyYearWindow(SimpleTestCase):
    def test_years_alone_apply_the_window(self):
        self.assertEqual(_windows({"start": 1000, "end": 1100}), [("bare", {"gte": "1000", "lte": "1100"})])

    def test_temporal_flag_still_accepted(self):
        self.assertEqual(_windows({"start": 1000, "end": 1100, "temporal": True}),
                         [("bare", {"gte": "1000", "lte": "1100"})])

    def test_start_only_runs_to_this_year(self):
        self.assertEqual(_windows({"start": 1000}), [("bare", {"gte": "1000", "lte": str(datetime.now().year)})])

    def test_end_only_is_open_start(self):
        self.assertEqual(_windows({"end": 1100}), [("bare", {"lte": "1100"})])

    def test_year_zero_is_a_year(self):
        self.assertEqual(_windows({"start": 0, "end": 10}), [("bare", {"gte": "0", "lte": "10"})])

    def test_undated_wraps_the_window(self):
        self.assertEqual(_windows({"start": 1000, "end": 1100, "undated": True}),
                         [("undated", {"gte": "1000", "lte": "1100"})])

    def test_no_years_no_window(self):
        self.assertTrue(_windows({"start": 1000}))                  # positive control: a window CAN appear
        for params in ({}, {"temporal": True}, {"start": None, "end": ""}, {"undated": True}):
            with self.subTest(params=params):
                self.assertEqual(_windows(params), [])
