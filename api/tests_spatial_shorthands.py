"""`bbox` shorthand and the spatial `properties` keys (place#217 items 3 and 4).

Two gaps this closes, both from the beta tester's failed integration:

* a bounding box is the commonest spatial constraint a caller has, and they had
  to hand-write a five-point GeoJSON ring for it;
* an **OpenRefine-style caller could not reach `contained_in` or any spatial
  filter at all** — so the constraint that most reliably disambiguates a place
  name was unavailable to exactly the audience most likely to need it.

🛑 The issue makes one guarantee a hard constraint: *"An OpenRefine client sees
no behaviour change."* The last class here asserts that directly, because it is
the only requirement that cannot be verified by looking at the new feature.

⚠️ Every rejection has a companion asserting the valid form still works. A
validator that rejected everything would otherwise pass a suite testing only the
rejections.
"""

from django.test import SimpleTestCase

from api.reconcile import (
    PROPERTY_FILTER_MAP,
    PROPERTY_FILTER_RADIUS_PID,
    bbox_to_polygon,
    normalise_query_params,
)


class BboxToPolygonTests(SimpleTestCase):

    def test_a_valid_bbox_becomes_a_closed_ring(self):
        poly = bbox_to_polygon([7, 44, 14, 47])
        self.assertEqual(poly["type"], "Polygon")
        ring = poly["coordinates"][0]
        self.assertEqual(len(ring), 5, "a GeoJSON ring must repeat its first point")
        self.assertEqual(ring[0], ring[-1], "ring must be closed")
        self.assertEqual(ring[0], [7.0, 44.0])
        self.assertEqual(ring[2], [14.0, 47.0], "opposite corner")

    def test_accepts_a_comma_separated_string(self):
        """So it works as a query-string parameter, not only in JSON."""
        self.assertEqual(bbox_to_polygon("7,44,14,47"), bbox_to_polygon([7, 44, 14, 47]))

    def test_rejects_the_wrong_number_of_values(self):
        for bad in ([7, 44, 14], [7, 44, 14, 47, 50], [], "7,44"):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    bbox_to_polygon(bad)

    def test_rejects_non_numeric(self):
        with self.assertRaises(ValueError):
            bbox_to_polygon(["west", 44, 14, 47])

    def test_rejects_out_of_range(self):
        for bad in ([-181, 44, 14, 47], [7, -91, 14, 47], [7, 44, 181, 47], [7, 44, 14, 91]):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    bbox_to_polygon(bad)

    def test_rejects_transposed_latitudes(self):
        """south > north — almost always the order misremembered."""
        with self.assertRaises(ValueError) as ctx:
            bbox_to_polygon([7, 47, 14, 44])
        self.assertIn("west, south, east, north", str(ctx.exception),
                      "the error should state the order rather than just refusing")

    def test_rejects_antimeridian_crossing_with_an_actionable_message(self):
        """🛑 The case that must not be silently accommodated.

        `west > east` means "across the Pacific" to a human and "an inside-out
        box" to every geometry library — and an inside-out ring is not an error
        downstream, it is a valid shape covering nearly the whole globe. The
        query would return everything and look like it worked."""
        with self.assertRaises(ValueError) as ctx:
            bbox_to_polygon([170, -10, -170, 10])
        msg = str(ctx.exception)
        self.assertIn("antimeridian", msg)
        self.assertIn("MultiPolygon", msg, "must name the workaround, not just refuse")

    def test_rejects_a_degenerate_box(self):
        """Zero area matches nothing — an empty result that reads as 'no such
        place' rather than 'your box has no area'."""
        for bad in ([7, 44, 7, 47], [7, 44, 14, 44]):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    bbox_to_polygon(bad)


class NormaliseBboxTests(SimpleTestCase):

    def test_bbox_populates_bounds(self):
        out = normalise_query_params({"query": "Venice", "bbox": [7, 44, 14, 47]})
        self.assertEqual(out["bounds"], bbox_to_polygon([7, 44, 14, 47]))

    def test_bounds_still_works_untouched(self):
        """The companion: the existing parameter must be unaffected."""
        poly = {"type": "Polygon", "coordinates": [[[7, 44], [14, 44], [14, 47], [7, 47], [7, 44]]]}
        out = normalise_query_params({"query": "Venice", "bounds": poly})
        self.assertEqual(out["bounds"]["type"], "Polygon")
        self.assertEqual(out["bounds"]["coordinates"], poly["coordinates"])

    def test_bbox_and_bounds_together_is_an_error(self):
        """A caller who sent both has a bug; picking a winner hides it."""
        with self.assertRaises(ValueError) as ctx:
            normalise_query_params({
                "query": "Venice", "bbox": [7, 44, 14, 47],
                "bounds": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]},
            })
        self.assertIn("not both", str(ctx.exception))

    def test_a_bbox_alone_satisfies_the_empty_query_rule(self):
        """`bounds` already counts as a spatial constraint for a text-less query;
        the shorthand must too, or it is not equivalent."""
        out = normalise_query_params({"bbox": [7, 44, 14, 47]})
        self.assertIsNone(out["query_text"])
        self.assertIsNotNone(out["bounds"])

    def test_a_circle_still_wins_over_bbox(self):
        """lat/lng/radius is evaluated first and must keep precedence, so adding
        the shorthand cannot change an existing caller's result."""
        out = normalise_query_params({
            "query": "Pittsburgh", "lat": 45.44, "lng": 12.33, "radius": 200,
            "bbox": [7, 44, 14, 47],
        })
        self.assertTrue(out["has_nearby"] if "has_nearby" in out else True)
        # The circle polygon has many vertices; the bbox ring has exactly 5.
        self.assertNotEqual(len(out["bounds"]["coordinates"][0]), 5,
                            "the circle must have produced the bounds, not the bbox")


class OpenRefinePropertyTests(SimpleTestCase):

    def test_contained_in_is_reachable_through_properties(self):
        out = normalise_query_params({
            "query": "Bego",
            "properties": [{"pid": "whg:contained_in", "v": "un:syr"}],
        })
        self.assertEqual(out["raw"]["contained_in"], "un:syr")

    def test_bbox_is_reachable_through_properties(self):
        out = normalise_query_params({
            "query": "Venice",
            "properties": [{"pid": "whg:within_bbox", "v": "7,44,14,47"}],
        })
        self.assertEqual(out["bounds"], bbox_to_polygon([7, 44, 14, 47]))

    def test_radius_sets_all_three_params_from_one_property(self):
        out = normalise_query_params({
            "query": "Pittsburgh",
            "properties": [{"pid": PROPERTY_FILTER_RADIUS_PID, "v": "45.44,12.33,200"}],
        })
        self.assertIsNotNone(out["bounds"], "a circle should have been built")
        self.assertGreater(len(out["bounds"]["coordinates"][0]), 5, "a circle, not a box")

    def test_a_malformed_radius_property_errors_rather_than_falling_through(self):
        """🛑 A partial circle must not silently become an unscoped query — that
        is the place#262 failure class: a constraint the caller believes was
        applied, silently wasn't."""
        with self.assertRaises(ValueError) as ctx:
            normalise_query_params({
                "query": "Pittsburgh",
                "properties": [{"pid": PROPERTY_FILTER_RADIUS_PID, "v": "45.44,12.33"}],
            })
        self.assertIn("lat,lng,radius", str(ctx.exception))

    def test_top_level_params_win_over_properties(self):
        """Pre-existing contract, asserted so the new keys do not change it."""
        out = normalise_query_params({
            "query": "Rome", "countries": ["FR"],
            "properties": [{"pid": "whg:countries_codes", "v": "IT"}],
        })
        self.assertEqual(out["raw"]["countries"], ["FR"])

    def test_input_pids_do_not_collide_with_extend_output_pids(self):
        """🛑 The issue's specific warning. `whg:geometry_bbox` is an EXTEND
        property meaning "return the matched place's bbox" — the opposite of
        "restrict to this bbox". Reusing it would give one pid two contradictory
        meanings."""
        from api.reconcile_helpers import PROPERTY_FIELD_MAP
        self.assertNotIn("whg:geometry_bbox", PROPERTY_FILTER_MAP)
        self.assertNotIn(PROPERTY_FILTER_RADIUS_PID, PROPERTY_FIELD_MAP)
        for pid in ("whg:contained_in", "whg:within_bbox"):
            self.assertIn(pid, PROPERTY_FILTER_MAP)
            self.assertNotIn(pid, PROPERTY_FIELD_MAP,
                             f"{pid} must not also be an extend property")


class OpenRefineNoBehaviourChangeTests(SimpleTestCase):
    """🛑 The issue's hard constraint: 'An OpenRefine client sees no behaviour change.'

    This is the only acceptance criterion that cannot be checked by looking at
    the new feature, so it gets its own class.
    """

    def test_a_v02_query_is_unaffected(self):
        """A conformant Reconciliation Service API v0.2 query — no WHG
        extensions at all — must normalise exactly as before."""
        out = normalise_query_params({"query": "rome", "limit": 3})
        self.assertEqual(out["query_text"], "rome")
        self.assertEqual(out["size"], 3)
        self.assertIsNone(out["bounds"])

    def test_an_unknown_property_is_still_ignored(self):
        """A client sending properties we do not recognise must not error — that
        is how a conformant client stays conformant while we add things."""
        out = normalise_query_params({
            "query": "rome",
            "properties": [{"pid": "some:other_service_property", "v": "whatever"}],
        })
        self.assertEqual(out["query_text"], "rome")
        self.assertIsNone(out["bounds"])

    def test_the_legacy_property_pids_are_untouched(self):
        for pid, key in (("whg:namespaces", "namespaces"),
                         ("whg:countries_codes", "countries"),
                         ("whg:classes_codes", "fclasses"),
                         ("whg:types_objects", "types")):
            with self.subTest(pid=pid):
                self.assertEqual(PROPERTY_FILTER_MAP[pid], key)
