"""places.gloss (place#299, limited; place#325 Stage 3): the data-derived gloss.

    GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test places.tests_gloss --settings=whg.settings_localtest
"""
from django.test import SimpleTestCase

from places.gloss import build_gloss, member_from_hit
from places.record_group import gateway_types, years_from_gateway


def m(key, title, source, names=(), types=(), ccodes=(), years=None, part_of=(), is_record=False, detail="full"):
    return {"key": key, "title": title, "source": source, "names": [title, *names], "types": list(types),
            "ccodes": list(ccodes), "years": years, "part_of": list(part_of), "is_record": is_record,
            "detail": detail, "repr_point": None}


class GlossTests(SimpleTestCase):
    def test_a_group_that_agrees(self):
        g = build_gloss([
            m("whg:1", "Lamberhurst", "Kain & Oliver", names=["Lamberherst"], types=["parish"], ccodes=["GB"],
              years=[1659, 1851], part_of=["Kent"], is_record=True),
            m("gn:2", "Lamberhurst", "GeoNames", types=["populated place"], ccodes=["GB"]),
            m("wd:3", "Lamberhurst", "Wikidata", types=["parish"], ccodes=["GB"], years=[1700, 1900]),
        ])
        text = " ".join(g["sentences"])
        self.assertIn("This record and 2 attested equivalents (exactMatch or closeMatch), 3 records from 3 sources", text)
        self.assertIn("Named “Lamberhurst” in every record; also “Lamberherst”.", text)
        self.assertIn("Typed as parish (2) and populated place (1); the records do not all use the same type.", text)
        self.assertIn("Country code GB in every record.", text)
        self.assertIn("Part of Kent (according to 1 record).", text)
        self.assertIn("Dated 1659 to 1900 across 2 of 3 records.", text)
        self.assertNotIn("disagree on country", text)
        self.assertNotIn("do not overlap", text)
        self.assertEqual(g["facts"]["types"], {"parish": 2, "populated place": 1})
        self.assertIn("1 of 3 records is undated.", g["unknown"])
        self.assertFalse(any("contains it" in u for u in g["unknown"]))     # containment WAS stated

    def test_disagreements_are_counted_not_resolved(self):
        g = build_gloss([
            m("whg:1", "Hungerford", "A", ccodes=["GB"], years=[1000, 1100], is_record=True),
            m("x:2", "Hungerford", "B", ccodes=["US"], years=[1800, 1900]),
            m("x:3", "Hungerford", "C", ccodes=["GB"]),
        ])
        text = " ".join(g["sentences"])
        self.assertIn("The records disagree on country: GB (2) and US (1).", text)
        self.assertIn("some records' dates do not overlap at all", text)
        self.assertNotIn("Country code GB in", text)
        self.assertTrue(g["facts"]["dates_disjoint"])

    def test_unknowns_are_said_and_are_not_none(self):
        g = build_gloss([
            m("whg:1", "Somewhere", "A", is_record=True),
            m("gn:9", "Somewhere", "GeoNames", detail="unavailable"),
        ])
        unknown = " ".join(g["unknown"])
        self.assertIn("The record carries no place type.", g["unknown"])
        self.assertIn("No record names a place that contains it. That is not evidence that nothing does", unknown)
        self.assertIn("containment in WHG is read only where a source states it, and is incomplete", unknown)
        self.assertIn("could not be read just now", unknown)
        self.assertIn("not a finding that they have no types", unknown)
        # The unread member is NOT counted as untyped/undated: only the record is.
        self.assertEqual(g["facts"]["untyped"], 1)
        self.assertNotIn("2 of 2", unknown)
        self.assertTrue(any("place#267" in x for x in g["limits"]))

    def test_record_alone(self):
        g = build_gloss([m("whg:1", "Lonely", "Dataset a", is_record=True)])
        self.assertIn("This record alone, from Dataset a", g["sentences"][0])
        self.assertNotIn("attested equivalent", g["sentences"][0])

    def test_locus_wording_and_names_counted_once_per_record(self):
        g = build_gloss([m("a", "Berlin", "gn", names=["Berlin", "BERLIN", "Berlino"]),
                         m("b", "Berlin", "wd"), m("c", "Berlín", "tgn")], kind="locus")
        self.assertTrue(g["sentences"][0].startswith("3 records currently in this cluster, from 3 sources"))
        # all-of-n unknowns read as "No record ...", never "Every record carry no ..."
        self.assertIn("No record carries a place type.", g["unknown"])
        self.assertIn("No record is dated.", g["unknown"])
        self.assertIn("No record has a country code.", g["unknown"])
        self.assertFalse(any(u.startswith("Every record") for u in g["unknown"]))
        names = next(s for s in g["sentences"] if s.startswith("Named"))
        self.assertEqual(names, "Named “Berlin” in 2 of 3 records; also “Berlino” and “Berlín”.")
        self.assertEqual(g["facts"]["names"]["in_records"], 2)

    def test_empty(self):
        g = build_gloss([])
        self.assertEqual((g["n_records"], g["sentences"]), (0, []))
        self.assertEqual(g["unknown"], ["There are no records to describe."])


class GatewayFieldTests(SimpleTestCase):
    def test_types_only_where_they_are_types(self):
        self.assertEqual(gateway_types({"types": [{"label": "P"}, {"label": "a"}, {"label": "zz"}]}, "gn"),
                         ["populated place", "administrative area"])
        self.assertEqual(gateway_types({"types": [{"label": "tgn"}]}, "tgn"), [])
        self.assertEqual(gateway_types({"types": [{"label": "trismegistos"}, {"label": "city"}]}, "tm"), ["city"])

    def test_years(self):
        self.assertEqual(years_from_gateway({"temporal_range": [1200, 2000]}), [1200, 2000])
        self.assertEqual(years_from_gateway({"timespans": [{"start": 1744, "end": 1747}, {"start": {"in": "1500"}}]}),
                         [1500, 1747])
        self.assertIsNone(years_from_gateway({"temporal_range": [None, None]}))

    def test_member_from_hit_keeps_plain_capped_values_only(self):
        mem = member_from_hit({"place_id": "gn:1", "title": "  Berlin  ", "namespace": "gn",
                               "repr_point": [13.4, 52.5], "ccodes": ["DE", 5, {"x": 1}],
                               "names": [{"label": "Berlin"}, {"label": "Berlino"}], "types": [{"label": "P"}],
                               "temporal_range": [1237, 2000], "evil": {"__proto__": 1}})
        self.assertEqual(mem["title"], "Berlin")
        self.assertEqual(mem["names"], ["Berlin", "Berlino"])
        self.assertEqual(mem["types"], ["populated place"])
        self.assertEqual(mem["ccodes"], ["DE"])
        self.assertEqual(mem["years"], [1237, 2000])
        self.assertEqual(mem["repr_point"], [13.4, 52.5])
        self.assertNotIn("evil", mem)
        self.assertIsNone(member_from_hit({"title": "no id"}))
        self.assertIsNone(member_from_hit({"place_id": "x:1", "repr_point": [500, 0]})["repr_point"])
