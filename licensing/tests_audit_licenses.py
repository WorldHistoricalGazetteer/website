"""``audit_licenses.collect_python``: which metadata field supplies a package's licence."""
from email.message import Message
from unittest.mock import patch

from django.test import SimpleTestCase

from licensing.management.commands import audit_licenses as al


class _Dist:
    def __init__(self, name, **fields):
        self.metadata = m = Message()
        m["Name"], m["Version"] = name, "1.0"
        for k, v in fields.items():
            if k == "Classifier":
                for c in v:
                    m["Classifier"] = c
            else:
                m[k.replace("_", "-")] = v


def rows(*dists):
    with patch.object(al.metadata, "distributions", return_value=list(dists)):
        return {r["name"]: r for r in al.collect_python()}


class CollectPythonLicenceTests(SimpleTestCase):
    def test_license_expression_fills_a_package_that_declares_nothing_else(self):
        r = rows(_Dist("exprpkg", License_Expression="BSD-3-Clause"),
                 _Dist("lgplpkg", License_Expression="LGPL-3.0-only"))
        self.assertEqual(r["exprpkg"]["license"], "BSD-3-Clause")
        self.assertTrue(r["exprpkg"]["declared"])
        self.assertEqual(r["exprpkg"]["category"], "permissive")
        self.assertEqual(r["lgplpkg"]["category"], "copyleft-weak")          # not hidden as permissive

    def test_older_fields_still_win_so_existing_rows_do_not_change(self):
        r = rows(_Dist("both", License="MIT", License_Expression="Apache-2.0"),
                 _Dist("classified", Classifier=["License :: OSI Approved :: ISC License (ISCL)"],
                       License_Expression="Apache-2.0"))
        self.assertEqual(r["both"]["license"], "MIT")
        self.assertEqual(r["classified"]["license"], "ISC License (ISCL)")

    def test_nothing_declared_is_still_reported_as_not_declared(self):
        r = rows(_Dist("bare"), _Dist("exprpkg", License_Expression="MIT"))
        self.assertEqual(r["bare"]["license"], "Not declared")               # absence, beside a presence
        self.assertFalse(r["bare"]["declared"])
        self.assertEqual(r["exprpkg"]["license"], "MIT")
