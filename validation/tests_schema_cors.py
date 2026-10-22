"""/schema/<name> leaves its CORS header to nginx (place#325).

nginx adds ``Access-Control-Allow-Origin: *`` to every GET on both hosts, so a second
copy from Django arrives as "*, *", which browsers reject (measured on prod,
2026-10-21). Run locally only:

    DB_NAME=whg_t_schema GDAL_LIBRARY_PATH=/usr/lib/x86_64-linux-gnu/libgdal.so.34 \\
        venv/bin/python manage.py test validation.tests_schema_cors --settings=whg.settings_localtest --noinput
"""
from django.test import SimpleTestCase

from validation.views import SCHEMA_FILES


class SchemaCorsTests(SimpleTestCase):
    def test_every_schema_document_is_served_without_a_django_cors_header(self):
        for name in SCHEMA_FILES:
            resp = self.client.get(f"/schema/{name}")
            # Presence in the same call: a 404 would also "lack" the header.
            self.assertEqual(resp.status_code, 200, name)
            self.assertIn("max-age", resp["Cache-Control"], name)
            self.assertNotIn("Access-Control-Allow-Origin", resp, name)

    def test_an_unknown_schema_is_still_a_404(self):
        self.assertEqual(self.client.get("/schema/nope.jsonld").status_code, 404)
