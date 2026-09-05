import json
import os
import shutil
import subprocess
import tempfile

from django.conf import settings
from django.test import SimpleTestCase

from validation.views import INDEXING_FIELDS_READ, extract_dataset_metadata


class MydContractTests(SimpleTestCase):
    """Contracts between Map your Data (browser) and WHG's ingest (server).

    Both halves of this have already failed in production, and neither failure was visible by
    reading either half on its own:

    * ``description`` was read here and written by nobody, so every contributed dataset arrived with
      a placeholder where its description should be — on a public page, with nothing anywhere saying
      so (place#227).
    * Column-role guessing failed in both directions: an unanchored hint claimed ``coordinate_method``
      and thereby silenced every real coordinate in a dataset, while ``ccodes`` — the spelling Linked
      Places itself uses — was claimed by nothing (place#225). Three instances were fixed one at a
      time with nothing in place to catch the fourth.

    The JS side is checked by ``scripts/check-myd-contracts.js``, which evaluates the real functions
    out of the real source rather than reimplementing them.
    """

    JS_CHECKER = os.path.join(settings.BASE_DIR, 'scripts', 'check-myd-contracts.js')

    def test_reader_takes_every_field_it_declares(self):
        """The reader half: every key in INDEXING_FIELDS_READ is actually read back.

        Behavioural rather than a source scrape — a full `indexing` block goes in, and every declared
        field must come out populated. If someone adds a key to the constant without teaching
        `extract_dataset_metadata` to read it, this fails.
        """
        doc = {
            'indexing': {
                '@context': 'https://schema.org/',
                '@type': 'Dataset',
                'creator': {'@type': 'Person', 'name': 'A Contributor'},
                'name': 'A dataset title',
                'description': 'What this dataset covers.',
                'url': 'https://doi.org/10.5281/zenodo.1',
                'citation': 'A Contributor (2026). A dataset title [Data set].',
            },
            'type': 'FeatureCollection',
            'features': [],
        }
        with tempfile.NamedTemporaryFile('w', suffix='.json', delete=False) as fh:
            json.dump(doc, fh)
            path = fh.name
        try:
            creator, title, description, webpage, citation = extract_dataset_metadata(path)
        finally:
            os.unlink(path)

        got = {
            'creator': creator, 'name': title, 'description': description,
            'url': webpage, 'citation': citation,
        }
        self.assertEqual(set(got), set(INDEXING_FIELDS_READ),
                         'INDEXING_FIELDS_READ and this test have drifted apart')
        for field in INDEXING_FIELDS_READ:
            with self.subTest(field=field):
                self.assertTrue(
                    got[field],
                    f"the ingest declares it reads `{field}` from `indexing`, but "
                    f"extract_dataset_metadata returned nothing for it",
                )

    def test_myd_writes_every_field_the_ingest_reads(self):
        """The writer half, plus column-role guessing — run in Node against the shipped source."""
        node = shutil.which('node')
        if not node:
            self.skipTest('node is not available; run `npm run test:myd` where it is')
        result = subprocess.run(
            [node, self.JS_CHECKER, ','.join(INDEXING_FIELDS_READ)],
            capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(
            result.returncode, 0,
            'Map your Data contract checks failed:\n' + (result.stdout or '') + (result.stderr or ''),
        )


class SymphonymTokeniserTests(SimpleTestCase):
    """The browser's Symphonym query vector must be the one the CRC gateway would have computed.

    whg3 embeds toponyms in the browser and posts the int8 vector to /api/reconcile as
    ``query_vector`` (``api/crc_client.py``), where it is compared against 72.7M vectors written by
    the canonical tokeniser in the indexing repo. Until 5 September 2026 four implementations of
    that tokeniser disagreed: on 46,483,973 documents (63.9% of the index) a query vector did not
    match that document's own stored vector, and for CJK/Kana/Hangul the two were *anti*-correlated
    (cos -0.30), leaving 3.9M documents at 0.3% rank-1 self-retrieval. The server side is fixed;
    whg3 was the fourth implementation, and a client that drifts again reintroduces the identical
    bug where it reads as a server regression.

    ``scripts/check-symphonym-tokeniser.mjs`` runs the shipped browser modules — not a copy —
    against fixtures generated from the canonical Python, and against the real ONNX encoder that
    ships to the browser. See ``scripts/fixtures/README-symphonym.md``.
    """

    JS_CHECKER = os.path.join(settings.BASE_DIR, 'scripts', 'check-symphonym-tokeniser.mjs')

    def test_browser_tokeniser_matches_the_gateway(self):
        node = shutil.which('node')
        if not node:
            self.skipTest('node is not available; run `npm run test:symphonym` where it is')
        result = subprocess.run(
            [node, self.JS_CHECKER], capture_output=True, text=True, timeout=300,
        )
        self.assertEqual(
            result.returncode, 0,
            'Symphonym tokeniser has drifted from the gateway:\n'
            + (result.stdout or '') + (result.stderr or ''),
        )
