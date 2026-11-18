"""``utils.livetest.start_sync_playwright``: a browser test keeps the test runner's database.

Opt-in with the browser tests (``WHG_BROWSER_TESTS=1``), because it starts Playwright's driver
(no browser is launched). Each test checks the thing that went wrong in the feed browser tests:
after Playwright started, ``serialized_rollback`` restored nothing, so the rows the migrations seed
(the gazetteer registry, e.g. ``gn``) were gone from every test after a class's first one. The two
tests below run in one class, so whichever runs second runs after a flush; both must see the
seeded rows. ``test_the_premise_still_holds`` shows the helper is needed at all: with a plain
``sync_playwright().start()`` the ORM in this thread is on another connection wrapper, one with no
serialized contents. If Playwright ever stops doing that, it fails and says so, and the helper can go.
That class (no ``serialized_rollback``) also runs before the serialized one here, which is the order
that made the restore collide on ``django_content_type`` without ``SerializedRollbackFromEmpty``.
"""
import os
import unittest

from django.db import connections
from django.test import TransactionTestCase

from utils.livetest import SerializedRollbackFromEmpty, start_sync_playwright

RUN = os.environ.get("WHG_BROWSER_TESTS") == "1"


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 (starts Playwright's driver)")
class StartSyncPlaywrightTests(SerializedRollbackFromEmpty, TransactionTestCase):
    serialized_rollback = True

    @classmethod
    def setUpClass(cls):
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
        super().setUpClass()
        cls._pw = start_sync_playwright()

    @classmethod
    def tearDownClass(cls):
        try:
            cls._pw.stop()
        finally:
            super().tearDownClass()

    def check_seeded_rows_present(self):
        from api.models import GazetteerRegistryEntry
        self.assertTrue(getattr(connections["default"], "_test_serialized_contents", None),
                        "this thread's connection has no serialized contents: serialized_rollback is inert")
        self.assertTrue(GazetteerRegistryEntry.objects.filter(id="gn").exists(),
                        "the registry rows the migrations seed are gone after a flush")

    def test_first(self):
        self.check_seeded_rows_present()

    def test_second(self):
        self.check_seeded_rows_present()


@unittest.skipUnless(RUN, "set WHG_BROWSER_TESTS=1 (starts Playwright's driver)")
class PlaywrightPremiseTests(TransactionTestCase):
    def test_the_premise_still_holds(self):
        import contextvars
        from playwright.sync_api import sync_playwright
        os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"

        def probe():
            before = connections["default"]
            pw = sync_playwright().start()
            try:
                return connections["default"] is not before, before
            finally:
                pw.stop()

        # In an EMPTY context: an earlier start_sync_playwright in this run has already put the
        # runner's wrapper into the context-var storage, which would hide the swap here.
        swapped, before = contextvars.Context().run(probe)
        self.assertTrue(swapped, "Playwright no longer swaps Django's connection: start_sync_playwright is unneeded")
        self.assertIs(connections["default"], before)
