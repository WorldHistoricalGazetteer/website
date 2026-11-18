"""Helpers for tests built on Django's LiveServerTestCase.

The symptom: ``manage.py test`` exits 1 even though every test passed, ending with

    database "test_whgv3beta" is being accessed by other users

because a live-server request thread was still alive, holding its database session, when the runner
dropped the test database.

The cause (found by tracing which thread opened each Postgres connection): an Atlas page makes the
browser fetch ``/types/vocab/``, whose view runs a real Elasticsearch aggregation
(``placetypes.es_tree.get_type_vocab``, ``request_timeout=45``). In a test nothing answers, so that
request thread sits in a connect for up to 45 s, still holding its session at teardown. The fix is
to stub it: ``stub_type_vocab(self)`` in ``setUp``.

``drain_live_server_threads()`` is the safety net for any other request that outlives the class:
call it from ``tearDownClass`` *after* ``super().tearDownClass()`` (which stops the server) so the
threads are gone, and their connections closed, before the database is destroyed.

``start_sync_playwright()`` replaces ``sync_playwright().start()`` in a test class. Once Playwright's
sync API has started, its event loop is "running" in the test thread too (the dispatcher greenlet is
parked inside ``run_until_complete``), so ``asyncio.get_running_loop()`` succeeds there. Django's
``connections`` is a thread-critical asgiref ``Local``, which then switches from thread-local to
context-var storage: from that line on, the test's ORM calls get a SECOND ``DatabaseWrapper``, one
the test runner never touched. It has no ``_test_serialized_contents``, so ``serialized_rollback``
silently restores nothing: every test after a class's first one runs on a flushed database with the
rows the migrations seed (gazetteer registry, licences) gone, and the flush re-creates the content
types under new ids, so the NEXT serialized class fails in ``setUpClass`` with a unique violation on
``django_content_type``. The helper points the test thread's connections back at the runner's own
wrappers, so a browser test sees exactly the database a plain one does.
"""
import gc
import threading
import time
from unittest.mock import patch

from django.db import connections

REQUEST_THREAD_MARKER = "process_request_thread"


class SerializedRollbackFromEmpty:
    """Mixin for a ``TransactionTestCase`` (``LiveServerTestCase``) that sets ``serialized_rollback``:
    empty the database before Django restores the serialized rows.

    Django restores them on top of whatever the previous test left. After a test class WITHOUT
    ``serialized_rollback`` (every other browser test here), the flush re-ran ``post_migrate``, which
    re-created the content types and permissions under new ids; restoring the originals then fails
    with a unique violation on ``django_content_type``, so the serialized class errors in
    ``setUpClass`` or depends on which classes happened to run before it. Flushing first (with
    ``post_migrate`` inhibited) makes the restore start from the same empty state every time."""
    serialized_rollback = True

    @classmethod
    def _fixture_setup(cls):
        from django.core.management import call_command
        for db_name in cls._databases_names(include_mirrors=False):
            call_command("flush", verbosity=0, interactive=False, database=db_name,
                         reset_sequences=False, inhibit_post_migrate=True)
        super()._fixture_setup()


def start_sync_playwright():
    """Start Playwright's sync API and keep this thread on the test runner's database connections
    (see the module docstring). Use it in ``setUpClass`` instead of ``sync_playwright().start()``."""
    from playwright.sync_api import sync_playwright
    runner_wrappers = {alias: connections[alias] for alias in connections}
    pw = sync_playwright().start()
    for alias, wrapper in runner_wrappers.items():
        if connections[alias] is not wrapper:
            connections[alias] = wrapper
    return pw


def stub_type_vocab(testcase, vocab=None):
    """Stop /types/vocab/ reaching Elasticsearch (see the module docstring). The view then serves
    ``{"byId": {...}}`` from ``vocab`` (default empty) and nothing blocks."""
    patcher = patch("placetypes.es_tree.get_type_vocab", return_value=vocab or {})
    patcher.start()
    testcase.addCleanup(patcher.stop)


def live_server_request_threads():
    return [t for t in threading.enumerate() if REQUEST_THREAD_MARKER in t.name and t.is_alive()]


def drain_live_server_threads(timeout=5.0, poll=0.05):
    """Wait (up to ``timeout`` s) for live-server request threads to finish, then close this
    thread's connections. Returns the request threads still alive when it gave up (normally none)."""
    deadline = time.monotonic() + timeout
    while live_server_request_threads() and time.monotonic() < deadline:
        time.sleep(poll)
    # A finished thread's connection lives in thread-local storage and is only closed when that
    # storage is garbage-collected; reference cycles can keep it (and its Postgres session) alive.
    gc.collect()
    connections.close_all()
    return live_server_request_threads()
