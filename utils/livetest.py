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
"""
import gc
import threading
import time
from unittest.mock import patch

from django.db import connections

REQUEST_THREAD_MARKER = "process_request_thread"


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
