"""Read-only Elasticsearch guard.

The dev (and local) environments have no Elasticsearch index of their own, so
they are configured to read the **production** indexes (`whg`/`pub`). To make
sure that publishing, re-indexing, record corrections, or any other write path
on dev can never mutate the live production index, we wrap ``settings.ES_CONN``
in a proxy that turns every write operation into a logged no-op.

Reads (search, count, get, mget, scroll, …) pass straight through, so search
and all read-only features work normally against production data.

Applied from ``whg/settings.py`` whenever a non-production context is pointed at
a production index name (see ``ES_READ_ONLY`` there).
"""
import logging

logger = logging.getLogger('elastic')

# Top-level client write operations to neutralise.
_WRITE_METHODS = frozenset({
    'index', 'create', 'update', 'delete', 'bulk',
    'update_by_query', 'delete_by_query', 'reindex',
})

# Index-admin write operations (es.indices.*) to neutralise.
_INDICES_WRITE_METHODS = frozenset({
    'create', 'delete', 'put_mapping', 'put_settings', 'put_alias',
    'delete_alias', 'update_aliases', 'put_template', 'delete_template',
    'put_index_template', 'delete_index_template', 'clone', 'rollover',
    'shrink', 'split', 'forcemerge',
})


def _noop(label):
    def _call(*args, **kwargs):
        logger.warning(
            "ES write blocked (read-only: dev/local is pointed at the "
            "production index): %s", label
        )
        # A benign response that satisfies the common callers: `result` for
        # index/update/delete, `items`/`errors` for bulk, `acknowledged` for
        # index-admin ops.
        return {
            'result': 'noop', 'acknowledged': True, 'errors': False,
            'items': [], '_shards': {'total': 0, 'successful': 0, 'failed': 0},
        }
    return _call


class _ReadOnlyIndices:
    def __init__(self, real):
        self._real = real

    def __getattr__(self, name):
        if name in _INDICES_WRITE_METHODS:
            return _noop('indices.' + name)
        return getattr(self._real, name)


class ReadOnlyElasticsearch:
    """Proxy that passes reads through to a real client and no-ops writes."""

    def __init__(self, real):
        object.__setattr__(self, '_real', real)

    def __getattr__(self, name):
        if name in _WRITE_METHODS:
            return _noop(name)
        attr = getattr(self._real, name)
        if name == 'indices':
            return _ReadOnlyIndices(attr)
        return attr

    def options(self, *args, **kwargs):
        # es.options(...) returns a new client; keep the wrapper on it so writes
        # made through an options() chain are still neutralised.
        return ReadOnlyElasticsearch(self._real.options(*args, **kwargs))


def make_read_only(es_conn):
    """Return a read-only proxy around an Elasticsearch client."""
    if isinstance(es_conn, ReadOnlyElasticsearch):
        return es_conn
    return ReadOnlyElasticsearch(es_conn)
