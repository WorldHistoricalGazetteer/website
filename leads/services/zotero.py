# leads/services/zotero.py
"""
Zotero group-library read sync.

Django reads the shared group library and upserts leads: the bibliography layer
feeds the triage tracker. Bibliographic fields are refreshed without clobbering
workflow fields (status/assignee/priority) — "augment, don't overwrite".

NOTE: full implementation lands in build-order step 4. Stubs raise until then.
"""


def upsert_from_zotero(since_version=None):
    """Pull the Zotero group library (incremental via ?since=) and upsert leads."""
    raise NotImplementedError(
        "upsert_from_zotero() is not implemented yet (build-order step 4)."
    )


def refresh_lead_from_zotero(lead):
    """Refresh a single lead's bibliographic fields from its linked Zotero item."""
    raise NotImplementedError(
        "refresh_lead_from_zotero() is not implemented yet (build-order step 4)."
    )
