"""
Three-way merge for the optimistic-lock sync protocol (Phase 1).

The Workbench ``project`` snapshot is a single JSON object. When a client pushes an edit built on a
stale ``base_version`` we merge its snapshot (``mine``) against the current server snapshot
(``theirs``) using the snapshot at ``base_version`` (``base``) as the common ancestor.

Two granularities:
  * KEYED_MAPS — dict overlays keyed by a stable string (e.g. ``decisions['<col>:<row>']``). Merged
    per key: a key changed on only one side takes that side; changed to *different* values on both
    sides is a conflict (server value kept, key reported).
  * STRUCT_FIELDS — structural/scalar fields compared whole. Diverged-on-both is a single conflict
    for that field. Row/cell edits stay coarse here on purpose; Yjs (Phase 2) makes them granular.

Non-conflicting merges are applied automatically. Conflicts are returned for a manual keep-mine /
keep-theirs decision on the client; the merged result always keeps the server ("theirs") value so a
conflicting push is never silently lost.
"""

KEYED_MAPS = ['matches', 'decisions', 'geom', 'rowTypes']
STRUCT_FIELDS = ['columns', 'rows', 'scope', 'submissionTypes', 'coordFormat', 'title']


def merge_snapshots(base, mine, theirs):
    """Return ``(merged, conflicts)``.

    ``conflicts`` is a list of ``{kind, key[, mine, theirs]}``. For KEYED_MAPS the small per-key
    values are included; for STRUCT_FIELDS (which can be huge, e.g. ``rows``) values are omitted —
    the client already holds ``mine`` and receives the server snapshot alongside the conflict list.
    """
    base = base or {}
    mine = mine or {}
    theirs = theirs or {}
    merged = dict(theirs)
    conflicts = []

    for field in KEYED_MAPS:
        b = base.get(field) or {}
        m = mine.get(field) or {}
        t = theirs.get(field) or {}
        # If any side isn't a dict, fall back to whole-field comparison.
        if not (isinstance(b, dict) and isinstance(m, dict) and isinstance(t, dict)):
            _merge_field(field, base, mine, theirs, merged, conflicts)
            continue
        out = dict(t)
        for key in set(b) | set(m) | set(t):
            bv, mv, tv = b.get(key), m.get(key), t.get(key)
            mine_changed = mv != bv
            theirs_changed = tv != bv
            if mine_changed and not theirs_changed:
                if key in m:
                    out[key] = mv
                else:
                    out.pop(key, None)  # deleted on my side, untouched on theirs
            elif mine_changed and theirs_changed and mv != tv:
                conflicts.append({'kind': field, 'key': key, 'mine': mv, 'theirs': tv})
            # else: no change on my side, or identical change → keep theirs (already in out)
        merged[field] = out

    for field in STRUCT_FIELDS:
        _merge_field(field, base, mine, theirs, merged, conflicts)

    return merged, conflicts


def _merge_field(field, base, mine, theirs, merged, conflicts):
    bv, mv, tv = base.get(field), mine.get(field), theirs.get(field)
    mine_changed = mv != bv
    theirs_changed = tv != bv
    if mine_changed and not theirs_changed:
        merged[field] = mv
    elif mine_changed and theirs_changed and mv != tv:
        conflicts.append({'kind': 'field', 'key': field})  # keep theirs (already in merged)
    # else: keep theirs
