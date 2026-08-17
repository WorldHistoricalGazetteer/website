"""
Fill in missing `places.ccodes` from each place's own geometry (PostGIS).

Contributed records often arrive with coordinates but no country attribution —
`ccodes` is set during reconciliation, so anything uploaded and published without
that step keeps an empty array. On production that is 27.9% of contributed place
records, which makes the Analytics "Contributed-data coverage" panel understate
real coverage by more than a quarter, and weakens every country filter that reads
`ccodes` (dataset browse, search facets, the reconciliation area constraint).

Roughly a quarter of the gap is recoverable: of 181,426 country-less places,
42,555 have usable geometry. The rest have no coordinates at all and are beyond
help here.

Assignment rules:

  * Point / line geometry — every country polygon it falls inside. Anything in
    open water (GEBCO soundings, shipwrecks) matches nothing and is left alone,
    which is the correct answer rather than a failure.
  * Areal geometry — countries covering at least `--min-share` of the place's
    area, so a border-straddling polygon gets both its countries but a sliver of
    overlap from an imprecise boundary does not. If nothing clears the bar, the
    single best-covering country is used.
  * `--tolerance-km` additionally accepts the nearest country within N km for
    non-areal geometry that matched nothing — for coastal and island places that
    fall just outside a coarse country outline. Off by default.

Only ever FILLS EMPTIES: a place that already has `ccodes` is never touched, so
curated attributions cannot be overwritten. Every change is appended to a JSONL
log (`--log`), and since each edited place had no codes before, the log is a
complete undo list: set `ccodes = '{}'` for its ids to revert.

Writes Postgres only. The Elasticsearch index picks the values up at the next
ingest; the Analytics panel reads Postgres directly and shows them immediately
(clear its cache key, `analytics:contributed_coverage:v1`).

    python manage.py backfill_ccodes --dry-run
    python manage.py backfill_ccodes --public-only --tolerance-km 5
    python manage.py backfill_ccodes --dataset SenegalSettlements
"""

import json
import logging
import os
from datetime import datetime, timezone

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.db.models import Exists, OuterRef, Q

from places.models import Place, PlaceGeom

logger = logging.getLogger(__name__)

# One place's geometry, collected across its place_geom rows and repaired if
# areal — a self-intersecting upload would otherwise abort the whole batch when
# PostGIS tried to intersect it.
_GEOM_CTE = """
    WITH t AS (
        SELECT g.place_id AS id, ST_Collect(g.geom) AS geom
          FROM place_geom g
         WHERE g.place_id = ANY(%(ids)s)
           AND g.geom IS NOT NULL AND NOT ST_IsEmpty(g.geom)
         GROUP BY g.place_id
    ), tv AS (
        SELECT id,
               CASE WHEN ST_Dimension(geom) = 2 THEN ST_MakeValid(geom) ELSE geom END AS geom
          FROM t
    )
"""


class Command(BaseCommand):
    help = "Fill empty places.ccodes from place geometry using the countries table."

    def add_arguments(self, parser):
        parser.add_argument(
            '--dataset', action='append', default=None, metavar='LABEL',
            help="Restrict to this dataset label. Repeatable.",
        )
        parser.add_argument(
            '--public-only', action='store_true',
            help="Restrict to public, non-core, non-authority datasets — the set "
                 "the Analytics coverage panel counts.",
        )
        parser.add_argument(
            '--min-share', type=float, default=0.05,
            help="Least fraction of an areal place's area a country must cover to "
                 "be credited (default 0.05).",
        )
        parser.add_argument(
            '--tolerance-km', type=float, default=0.0,
            help="Also accept the nearest country within this many km for "
                 "non-areal geometry that matched none (default 0 = off).",
        )
        parser.add_argument(
            '--batch-size', type=int, default=2000,
            help="Places per spatial query (default 2000).",
        )
        parser.add_argument(
            '--limit', type=int, default=None,
            help="Stop after considering this many places. For smoke-testing.",
        )
        parser.add_argument(
            '--log', default=None, metavar='PATH',
            help="JSONL change log (default: <BASE_DIR>/data_dumps/"
                 "backfill_ccodes_<timestamp>.jsonl). Doubles as the undo list.",
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help="Report what would be assigned, write nothing (no log either).",
        )

    def handle(self, *args, **opts):
        min_share = opts['min_share']
        tolerance_m = opts['tolerance_km'] * 1000.0
        batch_size = opts['batch_size']
        limit = opts['limit']
        dry_run = opts['dry_run']
        if not 0 <= min_share <= 1:
            raise CommandError("--min-share must be between 0 and 1.")

        targets = self._targets(opts['dataset'], opts['public_only'])
        total = targets.count()
        if limit:
            total = min(total, limit)
        self.stdout.write(
            f"{total:,} country-less places with geometry to consider"
            + (" — DRY RUN, nothing will be written" if dry_run else "")
        )
        if not total:
            return

        log_path = None
        if not dry_run:
            log_path = opts['log'] or os.path.join(
                settings.BASE_DIR, 'data_dumps',
                'backfill_ccodes_{}.jsonl'.format(
                    datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')),
            )
            os.makedirs(os.path.dirname(log_path), exist_ok=True)
            self.stdout.write(f"Change log: {log_path}")

        seen = assigned = multi = by_tolerance = unmatched = 0
        codes_seen = {}
        log = open(log_path, 'a', encoding='utf-8') if log_path else None
        try:
            for ids in self._batches(targets, batch_size, limit):
                seen += len(ids)
                found = self._match(ids, min_share)
                if tolerance_m:
                    missing = [i for i in ids if i not in found]
                    if missing:
                        near = self._match_nearest(missing, tolerance_m)
                        by_tolerance += len(near)
                        found.update(near)

                unmatched += len(ids) - len(found)
                multi += sum(1 for codes in found.values() if len(codes) > 1)
                for codes in found.values():
                    for code in codes:
                        codes_seen[code] = codes_seen.get(code, 0) + 1

                if found and not dry_run:
                    self._write(found, log)
                assigned += len(found)
                self._progress(seen, total)
        finally:
            if log:
                log.close()

        top = sorted(codes_seen.items(), key=lambda kv: -kv[1])[:10]
        self.stdout.write(self.style.SUCCESS(
            f"\nConsidered {seen:,} places; "
            f"{'would assign' if dry_run else 'assigned'} countries to {assigned:,} "
            f"({multi:,} in more than one country"
            + (f", {by_tolerance:,} via the {opts['tolerance_km']}km tolerance" if tolerance_m else "")
            + f"); {unmatched:,} matched no country."
        ))
        if unmatched:
            self.stdout.write(
                "  Unmatched places are usually genuinely countryless — open ocean "
                "(bathymetry, wrecks) or outside the countries table's coverage."
            )
        if top:
            self.stdout.write("  Most-assigned: " + ", ".join(
                f"{code} {n:,}" for code, n in top))

    def _targets(self, labels, public_only):
        """Places with no ccodes that have at least one geometry. EXISTS rather
        than a join, so the id stream needs no DISTINCT over place_geom."""
        # The column is NULL on older rows and `{}` on newer ones.
        qs = Place.objects.filter(
            Q(ccodes__isnull=True) | Q(ccodes=[]),
        ).filter(Exists(PlaceGeom.objects.filter(place_id=OuterRef('pk'))))
        if labels:
            qs = qs.filter(dataset__label__in=labels)
        if public_only:
            qs = qs.filter(dataset__public=True, dataset__core=False,
                           dataset__authority=False)
        return qs.order_by('id')

    def _batches(self, targets, batch_size, limit):
        """Yield id batches by keyset pagination — the target set shrinks as we
        write, so OFFSET paging would skip rows."""
        seen, last = 0, 0
        while True:
            ids = list(targets.filter(id__gt=last)
                       .values_list('id', flat=True)[:batch_size])
            if not ids:
                return
            if limit and seen + len(ids) > limit:
                ids = ids[:limit - seen]
            last = ids[-1]
            seen += len(ids)
            yield ids
            if limit and seen >= limit:
                return

    def _match(self, ids, min_share):
        """{place id: [ISO2, …]} for places whose geometry falls in a country."""
        with connection.cursor() as cur:
            cur.execute(_GEOM_CTE + """
                SELECT tv.id, ST_Dimension(tv.geom), c.iso
                  FROM tv JOIN countries c ON ST_Intersects(c.mpoly, tv.geom)
            """, {'ids': ids})
            rows = cur.fetchall()

        hits, areal = {}, set()
        for pid, dim, iso in rows:
            hits.setdefault(pid, []).append(iso)
            if dim == 2:
                areal.add(pid)

        # Only areal places matching several countries need the overlap test, and
        # ST_Intersection against a country the size of Russia is worth avoiding
        # everywhere else.
        contested = sorted(pid for pid in areal if len(hits[pid]) > 1)
        if contested:
            with connection.cursor() as cur:
                cur.execute(_GEOM_CTE + """
                    SELECT tv.id, c.iso,
                           ST_Area(ST_Intersection(c.mpoly, tv.geom))
                             / NULLIF(ST_Area(tv.geom), 0)
                      FROM tv JOIN countries c ON ST_Intersects(c.mpoly, tv.geom)
                """, {'ids': contested})
                shares = {}
                for pid, iso, share in cur.fetchall():
                    shares.setdefault(pid, []).append((iso, share or 0.0))
            for pid, pairs in shares.items():
                kept = [iso for iso, share in pairs if share >= min_share]
                if not kept:  # everything is a sliver — keep the best of them
                    kept = [max(pairs, key=lambda p: p[1])[0]]
                hits[pid] = kept

        return {pid: sorted(set(codes)) for pid, codes in hits.items()}

    def _match_nearest(self, ids, tolerance_m):
        """{place id: [ISO2]} for non-areal geometry just outside a country —
        coastal and island places lost to a coarse country outline."""
        with connection.cursor() as cur:
            cur.execute(_GEOM_CTE + """
                SELECT tv.id, n.iso
                  FROM tv
                  CROSS JOIN LATERAL (
                      SELECT c.iso,
                             ST_Distance(c.mpoly::geography, tv.geom::geography) AS dist
                        FROM countries c
                       ORDER BY c.mpoly <-> tv.geom
                       LIMIT 1
                  ) n
                 WHERE ST_Dimension(tv.geom) < 2 AND n.dist <= %(tol)s
            """, {'ids': ids, 'tol': tolerance_m})
            return {pid: [iso] for pid, iso in cur.fetchall()}

    def _write(self, found, log):
        with transaction.atomic():
            places = list(Place.objects.filter(id__in=list(found)).only('id', 'ccodes'))
            for place in places:
                place.ccodes = found[place.id]
            Place.objects.bulk_update(places, ['ccodes'], batch_size=len(places))
        if log:
            for pid, codes in found.items():
                log.write(json.dumps({'id': pid, 'ccodes': codes}) + '\n')
            log.flush()

    def _progress(self, seen, total):
        pct = (seen / total * 100) if total else 0
        self.stdout.write(f"  {seen:,} / {total:,} ({pct:.1f}%)", ending='\r')
        self.stdout.flush()
