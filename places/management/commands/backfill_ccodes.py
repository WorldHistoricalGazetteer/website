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
  * Areal geometry — countries where the overlap is at least `--min-share` of
    the place's area *or* of the country's, so a border-straddling polygon gets
    both its countries, "(British Empire)" gets the United Kingdom as well as
    Canada, and a sliver of overlap from an imprecise boundary gets neither. If
    nothing clears the bar, the single best-covering country is used. A place
    already matching more than `--max-share-countries` is taken at face value as
    genuinely multinational, and skips the test.
  * `--tolerance-km` additionally accepts the nearest country within N km for
    non-areal geometry that matched nothing — for coastal and island places that
    fall just outside a coarse country outline. Off by default; **5 is the
    measured sweet spot**. In a 3,000-place sample it recovered 416 of 732
    unmatched Pleiades places (Venice, Copenhagen, Tyre, the Bosphorus — all
    plainly right) while claiming just 2 of 854 GEBCO seamounts, so open water
    stays countryless. 10km starts pulling in ocean features for little gain.

Everything is computed one `place_geom` row at a time, never over a place's rows
collected together. That is not a detail: collecting the 122 rows of "(British
Empire)" produced a single 384,503-point geometry whose repair exhausted memory
and got the PostGIS backend OOM-killed, restarting the whole cluster. Taken
individually the very largest row in the corpus is 7,739 points (mean 174) and
all but 14 of 24,590 are already valid, so per-part work is bounded, cheap, and
needs almost no repair at all.

Only ever FILLS EMPTIES: a place that already has `ccodes` is never touched, so
curated attributions cannot be overwritten. Every change is appended to a JSONL
log (`--log`), and since each edited place had no codes before, the log is a
complete undo list: set `ccodes = '{}'` for its ids to revert. Interrupted runs
resume simply by running again — filled places drop out of the target set.

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
from django.db import DatabaseError, connection, transaction
from django.db.models import Exists, OuterRef, Q

from places.models import Place, PlaceGeom

logger = logging.getLogger(__name__)

# One row per place_geom row, repaired only if that row needs it — and never
# collected into a per-place geometry, for the reasons in the module docstring.
# Anything still invalid after ST_MakeValid degrades to the centre of its
# bounding box: pure coordinate arithmetic, so it cannot fail, and a point still
# lands in the right country.
_PARTS_CTE = """
    WITH raw AS (
        SELECT g.place_id AS id,
               CASE WHEN ST_IsValid(g.geom) THEN g.geom
                    ELSE ST_MakeValid(g.geom) END AS geom
          FROM place_geom g
         WHERE g.place_id = ANY(%(ids)s)
           AND g.geom IS NOT NULL AND NOT ST_IsEmpty(g.geom)
    ), parts AS (
        SELECT id,
               CASE WHEN ST_IsValid(geom) THEN geom
                    ELSE ST_PointOnSurface(ST_Envelope(geom)) END AS geom
          FROM raw
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
            help="Least fraction of an areal place's area — or of the country's — "
                 "an overlap must reach to be credited (default 0.05).",
        )
        parser.add_argument(
            '--max-share-countries', type=int, default=12,
            help="A place matching more than this many countries is taken as "
                 "genuinely multinational and keeps them all (default 12).",
        )
        parser.add_argument(
            '--max-share-parts', type=int, default=20, metavar='N',
            help="Skip the overlap test for places built from more than N "
                 "geometry rows (default 20). Those cost the most to test and "
                 "are the least likely to be border slivers — a 20-piece polygon "
                 "set is a deliberate multi-country shape.",
        )
        parser.add_argument(
            '--tolerance-km', type=float, default=0.0,
            help="Also accept the nearest country within this many km for "
                 "non-areal geometry that matched none (default 0 = off; 5 "
                 "recovers coastal places without claiming ocean features).",
        )
        parser.add_argument(
            '--batch-size', type=int, default=2000,
            help="Places per spatial query (default 2000).",
        )
        parser.add_argument(
            '--statement-timeout', type=int, default=300, metavar='SECONDS',
            help="Abandon any single spatial query that runs longer (default 300). "
                 "The batch is then retried a place at a time.",
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
        self.min_share = opts['min_share']
        self.max_share_countries = opts['max_share_countries']
        self.max_share_parts = opts['max_share_parts']
        tolerance_m = opts['tolerance_km'] * 1000.0
        batch_size = opts['batch_size']
        limit = opts['limit']
        dry_run = opts['dry_run']
        if not 0 <= self.min_share <= 1:
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

        # Belt and braces: no single spatial query may occupy a connection
        # indefinitely. On timeout the batch is retried a place at a time, so
        # only the offender is lost.
        with connection.cursor() as cur:
            cur.execute("SET statement_timeout = %s", [opts['statement_timeout'] * 1000])

        seen = assigned = multi = by_tolerance = unmatched = 0
        codes_seen = {}
        self.broken = []
        log = open(log_path, 'a', encoding='utf-8') if log_path else None
        try:
            for ids in self._batches(targets, batch_size, limit):
                seen += len(ids)
                found = self._match(ids)
                if tolerance_m:
                    missing = [i for i in ids if i not in found]
                    if missing:
                        try:
                            near = self._match_nearest(missing, tolerance_m)
                        except DatabaseError as e:
                            logger.warning("Tolerance pass failed (%s); skipped", e)
                            near = {}
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
        if self.broken:
            self.stdout.write(self.style.WARNING(
                f"  {len(self.broken):,} place(s) have geometry PostGIS could not "
                f"compare even after repair, and were skipped: "
                f"{', '.join(str(i) for i in self.broken[:20])}"
                + (" …" if len(self.broken) > 20 else "")
            ))

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

    def _match(self, ids):
        """{place id: [ISO2, …]} for places whose geometry falls in a country."""
        try:
            rows = self._intersect(ids)
        except DatabaseError as e:
            # Retry the batch one place at a time so an offender is skipped alone
            # instead of costing us the other 1,999.
            logger.warning("Batch intersect failed (%s); isolating places", e)
            rows = []
            for pid in ids:
                try:
                    rows.extend(self._intersect([pid]))
                except DatabaseError:
                    self.broken.append(pid)
                    logger.warning("Unusable geometry on place %s — skipped", pid)

        hits, areal, parts = {}, set(), {}
        for pid, dim, iso, nparts in rows:
            hits.setdefault(pid, []).append(iso)
            parts[pid] = max(parts.get(pid, 0), nparts)
            if dim == 2:
                areal.add(pid)

        # Only areal places matching several countries need the overlap test —
        # and a place matching a great many, or built from a great many pieces,
        # is multinational by nature rather than the victim of a border sliver,
        # so it keeps the lot. Those are also the ones that cost the most to
        # test, which is why the guards pay for themselves twice.
        contested = sorted(pid for pid in areal
                           if 1 < len(hits[pid]) <= self.max_share_countries
                           and parts.get(pid, 1) <= self.max_share_parts)
        if contested:
            shares = {}
            try:
                for pid, iso, of_place, of_country in self._shares(contested):
                    shares.setdefault(pid, []).append(
                        (iso, max(of_place or 0.0, of_country or 0.0)))
            except DatabaseError as e:
                # Keep every intersecting country rather than dropping the batch;
                # over-attribution beats losing the places entirely.
                logger.warning("Overlap test failed (%s); keeping all matches", e)
            for pid, pairs in shares.items():
                kept = [iso for iso, share in pairs if share >= self.min_share]
                if not kept:  # everything is a sliver — keep the best of them
                    kept = [max(pairs, key=lambda p: p[1])[0]]
                hits[pid] = kept

        return {pid: sorted(set(codes)) for pid, codes in hits.items()}

    def _intersect(self, ids):
        """[(place id, dimension, ISO2, geometry-row count), …] — one row per
        country a place meets, de-duplicated across its parts."""
        with connection.cursor() as cur:
            cur.execute(_PARTS_CTE + """
                , sizes AS (SELECT id, count(*) AS n FROM parts GROUP BY id)
                SELECT parts.id, max(ST_Dimension(parts.geom)), c.iso, max(sizes.n)
                  FROM parts
                  JOIN countries c ON ST_Intersects(c.mpoly, parts.geom)
                  JOIN sizes ON sizes.id = parts.id
                 GROUP BY parts.id, c.iso
            """, {'ids': ids})
            return cur.fetchall()

    def _shares(self, ids):
        """[(place id, ISO2, share of the place, share of the country), …].

        Both fractions matter. Judging by the place alone credits only the giants
        when the place is something like "(British Empire)" — the United Kingdom
        is a rounding error against that area, yet it is obviously one of its
        countries. Judging by the country alone credits every micro-state a large
        polygon happens to graze. A border sliver is small against *both*, which
        is exactly what we want to drop."""
        with connection.cursor() as cur:
            cur.execute(_PARTS_CTE + """
                , whole AS (
                    SELECT id, sum(ST_Area(geom)) AS area FROM parts GROUP BY id
                )
                SELECT o.id, o.iso,
                       o.overlap / NULLIF(whole.area, 0),
                       o.overlap / NULLIF(o.country_area, 0)
                  FROM (
                      SELECT parts.id, c.iso,
                             sum(ST_Area(ST_Intersection(c.mpoly, parts.geom))) AS overlap,
                             max(ST_Area(c.mpoly)) AS country_area
                        FROM parts JOIN countries c ON ST_Intersects(c.mpoly, parts.geom)
                       GROUP BY parts.id, c.iso
                  ) o
                  JOIN whole ON whole.id = o.id
            """, {'ids': ids})
            return cur.fetchall()

    def _match_nearest(self, ids, tolerance_m):
        """{place id: [ISO2]} for non-areal geometry just outside a country —
        coastal and island places lost to a coarse country outline."""
        with connection.cursor() as cur:
            cur.execute(_PARTS_CTE + """
                SELECT DISTINCT ON (p.id) p.id, n.iso
                  FROM (SELECT id, geom FROM parts WHERE ST_Dimension(geom) < 2) p
                  CROSS JOIN LATERAL (
                      SELECT c.iso,
                             ST_Distance(c.mpoly::geography, p.geom::geography) AS dist
                        FROM countries c
                       ORDER BY c.mpoly <-> p.geom
                       LIMIT 1
                  ) n
                 WHERE n.dist <= %(tol)s
                 ORDER BY p.id, n.dist
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
