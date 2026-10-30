"""Convert the REVIEWED v3 record-record links into Link statements (place#342, Phase 5).

    manage.py migrate_reviewed_links                 # dry run (default): writes nothing
    manage.py migrate_reviewed_links --dry-run
    manage.py migrate_reviewed_links --execute       # write rows; no gateway call
    manage.py migrate_reviewed_links --execute --forward   # also queue the gateway push

What is migrated: ``close_matches`` rows with ``basis='reviewed'`` ONLY. Each becomes one
``api.ContributorAttestation``: ``relation_type='closeMatch'``, ``user`` = the reviewer who
made the v3 match (``close_matches.created_by``), ``legacy_v3_2=True`` (so the gateway's
source id carries the ``:legacy_v3_2`` suffix), ``suggested_by='import'``, ``asserted_at`` =
the original ``created_at``, ``evidence`` holding the originating ``close_matches`` id and
task. Rows are grouped in one ``AttestationBundle`` (kind ``import``) per reviewer and each
gets an ``AttestationEvent``.

What is NOT migrated, by ruling: ``basis='imported'`` (37,919 on prod) and ``basis='authid'``
rows, and the reviewed ``place_link`` rows (73,785 on prod, all ``closeMatch``). They stay in
the overlay under the weaker ``imported`` category (place#334 ruling N1) and must not be
counted again by replay. There is no code path here that reads them as input; the command
only COUNTS them so the denominators are printed. Re-running is idempotent: the model's
UNIQUE (place_a, place_b, relation_type, user) key is checked first, and an existing row is
counted as ``already migrated`` and left alone.

Safety. Default is a dry run. ``--execute`` writes with ``bulk_create``, which fires no
``post_save`` signal, so NOTHING is sent to the gateway/ES; the gateway's Batch 12 replay
(part B) picks the rows up. ``--forward`` instead saves row by row, so the existing
``api/signals.py`` handler queues ``forward_attestation_post`` for each ``active`` row; that
push is itself refused unless ``ENV_CONTEXT`` is the production context
(``crc_client._link_writes_allowed``). ``--execute`` and ``--dry-run`` are mutually exclusive.
A row is ``active`` when both sides are in public, un-embargoed datasets or authority ids,
otherwise ``pending`` (never forwarded; Phase 3 activates it on publication), exactly as
``api.attestations.create`` decides. A side whose record has no source id, or whose source id
is not unique inside its dataset, cannot be named for the gateway and is SKIPPED with a reason.
Licence: ``ContributorAttestation`` has no licence column; CC BY 4.0 is the overlay-wide
ruling (2026-09-30), so there is nothing to store per row.

THE COUNT DISCREPANCY (place#342, "carried forward"). The issue quotes 22,851 record-record
pairs, 156 reviewed, 22,695 imported. Live prod (read-only SQL, 2026-10-30) holds 38,151
rows: 232 reviewed, 37,919 imported (dev: 38,903 / 240 / 38,663). Findings:
  * REVIEWED: growth explains it. 103 reviewed rows were made between 2026-04-17 and
    2026-04-30 and 74 more on 2026-05-04; the table stood at about 158 at the end of April,
    which is the issue's 156 to within two rows. The 156 was a count taken earlier, before the
    last reviewing sessions.
  * IMPORTED: growth does NOT explain it. All 37,919 imported rows were created on one day,
    2024-05-30. No filter I tried reproduces 22,695 or 22,851: distinct unordered pairs
    37,047 non-self (37,660 incl. self); both sides in public datasets 30,091; public and
    cross-dataset and non-self 27,697; excluding every pair with a ``tgn_filtered_01`` side
    12,309. The 22,851 figure is NOT reproduced; its source remains unidentified (the
    place#325 measurement query was not recorded). Treat the live table as the denominator.
  * Also found: 614 imported rows are self-pairs (place_a == place_b), 253 pairs are stored
    in both directions, 7 rows are exact duplicates. None of those touch the 232 reviewed rows
    (no reviewed self-pairs; 232 distinct unordered pairs).
Each run prints its own denominators so nobody has to rely on either figure.
"""
from collections import Counter, defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from api.attestations import WHG_NAMESPACE, _resolve_side, invalidate_concordances
from api.models import AttestationBundle, AttestationEvent, ContributorAttestation
from places.models import CloseMatch, Place, PlaceLink

RELATION = "closeMatch"
BATCH = 500


class Command(BaseCommand):
    help = ("Convert reviewed v3 close_matches into Link statements (legacy_v3_2). "
            "Dry run unless --execute. Never migrates imported/authid rows or place_link.")

    def add_arguments(self, parser):
        parser.add_argument("--dry-run", action="store_true",
                            help="Write nothing (the default).")
        parser.add_argument("--execute", action="store_true",
                            help="Write the rows. Without --forward nothing reaches the gateway.")
        parser.add_argument("--forward", action="store_true",
                            help="With --execute: save rows singly so the post_save handler "
                                 "queues the gateway push (still refused outside production).")

    # ------------------------------------------------------------------
    def handle(self, *args, **opts):
        if opts["execute"] and opts["dry_run"]:
            raise CommandError("--execute and --dry-run are mutually exclusive")
        if opts["forward"] and not opts["execute"]:
            raise CommandError("--forward only applies with --execute")
        execute = opts["execute"]

        plan, skipped, denominators = self.plan()
        self.report(plan, skipped, denominators, execute)
        if not execute:
            self.stdout.write("\nDRY RUN: nothing was written. Re-run with --execute to write.")
            return
        written = self.write(plan, forward=opts["forward"])
        self.stdout.write(self.style.SUCCESS(
            f"\nWrote {written} attestation(s); gateway forwarding "
            f"{'queued (production only)' if opts['forward'] else 'NOT requested'}."))

    # ------------------------------------------------------------------
    def plan(self):
        """``(rows to write, skip reasons Counter, denominators dict)``; reads only."""
        by_basis = Counter()
        for basis in CloseMatch.objects.values_list("basis", flat=True):
            by_basis[basis or "NULL"] += 1
        total = sum(by_basis.values())
        reviewed = (CloseMatch.objects.filter(basis="reviewed")
                    .select_related("place_a", "place_b", "created_by").order_by("id"))
        reviewed_place_links = PlaceLink.objects.filter(
            task_id__isnull=False, reviewer__isnull=False).count()

        skipped = Counter()
        seen = set()
        existing = set(ContributorAttestation.objects.filter(
            legacy_v3_2=True, relation_type=RELATION)
            .values_list("place_a", "place_b", "user_id"))
        plan = []
        for cm in reviewed.iterator(chunk_size=BATCH):
            if cm.place_a_id == cm.place_b_id:
                skipped["self pair"] += 1
                continue
            ga, pa = _resolve_side(*self._ref(cm.place_a))
            gb, pb = _resolve_side(*self._ref(cm.place_b))
            if ga is None or gb is None:
                skipped["unresolvable side: " + str(pa if ga is None else pb)] += 1
                continue
            if ga == gb:
                skipped["both sides name the same gateway id"] += 1
                continue
            a, b = sorted((ga, gb))
            key = (a, b, cm.created_by_id)
            if key in existing:
                skipped["already migrated"] += 1
                continue
            if key in seen:
                skipped["duplicate pair for the same reviewer"] += 1
                continue
            seen.add(key)
            ds = cm.place_a.dataset.pk  # both sides are contributed records
            pk_by_id = {ga: cm.place_a_id, gb: cm.place_b_id}
            plan.append({
                "cm": cm, "a": a, "b": b,
                "a_pk": pk_by_id[a], "b_pk": pk_by_id[b],
                "status": "active" if (pa is True and pb is True) else "pending",
                "dataset_id": f"{WHG_NAMESPACE}:{ds}" if ds is not None else "",
            })
        denominators = {"total": total, "by_basis": dict(by_basis),
                        "reviewed": by_basis.get("reviewed", 0),
                        "reviewed_place_links_not_migrated": reviewed_place_links}
        return plan, skipped, denominators

    @staticmethod
    def _ref(place):
        return f"{WHG_NAMESPACE}:{place.dataset.pk}:{place.src_id}", place.pk

    # ------------------------------------------------------------------
    def report(self, plan, skipped, d, execute):
        w = self.stdout.write
        w("migrate_reviewed_links -- denominators")
        w(f"  close_matches, all rows ................ {d['total']}")
        for basis, n in sorted(d["by_basis"].items()):
            note = "  (read as input)" if basis == "reviewed" else "  (NOT migrated, by ruling)"
            w(f"    basis={basis:<10} {n:>8}{note}")
        w(f"  reviewed place_link rows (NOT migrated)  {d['reviewed_place_links_not_migrated']}")
        w(f"  reviewed close_matches considered ...... {d['reviewed']}")
        w(f"    to migrate ........................... {len(plan)}")
        status = Counter(p["status"] for p in plan)
        for s, n in sorted(status.items()):
            w(f"      as {s:<8} {n}")
        w(f"    skipped ............................... {sum(skipped.values())}")
        for reason, n in skipped.most_common():
            w(f"      {n:>6}  {reason}")
        accounted = len(plan) + sum(skipped.values())
        w(f"  check: to migrate + skipped = {accounted}; reviewed = {d['reviewed']}"
          f"{'' if accounted == d['reviewed'] else '   MISMATCH'}")
        per_user = Counter(p["cm"].created_by_id for p in plan)
        w(f"  reviewers: {len(per_user)} (rows per user id: "
          f"{', '.join(f'{u}:{n}' for u, n in per_user.most_common(10))})")
        w("  count discrepancy vs the issue (22,851 / 156 / 22,695): see this command's docstring.")

    # ------------------------------------------------------------------
    @transaction.atomic
    def write(self, plan, forward=False):
        bundles = {}
        pending_rows = []
        for p in plan:
            cm = p["cm"]
            bundle = bundles.get(cm.created_by_id)
            if bundle is None:
                bundle = bundles[cm.created_by_id] = AttestationBundle.objects.create(
                    user=cm.created_by, kind="import",
                    note="Reviewed v3 close_matches migrated as legacy_v3_2 (place#342)")
            row = ContributorAttestation(
                user=cm.created_by, dataset_id=p["dataset_id"],
                place_a=p["a"], place_b=p["b"], place_a_pk=p["a_pk"], place_b_pk=p["b_pk"],
                relation_type=RELATION, asserted_at=cm.created_at,
                justification=f"Reviewed match made in WHG v3 (close_matches #{cm.pk}).",
                status=p["status"], legacy_v3_2=True, suggested_by="import",
                evidence={"source": "close_matches", "close_match_id": cm.pk,
                          "task_id": cm.task_id, "basis": "reviewed"},
                bundle=bundle)
            if forward:
                row.save()  # post_save queues the gateway push for active rows
                self._event(row)
            else:
                pending_rows.append(row)
        written = len(plan) if forward else 0
        if not forward:
            for i in range(0, len(pending_rows), BATCH):
                chunk = pending_rows[i:i + BATCH]
                created = ContributorAttestation.objects.bulk_create(chunk, ignore_conflicts=True)
                # ignore_conflicts leaves pks unset: look the rows up by their key.
                for row in chunk:
                    stored = ContributorAttestation.objects.filter(
                        user=row.user, place_a=row.place_a, place_b=row.place_b,
                        relation_type=RELATION, legacy_v3_2=True).first()
                    if stored is not None:
                        self._event(stored)
                        invalidate_concordances(stored)
                        written += 1
        return written

    @staticmethod
    def _event(row):
        AttestationEvent.objects.create(
            attestation=row, user=row.user, action="created",
            detail={"status": row.status, "migrated_from": "close_matches",
                    "close_match_id": row.evidence.get("close_match_id")})
