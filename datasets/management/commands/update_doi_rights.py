"""Bring the rights statement of already-minted dataset DOIs into line with the
dataset's recorded licence (place#158, C4).

Every DOI minted before 2026-09-04 registered a hard-coded CC-BY-NC-4.0 rights
statement at DataCite whatever the data's terms were. ``utils.doi`` has emitted
the real licence since then, but a DOI is only re-sent when its dataset is
saved, and the licence records made by ``apply_licence_proposals`` deliberately
avoid ``save()``. This command does the one remaining thing: for each selected
dataset that has a DOI, fetch what DataCite holds, compare its ``rightsList``
with what ``utils.doi.get_rights_list`` now says, and — only with ``--commit`` —
PUT a payload containing ``rightsList`` and nothing else. DataCite updates only
the attributes sent, so no other field of the record is touched, no DOI is
minted, and the DOI's state (draft / registered / findable) is left alone.

Scope: by default the datasets named by approved rows in the licence-proposals
file (the population that ``apply_licence_proposals`` just licensed); ``--ids``
to name datasets directly; ``--all-licensed`` for every dataset that has both a
DOI and a licence (the cohort-A backfill of 2026-09-04 left 105 DOIs stale too).

Refusals, before any network call: a selected dataset with no licence recorded
(there is nothing true to send, and clearing a statement is a different act
from correcting one). A dataset with ``doi=False`` is skipped and counted, not
refused. Idempotent: a DOI whose registered rights already match is reported
as unchanged and not sent. Credentials are read from settings and never
printed; the request headers are never logged.
"""

import json
from pathlib import Path

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from datasets.models import Dataset
from utils.doi import format_doi, get_rights_list

DEFAULT_PROPOSALS = (
    Path(__file__).resolve().parents[3] / "licensing" / "data" / "contributed_licence_proposals.json"
)

TIMEOUT = 30


def _normalise(rights):
    """Reduce a rightsList to the identities it asserts, so that what DataCite
    echoes back compares equal to what we sent.

    DataCite does not store our entry verbatim: for a recognised SPDX
    ``rightsIdentifier`` it lower-cases the id (we send ``CC-BY-4.0``, it
    returns ``cc-by-4.0``, verified against a live record) and rewrites
    ``rights`` / ``rightsUri`` from the SPDX list; its key casing also differs
    (``rightsUri`` for our ``rightsURI``). So: an entry with an identifier is
    compared by that identifier, case-insensitively; an entry without one (a
    custom licence, or a free-text rights statement) by its text. Order-insensitive.
    """
    out = []
    for entry in rights or []:
        if not isinstance(entry, dict):
            continue
        lower = {k.lower(): v for k, v in entry.items()}
        ident = (lower.get("rightsidentifier") or "").strip().lower()
        if ident:
            out.append(("id", ident))
        else:
            out.append(("text", " ".join((lower.get("rights") or "").split()).lower()))
    return sorted(out)


def _describe(rights):
    """One short token per entry: the SPDX id where there is one, else the text."""
    if not rights:
        return "(none)"
    parts = []
    for entry in rights:
        if not isinstance(entry, dict):
            continue
        parts.append(entry.get("rightsIdentifier") or entry.get("rights") or "?")
    return " + ".join(parts)


class Command(BaseCommand):
    help = "Update ONLY the rightsList of existing dataset DOIs at DataCite from the recorded licence. Dry by default."

    def add_arguments(self, parser):
        parser.add_argument("--commit", action="store_true",
                            help="Send the updates. Without this the command only reports.")
        scope = parser.add_mutually_exclusive_group()
        scope.add_argument("--ids", default="",
                           help="Comma-separated dataset ids to process.")
        scope.add_argument("--all-licensed", action="store_true",
                           help="Every dataset with doi=True and a licence recorded.")
        parser.add_argument("--proposals", default=str(DEFAULT_PROPOSALS),
                            help="Proposals file whose approved rows define the default scope.")

    # ----------------------------------------------------------------- scope
    def _select(self, opts):
        if opts["ids"]:
            ids = [int(x) for x in opts["ids"].split(",") if x.strip()]
            qs = Dataset.objects.filter(id__in=ids).select_related("license")
            missing = set(ids) - set(qs.values_list("id", flat=True))
            if missing:
                raise CommandError(f"Datasets do not exist: {sorted(missing)}.")
            return list(qs.order_by("id")), f"--ids ({len(ids)})"
        if opts["all_licensed"]:
            qs = Dataset.objects.filter(doi=True, license__isnull=False).select_related("license")
            return list(qs.order_by("id")), "--all-licensed"
        try:
            with open(opts["proposals"], encoding="utf-8") as fh:
                rows = json.load(fh)["proposals"]
        except (OSError, ValueError, KeyError) as exc:
            raise CommandError(f"Cannot read proposals file {opts['proposals']}: {exc}")
        ids = [r["dataset_id"] for r in rows
               if r.get("approved") is True and r.get("approved_by") and r.get("approved_on")]
        qs = Dataset.objects.filter(id__in=ids).select_related("license")
        missing = set(ids) - set(qs.values_list("id", flat=True))
        if missing:
            raise CommandError(
                f"Approved proposal rows name datasets that do not exist here: {sorted(missing)}.")
        return list(qs.order_by("id")), f"approved proposal rows ({len(ids)})"

    # --------------------------------------------------------------- network
    def _headers(self):
        # Never log or print this dict: it carries the DataCite credentials.
        return {
            "Content-Type": "application/vnd.api+json",
            "authorization": f"Basic {settings.DOI_ENCODED_CREDENTIALS}",
        }

    def _fetch(self, doi):
        resp = requests.get(f"{settings.DOI_API_URL}/{doi}", headers=self._headers(), timeout=TIMEOUT)
        if resp.status_code == 404:
            return None
        if resp.status_code != 200:
            raise RuntimeError(f"GET {doi}: HTTP {resp.status_code}")
        return resp.json()["data"]["attributes"]

    def _put(self, doi, rights):
        resp = requests.put(
            f"{settings.DOI_API_URL}/{doi}",
            json={"data": {"type": "dois", "attributes": {"rightsList": rights}}},
            headers=self._headers(), timeout=TIMEOUT,
        )
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"PUT {doi}: HTTP {resp.status_code}")

    # ----------------------------------------------------------------- handle
    def handle(self, *args, **opts):
        datasets, scope = self._select(opts)
        unlicensed = [ds.id for ds in datasets if ds.license_id is None]
        if unlicensed:
            raise CommandError(
                f"{len(unlicensed)} of {len(datasets)} selected datasets record no licence: "
                f"{unlicensed}. Nothing was sent. Record the licence first "
                f"(apply_licence_proposals) or narrow the scope."
            )

        with_doi = [ds for ds in datasets if ds.doi]
        no_doi = [ds.id for ds in datasets if not ds.doi]
        self.stdout.write(f"Scope: {scope}; selected {len(datasets)}; "
                          f"with a DOI {len(with_doi)}; without (skipped) {len(no_doi)}")
        if no_doi:
            self.stdout.write(f"  skipped, no DOI: {no_doi}")

        unchanged, to_update, not_registered, failed = [], [], [], []
        for ds in with_doi:
            doi_id = format_doi("dataset", ds.id)
            new = get_rights_list(ds)
            if not new:
                # Cannot happen after the licence check, but never send an empty
                # list: that would clear the statement, which is not this command's job.
                failed.append((doi_id, "computed an empty rightsList"))
                continue
            try:
                attrs = self._fetch(doi_id)
            except (requests.RequestException, RuntimeError, ValueError, KeyError) as exc:
                failed.append((doi_id, f"fetch failed: {exc}"))
                continue
            if attrs is None:
                not_registered.append(doi_id)
                self.stdout.write(f"  {doi_id:<28} not registered at DataCite (doi=True locally); skipped")
                continue
            old = attrs.get("rightsList") or []
            if _normalise(old) == _normalise(new):
                unchanged.append(doi_id)
                self.stdout.write(f"  {doi_id:<28} unchanged: {_describe(new)}")
                continue
            to_update.append((doi_id, new))
            self.stdout.write(f"  {doi_id:<28} {_describe(old)} -> {_describe(new)}")

        self.stdout.write(
            f"\nDOIs checked {len(with_doi)}: unchanged {len(unchanged)}, to update {len(to_update)}, "
            f"not registered {len(not_registered)}, failed {len(failed)}"
        )

        sent = 0
        if opts["commit"]:
            for doi_id, new in to_update:
                try:
                    self._put(doi_id, new)
                    sent += 1
                    self.stdout.write(f"  updated {doi_id}")
                except (requests.RequestException, RuntimeError) as exc:
                    failed.append((doi_id, f"update failed: {exc}"))
            self.stdout.write(self.style.SUCCESS(f"\nSent {sent} of {len(to_update)} updates."))
        else:
            self.stdout.write(self.style.WARNING(
                f"\nDry run — nothing sent. Pass --commit to send {len(to_update)} update(s)."))

        if failed:
            for doi_id, why in failed:
                self.stderr.write(f"  FAILED {doi_id}: {why}")
            raise CommandError(f"{len(failed)} DOI(s) failed; see above. Re-run to retry (idempotent).")
