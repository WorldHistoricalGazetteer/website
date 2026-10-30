"""Precision of the candidate tiers against what WHG already knows (place#334 4b; place#338).

    manage.py calibrate_match_tiers [--dataset ID ...] [--min-negatives 100] [--json]

Reads only. For every stored suggestion (``MatchCandidate``, not ``stale``) it looks for a human
decision about the same (record, place) pair, from five sources:

* ``feed``      the contributor feed: ``accepted`` = positive, ``rejected`` = negative;
* ``link``      a reviewed v3 link (``place_link`` with ``task_id`` and ``reviewer``): positive;
* ``hit``       a reviewed v3 hit (``hits.reviewed``): ``matched`` = positive, else negative;
* ``close``     a reviewed record-record pair (``close_matches.basis='reviewed'``): positive;
* ``statement`` a Link made by a person (``ContributorAttestation`` active/pending): ``distinct``
  = negative, any other relation = positive.

A pair with both kinds of label is CONFLICTING and left out (and counted). Precision is reported
per tier with its denominator, twice: over each record's top suggestion (what the record's tier
claims) and over every suggestion (what each candidate's tier claims), plus what type sanity
demoted. It REFUSES to print any precision when fewer than ``--min-negatives`` labelled
negatives exist: a precision with no negatives under it cannot fail. MyD decisions are not read
(owner consent needed).

Never run this against production data without Stephen: it reads every suggestion.
"""
import json
import re
from collections import Counter, defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q

TIERS = ("confident", "ambiguous", "weak")
BATCH = 2000
_WD_URI = re.compile(r"^https?://(?:www\.)?wikidata\.org/(?:entity|wiki)/(Q\d+)$", re.I)
_PLACEHOLDERS = {"none", "null", "nan", ""}


def norm_id(raw, authority=""):
    """A gateway-style id (``wd:Q90``, ``tgn:7011781``, ``gn:2988507``) from a stored identifier,
    or None for a placeholder (``wd:None``) or something unrecognisable."""
    s = str(raw or "").strip()
    m = _WD_URI.match(s)
    if m:
        return f"wd:{m.group(1)}"
    if ":" in s and not s.lower().startswith("http"):
        ns, _, local = s.partition(":")
        ns = {"wikidata": "wd", "geonames": "gn", "getty-tgn": "tgn", "tgn": "tgn"}.get(ns.lower(), ns.lower())
        return None if local.strip().lower() in _PLACEHOLDERS else f"{ns}:{local.strip()}"
    if s.lower() in _PLACEHOLDERS:
        return None
    if re.match(r"^Q\d+$", s):
        return f"wd:{s}"
    if authority in ("tgn",) and s.isdigit():
        return f"tgn:{s}"
    if authority in ("wd", "wdlocal") and s.startswith("Q"):
        return f"wd:{s}"
    return None


def labels_for(places):
    """``{(place_pk, gateway id): {source: True/False}}`` for a batch of Places."""
    from api.models import ContributorAttestation
    from datasets.models import Hit
    from places.models import CloseMatch, Place, PlaceLink
    pks = [p.pk for p in places]
    gw = {p.pk: f"whg:{p.dataset.pk}:{p.src_id}" for p in places}
    out = defaultdict(dict)

    def put(pk, cid, source, positive):
        if cid:
            prev = out[(pk, cid)].get(source)
            out[(pk, cid)][source] = positive if prev is None or prev == positive else "both"

    for pk, js in (PlaceLink.objects.filter(place_id__in=pks, task_id__isnull=False, reviewer__isnull=False)
                   .values_list("place_id", "jsonb")):
        put(pk, norm_id((js or {}).get("identifier")), "link", True)
    for pk, auth, rec, matched in (Hit.objects.filter(place_id__in=pks, reviewed=True)
                                   .values_list("place_id", "authority", "authrecord_id", "matched")):
        put(pk, norm_id(rec, auth), "hit", bool(matched))
    for a, b in (CloseMatch.objects.filter(basis="reviewed").filter(Q(place_a_id__in=pks) | Q(place_b_id__in=pks))
                 .values_list("place_a_id", "place_b_id")):
        for me, other in ((a, b), (b, a)):
            if me in gw:
                o = Place.objects.filter(pk=other).select_related("dataset").first()
                if o is not None:
                    put(me, f"whg:{o.dataset.pk}:{o.src_id}", "close", True)
    ids = {gw[pk]: pk for pk in pks} | {f"whg:{pk}": pk for pk in pks}
    for a, b, rel in (ContributorAttestation.objects.filter(status__in=("active", "pending"))
                      .filter(Q(place_a__in=list(ids)) | Q(place_b__in=list(ids)))
                      .values_list("place_a", "place_b", "relation_type")):
        for me, other in ((a, b), (b, a)):
            if me in ids:
                put(ids[me], other, "statement", rel != "distinct")
    return out


def calibrate(dataset_ids=None):
    """The report as a dict (no refusal here; the command refuses)."""
    from datasets.models_matching import MatchCandidate
    qs = MatchCandidate.objects.exclude(status="stale").select_related("place", "place__dataset")
    if dataset_ids:
        qs = qs.filter(dataset_id__in=dataset_ids)
    per_record = {t: Counter() for t in TIERS}
    per_candidate = {t: Counter() for t in TIERS}
    sanity, sources = Counter(), Counter()
    conflicting = 0
    last = 0
    while True:
        batch = list(qs.filter(pk__gt=last).order_by("pk")[:BATCH])
        if not batch:
            break
        last = batch[-1].pk
        places = {c.place_id: c.place for c in batch}
        lab = labels_for(list(places.values()))
        for c in batch:
            found = dict(lab.get((c.place_id, c.candidate_id), {}))
            if c.status in ("accepted", "rejected"):
                found["feed"] = c.status == "accepted"
            values = set(found.values())
            if "both" in values or (True in values and False in values):
                conflicting += 1
                label = None
            else:
                label = values.pop() if values else None
            for s in found:
                sources[s] += 1
            key = "unlabelled" if label is None else ("pos" if label else "neg")
            per_candidate[c.tier][key] += 1
            if c.rank == 1:
                per_record[c.tier][key] += 1
            if str((c.signals or {}).get("type_sanity", "ok")) != "ok":
                sanity[key] += 1

    def rows(table):
        out = {}
        for t in TIERS:
            n = table[t]
            labelled = n["pos"] + n["neg"]
            out[t] = {"positives": n["pos"], "negatives": n["neg"], "labelled": labelled,
                      "unlabelled": n["unlabelled"],
                      "precision": (n["pos"] / labelled) if labelled else None}
        return out

    negatives = sum(per_candidate[t]["neg"] for t in TIERS)
    return {"top_suggestion": rows(per_record), "every_suggestion": rows(per_candidate),
            "type_sanity_demoted": {"positives": sanity["pos"], "negatives": sanity["neg"],
                                    "unlabelled": sanity["unlabelled"]},
            "negatives": negatives,
            "positives": sum(per_candidate[t]["pos"] for t in TIERS),
            "conflicting": conflicting, "label_sources": dict(sources)}


class Command(BaseCommand):
    help = "Precision of the match-candidate tiers against reviewed links and feed decisions (read only)."

    def add_arguments(self, parser):
        parser.add_argument("--dataset", type=int, action="append", dest="datasets",
                            help="limit to this dataset id (repeatable)")
        parser.add_argument("--min-negatives", type=int, default=100)
        parser.add_argument("--json", action="store_true", help="print the report as JSON")

    def handle(self, *args, **opts):
        report = calibrate(opts.get("datasets"))
        need = opts["min_negatives"]
        if report["negatives"] < need:
            raise CommandError(
                f"refusing to report precision: {report['negatives']} labelled negatives "
                f"(need at least {need}); {report['positives']} positives, {report['conflicting']} conflicting. "
                "A precision with too few negatives under it cannot fail.")
        if opts["json"]:
            self.stdout.write(json.dumps(report, indent=2))
            return
        w = self.stdout.write
        w(f"Labelled pairs: {report['positives']} positive, {report['negatives']} negative, "
          f"{report['conflicting']} conflicting (left out). Sources: {report['label_sources']}")
        for title, key in (("Record tier (top suggestion of each record)", "top_suggestion"),
                           ("Candidate tier (every suggestion)", "every_suggestion")):
            w(title)
            for t in TIERS:
                r = report[key][t]
                p = "n/a" if r["precision"] is None else f"{100 * r['precision']:.1f}%"
                w(f"  {t:<10} precision {p:>7}  = {r['positives']} / {r['labelled']} labelled"
                  f"  ({r['unlabelled']} unlabelled)")
        s = report["type_sanity_demoted"]
        w(f"Demoted by type sanity: {s['negatives']} labelled negative, {s['positives']} labelled positive, "
          f"{s['unlabelled']} unlabelled")
