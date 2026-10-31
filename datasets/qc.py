"""Automatic checks on a dataset submitted for review (place#334 §4e).

One report, two audiences. The editor and the owner both see the checks; only the owner also
sees how many of their records are linked (``links``), because review judges the
contribution (validity, licence, sensitivity), never the share of records linked: the
editors' report contains NO link counts (ruling Q11). Nothing here blocks the owner from
working on a draft; ``blocking`` lists what would stop publication.

Every check returns ``{key, label, status, count, detail, examples}`` with ``status`` one of
``ok``, ``warn``, ``fail`` (blocks publication) or ``not_checked`` (said out loud rather than
reported as passing). Counts are over the dataset's own records; ``examples`` holds up to
five ``{place_id, title}`` so the editor can go and look.

Whether link identifiers resolve (§4e: a sample of 50 through the gateway) is reported here as
``not_checked``, so that an absent check is not read as a pass. In the editors' report, when an
editor asks for it (``sample_for``), that placeholder is replaced by the real check
(``datasets/qc_links.py``), run in the background like the sample below; the owner's report and
the summary kept at submission keep the placeholder.

The editors' report may also carry ``gateway_sample`` (``datasets/qc_sample.py``): a small,
reproducible sample of records searched for suggestions, run in the background and read from
the submission event. It is added only when an editor asks for the editors' report
(``sample_for``), never to the owner's report and never to the summary kept at submission.
"""
import datetime

from django.db import connection, transaction

EXAMPLES = 5
SENSITIVITY_FIELDS = ("sensitivity_indigenous", "sensitivity_restricted")
# Keys only the owner's report may carry (Q11). Tests assert the editor's has none of them.
OWNER_ONLY_KEYS = ("links",)


def _check(key, label, status, count=None, detail="", examples=None, fix=None, verdict=None):
    out = {"key": key, "label": label, "status": status, "count": count, "detail": detail,
           "examples": list(examples or [])[:EXAMPLES]}
    if fix:
        out["fix"] = fix
    if verdict:
        out["verdict"] = verdict
    return out


def _examples(place_ids):
    from places.models import Place
    ids = list(place_ids)[:EXAMPLES]
    titles = dict(Place.objects.filter(id__in=ids).values_list("id", "title"))
    return [{"place_id": i, "title": titles.get(i, "")} for i in ids]


# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------

def check_records(ds):
    n = ds.places.count()
    if n == 0:
        return _check("records", "Records loaded", "fail", 0,
                      "No records have been loaded from the file.")
    return _check("records", "Records loaded", "ok", n)


def check_licence(ds):
    lic = ds.license
    if lic is None:
        return _check("licence", "Licence", "fail", detail="No licence is recorded; a dataset "
                      "cannot be published without one.")
    if str(lic.spdx_id).startswith("custom-"):
        return _check("licence", "Licence", "warn",
                      detail=f"{lic.label} is not a standard (SPDX) licence; the editor "
                             "checks its terms.")
    return _check("licence", "Licence", "ok", detail=f"{lic.label} ({lic.spdx_id})")


def placeholder_links(ds):
    """The dataset's ``place_link`` rows whose identifier is a placeholder for "no value"
    (``wd:None``, ``gn:null``, an empty CURIE...), as a queryset of ids."""
    from places.concordance import is_placeholder_identifier
    from places.models import PlaceLink
    bad = [pk for pk, ident in (PlaceLink.objects.filter(place__dataset=ds.label)
                                .values_list("id", "jsonb__identifier").iterator())
           if is_placeholder_identifier(ident)]
    return PlaceLink.objects.filter(id__in=bad)


def check_placeholders(ds):
    rows = placeholder_links(ds)
    n = rows.count()
    if not n:
        return _check("placeholders", "Placeholder identifiers", "ok", 0)
    return _check("placeholders", "Placeholder identifiers", "warn", n,
                  "Links such as 'wd:None' name nothing. They are never used, and the owner "
                  "may remove them in one step.",
                  _examples(rows.values_list("place_id", flat=True).distinct()),
                  fix="strip_placeholders")


def check_duplicates(ds):
    """Two records with one ``src_id``, or the same title within 1 km."""
    from django.db.models import Count
    dup_src = list(ds.places.values("src_id").annotate(n=Count("id")).filter(n__gt=1)
                   .values_list("src_id", flat=True)[:1000])
    src_places = list(ds.places.filter(src_id__in=dup_src).values_list("id", flat=True))
    with connection.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT pa.id FROM places pa
            JOIN places pb ON pb.dataset = pa.dataset AND pb.id <> pa.id
                 AND lower(pb.title) = lower(pa.title)
            JOIN place_geom ga ON ga.place_id = pa.id
            JOIN place_geom gb ON gb.place_id = pb.id
            WHERE pa.dataset = %s AND ga.geom IS NOT NULL AND gb.geom IS NOT NULL
              AND ST_DWithin(ga.geom::geography, gb.geom::geography, 1000)
            LIMIT 1000""", [ds.label])
        near = [r[0] for r in cur.fetchall()]
    count = len(set(src_places) | set(near))
    if not count:
        return _check("duplicates", "Possible duplicates", "ok", 0)
    return _check("duplicates", "Possible duplicates", "warn", count,
                  f"{len(src_places)} record(s) share a source id; {len(near)} share a title "
                  "with another record less than 1 km away.", _examples(src_places + near))


def check_geometry(ds):
    with connection.cursor() as cur:
        cur.execute("""
            SELECT
              count(*) FILTER (WHERE GeometryType(g.geom) = 'POINT'
                               AND ST_X(g.geom) = 0 AND ST_Y(g.geom) = 0),
              count(*) FILTER (WHERE ST_XMin(g.geom) < -180 OR ST_XMax(g.geom) > 180
                               OR ST_YMin(g.geom) < -90 OR ST_YMax(g.geom) > 90),
              count(*) FILTER (WHERE GeometryType(g.geom) = 'POINT'
                               AND abs(ST_Y(g.geom)) > 90 AND abs(ST_X(g.geom)) <= 90),
              count(*) FILTER (WHERE GeometryType(g.geom) = 'MULTIPOINT'
                               AND ST_NPoints(g.geom) > 1
                               AND ST_XMin(g.geom) >= -180 AND ST_XMax(g.geom) <= 180
                               AND ST_YMin(g.geom) >= -90 AND ST_YMax(g.geom) <= 90
                               AND ST_Length(ST_LongestLine(g.geom, g.geom)::geography) > 500000),
              array_agg(DISTINCT p.id) FILTER (WHERE
                    (GeometryType(g.geom) = 'POINT' AND ST_X(g.geom) = 0 AND ST_Y(g.geom) = 0)
                    OR ST_XMin(g.geom) < -180 OR ST_XMax(g.geom) > 180
                    OR ST_YMin(g.geom) < -90 OR ST_YMax(g.geom) > 90)
            FROM place_geom g JOIN places p ON p.id = g.place_id
            WHERE p.dataset = %s AND g.geom IS NOT NULL""", [ds.label])
        zero, out_of_range, swapped, wide, bad_ids = cur.fetchone()
    parts = []
    if zero:
        parts.append(f"{zero} at 0,0")
    if out_of_range:
        parts.append(f"{out_of_range} outside -180..180 / -90..90"
                     + (f" ({swapped} look like latitude and longitude swapped)" if swapped else ""))
    if wide:
        parts.append(f"{wide} multipoint(s) spanning more than 500 km")
    if not parts:
        return _check("geometry", "Coordinates", "ok", 0)
    status = "fail" if out_of_range else "warn"
    return _check("geometry", "Coordinates", status, zero + out_of_range + wide,
                  "; ".join(parts) + ".", _examples(bad_ids or []))


def check_temporal(ds):
    limit = datetime.date.today().year + 1
    with connection.cursor() as cur:
        cur.execute("""
            SELECT id FROM places
            WHERE dataset = %s AND minmax IS NOT NULL
              AND ((minmax[1] IS NOT NULL AND minmax[2] IS NOT NULL AND minmax[2] < minmax[1])
                   OR minmax[1] > %s OR minmax[2] > %s)
            LIMIT 1000""", [ds.label, limit, limit])
        bad = [r[0] for r in cur.fetchall()]
    if not bad:
        return _check("temporal", "Dates", "ok", 0)
    return _check("temporal", "Dates", "warn", len(bad),
                  f"An end before its start, or a year after {limit}.", _examples(bad))


def check_attribution(ds):
    missing = []
    if not (ds.description or "").strip():
        missing.append("description")
    if not ((ds.creator or "").strip() or ds.creators_csl.exists() or (ds.citation or "").strip()):
        missing.append("creator or citation")
    if not missing:
        return _check("attribution", "Description and attribution", "ok")
    return _check("attribution", "Description and attribution", "warn", len(missing),
                  "Missing: " + ", ".join(missing) + ".")


def sensitivity_unanswered(ds):
    """Why the sensitivity declaration is not complete, or "" when it is. "Answered" means both
    questions hold a real answer ("no", "yes" or "unsure"); NULL or blank is unanswered, never
    "no". A "yes" also needs the owner's note (the form refuses one without)."""
    if any(getattr(ds, f, None) in (None, "") for f in SENSITIVITY_FIELDS):
        return ("Answer the two questions on Indigenous and sensitive data before submitting "
                "this dataset for review.")
    if ("yes" in [getattr(ds, f) for f in SENSITIVITY_FIELDS]
            and not (getattr(ds, "sensitivity_note", "") or "").strip()):
        return ("You answered Yes to a sensitivity question: say briefly what is sensitive "
                "before submitting this dataset for review.")
    return ""


def check_sensitivity(ds):
    answers = {f: getattr(ds, f, None) for f in SENSITIVITY_FIELDS}
    if any(v in (None, "") for v in answers.values()):
        return _check("sensitivity", "Sensitivity declaration", "warn",
                      detail="The two questions on Indigenous and sensitive data have not "
                             "both been answered; the dataset cannot be submitted for "
                             "review until they are.")
    flagged = [f for f, v in answers.items() if v in ("yes", "unsure")]
    if flagged:
        note = (getattr(ds, "sensitivity_note", "") or "").strip()
        return _check("sensitivity", "Sensitivity declaration", "warn", len(flagged),
                      "Declared as possibly sensitive: the editor considers an embargo."
                      + (f" Owner's note: {note}" if note else ""))
    return _check("sensitivity", "Sensitivity declaration", "ok")


def check_link_resolution(ds):
    return _check("link_resolution", "Link identifiers resolve", "not_checked",
                  detail="Not checked automatically yet.")


CHECKS = (check_records, check_licence, check_placeholders, check_duplicates, check_geometry,
          check_temporal, check_attribution, check_sensitivity, check_link_resolution)


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------

def link_counts(ds):
    """For the owner only (Q11): links the dataset carries and links people have made."""
    from api.attestations import WHG_NAMESPACE
    from api.models import ContributorAttestation
    from django.db.models import Q
    from places.models import PlaceLink
    prefix = f"{WHG_NAMESPACE}:{ds.pk}"
    rows = ContributorAttestation.objects.filter(
        Q(dataset_id=prefix) | Q(place_a__startswith=prefix + ":")
        | Q(place_b__startswith=prefix + ":"))
    return {
        "records_with_links": PlaceLink.objects.filter(place__dataset=ds.label)
        .values("place_id").distinct().count(),
        "links_public": rows.filter(status="active").count(),
        "links_private_until_published": rows.filter(status="pending").count(),
    }


def qc_report(ds, audience="editor", sample_for=None):
    """The checks for ``ds``. ``audience`` is ``editor`` (no link counts, Q11) or ``owner``.
    ``sample_for``, an editor, adds the sample check to the editors' report and replaces its
    ``link_resolution`` placeholder with the link check (each queued in the background if not
    yet taken); it is ignored for the owner's report."""
    if audience not in ("editor", "owner"):
        raise ValueError("audience must be 'editor' or 'owner'")
    checks = [fn(ds) for fn in CHECKS]
    if audience == "editor" and sample_for is not None:
        from datasets.qc_links import link_check
        from datasets.qc_sample import sample_check
        checks = [link_check(ds, requested_by=sample_for) if c["key"] == "link_resolution" else c
                  for c in checks]
        checks.append(sample_check(ds, requested_by=sample_for))
    report = {
        "dataset": ds.pk,
        "generated_at": datetime.datetime.now().isoformat(timespec="seconds"),
        "audience": audience,
        "checks": checks,
        "blocking": [c["key"] for c in checks if c["status"] == "fail"],
    }
    if audience == "owner":
        report["links"] = link_counts(ds)
    return report


def summary(report):
    """The compact form kept on the submission event: status and count per check."""
    return {"blocking": report["blocking"],
            "checks": {c["key"]: [c["status"], c["count"]] for c in report["checks"]}}


# ---------------------------------------------------------------------------
# The one-step fix the owner may accept
# ---------------------------------------------------------------------------

STRIPPABLE_STATES = (None, "uploaded", "validation_failed", "draft", "changes_requested")


def strip_placeholders(ds, user):
    """Delete the dataset's placeholder links (``wd:None`` and the like), at the owner's
    request and only while the dataset is private and not under review. Returns how many
    were removed. Refusals raise ``PermissionError`` / ``ValueError``."""
    if not ds.user_can_manage(user):
        raise PermissionError("not permitted")
    if ds.public or ds.lifecycle_state not in STRIPPABLE_STATES:
        raise ValueError("placeholders can be removed only from a private draft")
    with transaction.atomic():
        n, _ = placeholder_links(ds).delete()
    return n
