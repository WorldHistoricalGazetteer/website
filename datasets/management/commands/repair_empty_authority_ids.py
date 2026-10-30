"""place#342: repair authority ids lost by legacy reconciliation (``wd:None`` links etc.).

Before caf83082c, ``datasets.services._process_matching_decisions`` read
``hit_json['authrecord_id']`` (never present), so it wrote

* ``place_link`` rows with identifier ``<auth>:None``,
* ``place_geom`` rows whose ``jsonb.citation.id`` ends in ``:``,
* ``place_name`` rows whose ``jsonb.citations[].id`` ends in ``:``.

The reviewer's accept/match decision is NOT stored on ``Hit`` (``Hit.matched`` is never
written by the review code; only ``reviewed`` is, for every hit of the place). The only
trace of the decision is the rows it created, so the hit behind a bad row is recovered
by classification, never guessed:

link  (a) exactly one candidate hit (same place_id + task_id);
      (b) several candidates but exactly one whose own id (``<prefix><authrecord_id>``)
          is also the identifier of ANOTHER place_link of the same place+task (a matched
          Wikidata hit's own ``links`` list contains its own ``wd:Q..``, and the review
          code adds those as extra links) AND no un-evidenced candidate is a non-Wikidata
          (e.g. GeoNames) hit, which leaves no trace if matched;
      (c) several candidates, not narrowed to one;  (d) no candidate hit.
geom  (a) single candidate; (b) exactly one distinct authority id among hits whose
      first geometry equals the row's geometry; (c) several; (d) none.
name  (a) single candidate; (b) exactly one distinct authority id among hits whose names
      contain every toponym of the place+task's bad name rows; (c) several; (d) none.

Only (a)/(b) are repairable. Dry run is the default and writes nothing.

Actions (Stephen, 2026-10-30):
* a GeoNames hit is cited ``gn:`` (label GeoNames), not under the task's ``wd:`` -- links AND
  geom/name citations (matching the corrected live code);
* a bad link whose corrected identifier is ALREADY another link of the same place+task (a matched
  Wikidata hit leaves its own ``wd:Q..`` behind) is DELETED, not rewritten into a duplicate;
* ``--execute`` requires ``--backup PATH`` and writes every row it will change or delete there first;
* ``--sample N [--seed S]`` prints a stratified human-readable sample and writes nothing.
``--execute`` rewrites only the identifier / citation id of the repairable rows, in one
transaction, and is idempotent (repaired rows no longer match the "bad" filter).
"""
import csv
import datetime
import json
import random
from collections import Counter, defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

CHUNK = 400


# ---------------------------------------------------------------- pure classifiers
def _ids(hits):
    return sorted({h["authrecord_id"] for h in hits if h.get("authrecord_id")})


def classify_link(hits, other_identifiers, prefix):
    """-> (class, authrecord_id|None). ``prefix`` is e.g. ``"wd:"``."""
    hits = [h for h in hits if h.get("authrecord_id")]
    if not hits:
        return "d", None
    if len(hits) == 1:
        return "a", hits[0]["authrecord_id"]
    others = {str(i).strip() for i in other_identifiers if i}
    own = sorted({h["authrecord_id"] for h in hits if prefix + h["authrecord_id"] in others})
    # A matched Wikidata hit always leaves its own ``wd:Q..`` behind, so an un-evidenced Wikidata
    # candidate was not matched. A GeoNames hit leaves nothing (no ``links`` list): if one is
    # among the un-evidenced candidates it may have been the primary, so do not guess.
    unevidenced_opaque = [h for h in hits if h["authrecord_id"] not in own
                          and (h.get("json") or {}).get("dataset") != "wikidata"]
    if len(own) == 1 and not unevidenced_opaque:
        return "b", own[0]
    return "c", None


def hit_names(hj):
    """Names the legacy code would have added from this hit (services.py logic)."""
    hj = hj or {}
    if hj.get("dataset") == "wikidata":
        return {n.split("@")[0] for n in hj.get("variants", [])}
    if hj.get("dataset") == "geonames":
        return set(hj.get("variants", []))
    return set(hj.get("names") or [])


def classify_geom(hits, geom_jsonb):
    hits = [h for h in hits if h.get("authrecord_id")]
    if not hits:
        return "d", None
    if len(hits) == 1:
        return "a", hits[0]["authrecord_id"]
    want = (geom_jsonb.get("type"), geom_jsonb.get("coordinates"))
    ids = sorted({h["authrecord_id"] for h in hits
                  if (g := ((h.get("json") or {}).get("geoms") or [None])[0])
                  and (g.get("type"), g.get("coordinates")) == want})
    if len(ids) == 1:
        return "b", ids[0]
    return ("c" if ids or len(hits) > 1 else "d"), None


def classify_names(hits, toponyms):
    hits = [h for h in hits if h.get("authrecord_id")]
    if not hits:
        return "d", None
    if len(hits) == 1:
        return "a", hits[0]["authrecord_id"]
    tops = set(toponyms)
    ids = sorted({h["authrecord_id"] for h in hits if tops and tops <= hit_names(h.get("json"))})
    if len(ids) == 1:
        return "b", ids[0]
    return "c", None


def chosen_hit(hits, aid):
    return next((h for h in hits if h.get("authrecord_id") == aid), None) if aid else None


def is_geonames(hit):
    j = (hit or {}).get("json") or {}
    return j.get("dataset") == "geonames" or (hit or {}).get("authority") == "geonames"


def retarget(prefix, aid, hit):
    """-> (full new value, new citation label or None). GeoNames hits are cited gn:."""
    if is_geonames(hit):
        return f"gn:{aid}", "GeoNames"
    return f"{prefix}{aid}", None


def split_prefix(value):
    """'wd:None' -> 'wd:' ; 'wd:' -> 'wd:' ; None if no colon."""
    value = (value or "").strip()
    return value[: value.index(":") + 1] if ":" in value else None


# ---------------------------------------------------------------- data access
def _bad_links():
    from places.models import PlaceLink
    return PlaceLink.objects.filter(task_id__isnull=False, jsonb__identifier__regex=r":None$")


def _bad_geoms():
    from places.models import PlaceGeom
    return PlaceGeom.objects.filter(task_id__isnull=False, jsonb__citation__id__regex=r":$")


def _bad_names():
    from places.models import PlaceName
    return PlaceName.objects.filter(task_id__isnull=False, jsonb__citations__0__id__regex=r":$")


def _counts():
    return {"place_link": _bad_links().count(), "place_geom": _bad_geoms().count(),
            "place_name": _bad_names().count()}


def _hits_for(task_id, place_ids):
    from datasets.models import Hit
    by_place = defaultdict(list)
    for h in Hit.objects.filter(task_id=task_id, place_id__in=place_ids).values(
            "id", "place_id", "authrecord_id", "json", "authority"):
        by_place[h["place_id"]].append(h)
    return by_place


def _task_names(task_ids):
    try:
        from django_celery_results.models import TaskResult
        return dict(TaskResult.objects.filter(task_id__in=task_ids).values_list("task_id", "task_name"))
    except Exception:  # pragma: no cover - reporting nicety only
        return {}


def plan(limit_task=None):
    """Classify every bad row. Returns (decisions, stats).

    decisions: list of dicts {kind, pk, task_id, place_id, cls, new_id, prefix, old}
    Nothing is written."""
    decisions = []
    link_by_task = defaultdict(list)
    for pk, place_id, task_id, jb in _bad_links().values_list("id", "place_id", "task_id", "jsonb"):
        if limit_task and task_id != limit_task:
            continue
        link_by_task[task_id].append((pk, place_id, jb))
    geoms_by_task = defaultdict(list)
    for pk, place_id, task_id, jb in _bad_geoms().values_list("id", "place_id", "task_id", "jsonb"):
        if limit_task and task_id != limit_task:
            continue
        geoms_by_task[task_id].append((pk, place_id, jb))
    names_by_task = defaultdict(lambda: defaultdict(list))
    for pk, place_id, task_id, jb in _bad_names().values_list("id", "place_id", "task_id", "jsonb"):
        if limit_task and task_id != limit_task:
            continue
        names_by_task[task_id][place_id].append((pk, jb))

    from places.models import PlaceLink
    link_resolved = {}  # (task, place) -> authrecord id chosen for the link, for the agreement count
    for task_id in sorted(set(link_by_task) | set(geoms_by_task) | set(names_by_task)):
        # ---- links
        rows = link_by_task.get(task_id, [])
        for i in range(0, len(rows), CHUNK):
            chunk = rows[i:i + CHUNK]
            pids = {r[1] for r in chunk}
            hits = _hits_for(task_id, pids)
            others = defaultdict(list)
            for l_id, l_place, l_jb in PlaceLink.objects.filter(
                    task_id=task_id, place_id__in=pids).values_list("id", "place_id", "jsonb"):
                others[l_place].append((l_id, (l_jb or {}).get("identifier")))
            for pk, place_id, jb in chunk:
                prefix = split_prefix(jb.get("identifier"))
                cls, aid = classify_link(
                    hits.get(place_id, []), [x for (lid, x) in others[place_id] if lid != pk], prefix or "")
                if prefix is None:
                    cls, aid = "d", None
                hit = chosen_hit(hits.get(place_id, []), aid)
                target, label = retarget(prefix or "", aid, hit) if aid and prefix else (None, None)
                dup = next((lid for (lid, x) in others[place_id]
                            if lid != pk and target and str(x or "").strip() == target), None)
                link_resolved[(task_id, place_id)] = aid
                decisions.append({"kind": "link", "pk": pk, "task_id": task_id, "place_id": place_id,
                                  "cls": cls, "new_id": aid, "prefix": prefix, "target": target,
                                  "action": "delete" if dup else "rewrite", "dup_of": dup,
                                  "stratum": (f"a-{'gn' if is_geonames(hit) else 'wd'}" if cls == "a" else cls),
                                  "chosen_hit": hit["id"] if hit else None,
                                  "old": jb.get("identifier"), "n_hits": len(hits.get(place_id, []))})
        # ---- geoms
        rows = geoms_by_task.get(task_id, [])
        for i in range(0, len(rows), CHUNK):
            chunk = rows[i:i + CHUNK]
            hits = _hits_for(task_id, {r[1] for r in chunk})
            for pk, place_id, jb in chunk:
                prefix = split_prefix(jb["citation"]["id"])
                cls, aid = classify_geom(hits.get(place_id, []), jb)
                target, label = retarget(prefix or "", aid, chosen_hit(hits.get(place_id, []), aid))
                decisions.append({"kind": "geom", "pk": pk, "task_id": task_id, "place_id": place_id,
                                  "cls": cls, "new_id": aid, "prefix": prefix, "target": target,
                                  "new_label": label, "action": "rewrite",
                                  "old": jb["citation"]["id"], "n_hits": len(hits.get(place_id, []))})
        # ---- names (resolved per place+task: one hit supplied every name row)
        per_place = names_by_task.get(task_id, {})
        pl = sorted(per_place)
        for i in range(0, len(pl), CHUNK):
            chunk = pl[i:i + CHUNK]
            hits = _hits_for(task_id, set(chunk))
            for place_id in chunk:
                rows = per_place[place_id]
                cls, aid = classify_names(hits.get(place_id, []), [jb.get("toponym") for _, jb in rows])
                target, label = retarget(split_prefix(rows[0][1]["citations"][0]["id"]) or "", aid,
                                         chosen_hit(hits.get(place_id, []), aid))
                for pk, jb in rows:
                    cit = jb["citations"][0]["id"]
                    decisions.append({"kind": "name", "pk": pk, "task_id": task_id, "place_id": place_id,
                                      "cls": cls, "new_id": aid, "prefix": split_prefix(cit),
                                      "target": target, "new_label": label, "action": "rewrite",
                                      "old": cit, "n_hits": len(hits.get(place_id, []))})
    for d in decisions:
        d["repairable"] = d["cls"] in ("a", "b") and bool(d["new_id"]) and d["prefix"] is not None
    return decisions, link_resolved


def summarise(decisions, link_resolved, out):
    w = out.write
    for kind, label in (("link", "place_link wd:None-style"), ("geom", "place_geom citation ends ':'"),
                        ("name", "place_name citation ends ':'")):
        ds = [d for d in decisions if d["kind"] == kind]
        n = len(ds)
        c = Counter(d["cls"] for d in ds)
        rep = sum(1 for d in ds if d["repairable"])
        w(f"\n== {label}: {n} bad rows ==\n")
        for cls, name in (("a", "(a) exactly one hit"), ("b", "(b) one hit singled out by evidence"),
                          ("c", "(c) several candidates"), ("d", "(d) no candidate hit")):
            w(f"  {name:42s} {c[cls]:>7} of {n} ({(100 * c[cls] / n) if n else 0:.1f}%)\n")
        w(f"  REPAIRABLE (a)+(b)                         {rep:>7} of {n}\n")
        w(f"  distinct places                            {len({d['place_id'] for d in ds}):>7}\n")
    ls = [d for d in decisions if d["kind"] == "link" and d["repairable"]]
    w(f"\n== planned link actions: {len(ls)} repairable links ==\n")
    w(f"  rewrite to wd:                 {sum(1 for d in ls if d['action']=='rewrite' and d['target'].startswith('wd:')):>7} of {len(ls)}\n")
    w(f"  rewrite to gn: (GeoNames hit)  {sum(1 for d in ls if d['action']=='rewrite' and d['target'].startswith('gn:')):>7} of {len(ls)}\n")
    w(f"  delete (duplicate of real link){sum(1 for d in ls if d['action']=='delete'):>7} of {len(ls)}\n")
    for kind in ("geom", "name"):
        ks = [d for d in decisions if d["kind"] == kind and d["repairable"]]
        w(f"  {kind} citations to gn: (GeoNames hit) {sum(1 for d in ks if (d['target'] or '').startswith('gn:')):>7} of {len(ks)}\n")
    # agreement between the link's hit and the geom/name hit for the same place+task
    for kind in ("geom", "name"):
        ds = [d for d in decisions if d["kind"] == kind and d["repairable"]
              and link_resolved.get((d["task_id"], d["place_id"]))]
        agree = sum(1 for d in ds if d["new_id"] == link_resolved[(d["task_id"], d["place_id"])])
        w(f"\n{kind} rows repairable AND whose place+task link is also resolved: {len(ds)}; "
          f"same authority id as the link: {agree} of {len(ds)}\n")
    names = _task_names({d["task_id"] for d in decisions})
    w("\n== per task (rows: bad / repairable, per kind) ==\n")
    by_task = defaultdict(lambda: defaultdict(lambda: [0, 0]))
    for d in decisions:
        cell = by_task[d["task_id"]][d["kind"]]
        cell[0] += 1
        cell[1] += d["repairable"]
    w(f"{'task_id':38s} {'task_name':14s} {'link':>13s} {'geom':>13s} {'name':>13s}\n")
    for t in sorted(by_task):
        f = lambda k: f"{by_task[t][k][1]}/{by_task[t][k][0]}"
        w(f"{t:38s} {names.get(t, '?'):14s} {f('link'):>13s} {f('geom'):>13s} {f('name'):>13s}\n")


def write_report(path, decisions):
    with open(path, "w", newline="") as fh:
        wr = csv.writer(fh)
        wr.writerow(["kind", "row_id", "task_id", "place_id", "class", "n_candidate_hits", "old_value"])
        n = 0
        for d in decisions:
            if not d["repairable"]:
                wr.writerow([d["kind"], d["pk"], d["task_id"], d["place_id"], d["cls"], d["n_hits"], d["old"]])
                n += 1
    return n


BACKUP_MODELS = {"link": "places.PlaceLink", "geom": "places.PlaceGeom", "name": "places.PlaceName"}


def write_backup(path, decisions):
    """Full rows of everything --execute will update or delete, written BEFORE any change."""
    from django.apps import apps
    from django.forms.models import model_to_dict
    rows = []
    for d in decisions:
        if not d["repairable"]:
            continue
        m = apps.get_model(BACKUP_MODELS[d["kind"]])
        obj = m.objects.get(pk=d["pk"])
        full = {f.attname: getattr(obj, f.attname) for f in m._meta.concrete_fields
                if f.get_internal_type() != "GeometryField" and not hasattr(getattr(obj, f.attname), "wkt")}
        geom = getattr(obj, "geom", None)
        if geom is not None:
            full["geom_wkt"] = geom.wkt
        rows.append({"model": BACKUP_MODELS[d["kind"]], "pk": d["pk"], "action": d["action"],
                     "planned_value": d["target"], "row": full})
    with open(path, "w") as fh:
        json.dump({"generated": datetime.datetime.utcnow().isoformat() + "Z", "n_rows": len(rows),
                   "rows": rows}, fh, default=str, indent=1)
    return len(rows)


def execute(decisions):
    """Rewrite identifier / citation fields of repairable rows; delete duplicate bad links.
    Caller wraps in a transaction."""
    from places.models import PlaceGeom, PlaceLink, PlaceName
    done = Counter()
    for d in decisions:
        if not d["repairable"]:
            continue
        new = d["target"]
        if d["kind"] == "link":
            row = PlaceLink.objects.select_for_update().get(pk=d["pk"])
            jb = dict(row.jsonb or {})
            if jb.get("identifier") != d["old"]:
                continue  # changed since planning: leave it
            if d["action"] == "delete":
                # only if the real link is still there
                if not any(str((x or {}).get("identifier") or "").strip() == new for x in
                           PlaceLink.objects.filter(place_id=row.place_id, task_id=row.task_id)
                           .exclude(pk=row.pk).values_list("jsonb", flat=True)):
                    continue
                PlaceLink.objects.filter(pk=row.pk).delete()
                done["link_deleted"] += 1
                continue
            jb["identifier"] = new
            # queryset.update bypasses PlaceLink.save()'s reviewer guard and touches nothing else
            done["link_rewritten"] += PlaceLink.objects.filter(pk=row.pk).update(jsonb=jb)
        elif d["kind"] == "geom":
            row = PlaceGeom.objects.select_for_update().get(pk=d["pk"])
            jb = json.loads(json.dumps(row.jsonb))
            if jb["citation"]["id"] != d["old"]:
                continue
            jb["citation"]["id"] = new
            if d.get("new_label"):
                jb["citation"]["label"] = d["new_label"]
            done["geom"] += PlaceGeom.objects.filter(pk=row.pk).update(jsonb=jb)
        else:
            row = PlaceName.objects.select_for_update().get(pk=d["pk"])
            jb = json.loads(json.dumps(row.jsonb))
            if jb["citations"][0]["id"] != d["old"]:
                continue
            jb["citations"][0]["id"] = new
            if d.get("new_label"):
                jb["citations"][0]["label"] = d["new_label"]
            done["name"] += PlaceName.objects.filter(pk=row.pk).update(jsonb=jb)
    return done


def pick_sample(decisions, n, seed):
    """n repairable link decisions, stratified over a-wd / a-gn / b, spread across tasks."""
    rng = random.Random(seed)
    strata = {k: [d for d in decisions if d["kind"] == "link" and d["repairable"] and d["stratum"] == k]
              for k in ("a-wd", "a-gn", "b")}
    quota = {k: n // 3 + (1 if i < n % 3 else 0) for i, k in enumerate(strata)}
    picked = []
    for k, ds in strata.items():
        by_task = defaultdict(list)
        for d in ds:
            by_task[d["task_id"]].append(d)
        for lst in by_task.values():
            rng.shuffle(lst)
        order = sorted(by_task)
        rng.shuffle(order)
        take = []
        while len(take) < min(quota[k], len(ds)):
            for t in order:
                if by_task[t] and len(take) < quota[k]:
                    take.append(by_task[t].pop())
        picked.extend(take)
    return picked


def render_sample(picked, decisions, w):
    from datasets.models import Hit
    from places.models import Place, PlaceLink
    names = _task_names({d["task_id"] for d in picked})
    side = defaultdict(list)
    for d in decisions:
        if d["kind"] != "link":
            side[(d["task_id"], d["place_id"])].append(d)
    w(f"# Sample of {len(picked)} repairable links\n\n")
    for i, d in enumerate(picked, 1):
        place = Place.objects.get(pk=d["place_id"])
        hits = list(Hit.objects.filter(task_id=d["task_id"], place_id=d["place_id"]).values(
            "id", "authority", "authrecord_id", "json"))
        others = [(l.id, (l.jsonb or {}).get("identifier")) for l in
                  PlaceLink.objects.filter(task_id=d["task_id"], place_id=d["place_id"]).exclude(pk=d["pk"])]
        w(f"## {i}. place {place.id} \"{place.title}\" (src_id {place.src_id}) -- stratum {d['stratum']}\n")
        w(f"- task {d['task_id']} ({names.get(d['task_id'], '?')}); bad place_link {d['pk']} = `{d['old']}`\n")
        w("- candidate hits:\n")
        for h in hits:
            mark = "  **<- chosen**" if h["id"] == d["chosen_hit"] else ""
            w(f"  - hit {h['id']}: {h['authority']} `{h['authrecord_id']}` "
              f"\"{(h['json'] or {}).get('title', '')}\"{mark}\n")
        if d["cls"] == "a":
            why = "the only candidate hit"
        else:
            why = (f"its own id `{d['prefix']}{d['new_id']}` is already another link of this place+task, "
                   f"and no other candidate could have been matched unseen (all others are Wikidata hits with no such link)")
        w(f"- why: {why}\n")
        w(f"- other links on this place+task: {[x for _, x in others] or 'none'}\n")
        if d["action"] == "delete":
            w(f"- PLANNED: DELETE place_link {d['pk']} (`{d['old']}`): duplicate of link {d['dup_of']} (`{d['target']}`)\n")
        else:
            w(f"- PLANNED: rewrite place_link {d['pk']}: `{d['old']}` -> `{d['target']}`\n")
        for s_ in side.get((d["task_id"], d["place_id"]), []):
            if not s_["repairable"]:
                w(f"- {s_['kind']} row {s_['pk']}: UNREPAIRABLE (class {s_['cls']})\n")
            else:
                w(f"- {s_['kind']} row {s_['pk']}: `{s_['old']}` -> `{s_['target']}`\n")
        w("\n")


class Command(BaseCommand):
    help = ("place#342: classify (dry run, default) / repair (--execute) place_link, place_geom and "
            "place_name rows whose authority id was lost by legacy reconciliation.")

    def add_arguments(self, p):
        p.add_argument("--execute", action="store_true",
                       help="Apply the (a)/(b) repairs. Default is a read-only report.")
        p.add_argument("--backup", help="REQUIRED with --execute: JSON file receiving every row to be changed/deleted.")
        p.add_argument("--report", help="CSV path for the UNREPAIRABLE rows (classes c/d).")
        p.add_argument("--task", help="Restrict to one task_id.")
        p.add_argument("--sample", type=int, help="Print a stratified sample of N repairable links; writes nothing.")
        p.add_argument("--seed", type=int, default=0)

    def handle(self, *a, **o):
        out = self.stdout
        if o["execute"] and not o.get("backup"):
            raise CommandError("--execute requires --backup PATH (a JSON file for the rows about to change).")
        if o["execute"] and o.get("sample"):
            raise CommandError("--sample is read-only; do not combine with --execute.")
        before = _counts()
        out.write(f"bad rows now: {before}\n")
        decisions, link_resolved = plan(o.get("task"))
        if o.get("sample"):
            render_sample(pick_sample(decisions, o["sample"], o["seed"]), decisions, out.write)
            return
        summarise(decisions, link_resolved, out)
        if o.get("report"):
            n = write_report(o["report"], decisions)
            out.write(f"\nunrepairable rows written to {o['report']}: {n} of {len(decisions)}\n")
        if not o["execute"]:
            out.write("\nDRY RUN: nothing written. Re-run with --execute --backup PATH to apply the (a)/(b) repairs.\n")
            return
        n = write_backup(o["backup"], decisions)
        out.write(f"\nbackup of {n} rows written to {o['backup']}\n")
        with transaction.atomic():
            done = execute(decisions)
        after = _counts()
        out.write(f"\nEXECUTED. rows changed: {dict(done)}\nbad rows before: {before}\nbad rows after:  {after}\n")
