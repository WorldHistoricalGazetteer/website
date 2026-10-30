"""JSON endpoints for the v4 dataset lifecycle (place#334 Phase 3). Beta only.

Session auth and CSRF like every other datasets POST. A dataset the caller may not see is a
404 (never a 403), as everywhere in datasets/. The rules themselves live in
``datasets/lifecycle.py``; these views only translate a refusal into its HTTP status.

    GET  /datasets/<id>/lifecycle/                 state, service level, timeline, checks
    POST /datasets/<id>/lifecycle/start/           owner: opt a NULL (v3-era) dataset in (draft)
    POST /datasets/<id>/lifecycle/submit/          owner
    POST /datasets/<id>/lifecycle/publish/         owner, once accepted (the publish toggle)
    POST /datasets/<id>/lifecycle/withdraw/        owner
    POST /datasets/<id>/lifecycle/review/start/    editor
    POST /datasets/<id>/lifecycle/review/conclude/ editor: {"outcome", "comments"}
    POST /datasets/<id>/lifecycle/strip-placeholders/   owner, private draft only
    POST /datasets/<id>/lifecycle/sensitivity/     owner: the two questions

    GET  /datasets/review-queue/                   editor: HTML page of datasets awaiting
                                                   review (the owner's screen is a panel on
                                                   the status page, templatetags/lifecycle_tags)
"""
import json

from django.contrib.auth.views import redirect_to_login
from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET, require_POST

from datasets import lifecycle, qc
from datasets.forms import SensitivityDeclarationForm
from datasets.models import Dataset, DatasetLifecycleEvent


def _gate(request, dsid):
    """``(dataset, None)`` or ``(None, response)``: signed in, beta, and in the dataset's
    circle (editors are staff, so they are in every circle)."""
    user = request.user
    if not user.is_authenticated:
        return None, JsonResponse({"error": "sign in required"}, status=401)
    if not getattr(user, "can_access_beta", False):
        raise Http404("not found")
    ds = Dataset.objects.filter(pk=dsid).first()
    if ds is None or not ds.user_has_private_access(user):
        raise Http404("not found")
    return ds, None


def _body(request):
    try:
        return json.loads(request.body or b"{}")
    except ValueError:
        return None


def _is_owner(ds, user):
    return ds.owner_id == user.id or ds.owners.filter(id=user.id).exists()


def allowed_actions(ds, user):
    """What ``user`` may do to ``ds`` now, by name. The JSON state and the HTML screens
    (``datasets/templatetags/lifecycle_tags.py``, ``review_queue``) both read this, so a
    button is offered exactly when the endpoint behind it would accept the request."""
    editor = lifecycle.is_editor(user)
    manage = ds.user_can_manage(user)
    return {
        "start": manage and ds.lifecycle_state in lifecycle.TRANSITIONS["start"][0]
        and not ds.public,
        "submit": manage and ds.lifecycle_state in lifecycle.TRANSITIONS["submit"][0]
        and not ds.public,
        "publish": manage and ds.lifecycle_state in lifecycle.TRANSITIONS["publish"][0],
        "withdraw": manage and ds.lifecycle_state == "published",
        "review": editor and ds.lifecycle_state in ("submitted", "in_review"),
    }


def _state(ds, user):
    summary = lifecycle.status_summary(ds, user)
    if summary:
        for k in ("submitted_on", "usually_by"):
            summary[k] = summary[k].isoformat() if summary[k] else None
    return {
        "dataset": ds.pk,
        "summary": summary,
        "timeline": [dict(s, at=s["at"].isoformat()) for s in lifecycle.timeline(ds, user)],
        # Q11: link counts only for the contributor (owner or co-owner); everyone else in
        # the circle, editors included, gets the editors' report.
        "checks": qc.qc_report(ds, audience="owner" if _is_owner(ds, user) else "editor"),
        "can": allowed_actions(ds, user),
    }


def _act(request, dsid, fn):
    ds, refusal = _gate(request, dsid)
    if refusal:
        return refusal
    body = _body(request)
    if body is None:
        return JsonResponse({"error": "body must be JSON"}, status=400)
    try:
        fn(ds, body)
    except lifecycle.LifecycleRefused as exc:
        return JsonResponse({"error": exc.message}, status=exc.status)
    ds.refresh_from_db()
    return JsonResponse(_state(ds, request.user))


@require_GET
def lifecycle_state(request, dsid):
    ds, refusal = _gate(request, dsid)
    return refusal or JsonResponse(_state(ds, request.user))


@require_POST
def lifecycle_start(request, dsid):
    return _act(request, dsid, lambda ds, b: lifecycle.start(ds, request.user))


@require_POST
def lifecycle_submit(request, dsid):
    return _act(request, dsid, lambda ds, b: lifecycle.submit(ds, request.user, b.get("note", "")))


@require_POST
def lifecycle_publish(request, dsid):
    return _act(request, dsid, lambda ds, b: lifecycle.publish(ds, request.user))


@require_POST
def lifecycle_withdraw(request, dsid):
    return _act(request, dsid,
                lambda ds, b: lifecycle.withdraw(ds, request.user, b.get("reason", "")))


@require_POST
def lifecycle_review_start(request, dsid):
    return _act(request, dsid, lambda ds, b: lifecycle.start_review(ds, request.user))


@require_POST
def lifecycle_review_conclude(request, dsid):
    return _act(request, dsid, lambda ds, b: lifecycle.conclude_review(
        ds, request.user, str(b.get("outcome", "")), str(b.get("comments", ""))))


@require_POST
def lifecycle_strip_placeholders(request, dsid):
    ds, refusal = _gate(request, dsid)
    if refusal:
        return refusal
    try:
        n = qc.strip_placeholders(ds, request.user)
    except PermissionError:
        raise Http404("not found")
    except ValueError as exc:
        return JsonResponse({"error": str(exc)}, status=409)
    return JsonResponse({"removed": n})


REVIEW_QUEUE_STATES = ("submitted", "in_review")


@require_GET
def review_queue(request):
    """The editors' page: datasets waiting for review, with the editors' checks report (no
    link counts, Q11) and the decision buttons, which post to the endpoints above. Gated as
    those are: signed in (else the login page), beta, and an editor; anyone else gets 404.
    A dataset is listed only if the editor is in its circle, as each endpoint requires."""
    user = request.user
    if not user.is_authenticated:
        return redirect_to_login(request.get_full_path())
    if not getattr(user, "can_access_beta", False) or not lifecycle.is_editor(user):
        raise Http404("not found")
    rows = []
    for ds in (Dataset.objects.filter(lifecycle_state__in=REVIEW_QUEUE_STATES)
               .select_related("owner").order_by("id")):
        if not ds.user_has_private_access(user):
            continue
        can = allowed_actions(ds, user)
        if not can["review"]:
            continue
        rows.append({
            "ds": ds,
            "summary": lifecycle.status_summary(ds, user),
            "checks": qc.qc_report(ds, audience="editor"),
            "submit_note": _submission_note(ds),
            "sensitivity": [(Dataset._meta.get_field(f).help_text,
                             dict(Dataset._meta.get_field(f).choices).get(getattr(ds, f), "Not answered"))
                            for f in qc.SENSITIVITY_FIELDS],
        })
    return render(request, "datasets/review_queue.html", {"rows": rows})


def _submission_note(ds):
    """The contributor's note on their latest submission ("" if none)."""
    ev = (DatasetLifecycleEvent.objects.filter(dataset_pk=ds.pk, to_state="submitted")
          .order_by("-at", "-id").first())
    return ev.reason if ev else ""


@require_POST
def lifecycle_sensitivity(request, dsid):
    ds, refusal = _gate(request, dsid)
    if refusal:
        return refusal
    if not ds.user_can_manage(request.user):
        raise Http404("not found")
    body = _body(request)
    if body is None:
        return JsonResponse({"error": "body must be JSON"}, status=400)
    form = SensitivityDeclarationForm(data=body, instance=ds)
    if not form.is_valid():
        return JsonResponse({"errors": form.errors}, status=400)
    # Only these three columns: Dataset.save() would run the v3 pre_save handlers.
    Dataset.objects.filter(pk=ds.pk).update(**{f: form.cleaned_data[f] for f in form.Meta.fields})
    return JsonResponse({"saved": True})
