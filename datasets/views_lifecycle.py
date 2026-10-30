"""JSON endpoints for the v4 dataset lifecycle (place#334 Phase 3). Beta only.

Session auth and CSRF like every other datasets POST. A dataset the caller may not see is a
404 (never a 403), as everywhere in datasets/. The rules themselves live in
``datasets/lifecycle.py``; these views only translate a refusal into its HTTP status.

    GET  /datasets/<id>/lifecycle/                 state, service level, timeline, checks
    POST /datasets/<id>/lifecycle/submit/          owner
    POST /datasets/<id>/lifecycle/publish/         owner, once accepted (the publish toggle)
    POST /datasets/<id>/lifecycle/withdraw/        owner
    POST /datasets/<id>/lifecycle/review/start/    editor
    POST /datasets/<id>/lifecycle/review/conclude/ editor: {"outcome", "comments"}
    POST /datasets/<id>/lifecycle/strip-placeholders/   owner, private draft only
    POST /datasets/<id>/lifecycle/sensitivity/     owner: the two questions
"""
import json

from django.http import Http404, JsonResponse
from django.views.decorators.http import require_GET, require_POST

from datasets import lifecycle, qc
from datasets.forms import SensitivityDeclarationForm
from datasets.models import Dataset


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


def _state(ds, user):
    editor = lifecycle.is_editor(user)
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
        "can": {
            "submit": ds.user_can_manage(user) and ds.lifecycle_state in
            lifecycle.TRANSITIONS["submit"][0] and not ds.public,
            "publish": ds.user_can_manage(user) and ds.lifecycle_state in
            lifecycle.TRANSITIONS["publish"][0],
            "withdraw": ds.user_can_manage(user) and ds.lifecycle_state == "published",
            "review": editor and ds.lifecycle_state in ("submitted", "in_review"),
        },
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
