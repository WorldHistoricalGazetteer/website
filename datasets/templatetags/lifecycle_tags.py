"""The owner's "Review and publication" panel on the dataset status page (place#334 Phase 3).

Shown only to an owner or co-owner who is signed in with beta access, in the dataset's circle
(the gate of ``datasets/views_lifecycle._gate``), and only for a dataset that is on the v4
lifecycle: a NULL ``lifecycle_state`` (every v3-era dataset) renders nothing at all. The
buttons offered are exactly ``views_lifecycle.allowed_actions``; each posts to the existing
JSON endpoint, which still decides.
"""
from django import template
from django.urls import reverse

register = template.Library()

# What each state means to the owner, in plain words. "{by}" is the computed "usually by" date.
STATE_TEXT = {
    "uploaded": "Your file has been uploaded and is waiting to be checked.",
    "validating": "Your file is being checked.",
    "validation_failed": "Your file needs fixing before this dataset can go further.",
    "draft": "Private draft: only you and your collaborators can see it. When it is ready, "
             "submit it for review by the WHG team.",
    "submitted": "Submitted for review. The WHG team will usually reply by {by}.",
    "in_review": "A WHG editor is reviewing this dataset. You will usually hear by {by}.",
    "changes_requested": "The editor has asked for changes. Make them, then submit the "
                         "dataset again.",
    "declined": "The editor has declined this dataset. It stays private.",
    "accepted": "Accepted. You can publish it whenever you are ready.",
    "published": "Published: anyone can see it, and its records are in the search index.",
    "withdrawn": "Made private again. It is not in the search index. You can publish it again.",
}

PUBLISH_CONFIRM = ("Publish this dataset? It becomes public: anyone can see and download it, "
                   "its records are added to the search index, and the links you made from "
                   "its records become public too.")
WITHDRAW_CONFIRM = ("Withdraw this dataset? It becomes private again: it is removed from the "
                    "search index and its public page, and the links you made from its "
                    "records stop being public. You can publish it again later.")


def _is_owner(ds, user):
    return ds.owner_id == user.id or ds.owners.filter(id=user.id).exists()


def panel_context(ds, user):
    """The panel's context, or ``{"show": False}``. Separate from the tag for tests."""
    if (ds is None or ds.lifecycle_state is None or user is None
            or not getattr(user, "is_authenticated", False)
            or not getattr(user, "can_access_beta", False)
            or not ds.user_has_private_access(user) or not _is_owner(ds, user)):
        return {"show": False}
    from datasets import lifecycle, qc
    from datasets.forms import SensitivityDeclarationForm
    from datasets.views_lifecycle import allowed_actions
    can = allowed_actions(ds, user)
    summary = lifecycle.status_summary(ds, user) or {}
    by = summary.get("usually_by")
    text = STATE_TEXT.get(ds.lifecycle_state, "").format(
        by=by.strftime("%-d %B %Y") if by else "within about two weeks")
    # The editor's reason, when the latest decision came with one.
    note = ""
    if ds.lifecycle_state in ("changes_requested", "declined"):
        for step in reversed(lifecycle.timeline(ds, user)):
            if step["state"] == ds.lifecycle_state:
                note = step["note"]
                break
    return {
        "show": True, "ds": ds, "can": can,
        "state_label": summary.get("label") or ds.get_lifecycle_state_display(),
        "state_text": text, "editor_note": note,
        "report": qc.qc_report(ds, audience="owner"),
        # The questions are asked while the owner can still submit (they are part of what the
        # editor reads); the endpoint, not this page, decides what an answer must contain.
        "sensitivity_form": SensitivityDeclarationForm(instance=ds) if can["submit"] else None,
        "publish_confirm": PUBLISH_CONFIRM, "withdraw_confirm": WITHDRAW_CONFIRM,
        # The one-step placeholder fix, offered only where qc.strip_placeholders allows it.
        "strip_url": reverse("datasets:lifecycle-strip-placeholders", args=[ds.pk])
        if (not ds.public and ds.lifecycle_state in qc.STRIPPABLE_STATES
            and ds.user_can_manage(user)) else "",
    }


@register.inclusion_tag("datasets/_lifecycle_panel.html", takes_context=True)
def lifecycle_panel(context, ds):
    request = context.get("request")
    return panel_context(ds, getattr(request, "user", None))
