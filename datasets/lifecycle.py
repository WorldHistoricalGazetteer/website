"""The v4 lifecycle of a contributed dataset (place#334 §4e, §4k, Appendix E).

    uploaded -> validating -> validation_failed
                           -> draft (private; the owner links records meanwhile)
                              -> submitted -> in_review -> changes_requested -> submitted ...
                                                        -> declined
                                                        -> accepted -> published <-> withdrawn

Every transition is one function here and ONE transaction: the row is locked
(``select_for_update``), the expected ``from`` state is checked (a mismatch is refused, so two
editors cannot both conclude one review), the state is written, a ``DatasetLifecycleEvent`` is
appended, and GRACE's rows are written in the same transaction. Mail goes out only after
commit. No signal drives any of it (§4k: ``datasets/signals.py`` already re-fetches on every
save and posts to Zulip, and signals run inline in every test).

``ds_status`` and the v3 pages are untouched: ``lifecycle_state`` is a column beside it. One
v3 mechanism is reused on purpose: publish and withdraw flip ``Dataset.public`` through
``Dataset.save()``, so the existing publication side effects (``pub`` indexing, the DOI event,
the published / unpublished email) happen exactly as for any other change of ``public``.

Rulings applied: review is mandatory for every dataset in beta (Q3), so there is no
auto-accept; the review service level (Q4, 10 working days, "usually") is computed from the
review's ``sent_on`` or the submission date and never stored; the editors' QC report carries
no link counts (Q11, ``datasets/qc.py``).
"""
import datetime
import logging

from django.conf import settings
from django.db import transaction

from datasets.models import Dataset, DatasetLifecycleEvent
from whgmail.messaging import WHGmail

logger = logging.getLogger(__name__)

# name -> (states it may start from, state it ends in). None = never on the v4 lifecycle.
TRANSITIONS = {
    "upload": ({None, "draft", "validation_failed", "changes_requested", "declined"}, "uploaded"),
    "validate": ({"uploaded"}, "validating"),
    "fail_validation": ({"validating"}, "validation_failed"),
    "pass_validation": ({"validating"}, "draft"),
    "submit": ({None, "draft", "changes_requested"}, "submitted"),
    "start_review": ({"submitted"}, "in_review"),
    "accept": ({"in_review"}, "accepted"),
    "request_changes": ({"in_review"}, "changes_requested"),
    "decline": ({"in_review"}, "declined"),
    "publish": ({"accepted", "withdrawn"}, "published"),
    "withdraw": ({"published"}, "withdrawn"),
}

# conclude_review outcome -> (transition, GRACE ReviewRecommendation slug)
OUTCOMES = {
    "accept": ("accept", "approved"),
    "request_changes": ("request_changes", "changes-requested"),
    "decline": ("decline", "rejected"),
}

REVIEW_TYPE_SLUG = "pre-publication-qc"
STATE_LABELS = dict(Dataset._meta.get_field("lifecycle_state").choices)


class LifecycleRefused(Exception):
    """A transition the rules refuse; ``status`` is the HTTP status to answer with."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


def _today():
    # date.today(), not timezone.localdate(): USE_TZ is unset, so now() is naive.
    return datetime.date.today()


def is_editor(user):
    """WHG staff who may review a submission: staff, superusers, ``editorial`` and
    ``whg_admins``. External reviewers are not supported yet (Appendix E4)."""
    if not user or not getattr(user, "is_authenticated", False):
        return False
    if user.is_staff or user.is_superuser:
        return True
    return user.groups.filter(name__in=["editorial", "whg_admins"]).exists()


def _require(ok, message="not permitted"):
    if not ok:
        raise LifecycleRefused(403, message)


# ---------------------------------------------------------------------------
# The one transition routine
# ---------------------------------------------------------------------------

def _transition(dataset, name, actor, act=None, reason=""):
    """Run transition ``name`` on ``dataset`` (instance or pk) in one transaction.
    ``act(ds)`` does the transition's own work under the lock (permission checks first) and
    may return a payload dict for the event; anything it raises rolls the whole transition
    back. Returns ``(dataset, event)``."""
    froms, to = TRANSITIONS[name]
    pk = dataset.pk if isinstance(dataset, Dataset) else int(dataset)
    with transaction.atomic():
        try:
            ds = Dataset.objects.select_for_update().get(pk=pk)
        except Dataset.DoesNotExist:
            raise LifecycleRefused(404, "not found")
        current = ds.lifecycle_state
        if current not in froms:
            raise LifecycleRefused(
                409, f"cannot {name.replace('_', ' ')} a dataset that is "
                     f"{STATE_LABELS.get(current, 'not on the review workflow').lower()}")
        ds.lifecycle_state = to
        payload = (act(ds) if act else None) or {}
        # A plain UPDATE for the state: Dataset.save() fires the v3 pre_save handlers.
        # (publish / withdraw also save ``public`` themselves, deliberately.)
        Dataset.objects.filter(pk=ds.pk).update(lifecycle_state=to)
        event = DatasetLifecycleEvent.objects.create(
            dataset=ds, dataset_pk=ds.pk, dataset_label=ds.label or "", from_state=current,
            to_state=to, actor=actor if getattr(actor, "pk", None) else None,
            reason=(reason or "")[:4000], payload=payload)
    if isinstance(dataset, Dataset) and dataset is not ds:
        # The caller's instance would otherwise go on reporting the old state.
        dataset.lifecycle_state, dataset.public = ds.lifecycle_state, ds.public
    return ds, event


# ---------------------------------------------------------------------------
# GRACE (Appendix E): lazy imports, as datasets/admin.py does; GRACE depends on datasets.
# ---------------------------------------------------------------------------

def _term(model, slug, label, order):
    """A vocabulary term by slug (never by label: an editor may rename it); created if a
    fresh database has not been seeded."""
    term = model.objects.filter(slug=slug).first()
    if term is None:
        term = model.objects.filter(label=label).first() or model.objects.create(
            slug=slug, label=label, sort_order=order)
    return term


def _track(ds):
    """The GRACE record for ``ds``, created at submission (idempotent). No Person is created,
    so nothing enters the Article 14 queue; the count is read through, not copied."""
    from grace.tracking import track_dataset
    tracked, _ = track_dataset(ds, copy_count=False)
    return tracked


def _person_for(user):
    """The GRACE Person for a WHG editor, created with the internal "WHG staff" role (an
    Art. 13 case, kept out of the Art. 14 queue) if they have none."""
    from grace.admin_links import user_label
    from grace.models import Person
    from grace.vocabularies import PersonRole
    person = Person.objects.filter(user=user).first()
    if person is None:
        role = PersonRole.objects.filter(slug="whg-staff").first()
        if role is None:
            role = PersonRole.objects.create(slug="whg-staff", label="WHG staff",
                                             sort_order=100, is_internal=True)
        person = Person.objects.create(name=user_label(user), user=user, role=role)
    return person


def _open_gate_reviews(tracked):
    from grace.models import Review
    return Review.objects.filter(dataset=tracked, review_type__slug=REVIEW_TYPE_SLUG,
                                 returned_on__isnull=True)


# ---------------------------------------------------------------------------
# Mail: after commit only, never raising into the transition
# ---------------------------------------------------------------------------

def _mail_after_commit(ds, template, subject, on_sent=None, editorial_copy=False, **extra):
    owner = ds.owner
    context = {
        "template": template,
        "to_email": owner.email,
        "subject": subject,
        "greeting_name": owner.name,
        "reply_to": settings.DEFAULT_FROM_EDITORIAL,
        "dataset_title": ds.title,
        "dataset_label": ds.label,
        "dataset_id": ds.id,
        **extra,
    }
    if editorial_copy:
        context["bcc"] = [settings.DEFAULT_FROM_EDITORIAL]

    def send():
        try:
            ok = WHGmail(context=context)
        except Exception:  # noqa: BLE001 — a mail failure must not surface as a failed transition
            logger.exception("lifecycle mail %s for dataset %s failed", template, ds.pk)
            ok = False
        if ok and on_sent:
            on_sent()
    transaction.on_commit(send)


# ---------------------------------------------------------------------------
# Upload and validation (wiring into the upload path is a later step)
# ---------------------------------------------------------------------------

def record_upload(dataset, actor=None):
    return _transition(dataset, "upload", actor)


def begin_validation(dataset):
    return _transition(dataset, "validate", None, reason="validation started")


def validation_failed(dataset, errors=None):
    return _transition(dataset, "fail_validation", None, reason="validation failed",
                       act=lambda ds: {"errors": list(errors or [])[:20]})


def validation_passed(dataset):
    return _transition(dataset, "pass_validation", None, reason="validation passed")


# ---------------------------------------------------------------------------
# The contributor and the editor
# ---------------------------------------------------------------------------

def submit(dataset, actor, note=""):
    """The owner asks for review. Runs the automatic checks (their summary, with no link
    counts, is kept on the event), starts GRACE tracking, and tells the owner when to expect
    an answer; the editorial address is copied in."""
    def act(ds):
        _require(ds.user_can_manage(actor))
        if ds.public:
            raise LifecycleRefused(409, "this dataset is already public")
        from datasets.qc import qc_report, summary
        report = qc_report(ds, audience="editor")
        _track(ds)
        _mail_after_commit(
            ds, "dataset_submitted", "Your WHG dataset has been submitted for review",
            editorial_copy=True, sla_days=sla_days(),
            usually_by=review_expected_by(_today()).strftime("%-d %B %Y"))
        return {"qc": summary(report)}
    return _transition(dataset, "submit", actor, act, reason=note)


def start_review(dataset, editor):
    """An editor takes the submission: one GRACE ``Review`` (type Pre-publication QC,
    ``sent_on`` today, the editor as reviewer). A second open review is refused."""
    def act(ds):
        _require(is_editor(editor), "only WHG editors may review")
        from grace.models import Review
        from grace.vocabularies import ReviewType
        tracked = _track(ds)
        if _open_gate_reviews(tracked).exists():
            raise LifecycleRefused(409, "a review of this dataset is already open")
        review = Review.objects.create(
            dataset=tracked, reviewer=_person_for(editor), sent_on=_today(),
            review_type=_term(ReviewType, REVIEW_TYPE_SLUG, "Pre-publication QC", 15))
        return {"review": review.pk}
    return _transition(dataset, "start_review", editor, act)


def conclude_review(dataset, editor, outcome, comments=""):
    """Record the editor's decision: ``accept``, ``request_changes`` or ``decline`` (the
    last two need a reason, which the contributor is sent). The open review gets its
    ``returned_on``, recommendation and comments; ``shared_with_author_on`` is set only once
    the email has gone, so GRACE's "author not told" alarm fires on a failed send."""
    if outcome not in OUTCOMES:
        raise LifecycleRefused(400, f"outcome must be one of {sorted(OUTCOMES)}")
    name, slug = OUTCOMES[outcome]
    comments = (comments or "").strip()
    if outcome != "accept" and not comments:
        raise LifecycleRefused(400, "give the contributor a reason")
    labels = {"approved": "Approved", "changes-requested": "Changes requested",
              "rejected": "Rejected"}
    orders = {"approved": 20, "changes-requested": 25, "rejected": 40}

    def act(ds):
        _require(is_editor(editor), "only WHG editors may review")
        from grace.models import Review
        from grace.vocabularies import ReviewRecommendation
        reviews = list(_open_gate_reviews(_track(ds)).select_for_update())
        if len(reviews) != 1:
            raise LifecycleRefused(409, f"expected one open review, found {len(reviews)}")
        review = reviews[0]
        review.returned_on = _today()
        review.recommendation = _term(ReviewRecommendation, slug, labels[slug], orders[slug])
        review.comments = comments
        review.save()

        def mark_shared():
            Review.objects.filter(pk=review.pk).update(shared_with_author_on=_today())
        _mail_after_commit(
            ds, "dataset_review_outcome", _OUTCOME_SUBJECTS[outcome], on_sent=mark_shared,
            outcome=outcome, comments=comments)
        return {"review": review.pk}
    return _transition(dataset, name, editor, act, reason=comments)


_OUTCOME_SUBJECTS = {
    "accept": "Your WHG dataset has been accepted",
    "request_changes": "Your WHG dataset: changes requested",
    "decline": "Your WHG dataset: review outcome",
}


def accept(dataset, editor, comments=""):
    return conclude_review(dataset, editor, "accept", comments)


def request_changes(dataset, editor, comments):
    return conclude_review(dataset, editor, "request_changes", comments)


def decline(dataset, editor, comments):
    return conclude_review(dataset, editor, "decline", comments)


def publish(dataset, actor):
    """The owner's toggle, once the dataset is accepted (or to re-publish one they
    withdrew). Makes it public through ``Dataset.save()`` (the v3 publication side effects
    follow, see the module docstring), then flips its pending attestations to active one
    row at a time so each is forwarded to the gateway once, after commit."""
    def act(ds):
        _require(ds.user_can_manage(actor))
        if not ds.license_id:
            raise LifecycleRefused(409, "record a licence before publishing")
        from api.attestations import activate_for_dataset
        from grace.tracking import track_dataset
        ds.public = True
        ds.save(update_fields=["public", "lifecycle_state"])
        flipped = activate_for_dataset(ds.pk, by=actor)
        track_dataset(ds, copy_count=False)  # G3: upgrades the Register link once it exists
        return {"attestations_activated": flipped["activated"],
                "attestations_still_pending": flipped["still_pending"],
                "attestations_unresolved": [list(x) for x in flipped["unresolved"]][:50]}
    return _transition(dataset, "publish", actor, act)


def withdraw(dataset, actor, reason=""):
    """The owner makes a published dataset private again. Its attestations go back to
    pending one row at a time (each revoked from the gateway overlay after commit), so
    they leave every public Concordance in the same request. Refused for a dataset with
    places in the shared v3 index (place#322: that needs an editor)."""
    def act(ds):
        _require(ds.user_can_manage(actor))
        if ds.has_accessioned_places:
            raise LifecycleRefused(
                409, "this dataset has places in the shared index; contact the editorial team")
        from api.attestations import deactivate_for_dataset
        ds.public = False
        ds.save(update_fields=["public", "lifecycle_state"])
        return {"attestations_made_private": deactivate_for_dataset(ds.pk, by=actor)}
    return _transition(dataset, "withdraw", actor, act, reason=reason)


# ---------------------------------------------------------------------------
# Service level (Q4): computed, never stored
# ---------------------------------------------------------------------------

def sla_days():
    from grace.models import editorial_review_sla_days
    return editorial_review_sla_days()


def review_expected_by(start):
    from grace.models import add_working_days
    return add_working_days(start, sla_days())


# ---------------------------------------------------------------------------
# The timeline: one function, two renderings (the contributor's page and GRACE's read-out)
# ---------------------------------------------------------------------------

_STEP_LABELS = {
    "uploaded": "Uploaded",
    "validating": "File being checked",
    "validation_failed": "File needs fixing",
    "draft": "Draft",
    "submitted": "Submitted for review by the WHG team",
    "in_review": "Review started",
    "changes_requested": "Changes requested",
    "declined": "Declined",
    "accepted": "Accepted",
    "published": "Published",
    "withdrawn": "Made private",
}


def staff_timeline(dataset):
    """Every step, oldest first: ``{state, label, at, actor, note}``. ``actor`` is a display
    name (never the ORCiD-derived username) or "" for the system; ``note`` is the reason the
    contributor was given. A returned review whose outcome reached the contributor adds a
    "told" step from GRACE's ``shared_with_author_on``."""
    from grace.admin_links import user_label
    steps, concluded = [], {}
    for ev in (DatasetLifecycleEvent.objects.filter(dataset_pk=dataset.pk)
               .select_related("actor").order_by("at", "id")):
        if ev.to_state in ("accepted", "changes_requested", "declined"):
            concluded[(ev.payload or {}).get("review")] = ev.at
        steps.append({"state": ev.to_state, "label": _STEP_LABELS.get(ev.to_state, ev.to_state),
                      "at": ev.at, "actor": user_label(ev.actor) if ev.actor_id else "",
                      "note": ev.reason if ev.to_state in ("changes_requested", "declined") else ""})
    from grace.models import Review
    for r in (Review.objects.filter(dataset__dataset_id=dataset.pk,
                                    review_type__slug=REVIEW_TYPE_SLUG,
                                    shared_with_author_on__isnull=False)):
        # Only a date is stored; place the step just after the decision it reports.
        at = datetime.datetime.combine(r.shared_with_author_on, datetime.time.min)
        decided = concluded.get(r.pk)
        if decided and decided.date() == r.shared_with_author_on:
            at = decided + datetime.timedelta(microseconds=1)
        steps.append({"state": "told", "label": "Review outcome sent to the contributor",
                      "at": at, "actor": "", "note": ""})
    steps.sort(key=lambda s: s["at"])
    return steps


def timeline(dataset, viewer):
    """The contributor's timeline: the same steps as GRACE shows, for the dataset's own
    circle (owners, collaborators, staff) only. Anyone else gets an empty list, public
    dataset or not: review history is not public."""
    if not dataset.user_has_private_access(viewer):
        return []
    return staff_timeline(dataset)


def status_summary(dataset, viewer):
    """What the contributor's page says about review: the state, when it was submitted, and
    the computed "usually by" date while it is waiting. None for strangers."""
    if not dataset.user_has_private_access(viewer):
        return None
    state = (Dataset.objects.filter(pk=dataset.pk)
             .values_list("lifecycle_state", flat=True).first())
    out = {"state": state, "label": STATE_LABELS.get(state), "submitted_on": None,
           "usually_by": None, "sla_days": sla_days()}
    if state in ("submitted", "in_review"):
        ev = (DatasetLifecycleEvent.objects.filter(dataset_pk=dataset.pk, to_state="submitted")
              .order_by("-at").first())
        if ev:
            out["submitted_on"] = ev.at.date()
            out["usually_by"] = review_expected_by(ev.at.date())
    return out
