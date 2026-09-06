"""Choosing which rows to put in front of which reviewer.

A Burmese speaker is not a Sinhala expert, and the scarce resource here is not
rows — there are only ~6,050 — but the attention of people who can read the
script. Everything in this module is about spending that attention well.

Three orderings, in priority order, and each exists for a stated reason:

* **Lint first.** A row with a machine-detectable defect is not a question for a
  linguist; it is broken. Putting those at the head of a queue gives a reviewer
  an immediate, uncontroversial win and keeps expert judgement off work a regex
  can do.
* **Then corpus frequency, descending, with unknowns last but visible.** The
  corpus counts span two orders of magnitude, so a row's reach is the best
  available proxy for whether correcting it is worth anyone's time. An
  unmeasured row sorts after the measured ones but is never treated as
  zero-frequency; ``NULL`` means "not counted", not "affects nothing".
* **Then unreviewed before reviewed.** Coverage before depth — but a reviewed
  row is still offered, because a second independent opinion is the mechanism
  that turns one person's answer into evidence.
"""

from django.db.models import Case, F, IntegerField, Q, Value, When

from .models import Posture, Rule, RuleSet


def competent_rulesets(user):
    """Rule sets the user has declared competence in."""
    if not user.is_authenticated:
        return RuleSet.objects.none()
    competences = list(user.phonetic_competences.all())
    if not competences:
        return RuleSet.objects.none()
    query = Q(pk__in=[])
    for competence in competences:
        clause = Q(language_code=competence.language_code)
        if competence.script_code:
            clause &= Q(script_code=competence.script_code)
        query |= clause
    return RuleSet.objects.filter(query, present_upstream=True)


def suggested_rulesets(accept_language, limit=8):
    """Rule sets matching the browser's advertised languages.

    An *offer*, never a claim: the browser knows how someone's interface is
    configured, which hints at what they may read and evidences nothing about
    what they can judge. Acting on a suggestion still means declaring competence.
    """
    from .iso import parse_accept_language
    matches, seen = [], set()
    for language, script, _quality in parse_accept_language(accept_language):
        if language in ('eng', ''):
            # English advertises nothing useful here: it is the default in most
            # browsers and no rule set under review is English.
            continue
        qs = RuleSet.objects.filter(language_code=language, present_upstream=True)
        if script:
            qs = qs.filter(script_code=script)
        for ruleset in qs:
            if ruleset.code not in seen:
                seen.add(ruleset.code)
                matches.append(ruleset)
        if len(matches) >= limit:
            break
    return matches[:limit]


def queue(user=None, ruleset=None, posture=None, only_lint=False, only_unreviewed=False,
          exclude_reviewed_by=None):
    """The ordered work queue. See the module docstring for why this order."""
    qs = Rule.objects.filter(present_upstream=True, ruleset__present_upstream=True)
    if ruleset is not None:
        qs = qs.filter(ruleset=ruleset)
    elif user is not None:
        qs = qs.filter(ruleset__in=competent_rulesets(user))
    if posture:
        qs = qs.filter(ruleset__posture=posture)
    if only_lint:
        qs = qs.exclude(lint_codes=[])
    if only_unreviewed:
        qs = qs.filter(review_count=0)
    if exclude_reviewed_by is not None and exclude_reviewed_by.is_authenticated:
        qs = qs.exclude(reviews__reviewer=exclude_reviewed_by, reviews__is_latest=True)
    return (qs.select_related('ruleset')
            .annotate(
                has_lint=Case(When(lint_codes=[], then=Value(0)), default=Value(1),
                              output_field=IntegerField()),
                frequency_known=Case(When(corpus_frequency__isnull=True, then=Value(0)),
                                     default=Value(1), output_field=IntegerField()),
                unreviewed=Case(When(review_count=0, then=Value(1)), default=Value(0),
                                output_field=IntegerField()),
            )
            .order_by('-has_lint', '-frequency_known', F('corpus_frequency').desc(nulls_last=True),
                      '-unreviewed', 'ruleset__code', 'row_index'))


def ruleset_progress(ruleset):
    """Counts for the progress strip. Every state is named, including 'nobody looked'."""
    rules = Rule.objects.filter(ruleset=ruleset, present_upstream=True)
    total = rules.count()
    unreviewed = rules.filter(review_count=0).count()
    disputed = rules.filter(Q(distinct_proposal_count__gt=1)
                            | (Q(correct_count__gt=0) & Q(accept_count__gt=0))).count()
    corrected = rules.filter(correct_count__gt=0).exclude(
        Q(distinct_proposal_count__gt=1) | Q(accept_count__gt=0)).count()
    unsure = rules.filter(unsure_count__gt=0, correct_count=0, accept_count=0).count()
    lint = rules.exclude(lint_codes=[]).count()
    return {
        'total': total,
        'unreviewed': unreviewed,
        'accepted': total - unreviewed - disputed - corrected - unsure,
        'corrected': corrected,
        'disputed': disputed,
        'unsure': unsure,
        'lint': lint,
        'reviewed_pct': round(100 * (total - unreviewed) / total, 1) if total else 0.0,
    }
