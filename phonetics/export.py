"""Export a reviewed rule set back to Epitran's ``Orth,Phon`` CSV.

Round-tripping to that format is the point: it is the consuming artefact, and a
review that cannot be handed back in the form the pipeline eats is a review
nobody can act on.

**What comes out is a proposal.** This never writes to the ``indexing`` repo and
never installs anything; it produces a candidate file for a person to look at.
Two things follow from that and are deliberate:

* Rows nobody reviewed are exported **unchanged**, and the accompanying report
  says so. Silence is not agreement, and a diff that quietly folds unexamined
  rows into "reviewed output" would launder it into agreement.
* A **disputed** row is exported unchanged too, with the competing proposals
  listed in the report. Picking a winner here would resolve by algorithm exactly
  the disagreement the app exists to preserve.
"""

import csv
import io

from .models import Rule, Verdict


def resolve(rule):
    """What this row should say, and why. Never guesses.

    Returns ``(ipa, decision, detail)`` where ``decision`` is one of
    ``unchanged``, ``corrected``, ``disputed``, ``unsure``.
    """
    latest = [r for r in rule.reviews.all() if r.is_latest]
    # Reviews of a value that has since changed upstream say nothing about the
    # value now in the file, so they cannot drive an export of it.
    current = [r for r in latest if r.reviewed_ipa == rule.current_ipa]
    if not current:
        return rule.current_ipa, 'unchanged', 'no current review'
    proposals = {r.proposed_ipa for r in current if r.verdict == Verdict.CORRECT}
    if len(proposals) > 1:
        return rule.current_ipa, 'disputed', ' / '.join(sorted(proposals))
    if proposals:
        accepts = [r for r in current if r.verdict == Verdict.ACCEPT]
        if accepts:
            return (rule.current_ipa, 'disputed',
                    f'{len(accepts)} accept vs proposal “{next(iter(proposals))}”')
        return next(iter(proposals)), 'corrected', f'{len(current)} review(s)'
    if any(r.verdict == Verdict.UNSURE for r in current) and \
            not any(r.verdict == Verdict.ACCEPT for r in current):
        return rule.current_ipa, 'unsure', 'flagged, not corrected'
    return rule.current_ipa, 'unchanged', f'{len(current)} accept(s)'


def build(ruleset):
    """``(csv_text, report)`` for one rule set."""
    rules = (Rule.objects.filter(ruleset=ruleset, present_upstream=True)
             .order_by('row_index').prefetch_related('reviews'))
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator='\n')
    writer.writerow(['Orth', 'Phon'])
    report = {'ruleset': ruleset.code, 'rows': 0, 'unchanged': 0, 'corrected': 0,
              'disputed': 0, 'unsure': 0, 'changes': [], 'held_back': []}
    for rule in rules:
        ipa, decision, detail = resolve(rule)
        writer.writerow([rule.orth_source or rule.orth, ipa])
        report['rows'] += 1
        report[decision] += 1
        if decision == 'corrected':
            report['changes'].append({'orth': rule.orth_source or rule.orth,
                                      'from': rule.current_ipa, 'to': ipa, 'detail': detail})
        elif decision in ('disputed', 'unsure'):
            report['held_back'].append({'orth': rule.orth_source or rule.orth,
                                        'ipa': rule.current_ipa, 'reason': decision,
                                        'detail': detail})
    return buffer.getvalue(), report


def suggestions_payload(ruleset=None, since=None, include_applied=False):
    """Every logged suggestion, in the form an upstream agent can act on.

    This is the machine-readable half of the round trip: agents working in the
    ``indexing`` repo read it, decide what to take up, and change the CSVs
    there. They do not have to report back — the next sync notices that a value
    now equals a proposal and stamps it adopted.
    """
    from .models import Review
    qs = (Review.objects.filter(is_latest=True, verdict=Verdict.CORRECT)
          .select_related('rule', 'rule__ruleset', 'reviewer', 'agreement'))
    if ruleset:
        qs = qs.filter(rule__ruleset=ruleset)
    if since:
        qs = qs.filter(created__gte=since)
    if not include_applied:
        qs = qs.filter(adopted_upstream_at__isnull=True)
    out = []
    for review in qs.order_by('rule__ruleset__code', 'rule__row_index', '-created'):
        agreement = review.agreement
        out.append({
            'ruleset': review.rule.ruleset.code,
            'posture': review.rule.ruleset.posture,
            'source_path': review.rule.ruleset.source_path,
            'orth': review.rule.orth_source or review.rule.orth,
            'current_ipa': review.rule.current_ipa,
            'reviewed_ipa': review.reviewed_ipa,
            'proposed_ipa': review.proposed_ipa,
            # True when the value has moved on since this was written: the
            # suggestion is about a value no longer in the file.
            'stale': review.reviewed_ipa != review.rule.current_ipa,
            'confidence': review.confidence,
            'competence': review.competence_level,
            'comment': review.comment,
            'created': review.created.isoformat(),
            'credit': (agreement.credit_name if agreement and agreement.credit_public else None),
            'licence': (agreement.terms.licence_spdx if agreement else None),
            'row_status': review.rule.status,
            'reviews_on_row': review.rule.review_count,
        })
    return out
