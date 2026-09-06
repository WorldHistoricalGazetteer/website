"""Views for the grapheme→IPA review UI (place#252).

Access, in three layers, because they answer three different questions.

**Reading is public.** Anyone can browse the rule sets, see what is proposed,
see where reviewers disagree, and run a name through the sandbox without an
account. That is deliberate: this is scholarly work in progress, and a review
record nobody outside can inspect is not a review record. Signing in is required
only to *contribute* — a suggestion has to be attributable, and a contributor
has to have been shown the licence they are agreeing to.

**Before launch, reading is not yet public.** ``settings.PHONETICS_PUBLIC``
holds the app to staff and beta users while it is being finished; everyone else
gets a 404 rather than a "not yet" page. Flipping it is not sufficient on its
own — :func:`_gate` also requires contribution terms someone has signed off,
because non-negotiable 6 of place#252 is that licensing is settled before launch
and a flag someone has to remember is not a settlement.

**Staff only** for the sync, and for the machine-readable suggestion feed, which
carries reviewer comments and attribution.
"""

import json
from datetime import timedelta

from django.conf import settings
from django.contrib import messages
from django.contrib.auth.decorators import login_required
from django.http import (Http404, HttpResponse, HttpResponseBadRequest,
                         JsonResponse)
from django.shortcuts import get_object_or_404, redirect, render
from django.utils import timezone
from django.views.decorators.http import require_POST

from . import export as export_mod
from . import routing
from .forms import AgreementForm, CompetenceForm, PolicyAnswerForm, ReviewForm
from .iso import language_name, script_name
from .lint import LINT_CODES
from .models import (ContributionTerms, Posture, PolicyAnswer, PolicyQuestion,
                     Review, ReviewerAgreement, ReviewerCompetence, Rule,
                     RuleSet, Verdict, active_terms)
from .transcribe import build_map, compare, transcribe
from .validation import codepoints, nfd, panphon_provenance, validate_ipa


def _gate(request):
    """404 unless this visitor may see the app at all.

    Once launched the answer is "everyone", including anonymous visitors —
    reading is public and only contributing needs an account. Until then it is
    staff and beta testers. Launch takes two things, not one: the flag *and*
    contribution terms someone has actually signed off, so the app cannot go
    live collecting work under wording nobody approved.
    """
    terms = active_terms()
    if getattr(settings, 'PHONETICS_PUBLIC', False) and terms and terms.signed_off:
        return
    if not request.user.is_authenticated or not getattr(request.user, 'can_access_beta', False):
        raise Http404()


def _contribution_state(request):
    """What this visitor may do, resolved once and handed to the template.

    Anonymous visitors see everything and are told plainly what signing in would
    let them add — rather than being shown a form that fails.
    """
    if not request.user.is_authenticated:
        return {'can_contribute': False, 'needs_login': True, 'needs_terms': False}
    terms, agreement = _agreement(request.user)
    return {'can_contribute': bool(terms and agreement),
            'needs_login': False,
            'needs_terms': bool(terms and not agreement),
            'terms_closed': terms is None}


def _agreement(user):
    terms = active_terms()
    if not terms or not user.is_authenticated:
        return terms, None
    return terms, ReviewerAgreement.objects.filter(user=user, terms=terms).first()


def _require_agreement(request):
    """Returns a redirect when the reviewer has not yet accepted current terms."""
    terms, agreement = _agreement(request.user)
    if terms is None:
        messages.error(request, 'Contributions are closed: no contribution terms are '
                                'active, so there is nothing you could be agreeing to.')
        return redirect('phonetics:home')
    if agreement is None:
        return redirect('phonetics:terms')
    return None


# ── Landing ──────────────────────────────────────────────────────────────────

def home(request):
    _gate(request)
    terms, agreement = _agreement(request.user)
    competences = (list(request.user.phonetic_competences.all())
                   if request.user.is_authenticated else [])
    mine = (routing.competent_rulesets(request.user).order_by('language_name')
            if request.user.is_authenticated else RuleSet.objects.none())
    suggested = [rs for rs in routing.suggested_rulesets(
        request.META.get('HTTP_ACCEPT_LANGUAGE', ''))
        if not any(c.matches(rs) for c in competences)]

    totals = {
        'rulesets': RuleSet.objects.filter(present_upstream=True).count(),
        'rules': Rule.objects.filter(present_upstream=True).count(),
        'lint': Rule.objects.filter(present_upstream=True).exclude(lint_codes=[]).count(),
        'reviews': Review.objects.filter(is_latest=True).count(),
        'reviewed_rules': Rule.objects.filter(present_upstream=True,
                                              review_count__gt=0).count(),
    }
    return render(request, 'phonetics/home.html', {
        'terms': terms,
        'agreement': agreement,
        'competences': competences,
        'my_rulesets': mine,
        'suggested_rulesets': suggested,
        'open_questions': PolicyQuestion.objects.filter(status=PolicyQuestion.OPEN)[:10],
        'totals': totals,
        'my_reviews': (Review.objects.filter(reviewer=request.user, is_latest=True)
                       .select_related('rule', 'rule__ruleset')[:10]
                       if request.user.is_authenticated else []),
        'my_review_count': (Review.objects.filter(reviewer=request.user).count()
                            if request.user.is_authenticated else 0),
        'my_adopted_count': (Review.objects.filter(reviewer=request.user,
                                                   adopted_upstream_at__isnull=False).count()
                             if request.user.is_authenticated else 0),
        **_contribution_state(request),
    })


@login_required
def competence(request):
    """Declare what you can judge. Self-declared, and recorded as such."""
    _gate(request)
    if request.method == 'POST':
        form = CompetenceForm(request.POST)
        if form.is_valid():
            obj, created = ReviewerCompetence.objects.update_or_create(
                user=request.user,
                language_code=form.cleaned_data['language_code'],
                script_code=form.cleaned_data['script_code'],
                defaults={'level': form.cleaned_data['level'],
                          'note': form.cleaned_data['note']})
            messages.success(request, f'Recorded: {language_name(obj.language_code)}'
                                      f'{" / " + script_name(obj.script_code) if obj.script_code else ""}.')
            return redirect('phonetics:competence')
    else:
        form = CompetenceForm()

    # Only offer languages that actually have a rule set to review; a picker of
    # all 7,923 ISO codes would be a list of things this tool cannot do.
    available = (RuleSet.objects.filter(present_upstream=True)
                 .values('language_code', 'language_name', 'script_code', 'script_name', 'code')
                 .order_by('language_name'))
    return render(request, 'phonetics/competence.html', {
        'form': form,
        'competences': request.user.phonetic_competences.all(),
        'available': available,
        'suggested': routing.suggested_rulesets(request.META.get('HTTP_ACCEPT_LANGUAGE', '')),
        **_contribution_state(request),
    })


@login_required
@require_POST
def competence_delete(request, pk):
    _gate(request)
    obj = get_object_or_404(ReviewerCompetence, pk=pk, user=request.user)
    obj.delete()
    messages.success(request, 'Removed.')
    return redirect('phonetics:competence')


@login_required
def terms(request):
    """Read and accept the contribution terms; settle attribution at the same time."""
    _gate(request)
    current = active_terms()
    if current is None:
        return render(request, 'phonetics/terms.html', {'terms': None})
    existing = ReviewerAgreement.objects.filter(user=request.user, terms=current).first()
    if request.method == 'POST' and existing is None:
        form = AgreementForm(request.POST)
        if form.is_valid():
            ReviewerAgreement.objects.create(
                user=request.user, terms=current,
                credit_name=form.cleaned_data['credit_name'],
                credit_public=form.cleaned_data['credit_public'],
                orcid=form.cleaned_data['orcid'])
            messages.success(request, 'Thank you — you can now record reviews.')
            return redirect(request.GET.get('next') or 'phonetics:home')
    else:
        initial = {'credit_name': (request.user.get_full_name() or '').strip(),
                   'credit_public': True}
        form = AgreementForm(initial=initial)
    return render(request, 'phonetics/terms.html',
                  {'terms': current, 'form': form, 'agreement': existing})


# ── Reviewing ────────────────────────────────────────────────────────────────

def review_queue(request):
    _gate(request)
    code = request.GET.get('ruleset') or ''
    ruleset = RuleSet.objects.filter(code=code).first() if code else None
    signed_in = request.user.is_authenticated
    rules = routing.queue(
        user=request.user if (signed_in and not ruleset) else None,
        ruleset=ruleset,
        posture=request.GET.get('posture') or None,
        only_lint=request.GET.get('lint') == '1',
        only_unreviewed=request.GET.get('unreviewed') == '1',
        exclude_reviewed_by=(request.user if signed_in and request.GET.get('mine') != '1'
                             else None),
    )
    return render(request, 'phonetics/queue.html', {
        'ruleset': ruleset,
        'rules': rules[:50],
        'remaining': rules.count(),
        'lint_codes': LINT_CODES,
        'postures': Posture.choices,
        'filters': {'lint': request.GET.get('lint') == '1',
                    'unreviewed': request.GET.get('unreviewed') == '1',
                    'posture': request.GET.get('posture') or '',
                    'mine': request.GET.get('mine') == '1'},
        'has_competence': signed_in and request.user.phonetic_competences.exists(),
        **_contribution_state(request),
    })


def rule_detail(request, pk):
    """One row: the unit of work. Judgeable in seconds, or skippable in one."""
    _gate(request)
    rule = get_object_or_404(
        Rule.objects.select_related('ruleset').prefetch_related('reviews__reviewer',
                                                                'reviews__agreement'),
        pk=pk)
    redirect_response = None
    if request.method == 'POST':
        if not request.user.is_authenticated:
            # Reading is public; contributing is not. Send them to sign in and
            # back, rather than silently discarding what they typed.
            return redirect(f"{settings.LOGIN_URL}?next={request.path}")
        redirect_response = _require_agreement(request)

    if request.method == 'POST' and redirect_response is None:
        form = ReviewForm(request.POST, rule=rule)
        if form.is_valid():
            _, agreement = _agreement(request.user)
            competence = next(
                (c for c in request.user.phonetic_competences.all() if c.matches(rule.ruleset)),
                None)
            Review.objects.create(
                rule=rule, reviewer=request.user,
                verdict=form.cleaned_data['verdict'],
                proposed_ipa=form.cleaned_data['proposed_ipa'],
                # Snapshot, not a reference: an upstream edit must never
                # silently reassign this judgement to a value nobody saw.
                reviewed_ipa=rule.current_ipa,
                reviewed_version=rule.ruleset.current_version,
                confidence=form.cleaned_data.get('confidence') or 'medium',
                comment=form.cleaned_data['comment'],
                competence_level=competence.level if competence else '',
                agreement=agreement,
                **form.provenance and {'panphon_version': form.provenance['panphon_version'],
                                       'ipa_all_sha256': form.provenance['ipa_all_sha256']} or {},
            )
            messages.success(request, 'Recorded — thank you.')
            nxt = request.POST.get('next')
            return redirect(nxt) if nxt else redirect('phonetics:queue')
    elif redirect_response is not None:
        return redirect_response
    else:
        form = ReviewForm(rule=rule)

    me = request.user.pk if request.user.is_authenticated else None
    others = [r for r in rule.reviews.all() if r.is_latest and r.reviewer_id != me]
    mine = next((r for r in rule.reviews.all()
                 if r.is_latest and r.reviewer_id == me), None)
    history = [r for r in rule.reviews.all() if not r.is_latest]

    siblings = (Rule.objects.filter(ruleset=rule.ruleset, present_upstream=True)
                .order_by('row_index'))
    return render(request, 'phonetics/rule.html', {
        'rule': rule,
        'form': form,
        'others': others,
        'mine': mine,
        'history': history,
        'lint_codes': LINT_CODES,
        'next_rule': siblings.filter(row_index__gt=rule.row_index).first(),
        'questions': _questions_for(rule.ruleset),
        **_contribution_state(request),
        # Sample of the whole map, so the sandbox on this page can run without
        # a second round trip.
        'map_json': json.dumps({r.orth: r.current_ipa for r in siblings}),
    })


def _questions_for(ruleset):
    return PolicyQuestion.objects.filter(
        status=PolicyQuestion.OPEN).filter(
        models_q(ruleset)).distinct()


def models_q(ruleset):
    from django.db.models import Q
    return Q(ruleset=ruleset) | Q(ruleset__isnull=True, language_code=ruleset.language_code)


# ── Rule sets ────────────────────────────────────────────────────────────────

def ruleset_list(request):
    _gate(request)
    rulesets = RuleSet.objects.filter(present_upstream=True)
    query = (request.GET.get('q') or '').strip()
    if query:
        from django.db.models import Q
        rulesets = rulesets.filter(
            Q(code__icontains=query) | Q(language_name__icontains=query)
            | Q(script_name__icontains=query))
    posture = request.GET.get('posture')
    if posture:
        rulesets = rulesets.filter(posture=posture)
    rows = [{'ruleset': rs, 'progress': routing.ruleset_progress(rs)} for rs in rulesets]
    return render(request, 'phonetics/ruleset_list.html', {
        'rows': rows, 'q': query, 'posture': posture or '',
        'postures': Posture.choices,
        'suggested': routing.suggested_rulesets(request.META.get('HTTP_ACCEPT_LANGUAGE', '')),
    })


def ruleset_detail(request, code):
    _gate(request)
    ruleset = get_object_or_404(RuleSet, code=code)
    rules = (Rule.objects.filter(ruleset=ruleset).order_by('row_index')
             .prefetch_related('reviews__reviewer'))
    return render(request, 'phonetics/ruleset.html', {
        'ruleset': ruleset,
        'rules': rules,
        'progress': routing.ruleset_progress(ruleset),
        'version': ruleset.current_version,
        'questions': _questions_for(ruleset),
        'lint_codes': LINT_CODES,
        'my_competence': (next((c for c in request.user.phonetic_competences.all()
                                if c.matches(ruleset)), None)
                          if request.user.is_authenticated else None),
        **_contribution_state(request),
    })


def sandbox(request, code):
    """Run the rule sheet over a name and see exactly what it does to it.

    ⚠ This applies the *map only* — no Epitran pre/post-processing — and says so
    on the page. It is here because residue is the measurement that matters:
    the characters no rule matched are shown in brackets, which is what turns
    "16.6% of names convert" from a statistic into something you can look at.
    """
    _gate(request)
    ruleset = get_object_or_404(RuleSet, code=code)
    rules = list(Rule.objects.filter(ruleset=ruleset, present_upstream=True)
                 .order_by('row_index'))
    pairs = [(r.orth, r.current_ipa) for r in rules]
    samples = []
    seen = set()
    for rule in rules:
        for example in (rule.examples or []):
            name = example.get('name')
            if name and name not in seen:
                seen.add(name)
                samples.append(name)
    names = [n for n in (request.POST.get('names') or '').splitlines() if n.strip()]
    results = [transcribe(n.strip(), build_map(pairs)) | {'name': n.strip()} for n in names]
    return render(request, 'phonetics/sandbox.html', {
        'ruleset': ruleset, 'results': results,
        'names_text': request.POST.get('names') or '\n'.join(samples[:10]),
        'sample_count': len(samples),
        'complete': sum(1 for r in results if r['complete']),
    })


def export_csv(request, code):
    """The proposed rule set, in the format the consumer eats."""
    _gate(request)
    ruleset = get_object_or_404(RuleSet, code=code)
    text, _report = export_mod.build(ruleset)
    response = HttpResponse(text, content_type='text/csv; charset=utf-8')
    response['Content-Disposition'] = f'attachment; filename="{ruleset.code}.csv"'
    return response


def export_report(request, code):
    """What the export did and, more usefully, what it declined to do."""
    _gate(request)
    ruleset = get_object_or_404(RuleSet, code=code)
    _text, report = export_mod.build(ruleset)
    return JsonResponse(report)


def policy_question(request, slug):
    """A decision that changes many rows at once."""
    _gate(request)
    question = get_object_or_404(PolicyQuestion, slug=slug)
    if request.method == 'POST':
        if not request.user.is_authenticated:
            return redirect(f"{settings.LOGIN_URL}?next={request.path}")
        redirect_response = _require_agreement(request)
        if redirect_response is not None:
            return redirect_response
        form = PolicyAnswerForm(request.POST, question=question)
        if form.is_valid():
            competence = None
            if question.ruleset_id:
                competence = next((c for c in request.user.phonetic_competences.all()
                                   if c.matches(question.ruleset)), None)
            _, agreement = _agreement(request.user)
            PolicyAnswer.objects.create(
                question=question, user=request.user,
                option_key=form.cleaned_data['option_key'],
                comment=form.cleaned_data['comment'],
                competence_level=competence.level if competence else '',
                agreement=agreement)
            messages.success(request, 'Answer recorded.')
            return redirect('phonetics:question', slug=slug)
    else:
        form = PolicyAnswerForm(question=question)
    return render(request, 'phonetics/question.html', {
        'question': question,
        'form': form,
        'tally': question.tally(),
        'answers': question.answers.filter(is_latest=True).select_related('user'),
        'mine': (question.answers.filter(user=request.user, is_latest=True).first()
                 if request.user.is_authenticated else None),
        **_contribution_state(request),
    })


def lint_queue(request):
    """Every machine-detectable defect, across every rule set.

    Not a review queue: these are not questions. They are rows a regex can prove
    are broken, listed so they get fixed upstream instead of consuming a
    linguist's attention.
    """
    _gate(request)
    rules = (Rule.objects.filter(present_upstream=True).exclude(lint_codes=[])
             .select_related('ruleset').order_by('ruleset__code', 'row_index'))
    code = request.GET.get('code')
    if code:
        rules = [r for r in rules if code in r.lint_codes]
    else:
        rules = list(rules)
    tally = {}
    for rule in Rule.objects.filter(present_upstream=True).exclude(lint_codes=[]):
        for defect in rule.lint_codes:
            tally[defect] = tally.get(defect, 0) + 1
    return render(request, 'phonetics/lint.html', {
        'rules': rules[:500],
        'total': len(rules),
        'tally': [{'code': k, 'label': LINT_CODES.get(k, (k, ''))[0],
                   'why': LINT_CODES.get(k, ('', ''))[1], 'count': v}
                  for k, v in sorted(tally.items(), key=lambda kv: -kv[1])],
        'selected': code or '',
        'files': (Rule.objects.filter(present_upstream=True).exclude(lint_codes=[])
                  .values('ruleset__code').distinct().count()),
        **_contribution_state(request),
    })


@login_required
def suggestions_json(request):
    """Machine-readable feed of logged suggestions, for agents working upstream.

    Staff-only: it carries reviewer comments and attribution, and it is a work
    queue for the ``indexing`` repo rather than a public artefact.
    """
    _gate(request)
    if not request.user.is_staff:
        raise Http404()
    code = request.GET.get('ruleset')
    ruleset = RuleSet.objects.filter(code=code).first() if code else None
    days = request.GET.get('days')
    since = timezone.now() - timedelta(days=int(days)) if days and days.isdigit() else None
    return JsonResponse({
        'generated': timezone.now().isoformat(),
        'validated_with': panphon_provenance(),
        'note': 'Proposals only. Nothing here has been applied; apply upstream in '
                'the indexing repo and the next sync will mark adopted proposals.',
        'suggestions': export_mod.suggestions_payload(
            ruleset=ruleset, since=since,
            include_applied=request.GET.get('applied') == '1'),
    }, json_dumps_params={'ensure_ascii': False})


# ── AJAX ─────────────────────────────────────────────────────────────────────

def api_validate(request):
    """Live validation as the reviewer types. Same checks as the form."""
    _gate(request)
    value = request.GET.get('ipa', '')
    normalised, errors, segments = validate_ipa(value)
    return JsonResponse({
        'value': normalised,
        'codepoints': codepoints(normalised),
        'segments': segments,
        'errors': errors,
        'ok': not errors,
    }, json_dumps_params={'ensure_ascii': False})


@require_POST
def api_transcribe(request):
    """What a name becomes under this rule set, optionally with an override applied."""
    _gate(request)
    try:
        payload = json.loads(request.body or '{}')
    except ValueError:
        return HttpResponseBadRequest('bad json')
    ruleset = RuleSet.objects.filter(code=payload.get('ruleset')).first()
    if ruleset is None:
        raise Http404()
    pairs = list(Rule.objects.filter(ruleset=ruleset, present_upstream=True)
                 .values_list('orth', 'current_ipa'))
    names = [n for n in (payload.get('names') or []) if n]
    overrides = payload.get('overrides') or {}
    return JsonResponse({
        'results': [compare(name, pairs, overrides) for name in names[:25]],
    }, json_dumps_params={'ensure_ascii': False})


@login_required
@require_POST
def api_sync(request):
    """Staff 'sync now'. Scheduled sync does the same thing on a timer."""
    _gate(request)
    if not request.user.is_staff:
        raise Http404()
    from .sync import sync_all
    try:
        summary = sync_all()
    except Exception as exc:
        return JsonResponse({'ok': False, 'error': str(exc)}, status=502)
    return JsonResponse({'ok': True, 'summary': summary})
