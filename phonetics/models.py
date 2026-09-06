"""Grapheme→IPA rule review (place#252).

Reviewers correct the rule sets that turn a place name into IPA for
cross-script matching. Three things about the shape here are deliberate and
easy to undo by accident:

**Nothing in this app edits a rule set.** The CSVs live in the ``indexing``
repo and are changed there, by hand or by agents processing what is recorded
here. :class:`Rule` is a read-only mirror of the file as fetched; a reviewer's
work is a :class:`Review`, which is a *proposal*. Export writes a candidate
file for a human to look at; installation stays a separate, deliberate step
somewhere else.

**Silence is not agreement.** A row nobody has looked at and a row every
reviewer accepted must never be the same state. There is no "approved"
boolean; status is derived from the reviews that exist, and the absence of
reviews is its own named state (:attr:`Rule.UNREVIEWED`).

**Reviews are append-only.** Disagreement is the signal we are collecting, so
nothing is overwritten and no minority answer is resolved away. A reviewer who
changes their mind adds a row and the old one stops being ``is_latest``.
"""

import unicodedata

from django.conf import settings
from django.db import models
from django.db.models import Q
from django.urls import reverse
from django.utils import timezone

from .validation import codepoints, nfd


class Posture(models.TextChoices):
    """What a reviewer is actually being asked, which differs by rule set.

    Conflating these produces rubber-stamping: shown a shipped value in the
    same queue as a proposed one, a reviewer approves production values they
    never examined. The distinction is carried in the data and shown on screen.
    """

    PROPOSED = 'proposed', 'Proposed — drafted by WHG, not installed, awaiting judgement'
    SHIPPED = 'shipped', 'In production — presumed adequate until someone says otherwise'


class RuleSet(models.Model):
    """One language+script rule set from one source, e.g. shipped ``mya-Mymr``.

    ⚠ **A code is not unique.** ``mya-Mymr`` exists both as a shipped rule set and
    as a WHG draft proposing to replace it, and they are different artefacts with
    different postures and separate review histories. Keying on the code alone
    would let a draft sync overwrite the live rows, flip their posture, and
    silently re-point every review made against them onto values nobody had seen
    — which is precisely the conflation the two postures exist to prevent. The
    identity is therefore ``slug`` (``mya-Mymr`` / ``mya-Mymr.draft``), with
    ``(code, posture)`` unique as a second line of defence.
    """

    slug = models.CharField(max_length=48, unique=True,
                            help_text="URL key: the code, plus '.draft' for a proposed set.")
    code = models.CharField(max_length=32, db_index=True,
                            help_text="Epitran mode code, e.g. 'mya-Mymr'. NOT unique.")
    language_code = models.CharField(max_length=8, db_index=True, help_text='ISO 639-3.')
    script_code = models.CharField(max_length=8, db_index=True, help_text='ISO 15924.')
    language_name = models.CharField(max_length=128, blank=True)
    script_name = models.CharField(max_length=128, blank=True)

    posture = models.CharField(max_length=16, choices=Posture.choices, default=Posture.SHIPPED)

    source_repo = models.CharField(max_length=128, blank=True)
    source_ref = models.CharField(max_length=128, blank=True)
    source_path = models.CharField(max_length=512, blank=True)

    # How many real place names in the corpus are written in this script, from
    # the indexing side. NULL is "not measured", which must not sort as zero:
    # prioritisation by frequency is only meaningful where frequency is known.
    corpus_name_count = models.PositiveIntegerField(null=True, blank=True)
    # Share of corpus names the current rules convert completely (place#251's
    # measured table). NULL where unmeasured.
    conversion_rate = models.FloatField(null=True, blank=True)

    notes = models.TextField(blank=True)
    # False once a sync no longer finds the file upstream. Rows and reviews are
    # kept: a rule set withdrawn upstream must not silently delete the record of
    # work people did on it.
    present_upstream = models.BooleanField(default=True)

    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['language_name', 'code']
        unique_together = [('code', 'posture')]

    def __str__(self):
        return self.slug

    def get_absolute_url(self):
        return reverse('phonetics:ruleset', args=[self.slug])

    @staticmethod
    def make_slug(code, posture):
        return code if posture == Posture.SHIPPED else f'{code}.draft'

    @property
    def counterpart(self):
        """The same code under the other posture, where one exists.

        A draft is a *proposal to replace* something live, and a reviewer judging
        it should be one click from what it would replace. Where no counterpart
        exists the draft is new coverage rather than a replacement, which is also
        worth being able to see."""
        return (RuleSet.objects.filter(code=self.code, present_upstream=True)
                .exclude(pk=self.pk).first())

    @property
    def label(self):
        name = self.language_name or self.language_code
        script = self.script_name or self.script_code
        return f'{name} ({script})'

    @property
    def is_proposed(self):
        return self.posture == Posture.PROPOSED

    @property
    def current_version(self):
        return self.versions.filter(is_current=True).first()


class RuleSetVersion(models.Model):
    """One fetched state of one rule-set file.

    Keyed by the git blob sha, which *is* the content identity — so a sync that
    finds the same blob writes nothing, and a review can name exactly which
    bytes it was made against.
    """

    ruleset = models.ForeignKey(RuleSet, on_delete=models.CASCADE, related_name='versions')
    blob_sha = models.CharField(max_length=64, db_index=True)
    commit_sha = models.CharField(max_length=64, blank=True)
    fetched_at = models.DateTimeField(default=timezone.now)
    row_count = models.PositiveIntegerField(default=0)
    is_current = models.BooleanField(default=True)

    class Meta:
        ordering = ['-fetched_at']
        unique_together = [('ruleset', 'blob_sha')]

    def __str__(self):
        return f'{self.ruleset.code}@{self.blob_sha[:8]}'


class Rule(models.Model):
    """One row of one rule set: a grapheme and the IPA it currently produces.

    A mirror of the upstream file, never edited here. ``orth`` and
    ``current_ipa`` are stored NFD — see :mod:`phonetics.validation` for why
    that choice is load-bearing rather than cosmetic.
    """

    UNREVIEWED = 'unreviewed'
    ACCEPTED = 'accepted'
    DISPUTED = 'disputed'
    UNSURE = 'unsure'
    CORRECTION_PROPOSED = 'correction_proposed'

    STATUS_LABELS = {
        UNREVIEWED: 'Not yet reviewed',
        ACCEPTED: 'Accepted by every reviewer so far',
        CORRECTION_PROPOSED: 'Correction proposed',
        DISPUTED: 'Reviewers disagree',
        UNSURE: 'Reviewer(s) unsure',
    }

    ruleset = models.ForeignKey(RuleSet, on_delete=models.CASCADE, related_name='rules')
    orth = models.CharField(max_length=64, help_text='Grapheme, NFD-normalised.')
    orth_source = models.CharField(max_length=64, blank=True,
                                   help_text='Grapheme exactly as written in the file.')
    current_ipa = models.CharField(max_length=64, blank=True, help_text='NFD-normalised.')
    current_ipa_source = models.CharField(max_length=64, blank=True)
    row_index = models.PositiveIntegerField(default=0)

    first_version = models.ForeignKey(RuleSetVersion, on_delete=models.SET_NULL, null=True,
                                      blank=True, related_name='rules_first_seen')
    last_version = models.ForeignKey(RuleSetVersion, on_delete=models.SET_NULL, null=True,
                                     blank=True, related_name='rules_last_seen')
    # False once the row disappears from upstream. Kept, with its reviews.
    present_upstream = models.BooleanField(default=True)

    # How many corpus names contain this grapheme. NULL = not measured; the UI
    # must say so rather than render it as 0, which would read as "affects
    # nothing" and deprioritise a row we simply have not counted.
    corpus_frequency = models.PositiveIntegerField(null=True, blank=True)
    # [{'name': 'ကရပ်ကွက်', 'output': 'krpkwk', 'complete': false}, …]
    examples = models.JSONField(default=list, blank=True)

    # Why this value was drafted, from the rule set's .NOTES.tsv companion. Only
    # newly drafted rows have one, and it is the most useful thing on the page for
    # a reviewer: it says what the drafter was reasoning from and where they were
    # unsure, which is exactly where an expert's judgement is worth most.
    draft_note = models.TextField(blank=True)

    # Machine-detectable defects, from phonetics.lint. Cached so the queue can
    # be ordered and filtered in SQL; recomputed on every sync.
    lint_codes = models.JSONField(default=list, blank=True)

    # Denormalised review tallies, maintained in Review.save(). Present so a
    # 6,000-row queue can be sorted and filtered without a subquery per row;
    # `review_count == 0` is the UNREVIEWED state and is never inferred from
    # the absence of anything else.
    review_count = models.PositiveIntegerField(default=0)
    accept_count = models.PositiveIntegerField(default=0)
    correct_count = models.PositiveIntegerField(default=0)
    unsure_count = models.PositiveIntegerField(default=0)
    distinct_proposal_count = models.PositiveIntegerField(default=0)
    # Reviews made against an IPA value that has since changed upstream. They
    # are evidence about the old value and must not be read as being about the
    # new one.
    stale_review_count = models.PositiveIntegerField(default=0)

    class Meta:
        ordering = ['ruleset', 'row_index']
        unique_together = [('ruleset', 'orth')]
        indexes = [
            models.Index(fields=['ruleset', 'review_count']),
            models.Index(fields=['corpus_frequency']),
        ]

    def __str__(self):
        return f'{self.ruleset.code} {self.orth} → {self.current_ipa or "∅(blank)"}'

    def save(self, *args, **kwargs):
        self.orth = nfd(self.orth)
        self.current_ipa = nfd(self.current_ipa)
        super().save(*args, **kwargs)

    @property
    def orth_codepoints(self):
        return codepoints(self.orth)

    @property
    def ipa_codepoints(self):
        return codepoints(self.current_ipa)

    @property
    def lint_details(self):
        """``[{'code', 'label', 'why'}, …]`` — resolved for display."""
        from .lint import LINT_CODES
        return [{'code': c, 'label': LINT_CODES.get(c, (c, ''))[0],
                 'why': LINT_CODES.get(c, ('', ''))[1]} for c in (self.lint_codes or [])]

    @property
    def status(self):
        """Derived, never stored. ``UNREVIEWED`` is a first-class answer."""
        if self.review_count == 0:
            return self.UNREVIEWED
        if self.correct_count and self.accept_count:
            return self.DISPUTED
        if self.distinct_proposal_count > 1:
            return self.DISPUTED
        if self.correct_count:
            return self.CORRECTION_PROPOSED
        if self.unsure_count and not self.accept_count:
            return self.UNSURE
        return self.ACCEPTED

    @property
    def status_label(self):
        return self.STATUS_LABELS[self.status]

    def recount(self, save=True):
        """Recompute the cached tallies from the reviews that exist."""
        latest = self.reviews.filter(is_latest=True)
        self.review_count = latest.count()
        self.accept_count = latest.filter(verdict=Verdict.ACCEPT).count()
        self.correct_count = latest.filter(verdict=Verdict.CORRECT).count()
        self.unsure_count = latest.filter(verdict=Verdict.UNSURE).count()
        self.distinct_proposal_count = (
            latest.filter(verdict=Verdict.CORRECT)
            .values('proposed_ipa').distinct().count()
        )
        self.stale_review_count = latest.exclude(reviewed_ipa=self.current_ipa).count()
        if save:
            super(Rule, self).save(update_fields=[
                'review_count', 'accept_count', 'correct_count', 'unsure_count',
                'distinct_proposal_count', 'stale_review_count'])


class Verdict(models.TextChoices):
    ACCEPT = 'accept', 'The current value is right'
    CORRECT = 'correct', 'The current value is wrong — here is a better one'
    UNSURE = 'unsure', 'I am not sure — flag for someone else'


class Confidence(models.TextChoices):
    HIGH = 'high', 'Confident'
    MEDIUM = 'medium', 'Fairly confident'
    LOW = 'low', 'Tentative'


class CompetenceLevel(models.TextChoices):
    NATIVE = 'native', 'Native speaker'
    FLUENT = 'fluent', 'Fluent'
    ACADEMIC = 'academic', 'Academic/professional knowledge of the language'
    WORKING = 'working', 'Working knowledge'
    SCRIPT_ONLY = 'script_only', 'I read the script but do not speak the language'


class ReviewerCompetence(models.Model):
    """A reviewer's own statement of what they can judge.

    Self-declared, and recorded as such. Nobody is vetted and no claim here is
    treated as authority: it routes work to people, and the disagreement data
    does the rest.
    """

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE,
                             related_name='phonetic_competences')
    language_code = models.CharField(max_length=8, db_index=True)
    # Blank means "any script this language is written in".
    script_code = models.CharField(max_length=8, blank=True, db_index=True)
    level = models.CharField(max_length=16, choices=CompetenceLevel.choices)
    note = models.TextField(blank=True)
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = [('user', 'language_code', 'script_code')]
        ordering = ['language_code', 'script_code']

    def __str__(self):
        scope = f'{self.language_code}-{self.script_code}' if self.script_code else self.language_code
        return f'{self.user} {scope} ({self.level})'

    @property
    def language_display(self):
        from .iso import autonym, language_name
        name = language_name(self.language_code)
        own = autonym(self.language_code)
        return f'{name} — {own}' if own else name

    @property
    def script_display(self):
        from .iso import script_name
        return script_name(self.script_code) if self.script_code else ''

    def matches(self, ruleset):
        return (self.language_code == ruleset.language_code
                and self.script_code in ('', ruleset.script_code))


class ContributionTerms(models.Model):
    """The licence a reviewer agrees to at the point of contributing.

    Non-negotiable 6 of place#252: settled before launch, not after. That is
    enforced rather than documented — :func:`active_terms` is what the submit
    path requires, and the public launch gate additionally requires
    ``signed_off``. Draft terms let the tool be exercised in beta without ever
    letting it collect a contribution under wording nobody approved.
    """

    version = models.CharField(max_length=32, unique=True)
    title = models.CharField(max_length=200)
    body = models.TextField(help_text='Shown in full at the point of contribution.')
    licence_spdx = models.CharField(
        max_length=64, default='CC-BY-4.0',
        help_text="SPDX id, matching the licensing app's vocabulary. Must permit "
                  "inclusion in the rule sets these corrections feed, which are MIT.")
    licence = models.ForeignKey(
        'licensing.License', on_delete=models.PROTECT, null=True, blank=True,
        related_name='phonetic_contribution_terms',
        help_text="The row in WHG's licence vocabulary, so this page and /licenses/ "
                  "cannot drift apart.")
    is_active = models.BooleanField(default=False)
    # False while the wording is a draft. The app will not go public on it.
    signed_off = models.BooleanField(default=False)
    signed_off_note = models.TextField(blank=True)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ['-created']
        verbose_name_plural = 'contribution terms'

    def __str__(self):
        return f'{self.version} ({self.licence_spdx})'


def active_terms():
    return ContributionTerms.objects.filter(is_active=True).order_by('-created').first()


class ReviewerAgreement(models.Model):
    """A reviewer's acceptance of a specific version of the terms.

    Also where attribution is settled: contributors must be creditable by name
    if they wish, and must be able to decline that.
    """

    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE,
                             related_name='phonetic_agreements')
    terms = models.ForeignKey(ContributionTerms, on_delete=models.PROTECT,
                              related_name='agreements')
    accepted_at = models.DateTimeField(auto_now_add=True)
    credit_name = models.CharField(max_length=200, blank=True,
                                   help_text='Name to credit, if the reviewer wants crediting.')
    credit_public = models.BooleanField(default=True)
    # Canonical https://orcid.org/XXXX-XXXX-XXXX-XXXX. Prefilled from the account
    # but stored here separately and deliberately: this is what the contributor
    # asserted when they agreed, and a later profile edit must not rewrite the
    # attribution on work already contributed.
    orcid = models.URLField(max_length=64, blank=True)

    class Meta:
        unique_together = [('user', 'terms')]
        ordering = ['-accepted_at']

    def __str__(self):
        return f'{self.user} accepted {self.terms.version}'


class Review(models.Model):
    """One reviewer's judgement of one rule, at one moment.

    Append-only. ``reviewed_ipa`` is a snapshot of what the rule said when the
    judgement was made — without it, an upstream edit would silently reassign
    every existing opinion to a value nobody looked at.
    """

    rule = models.ForeignKey(Rule, on_delete=models.CASCADE, related_name='reviews')
    reviewer = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE,
                                 related_name='phonetic_reviews')
    verdict = models.CharField(max_length=16, choices=Verdict.choices)
    proposed_ipa = models.CharField(max_length=64, blank=True,
                                    help_text='NFD-normalised; only for a CORRECT verdict.')
    reviewed_ipa = models.CharField(max_length=64, blank=True,
                                    help_text='rule.current_ipa as it stood when reviewed.')
    reviewed_version = models.ForeignKey(RuleSetVersion, on_delete=models.SET_NULL,
                                         null=True, blank=True, related_name='reviews')
    confidence = models.CharField(max_length=16, choices=Confidence.choices,
                                  default=Confidence.MEDIUM)
    comment = models.TextField(blank=True)

    # Snapshot of the competence claimed at the time, so a later edit to the
    # reviewer's profile cannot rewrite the standing of an old judgement.
    competence_level = models.CharField(max_length=16, blank=True)
    agreement = models.ForeignKey(ReviewerAgreement, on_delete=models.PROTECT,
                                  null=True, blank=True, related_name='reviews')

    # What validated this value. The consumer runs a different install on a
    # different host; "it validated" means little without saying against what.
    panphon_version = models.CharField(max_length=32, blank=True)
    ipa_all_sha256 = models.CharField(max_length=64, blank=True)

    created = models.DateTimeField(auto_now_add=True)
    # False once this reviewer supersedes it with a later judgement. Old rows
    # are retained: the history of one person changing their mind is evidence.
    is_latest = models.BooleanField(default=True)
    # Set by the sync when the upstream file adopts exactly this proposal.
    # Detected, not asserted by anyone — nobody has to remember to tick a box.
    adopted_upstream_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-created']
        indexes = [
            models.Index(fields=['rule', 'is_latest']),
            models.Index(fields=['reviewer', '-created']),
        ]

    def __str__(self):
        return f'{self.reviewer} {self.verdict} {self.rule}'

    @property
    def is_stale(self):
        """True when the rule has moved on since this judgement was made."""
        return self.reviewed_ipa != self.rule.current_ipa

    @property
    def effective_ipa(self):
        return self.proposed_ipa if self.verdict == Verdict.CORRECT else self.reviewed_ipa

    def save(self, *args, **kwargs):
        self.proposed_ipa = nfd(self.proposed_ipa)
        self.reviewed_ipa = nfd(self.reviewed_ipa)
        new = self._state.adding
        if new:
            (Review.objects
             .filter(rule=self.rule, reviewer=self.reviewer, is_latest=True)
             .update(is_latest=False))
        super().save(*args, **kwargs)
        self.rule.recount()


class NewRuleProposal(models.Model):
    """A grapheme the rule set does not cover at all, and what it should produce.

    This is the most valuable thing a reviewer can contribute, and for a while it
    was the one thing this app could not accept. place#251's diagnosis of every
    underperforming rule set is not that the values are wrong but that **rows are
    missing** — "the consonants are mapped and the independent vowels are entirely
    absent" — and drafting the absent vowels alone took Myanmar from 16.6% to
    98.7%. A review tool that can only correct rows that already exist cannot
    touch that.

    Kept separate from :class:`Review` because it is a different act with a
    different check. A review is judged against a value; a proposal is judged
    against a gap, and it must additionally not collide with a grapheme the rule
    set already defines — including one that only looks different because it is
    spelled with a different normalisation.
    """

    OPEN = 'open'
    DECLINED = 'declined'
    STATUS_CHOICES = [(OPEN, 'Open'), (DECLINED, 'Declined')]

    ruleset = models.ForeignKey(RuleSet, on_delete=models.CASCADE,
                                related_name='new_rule_proposals')
    proposer = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE,
                                 related_name='phonetic_new_rules')
    orth = models.CharField(max_length=64, help_text='Grapheme, NFD-normalised.')
    orth_source = models.CharField(max_length=64, blank=True)
    proposed_ipa = models.CharField(max_length=64, blank=True,
                                    help_text='NFD-normalised; blank means "produces nothing".')
    comment = models.TextField(blank=True)
    confidence = models.CharField(max_length=16, choices=Confidence.choices,
                                  default=Confidence.MEDIUM)
    competence_level = models.CharField(max_length=16, blank=True)
    agreement = models.ForeignKey(ReviewerAgreement, on_delete=models.PROTECT,
                                  null=True, blank=True, related_name='new_rule_proposals')
    # Where the proposer found the grapheme, if they came from the sandbox — a
    # real name the current rules could not convert. Concrete evidence that the
    # gap is real, rather than an assertion that it is.
    example_name = models.CharField(max_length=200, blank=True)

    panphon_version = models.CharField(max_length=32, blank=True)
    ipa_all_sha256 = models.CharField(max_length=64, blank=True)

    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=OPEN)
    created = models.DateTimeField(auto_now_add=True)
    is_latest = models.BooleanField(default=True)
    # Set by the sync when the grapheme appears upstream with this value.
    adopted_upstream_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['-created']
        indexes = [models.Index(fields=['ruleset', 'orth'])]

    def __str__(self):
        return f'+{self.orth} → {self.proposed_ipa or "(blank)"} ({self.ruleset.slug})'

    def save(self, *args, **kwargs):
        self.orth = nfd(self.orth)
        self.proposed_ipa = nfd(self.proposed_ipa)
        if self._state.adding:
            (NewRuleProposal.objects
             .filter(ruleset=self.ruleset, orth=self.orth, proposer=self.proposer,
                     is_latest=True)
             .update(is_latest=False))
        super().save(*args, **kwargs)

    @property
    def orth_codepoints(self):
        return codepoints(self.orth)

    @property
    def competing(self):
        """Other people's live proposals for the same grapheme.

        Shown rather than blocked. Two experts proposing different values for a
        letter nobody has mapped is the same signal as two disagreeing about one
        that exists, and it is worth exactly as much.
        """
        return (NewRuleProposal.objects
                .filter(ruleset=self.ruleset, orth=self.orth, is_latest=True,
                        status=self.OPEN)
                .exclude(pk=self.pk).exclude(proposer=self.proposer))


class PolicyQuestion(models.Model):
    """A decision that changes many rows at once and cannot be asked row by row.

    The live example is whether the Myanmar rules should target modern spoken
    Burmese or Pali/orthographic values. Asked once, against the whole rule set,
    of people who declare competence in it.
    """

    OPEN = 'open'
    RESOLVED = 'resolved'
    STATUS_CHOICES = [(OPEN, 'Open'), (RESOLVED, 'Resolved')]

    ruleset = models.ForeignKey(RuleSet, on_delete=models.CASCADE, null=True, blank=True,
                                related_name='policy_questions')
    # Set instead of `ruleset` for a question spanning every set in a language.
    language_code = models.CharField(max_length=8, blank=True, db_index=True)
    slug = models.SlugField(max_length=80, unique=True)
    title = models.CharField(max_length=300)
    body = models.TextField(help_text='The question, and what turns on the answer.')
    # [{'key': 'spoken', 'label': 'Modern spoken Burmese', 'detail': '…'}, …]
    options = models.JSONField(default=list)
    status = models.CharField(max_length=16, choices=STATUS_CHOICES, default=OPEN)
    resolution = models.TextField(blank=True)
    resolved_option = models.CharField(max_length=40, blank=True)
    created_by = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.SET_NULL,
                                   null=True, blank=True, related_name='phonetic_questions')
    created = models.DateTimeField(auto_now_add=True)
    # Questions that turn on the same underlying decision. Myanmar Q1 (register)
    # and Q13 (the value for ရှ/ယှ) are one judgement asked twice; answered apart
    # they can be answered inconsistently, which would be worse than not asking.
    # symmetrical: a reviewer arriving at either question must be told about the
    # other. An asymmetric link would warn one direction and not the other, which
    # is the failure mode the link exists to prevent.
    related = models.ManyToManyField('self', blank=True, symmetrical=True)

    class Meta:
        ordering = ['status', '-created']

    def __str__(self):
        return self.title

    def option_label(self, key):
        for opt in self.options:
            if opt.get('key') == key:
                return opt.get('label', key)
        return key

    @property
    def resolved_label(self):
        return self.option_label(self.resolved_option) if self.resolved_option else ''

    def tally(self):
        """Answers per option, including options nobody chose.

        Zero-count options are kept so that "nobody picked this" is visible
        rather than absent — the same reason an unreviewed row is a state.
        """
        counts = {opt.get('key'): 0 for opt in self.options}
        for answer in self.answers.filter(is_latest=True):
            counts[answer.option_key] = counts.get(answer.option_key, 0) + 1
        return [{'key': o.get('key'), 'label': o.get('label', o.get('key')),
                 'detail': o.get('detail', ''), 'count': counts.get(o.get('key'), 0)}
                for o in self.options]


class PolicyAnswer(models.Model):
    question = models.ForeignKey(PolicyQuestion, on_delete=models.CASCADE, related_name='answers')
    user = models.ForeignKey(settings.AUTH_USER_MODEL, on_delete=models.CASCADE,
                             related_name='phonetic_policy_answers')
    option_key = models.CharField(max_length=40)
    comment = models.TextField(blank=True)
    competence_level = models.CharField(max_length=16, blank=True)
    agreement = models.ForeignKey(ReviewerAgreement, on_delete=models.PROTECT,
                                  null=True, blank=True, related_name='policy_answers')
    created = models.DateTimeField(auto_now_add=True)
    is_latest = models.BooleanField(default=True)

    class Meta:
        ordering = ['-created']

    def __str__(self):
        return f'{self.user} → {self.option_key}'

    @property
    def option_label(self):
        return self.question.option_label(self.option_key)

    def save(self, *args, **kwargs):
        if self._state.adding:
            (PolicyAnswer.objects
             .filter(question=self.question, user=self.user, is_latest=True)
             .update(is_latest=False))
        super().save(*args, **kwargs)
