import re

from django import forms

from .models import (Confidence, CompetenceLevel, ReviewerCompetence, Verdict)
from .validation import panphon_provenance, validate_ipa


class CompetenceForm(forms.ModelForm):
    class Meta:
        model = ReviewerCompetence
        fields = ['language_code', 'script_code', 'level', 'note']
        widgets = {'note': forms.Textarea(attrs={'rows': 2})}

    def clean_language_code(self):
        return (self.cleaned_data['language_code'] or '').strip().lower()

    def clean_script_code(self):
        return (self.cleaned_data['script_code'] or '').strip().title()


class ReviewForm(forms.Form):
    """One row's verdict.

    The IPA check here is the same one the API endpoint runs, and it is the only
    gate: a value that fails cannot be stored. Rejecting at input rather than at
    export is the whole point — an expert's time spent on a value the pipeline
    would silently discard is worse than their time not spent at all.
    """

    verdict = forms.ChoiceField(choices=Verdict.choices)
    proposed_ipa = forms.CharField(required=False, max_length=64, strip=False)
    confidence = forms.ChoiceField(choices=Confidence.choices, required=False)
    comment = forms.CharField(required=False, widget=forms.Textarea(attrs={'rows': 3}))

    def __init__(self, *args, rule=None, **kwargs):
        self.rule = rule
        self.segments = []
        self.provenance = {}
        super().__init__(*args, **kwargs)

    def clean(self):
        data = super().clean()
        verdict = data.get('verdict')
        proposed = data.get('proposed_ipa') or ''
        if verdict == Verdict.CORRECT:
            normalised, errors, segments = validate_ipa(proposed)
            for error in errors:
                self.add_error('proposed_ipa', error['message'])
            if self.rule is not None and not errors and normalised == self.rule.current_ipa:
                self.add_error(
                    'proposed_ipa',
                    'That is the value already in the rule set. To say it is right, '
                    'choose “The current value is right” instead — an accept and a '
                    'correction that changes nothing are different evidence.')
            data['proposed_ipa'] = normalised
            self.segments = segments
            self.provenance = panphon_provenance()
        else:
            # A verdict that is not a correction carries no proposal. Keeping a
            # stray value here would let an abandoned edit reappear as a proposal
            # nobody meant to make.
            data['proposed_ipa'] = ''
        return data


class NewRuleForm(forms.Form):
    """Propose a grapheme the rule set does not cover.

    Two checks, and the second is the one place#252 lists as an acceptance
    criterion that nothing else in this app could satisfy: **a duplicate grapheme
    cannot be created, including an NFD-equivalent one.** The Gurmukhi rule set
    that will not load got that way exactly here — someone added a letter that was
    already present, spelled with a different normalisation, and the two render
    identically so nobody saw it.
    """

    orth = forms.CharField(max_length=64, strip=True,
                           label='Grapheme (the letter or letter sequence)')
    proposed_ipa = forms.CharField(required=False, max_length=64, strip=False,
                                   label='IPA it should produce')
    confidence = forms.ChoiceField(choices=Confidence.choices, required=False)
    comment = forms.CharField(required=False, widget=forms.Textarea(attrs={'rows': 3}))
    example_name = forms.CharField(required=False, max_length=200, widget=forms.HiddenInput)

    def __init__(self, *args, ruleset=None, **kwargs):
        self.ruleset = ruleset
        self.segments = []
        self.provenance = {}
        super().__init__(*args, **kwargs)

    def clean_orth(self):
        from .models import Rule
        from .validation import codepoints, nfd
        value = nfd(self.cleaned_data['orth'])
        if not value:
            raise forms.ValidationError('Enter the letter you want to add.')
        if self.ruleset is not None:
            clash = Rule.objects.filter(ruleset=self.ruleset, orth=value).first()
            if clash is not None:
                raise forms.ValidationError(
                    f'This rule set already has “{clash.orth_source or clash.orth}” '
                    f'({codepoints(value)}). Two rules for one letter stop the whole '
                    f'rule set from loading, and the two spellings can look identical '
                    f'on screen. Edit the existing row instead.')
        return value

    def clean(self):
        data = super().clean()
        normalised, errors, segments = validate_ipa(data.get('proposed_ipa') or '')
        for error in errors:
            self.add_error('proposed_ipa', error['message'])
        data['proposed_ipa'] = normalised
        self.segments = segments
        self.provenance = panphon_provenance()
        return data


class PolicyAnswerForm(forms.Form):
    option_key = forms.CharField(max_length=40)
    comment = forms.CharField(required=False, widget=forms.Textarea(attrs={'rows': 3}))

    def __init__(self, *args, question=None, **kwargs):
        self.question = question
        super().__init__(*args, **kwargs)

    def clean_option_key(self):
        key = self.cleaned_data['option_key']
        valid = {o.get('key') for o in (self.question.options if self.question else [])}
        if key not in valid:
            raise forms.ValidationError('Choose one of the listed options.')
        return key


# 0000-0002-1825-0097, optionally as a full orcid.org URL. The final character
# may be an X — the ISO 7064 check digit.
ORCID_RE = re.compile(r'^(?:https?://(?:sandbox\.)?orcid\.org/)?(\d{4}-\d{4}-\d{4}-\d{3}[\dX])$',
                      re.IGNORECASE)


def canonical_orcid(value):
    """Bare iD or any orcid.org URL → the canonical https URL. '' if unparseable."""
    match = ORCID_RE.match((value or '').strip())
    return f'https://orcid.org/{match.group(1).upper()}' if match else ''


class AgreementForm(forms.Form):
    """Accepting the terms, and settling attribution at the same moment.

    Name and ORCiD are prefilled from the account — most WHG users signed in with
    ORCiD, so asking them to retype it would be busywork — but they stay editable.
    A contributor may want crediting under a different form of their name, or
    under none, and the account's name is a login detail rather than a byline.
    """

    accept = forms.BooleanField(
        required=True,
        label='I have read the contribution terms above and agree to them.')
    credit_name = forms.CharField(
        required=False, max_length=200,
        label='Name to credit (leave blank to contribute without attribution)')
    credit_public = forms.BooleanField(
        required=False, initial=True,
        label='Show my name publicly as a contributor')
    orcid = forms.CharField(
        required=False, max_length=64, label='ORCiD (optional)',
        help_text='e.g. 0000-0002-1825-0097')

    def clean_orcid(self):
        raw = (self.cleaned_data.get('orcid') or '').strip()
        if not raw:
            return ''
        canonical = canonical_orcid(raw)
        if not canonical:
            raise forms.ValidationError(
                'That does not look like an ORCiD. Expected 16 digits in groups of '
                'four, e.g. 0000-0002-1825-0097.')
        return canonical


class CreditForm(AgreementForm):
    """Revising how you are credited, after you have already agreed.

    Attribution is a standing preference rather than a one-off keystroke, and a
    contributor who decides they would rather not be named — or would rather be
    named differently — should not have to ask anyone.
    """

    accept = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields.pop('accept', None)
