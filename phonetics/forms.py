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


class AgreementForm(forms.Form):
    accept = forms.BooleanField(
        required=True,
        label='I have read the contribution terms above and agree to them.')
    credit_name = forms.CharField(
        required=False, max_length=200,
        label='Name to credit (leave blank to contribute without attribution)')
    credit_public = forms.BooleanField(
        required=False, initial=True,
        label='Show my name publicly as a contributor')
    orcid = forms.CharField(required=False, max_length=32, label='ORCiD (optional)')
