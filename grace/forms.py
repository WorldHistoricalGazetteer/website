"""The public suggest-a-source form.

Decision 5: ``/contribute/`` is a suggest-a-source tool, not a general front
door. It asks for a printed gazetteer or dataset someone thinks we should know
about, and nothing else — people who want to contribute their own data go
through the existing upload and validation flow, which already exists and does
the job properly.

The anti-spam here is lifted from the earlier ``leads`` prototype rather than
reinvented: it was already debugged in production, and two of its commits were
fixes to exactly the gating subtleties below. See
``developer/grace-leads-prototype-notes.md``.
"""
from django import forms

from .models import SourceSuggestion


class SourceSuggestionForm(forms.ModelForm):
    """Exposes only safe bibliographic fields.

    ``status``, ``submitter_user`` and the discovery source are set server-side
    in the view — never trusted from the POST.

    Pass ``trusted=True`` (for a signed-in user) to drop the honeypot entirely:
    an authenticated submitter does not need anti-spam friction, and leaving a
    hidden field on the page for them is just something else to go wrong.
    """

    # Honeypot: visually hidden, off the tab order. Bots fill it; humans do not.
    website = forms.CharField(
        required=False,
        label="Leave this field blank",
        widget=forms.TextInput(attrs={
            "autocomplete": "off",
            "tabindex": "-1",
            "class": "grace-hp",
        }),
    )

    class Meta:
        model = SourceSuggestion
        fields = [
            "title", "author_compiler", "publication_years",
            "region_covered", "source_url", "notes",
            "submitter_name", "submitter_email",
        ]
        labels = {
            "title": "Title of the gazetteer or dataset",
            "author_compiler": "Author / compiler",
            "publication_years": "Publication year(s)",
            "region_covered": "Region or places covered",
            "source_url": "Link (catalogue, Internet Archive, HathiTrust, …)",
            "notes": "Anything else we should know",
            "submitter_name": "Your name",
            "submitter_email": "Your email address",
        }
        help_texts = {
            "source_url": "A link to the catalogue record or full text, if you "
                          "have one.",
            "submitter_name": "Optional — helps us credit the suggestion.",
            "submitter_email": "Optional. Only used to follow up on this "
                               "suggestion.",
        }
        widgets = {
            "title": forms.TextInput(attrs={
                "class": "form-control", "required": True,
                "placeholder": "e.g. Gazetteer of the Bombay Presidency"}),
            "author_compiler": forms.TextInput(attrs={"class": "form-control"}),
            "publication_years": forms.TextInput(attrs={
                "class": "form-control", "placeholder": "e.g. 1877–1896"}),
            "region_covered": forms.Textarea(attrs={
                "class": "form-control", "rows": 2}),
            "source_url": forms.URLInput(attrs={
                "class": "form-control", "placeholder": "https://"}),
            "notes": forms.Textarea(attrs={"class": "form-control", "rows": 3}),
            "submitter_name": forms.TextInput(attrs={"class": "form-control"}),
            "submitter_email": forms.EmailInput(attrs={"class": "form-control"}),
        }

    def __init__(self, *args, trusted=False, **kwargs):
        super().__init__(*args, **kwargs)
        # Signed-in users are trusted: remove the spam trap so it is neither
        # rendered nor validated, and clean_website() never runs.
        if trusted:
            self.fields.pop("website", None)
            # We already know who they are.
            self.fields.pop("submitter_name", None)
            self.fields.pop("submitter_email", None)

    def clean_website(self):
        if self.cleaned_data.get("website"):
            raise forms.ValidationError("Spam detected.")
        return ""
