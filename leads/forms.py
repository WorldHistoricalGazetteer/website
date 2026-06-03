# leads/forms.py
from django import forms

from .models import DatasetLead


class PublicLeadForm(forms.ModelForm):
    """
    Public suggestion form. Exposes only safe bibliographic fields;
    status/provenance are set server-side in the view. Includes a hidden honeypot
    ('website') that real users never see — a filled value means a bot.

    Pass ``trusted=True`` (e.g. for a logged-in user) to drop the spam-trap field
    entirely: authenticated submitters don't need anti-spam friction.
    """

    # Honeypot: visually hidden, off the tab order. Bots fill it; humans don't.
    website = forms.CharField(
        required=False,
        label='Leave this field blank',
        widget=forms.TextInput(attrs={
            'autocomplete': 'off',
            'tabindex': '-1',
            'class': 'leads-hp',
        }),
    )

    class Meta:
        model = DatasetLead
        fields = [
            'title', 'author_compiler', 'publication_years',
            'region_covered', 'source_url', 'notes', 'recommended_by',
        ]
        labels = {
            'title': 'Title of the gazetteer or dataset',
            'author_compiler': 'Author / compiler',
            'publication_years': 'Publication year(s)',
            'region_covered': 'Region or places covered',
            'source_url': 'Link (catalogue, Internet Archive, HathiTrust, …)',
            'notes': 'Anything else we should know',
            'recommended_by': 'Your name / how you found this',
        }
        help_texts = {
            'source_url': 'A link to the catalogue record or full text, if you have one.',
            'recommended_by': 'Optional — helps us credit the suggestion and follow up.',
        }
        widgets = {
            'title': forms.TextInput(attrs={'class': 'form-control', 'required': True,
                                            'placeholder': 'e.g. Gazetteer of the Bombay Presidency'}),
            'author_compiler': forms.TextInput(attrs={'class': 'form-control'}),
            'publication_years': forms.TextInput(attrs={'class': 'form-control',
                                                        'placeholder': 'e.g. 1877–1896'}),
            'region_covered': forms.Textarea(attrs={'class': 'form-control', 'rows': 2}),
            'source_url': forms.URLInput(attrs={'class': 'form-control',
                                                'placeholder': 'https://'}),
            'notes': forms.Textarea(attrs={'class': 'form-control', 'rows': 3}),
            'recommended_by': forms.TextInput(attrs={'class': 'form-control'}),
        }

    def __init__(self, *args, trusted=False, **kwargs):
        super().__init__(*args, **kwargs)
        # Logged-in users are trusted: remove the spam-trap field so it is neither
        # rendered nor validated. clean_website() then never runs.
        if trusted:
            self.fields.pop('website', None)

    def clean_website(self):
        # Honeypot must stay empty.
        if self.cleaned_data.get('website'):
            raise forms.ValidationError('Spam detected.')
        return ''
