from bootstrap_modal_forms.forms import BSModalForm
from django import forms

from main.choices import COMMENT_TAGS, COMMENT_TAGS_REVIEW
from main.models import Comment
from .models import Announcement


class AnnouncementForm(forms.ModelForm):
    class Meta:
        model = Announcement
        fields = ['headline', 'content', 'link', 'active']
        widgets = {
            'headline': forms.TextInput(attrs={'class': 'form-control', 'size': '50'}),
            'content': forms.Textarea(attrs={'class': 'form-control', 'columns': '50', 'rows': '3'}),
            'link': forms.TextInput(attrs={'class': 'form-control', 'size': '50'}),
        }


class InviteForm(forms.Form):
    """Email invitation (place#155). Deliberately minimal: one recipient address, the
    kind of invitation, and — for a "share this page" invitation — the link being shared.

    There is no message field. A sender-authored note is the whole spam-payload surface;
    without it the message body is fixed template text, so the only sender-controlled
    content reaching the recipient is a URL validated against a WHG-origin allowlist
    (``main.invitations.validate_target_url``). We also don't ask for the recipient's
    name — it would be one more piece of someone else's personal data, transient or not.
    """
    to_email = forms.EmailField(
        required=True,
        label="Their email address",
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
            'placeholder': 'name@example.org',
            'autocomplete': 'off',
        }),
    )
    kind = forms.ChoiceField(
        choices=[('view', 'Share a page'), ('join', 'Invitation to join')],
        required=True,
        widget=forms.HiddenInput(),
    )
    target_url = forms.CharField(required=False, widget=forms.HiddenInput())

    def clean(self):
        cleaned = super().clean()
        if cleaned.get('kind') == 'view' and not (cleaned.get('target_url') or '').strip():
            raise forms.ValidationError('There is no link to share.')
        return cleaned


class ContactForm(forms.Form):
    name = forms.CharField(
        widget=forms.TextInput(attrs={'size': 50}),
        required=True,
        label="Your name"
    )
    from_email = forms.EmailField(
        widget=forms.EmailInput(attrs={'size': 50}),
        required=True,
        label="Your email address"
    )
    subject = forms.CharField(
        widget=forms.TextInput(attrs={'size': 50}),
        required=True
    )
    message = forms.CharField(
        widget=forms.Textarea(attrs={'cols': 50, 'rows': 5}),
        required=True
    )
    username = forms.CharField(
        widget=forms.HiddenInput(),
        required=False
    )
    dataset_id = forms.IntegerField(
        widget=forms.HiddenInput(),
        required=False
    )

    def __init__(self, *args, **kwargs):
        # Allow passing an initial subject directly
        initial_subject = kwargs.pop('initial_subject', None)
        super().__init__(*args, **kwargs)
        if initial_subject:
            self.fields['subject'].initial = initial_subject


class VolunteerForm(ContactForm):
    subject = forms.CharField(initial='WHG Volunteer for Review')


class CommentModalForm(BSModalForm):
    class Meta:
        model = Comment
        # all fields: user, place_id, tag, note, created
        fields = ['tag', 'note', 'place_id']
        hidden_fields = ['created']
        exclude = ['user', 'place_id']
        widgets = {
            'place_id': forms.TextInput(),
            'tag': forms.RadioSelect(choices=COMMENT_TAGS, attrs={'class': 'no-bullet'}),
            'note': forms.Textarea(attrs={
                'rows': 2, 'cols': 40, 'class': 'textarea'})
        }

    def __init__(self, *args, **kwargs):
        super(CommentModalForm, self).__init__(*args, **kwargs)
        self.fields['tag'].label = "Issue"
        if '/def' in kwargs['initial']['next']:
            self.fields['tag'].choices = COMMENT_TAGS_REVIEW
