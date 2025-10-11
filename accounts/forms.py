import re

from django import forms
from django.contrib.auth import authenticate
from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.validators import validate_email

User = get_user_model()
# from accounts.models import Profile

URL_PATTERN = re.compile(r'https?://|www\.|tinyurl\.com|bit\.ly|\/')


def validate_no_urls(value):
    if URL_PATTERN.search(value):
        if value and URL_PATTERN.search(value):
            raise ValidationError('Please do not include URLs or links in this field.')


class LoginForm(forms.Form):
    username = forms.CharField(max_length=100, required=True)
    email = forms.CharField(max_length=255, required=True)
    password = forms.CharField(widget=forms.PasswordInput, required=True)

    def clean(self):
        name = self.cleaned_data.get('name')
        username = self.cleaned_data.get('username')
        email = self.cleaned_data.get('email')
        password = self.cleaned_data.get('password')
        user = authenticate(email=email, password=password)
        if not user or not user.is_active:
            raise forms.ValidationError("Sorry, that login was invalid. Please try again.")
        return self.cleaned_data

    def login(self, request):
        username = self.cleaned_data.get('username')
        name = self.cleaned_data.get('name')
        email = self.cleaned_data.get('email')
        password = self.cleaned_data.get('password')
        user = authenticate(username=username, password=password)
        # user = authenticate(email=email, password=password)
        return user


class UserModelForm(forms.ModelForm):
    class Meta:
        model = User
        fields = ('news_permitted',)
        exclude = ('password',)

        widgets = {
            'news_permitted': forms.CheckboxInput(attrs={
                'class': 'form-check-input',
                'style': 'cursor: pointer;',
                'data-bs-toggle': 'tooltip',
                'data-bs-title': (
                    '<i class="fas fa-newspaper"></i> By checking this box, you consent to our sending occasional newsletters and updates via email.<br><br>'
                    '<strong>Note:</strong> <i>You can withdraw your consent at any time by unchecking this box.</i>'
                ),
            }),
        }
