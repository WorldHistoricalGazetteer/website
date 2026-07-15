from django import forms
from django.contrib.postgres.forms import SimpleArrayField
from django.db import models
from .models import Resource


# Django 4.2 removed multiple-file support from ClearableFileInput/FileInput
# (attrs={'multiple': True} now raises). Use the documented dedicated classes.
class MultipleFileInput(forms.ClearableFileInput):
    allow_multiple_selected = True


class MultipleFileField(forms.FileField):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("widget", MultipleFileInput())
        super().__init__(*args, **kwargs)

    def clean(self, data, initial=None):
        single_file_clean = super().clean
        if isinstance(data, (list, tuple)):
            result = [single_file_clean(d, initial) for d in data]
        else:
            result = single_file_clean(data, initial)
        return result


class ResourceModelForm(forms.ModelForm):
    keywords = SimpleArrayField(forms.CharField())
    gradelevels = SimpleArrayField(forms.CharField())
    files = MultipleFileField()
    images = MultipleFileField()

    class Meta:
        model = Resource
        fields = ('id', 'pub_date', 'owner', 'title', 'type', 'description', 
            'subjects', 'gradelevels', 'keywords', 'authors', 'contact', 'webpage', 
            'files', 'images', 'public', 'featured', 'status')
        widgets = {
            'title': forms.TextInput(attrs={'size': 50}),
            'keywords': forms.TextInput(attrs={'size': 50}),
            'contact': forms.TextInput(attrs={'size': 50}),
            'webpage': forms.TextInput(attrs={'size': 50}),
            'description': forms.Textarea(attrs={
                'rows': 3, 'cols': 49, 'class': 'textarea'
            }),
        }

    def __init__(self, *args, **kwargs):
        super(ResourceModelForm, self).__init__(*args, **kwargs)
