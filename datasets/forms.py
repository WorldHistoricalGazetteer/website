# datasets.forms

from django import forms
from django.conf import settings
from django.core.validators import RegexValidator

# from django.core.exceptions import ValidationError
from datasets.models import Dataset, Hit, DatasetFile
# from datasets.utils import insert_delim, insert_lpf, validator_delim, validate_lpf, sniff_file_type
from main.choices import FORMATS, STATUS_FILE, FEATURE_CLASSES

import codecs, os, tempfile, logging, json
from chardet import detect
from datasets.static.hashes import mimetypes_plus as mthash_plus

MATCHTYPES = [
    ('closeMatch', 'closeMatch'),
    ('none', 'no match'),
]


class DatasetUploadForm(forms.ModelForm):
    title = forms.CharField(label='Title', required=False, widget=forms.TextInput(attrs={
        'placeholder': '5-100 characters (optional)',
        'size': 45,
        'minlength': 5,
        'maxlength': 100,
        'class': 'form-control',
    }))

    label = forms.CharField(label='Label', required=False, widget=forms.TextInput(attrs={
        'placeholder': '3-20 characters, no spaces (optional)',
        'size': 45,
        'minlength': 3,
        'maxlength': 20,
        'class': 'form-control',
    }))

    description = forms.CharField(label='Description', required=False, widget=forms.Textarea(attrs={
        'rows': 2,
        'cols': 45,
        'minlength': 10,
        'class': 'form-control',
        'placeholder': 'At least 10 characters (optional)',
    }))

    creator = forms.CharField(label='Creator(s)', required=False, widget=forms.TextInput(attrs={
        'placeholder': 'Name(s) of dataset creator(s)',
        'size': 45,
        'class': 'form-control',
    }))

    source = forms.CharField(label='Source(s)', required=False, widget=forms.TextInput(attrs={
        'placeholder': 'Name(s) of sources used',
        'size': 45,
        'class': 'form-control',
    }))

    contributors = forms.CharField(label='Contributors', required=False, widget=forms.TextInput(attrs={
        'placeholder': 'Name(s) of any contributor(s)',
        'size': 45,
        'class': 'form-control',
    }))

    NAMESPACE_REGEX = r'^\w+:$'
    URL_REGEX = r'^(https?:\/\/)(www\.)?[a-zA-Z0-9\-\.]{2,256}\.[a-z]{2,63}(\:[0-9]{1,5})?(\/.*)?$'
    uri_base = forms.CharField(
        label='URI base',
        required=False,
        widget=forms.TextInput(attrs={
            'placeholder': 'Enter a URI base or namespace',
            'size': 45,
            'class': 'form-control',
            'data-bs-title': 'Enter a URI base if applicable, either as a URL or a namespace, e.g., <span class="text-danger">https://example.com/resource/</span> or <span class="text-danger">example:</span>',
            'data-bs-toggle': 'tooltip',
        }),
        validators=[
            RegexValidator(
                regex=f'({URL_REGEX})|({NAMESPACE_REGEX})',
                message='Enter a valid URL or namespace term (e.g., https://example.com/resource/ or example:).',
                code='invalid_url_or_namespace'
            )
        ]
    )

    webpage = forms.URLField(label='Web page', required=False, widget=forms.URLInput(attrs={
        'size': 45,
        'placeholder': 'Project home page',
        'class': 'form-control',
    }))

    pdf = forms.FileField(label='Essay file', required=False, widget=forms.FileInput(attrs={
        'class': 'form-control',
        'accept': '.pdf',
    }))

    file = forms.FileField(label='Data file', widget=forms.FileInput(attrs={
        'class': 'form-control',
        'required': 'required',
        'accept': ','.join(settings.VALIDATION_ALLOWED_EXTENSIONS),
    }))

    license_acceptance = forms.CharField(label='Licence Acceptance', widget=forms.CheckboxInput(attrs={
        'class': 'form-check-input',
        'required': 'required',
    }))

    class Meta:
        model = Dataset
        fields = (
        'title', 'label', 'description', 'creator', 'source', 'contributors', 'uri_base', 'webpage', 'pdf', 'file')


class HitModelForm(forms.ModelForm):
    match = forms.CharField(
        initial='none',
        widget=forms.RadioSelect(choices=MATCHTYPES)
    )

    class Meta:
        model = Hit
        fields = ['id', 'authority', 'authrecord_id',
                  'query_pass', 'score', 'json']
        hidden_fields = ['id', 'authority',
                         'authrecord_id', 'query_pass', 'score', 'json']
        widgets = {
            'id': forms.HiddenInput(),
            'authority': forms.HiddenInput(),
            'authrecord_id': forms.HiddenInput(),
            'json': forms.HiddenInput()
        }

    def __init__(self, *args, **kwargs):
        super(HitModelForm, self).__init__(*args, **kwargs)

        for key in self.fields:
            self.fields[key].required = False


class DatasetFileModelForm(forms.ModelForm):
    class Meta:
        model = DatasetFile
        fields = ('dataset_id', 'file', 'rev', 'format', 'delimiter',
                  'df_status', 'datatype', 'header', 'numrows')

    def __init__(self, *args, **kwargs):
        super(DatasetFileModelForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            field.error_messages = {'required': 'The field {fieldname} is required'.format(
                fieldname=field.label)}


class DatasetDetailModelForm(forms.ModelForm):
    class Meta:
        model = Dataset
        # file fields = ('file','rev','uri_base','format','dataset_id','delimiter',
        #   'status','accepted_date','header','numrows')
        fields = ('owner', 'creator', 'contributors', 'source', 'id', 'label', 'title', 'uri_base', 'description',
                  'citation', 'datatype', 'numlinked', 'webpage', 'web_item', 'featured', 'image_file', 'public', 'pdf')
        widgets = {
            'description': forms.Textarea(attrs={
                'rows': 2, 'cols': 55, 'class': 'textarea', 'placeholder': 'Brief description'}),
            'citation': forms.Textarea(attrs={
                'rows': 2, 'cols': 55, 'class': 'textarea'}),
            'creator': forms.TextInput(attrs={'size': 50}),
            'source': forms.TextInput(attrs={'size': 50}),
            'contributors': forms.TextInput(attrs={'size': 50}),
            'webpage': forms.TextInput(attrs={'size': 20}),
            'web_item': forms.TextInput(attrs={
                'size': 50,
                'placeholder': 'https://example.org/place/<id>'}),
            'uri_base': forms.TextInput(attrs={'size': 50}),
            'featured': forms.TextInput(attrs={'size': 4}),
            'pdf': forms.FileInput(attrs={'class': 'fileinput'}),
        }

    def clean_label(self):
        label = self.cleaned_data['label']
        if ' ' in label:
            raise forms.ValidationError('label cannot contain any space')
        return label

    file = forms.FileField(required=False)

    # uri_base = forms.URLField(
    #     widget=forms.URLInput(
    #         attrs={'placeholder': 'Leave blank unless changed', 'size': 40}
    #     ),
    #     required=False
    # )

    NAMESPACE_REGEX = r'^\w+:$'
    URL_REGEX = r'^(https?:\/\/)(www\.)?[a-zA-Z0-9\-\.]{2,256}\.[a-z]{2,63}(\:[0-9]{1,5})?(\/.*)?$'
    uri_base = forms.CharField(
        label='URI base',
        required=False,
        widget=forms.TextInput(attrs={
            'placeholder': 'Enter a URI base or namespace',
            'size': 45,
            'class': 'form-control',
            'data-bs-title': 'Enter a URI base if applicable, either as a URL or a namespace, e.g., <span class="text-danger">https://example.com/resource/</span> or <span class="text-danger">example:</span>',
            'data-bs-toggle': 'tooltip',
        }),
        validators=[
            RegexValidator(
                regex=f'({URL_REGEX})|({NAMESPACE_REGEX})',
                message='Enter a valid URL or namespace term (e.g., https://example.com/resource/ or example:).',
                code='invalid_url_or_namespace'
            )
        ]
    )

    format = forms.ChoiceField(choices=FORMATS, initial="delimited")

    def __init__(self, *args, **kwargs):
        super(DatasetDetailModelForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            field.error_messages = {'required': 'The field {fieldname} is required'.format(
                fieldname=field.label)}


class DatasetCreateEmptyModelForm(forms.ModelForm):
    class Meta:
        model = Dataset
        fields = ('owner', 'id', 'title', 'label', 'datatype', 'description', 'uri_base', 'public',
                  'creator', 'contributors', 'source', 'webpage', 'image_file', 'featured', 'ds_status')

        widgets = {
            'description': forms.Textarea(attrs={
                'rows': 2, 'cols': 45, 'class': 'textarea', 'placeholder': 'Brief description'}),
            'uri_base': forms.TextInput(attrs={
                'placeholder': 'Leave blank unless record IDs are URIs', 'size': 45}),
            'title': forms.TextInput(attrs={'size': 45}),
            'label': forms.TextInput(attrs={'placeholder': '20 char max; no spaces', 'size': 22}),
            'creator': forms.TextInput(attrs={'size': 45}),
            'source': forms.TextInput(attrs={'size': 45}),
            'featured': forms.TextInput(attrs={'size': 4}),
            'webpage': forms.URLInput(attrs={'size': 45, 'placeholder': 'Project home page, if any'})
        }

    def clean_label(self):
        label = self.cleaned_data['label']
        if ' ' in label:
            raise forms.ValidationError('Label cannot contain any spaces; replace with underscores (_)')
        return label

    def __init__(self, *args, **kwargs):
        super(DatasetCreateEmptyModelForm, self).__init__(*args, **kwargs)
        for field in self.fields.values():
            field.error_messages = {'required': 'The field {fieldname} is required'.format(
                fieldname=field.label)}
