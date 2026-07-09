"""
Template tag for the centralised terminology map (main/labels.py, plan §11).

Usage in templates:  {% load labels %}  …  {% label 'gazetteer_group' %}
Also available as a filter:              {{ 'gazetteer'|label }}
"""
from django import template

from main.labels import label as _label

register = template.Library()


@register.simple_tag(name='label')
def label_tag(key):
    return _label(key)


@register.filter(name='label')
def label_filter(key):
    return _label(key)
