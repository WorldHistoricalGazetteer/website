"""Seed CC BY-NC 2.5 (place#158, part 3).

One contributed dataset (OWTRAD, `12`) is published upstream under the Creative
Commons Attribution-NonCommercial 2.5 licence and nothing else; the vocabulary
had every other CC licence the audit met but not this one, so the only honest
record for that dataset was NULL. A plain, current SPDX licence: selectable by a
contributor like the other CC rows (the "no NC in the picker" suggestion in the
Pitt correspondence was a suggestion, not a ruling, and CC-BY-NC-4.0 is already
offered). Same tuple shape as 0005.
"""
from django.db import migrations

# spdx_id, label, url, permits_commercial, share_alike, attribution_required,
# no_derivatives, custom, contributor_selectable, notes
SEED = [
    ("CC-BY-NC-2.5",
     "Creative Commons Attribution-NonCommercial 2.5 Generic",
     "https://creativecommons.org/licenses/by-nc/2.5/",
     False, False, True, False, False, True,
     ""),
]

_FIELDS = ("label", "url", "permits_commercial", "share_alike",
           "attribution_required", "no_derivatives", "custom",
           "contributor_selectable", "notes")


def seed(apps, schema_editor):
    License = apps.get_model("licensing", "License")
    for spdx_id, *values in SEED:
        License.objects.update_or_create(
            spdx_id=spdx_id,
            defaults=dict(zip(_FIELDS, values)),
        )


def unseed(apps, schema_editor):
    License = apps.get_model("licensing", "License")
    License.objects.filter(spdx_id__in=[row[0] for row in SEED]).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("licensing", "0005_seed_authority_licences"),
    ]

    operations = [
        migrations.RunPython(seed, unseed),
    ]
