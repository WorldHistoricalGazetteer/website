"""Seed the Open Government Licence – Yukon 2.0 (place#158, browser reads).

One contributed dataset (`1052`, the Yukon gazetteer) is published upstream
under the Government of Yukon's own open licence, read in a browser on
2026-10-10: worldwide, royalty-free, commercial use permitted, attribution
required, no share-alike, adaptations permitted. It is not in the SPDX list, so
by this model's convention it takes a ``custom-*`` id and ``custom`` True. Not
contributor-selectable. Same tuple shape as 0005 and 0006.
"""
from django.db import migrations

# spdx_id, label, url, permits_commercial, share_alike, attribution_required,
# no_derivatives, custom, contributor_selectable, notes
SEED = [
    ("custom-ogl-yukon-2.0",
     "Open Government Licence – Yukon 2.0",
     "https://yukon.ca/en/your-government/open-government/open-government-licence-yukon",
     True, False, True, False, True, False,
     "Government of Yukon open licence v2.0: use, adapt and redistribute, including "
     "commercially, with attribution ('Contains information licensed under the Open "
     "Government Licence – Yukon'). Not an SPDX licence."),
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
        ("licensing", "0006_seed_cc_by_nc_25"),
    ]

    operations = [
        migrations.RunPython(seed, unseed),
    ]
