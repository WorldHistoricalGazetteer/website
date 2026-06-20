from django.db import migrations

# Additional licence vocabulary rows, found while auditing candidate gazetteers
# in the Baserow tracker (datasets hosted on Dataverse, eScholarship, AWMC, etc.).
# Same tuple shape as 0002:
# spdx_id, label, url, permits_commercial, share_alike, attribution_required, custom
SEED = [
    ("CC-BY-SA-3.0", "Creative Commons Attribution-ShareAlike 3.0 Unported",
     "https://creativecommons.org/licenses/by-sa/3.0/", True, True, True, False),
    ("CC-BY-NC-SA-4.0", "Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International",
     "https://creativecommons.org/licenses/by-nc-sa/4.0/", False, True, True, False),
    ("CC-BY-NC-ND-3.0", "Creative Commons Attribution-NonCommercial-NoDerivatives 3.0 Unported",
     "https://creativecommons.org/licenses/by-nc-nd/3.0/", False, False, True, False),
    ("CC-BY-NC-ND-4.0", "Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International",
     "https://creativecommons.org/licenses/by-nc-nd/4.0/", False, False, True, False),
    ("CC-BY-3.0-IGO", "Creative Commons Attribution 3.0 IGO",
     "https://creativecommons.org/licenses/by/3.0/igo/", True, False, True, False),
    ("custom-all-rights-reserved", "All Rights Reserved",
     "", False, False, False, True),
    ("custom-academic-use", "Academic Use Only (no redistribution or commercial use)",
     "", False, False, True, True),
]

_FIELDS = ("label", "url", "permits_commercial", "share_alike",
           "attribution_required", "custom")


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
        ("licensing", "0002_seed_licenses"),
    ]

    operations = [
        migrations.RunPython(seed, unseed),
    ]
