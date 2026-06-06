from django.db import migrations

# spdx_id, label, url, permits_commercial, share_alike, attribution_required, custom
#
# Covers every licence currently referenced in the indexing-repo AUTHORITIES
# blobs, plus the CC family and the WHG overlay. The source->licence mapping is
# finalised by the Phase 6 indexing-repo audit; this seed only establishes the
# vocabulary rows themselves.
SEED = [
    ("CC-BY-4.0", "Creative Commons Attribution 4.0 International",
     "https://creativecommons.org/licenses/by/4.0/", True, False, True, False),
    ("CC-BY-3.0", "Creative Commons Attribution 3.0 Unported",
     "https://creativecommons.org/licenses/by/3.0/", True, False, True, False),
    ("CC-BY-SA-4.0", "Creative Commons Attribution-ShareAlike 4.0 International",
     "https://creativecommons.org/licenses/by-sa/4.0/", True, True, True, False),
    ("CC-BY-NC-4.0", "Creative Commons Attribution-NonCommercial 4.0 International",
     "https://creativecommons.org/licenses/by-nc/4.0/", False, False, True, False),
    ("CC0-1.0", "CC0 1.0 Universal (Public Domain Dedication)",
     "https://creativecommons.org/publicdomain/zero/1.0/", True, False, False, False),
    ("ODbL-1.0", "Open Data Commons Open Database License v1.0",
     "https://opendatacommons.org/licenses/odbl/1-0/", True, True, True, False),
    ("ODC-By-1.0", "Open Data Commons Attribution License v1.0",
     "https://opendatacommons.org/licenses/by/1-0/", True, False, True, False),
    ("custom-public-domain", "Public Domain",
     "", True, False, False, True),
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
        ("licensing", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(seed, unseed),
    ]
