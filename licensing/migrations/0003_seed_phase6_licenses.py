from django.db import migrations

# Licence rows requested by the Phase 6 indexing-repo audit
# (indexing/developer/handoff-citation-metadata.md §3):
#
#   * CC-BY-ND-4.0 — the one genuine SPDX licence not in the 0002 seed set
#     (needed by the DGSD authority; indexing already sends this license_spdx).
#   * three custom=True rows for bespoke, non-SPDX source terms. Their spdx_id
#     values are WHG-local lookup keys (custom- prefix, mirroring
#     custom-public-domain) — the indexing AUTHORITIES entries set
#     ``license_spdx`` to these so the inventory push can resolve the FK:
#       nl    -> custom-nativeland-dst
#       ukhc  -> custom-historic-counties
#       chgis -> custom-chgis-academic
#
# url values mirror each source's verified license_url so the registry deed
# link matches what the push sends.
#
# tuple: (spdx_id, label, url, permits_commercial, share_alike,
#         attribution_required, custom, notes)
SEED = [
    ("CC-BY-ND-4.0",
     "Creative Commons Attribution-NoDerivatives 4.0 International",
     "https://creativecommons.org/licenses/by-nd/4.0/",
     True, False, True, False,
     "No derivatives. Used by DGSD (endorsed for WHG use by its author)."),

    ("custom-nativeland-dst",
     "Native Land Digital — Data Sovereignty Treaty",
     "https://api-docs.native-land.ca/data-sovereignty-treaty",
     False, False, True, True,
     "Non-commercial; redistribution by permission. Indigenous communities "
     "are the rightful stewards of this data (OCAP®)."),

    ("custom-historic-counties",
     "Historic Counties Trust — Permissive Terms",
     "https://county-borders.co.uk/",
     True, False, True, True,
     "Bespoke permissive terms; commercial use allowed; attribution requested."),

    ("custom-chgis-academic",
     "CHGIS — Academic Research Only",
     "https://chgis.fas.harvard.edu/data/chgis/v6/",
     False, False, True, True,
     "Academic research only; no commercial use, resale, or redistribution."),
]

_FIELDS = ("label", "url", "permits_commercial", "share_alike",
           "attribution_required", "custom", "notes")


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
