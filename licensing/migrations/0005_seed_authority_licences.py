"""Seed the licences the gazetteer authorities actually send (place#157).

The inventory endpoint silently skips any ``license_spdx`` this table doesn't
know, leaving the registry row with no licence on a push that returns 200 — so
six sources have been arriving with terms that were simply dropped. Values are
taken verbatim from ``processing/verify_licences.py --seed-json`` in the indexing
repo, which is the source of truth for what each authority declares.

Five of the six are one named institution's terms and are therefore marked
``contributor_selectable=False``: they must exist so those sources can be
described accurately, but a WHG contributor cannot meaningfully license their own
data under, say, the UK Data Service End User Licence.
"""
from django.db import migrations

# spdx_id, label, url, permits_commercial, share_alike, attribution_required,
# no_derivatives, custom, contributor_selectable, notes
SEED = [
    ("custom-nativeland-dst",
     "Native Land Digital Data Sovereignty Treaty",
     "https://api-docs.native-land.ca/data-sovereignty-treaty",
     False, False, True, False, True, False,
     "OCAP®-aligned bespoke terms. Non-commercial use only; redistribution by "
     "explicit permission; attribution plus acknowledgement of Indigenous "
     "communities as the rightful stewards of the data is mandatory."),

    ("custom-historic-counties",
     "Historic County Borders Project terms",
     "https://county-borders.co.uk/",
     True, False, False, False, True, False,
     "Free for personal, educational, non-commercial AND commercial use. "
     "Attribution requested but not required."),

    ("custom-chgis-academic",
     "CHGIS academic-use-only terms (Harvard / Fudan)",
     "https://chgis.fas.harvard.edu/data/chgis/v6/",
     False, False, True, False, True, False,
     "Academic research only — no commercial use, no resale, and no "
     "redistribution (stricter than any CC licence). Indexing for "
     "search/reconciliation only; re-hosting would need direct permission from "
     "the rights holders."),

    ("custom-ukds-eul",
     "UK Data Service End User Licence",
     "https://ukdataservice.ac.uk/app/uploads/cd137-enduserlicence.pdf",
     False, False, True, False, True, False,
     "Registration-gated bespoke EULA. Use for research/teaching only, no "
     "commercial use, no onward distribution — the data is indexed in place and "
     "never offered for download. Each user must obtain their own copy from the "
     "UK Data Service."),

    # permits_commercial / no_derivatives are NULL, not False: the UN makes no
    # grant of rights at all, so neither permission nor prohibition is evidenced.
    ("custom-un-geodata",
     "UN Geospatial Data terms",
     "https://www.un.org/geospatial/",
     None, False, True, None, True, False,
     "No explicit licence grant from the UN. Attribution to the United Nations "
     "is required and the boundary/designation disclaimer must accompany any "
     "use. NOT public domain — the registry's former \"custom-public-domain\" "
     "value was inherited from the retired Natural Earth source."),

    # A standard SPDX licence that simply hadn't been seeded. Selectable: a
    # contributor may legitimately choose it for their own data.
    ("CC-BY-ND-4.0",
     "Creative Commons Attribution-NoDerivatives 4.0 International",
     "https://creativecommons.org/licenses/by-nd/4.0/",
     True, False, True, True, False, True,
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
        ("licensing", "0004_licence_tristate_and_selectable"),
    ]

    operations = [
        migrations.RunPython(seed, unseed),
    ]
