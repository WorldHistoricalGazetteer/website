from django.db import migrations

# Canonical per-item web templates for the authority gazetteers that publish a
# public per-place page (place#121). These mirror the values that the indexing
# ``AUTHORITIES`` config now pushes; seeding here means the Atlas popup
# "view at source" links work immediately, before the next inventory push. A
# push refreshes them from the canonical source (never clobbers with null).
WEB_ITEMS = {
    'pl':  'https://pleiades.stoa.org/places/<id>',
    'gn':  'https://www.geonames.org/<id>',
    'tgn': 'https://vocab.getty.edu/tgn/<id>',
    'wd':  'https://www.wikidata.org/wiki/<id>',
    'loc': 'https://www.loc.gov/item/<id>/',
    'tm':  'https://www.trismegistos.org/place/<id>',
    'og':  'https://ottgaz.org/wiki/Item:<id>',
}


def seed(apps, schema_editor):
    Entry = apps.get_model('api', 'GazetteerRegistryEntry')
    for ns, tpl in WEB_ITEMS.items():
        Entry.objects.filter(namespace=ns).update(web_item=tpl)


def unseed(apps, schema_editor):
    Entry = apps.get_model('api', 'GazetteerRegistryEntry')
    Entry.objects.filter(namespace__in=list(WEB_ITEMS)).update(web_item=None)


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0009_gazetteerregistryentry_web_item'),
    ]

    operations = [
        migrations.RunPython(seed, unseed),
    ]
