from django.core.management.base import BaseCommand
from places.models import PlaceName, Place

class Command(BaseCommand):
    help = 'Fix @-delimited toponyms: updates PlaceName topoynm/jsonb and Place.title'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be updated without saving changes'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Number of rows to process per batch'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']

        # 1️⃣ Fix PlaceName entries
        pn_qs = PlaceName.objects.filter(toponym__contains='@')
        total_pn = pn_qs.count()
        self.stdout.write(f"Found {total_pn} PlaceName entries with '@' in toponym")

        for start in range(0, total_pn, batch_size):
            batch = pn_qs[start:start+batch_size]
            for pn in batch:
                if '@' in pn.toponym:
                    toponym_part, lang_part = pn.toponym.split('@', 1)
                    old_toponym = pn.toponym
                    old_jsonb_toponym = pn.jsonb.get('toponym') if pn.jsonb else None
                    old_lang = pn.jsonb.get('lang') if pn.jsonb else None

                    pn.toponym = toponym_part

                    if pn.jsonb is None:
                        pn.jsonb = {}
                    pn.jsonb['toponym'] = toponym_part
                    if lang_part:
                        pn.jsonb['lang'] = lang_part

                    if dry_run:
                        self.stdout.write(
                            f"[DRY RUN] PlaceName id={pn.id} | toponym='{old_toponym}' -> '{pn.toponym}', "
                            f"jsonb.toponym='{old_jsonb_toponym}' -> '{pn.jsonb['toponym']}', "
                            f"lang='{old_lang}' -> '{pn.jsonb.get('lang')}'"
                        )
                    else:
                        pn.save()
                        self.stdout.write(
                            f"Updated PlaceName id={pn.id} | toponym='{old_toponym}' -> '{pn.toponym}', "
                            f"jsonb.toponym='{old_jsonb_toponym}' -> '{pn.jsonb['toponym']}', "
                            f"lang='{old_lang}' -> '{pn.jsonb.get('lang')}'"
                        )

        # 2️⃣ Fix Place.title entries
        place_qs = Place.objects.filter(title__contains='@')
        total_places = place_qs.count()
        self.stdout.write(f"Found {total_places} Place entries with '@' in title")

        for start in range(0, total_places, batch_size):
            batch = place_qs[start:start+batch_size]
            for place in batch:
                if '@' in place.title:
                    title_part, _lang_part = place.title.split('@', 1)
                    old_title = place.title
                    place.title = title_part.strip()  # remove any trailing whitespace

                    if dry_run:
                        self.stdout.write(
                            f"[DRY RUN] Place id={place.id} | title='{old_title}' -> '{place.title}'"
                        )
                    else:
                        place.save()
                        self.stdout.write(
                            f"Updated Place id={place.id} | title='{old_title}' -> '{place.title}'"
                        )
