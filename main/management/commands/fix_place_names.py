from django.core.management.base import BaseCommand
from places.models import PlaceName

class Command(BaseCommand):
    help = 'Split @-delimited toponyms and store language in lang field, correcting both toponym and jsonb'

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

        qs = PlaceName.objects.filter(toponym__contains='@')
        total = qs.count()
        self.stdout.write(f"Found {total} PlaceName entries with '@' in toponym")

        for start in range(0, total, batch_size):
            batch = qs[start:start+batch_size]
            for pn in batch:
                if '@' in pn.toponym:
                    # Split toponym
                    toponym_part, lang_part = pn.toponym.split('@', 1)
                    old_toponym = pn.toponym
                    old_jsonb_toponym = pn.jsonb.get('toponym') if pn.jsonb else None
                    old_lang = pn.jsonb.get('lang') if pn.jsonb else None

                    # Update main field
                    pn.toponym = toponym_part

                    # Update jsonb
                    if pn.jsonb is None:
                        pn.jsonb = {}
                    pn.jsonb['toponym'] = toponym_part
                    if lang_part:
                        pn.jsonb['lang'] = lang_part

                    if dry_run:
                        self.stdout.write(
                            f"[DRY RUN] id={pn.id} | toponym='{old_toponym}' -> '{pn.toponym}', "
                            f"jsonb.toponym='{old_jsonb_toponym}' -> '{pn.jsonb['toponym']}', "
                            f"lang='{old_lang}' -> '{pn.jsonb.get('lang')}'"
                        )
                    else:
                        pn.save()
                        self.stdout.write(
                            f"Updated id={pn.id} | toponym='{old_toponym}' -> '{pn.toponym}', "
                            f"jsonb.toponym='{old_jsonb_toponym}' -> '{pn.jsonb['toponym']}', "
                            f"lang='{old_lang}' -> '{pn.jsonb.get('lang')}'"
                        )
