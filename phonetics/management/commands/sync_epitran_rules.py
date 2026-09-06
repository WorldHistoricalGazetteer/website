from django.core.management.base import BaseCommand

from phonetics.sync import sync_all


class Command(BaseCommand):
    help = ('Pull the Epitran rule sets from the indexing repo into the review '
            'database. Never writes upstream.')

    def add_arguments(self, parser):
        parser.add_argument('--only', nargs='*', default=None,
                            help='Rule-set codes to sync, e.g. mya-Mymr sin-Sinh.')

    def handle(self, *args, **options):
        summary = sync_all(only=set(options['only']) if options['only'] else None)
        for source in summary['sources']:
            self.stdout.write(f"{source['repo']}@{source['ref']}/{source['path']} "
                              f"[{source['posture']}] — {source['files']} file(s), "
                              f"commit {source['commit'][:8]}")
        self.stdout.write(self.style.SUCCESS(
            f"{summary['rulesets']} rule set(s) synced, {summary['changed']} changed"))
        for error in summary['errors']:
            self.stderr.write(self.style.ERROR(error))
