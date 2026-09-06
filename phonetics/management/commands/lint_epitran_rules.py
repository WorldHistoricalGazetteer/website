"""Report every machine-detectable defect in the synced rule sets.

Intended for CI as much as for the console: place#251 asks for a
duplicate-grapheme and PanPhon-parseability lint precisely because these defects
are invisible to the obvious check. ``--fail-on-defect`` gives it an exit code.
"""

from collections import Counter

from django.core.management.base import BaseCommand

from phonetics.lint import LINT_CODES
from phonetics.models import Rule


class Command(BaseCommand):
    help = 'List machine-detectable defects across the synced rule sets.'

    def add_arguments(self, parser):
        parser.add_argument('--ruleset', default=None)
        parser.add_argument('--fail-on-defect', action='store_true',
                            help='Exit non-zero if any defect is found (for CI).')

    def handle(self, *args, **options):
        rules = Rule.objects.filter(present_upstream=True).exclude(lint_codes=[])
        if options['ruleset']:
            rules = rules.filter(ruleset__code=options['ruleset'])
        rules = rules.select_related('ruleset').order_by('ruleset__code', 'row_index')

        tally = Counter()
        files = set()
        for rule in rules:
            files.add(rule.ruleset.code)
            for code in rule.lint_codes:
                tally[code] += 1
            self.stdout.write(
                f'{rule.ruleset.code:14s} {rule.orth!r:>12s} → {rule.current_ipa!r:<12s} '
                f'{",".join(rule.lint_codes)}')

        self.stdout.write('')
        for code, count in tally.most_common():
            label, why = LINT_CODES.get(code, (code, ''))
            self.stdout.write(f'{count:5d}  {label} — {why}')
        total = rules.count()
        self.stdout.write(self.style.WARNING(
            f'{total} defective row(s) in {len(files)} rule set(s)') if total
            else self.style.SUCCESS('No machine-detectable defects.'))
        if total and options['fail_on_defect']:
            raise SystemExit(1)
