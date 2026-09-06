"""Report every machine-detectable defect in the synced rule sets.

Intended for CI as much as for the console: place#251 asks for a
duplicate-grapheme and PanPhon-parseability lint precisely because these defects
are invisible to the obvious check. ``--fail-on-defect`` gives it an exit code.
"""

from collections import Counter

from django.core.management.base import BaseCommand

from phonetics.lint import LINT_CODES, lint_rows
from phonetics.models import Rule, RuleSet


class Command(BaseCommand):
    help = 'List machine-detectable defects across the synced rule sets.'

    def add_arguments(self, parser):
        parser.add_argument('--ruleset', default=None)
        parser.add_argument('--fail-on-defect', action='store_true',
                            help='Exit non-zero if any defect is found (for CI).')
        parser.add_argument('--recompute', action='store_true',
                            help='Re-run the lint over the stored rows before reporting. '
                                 'Needed after the lint rules themselves change: the sync '
                                 'skips a file whose bytes have not moved, so cached '
                                 'lint_codes would otherwise keep an old verdict for ever.')

    def handle(self, *args, **options):
        if options['recompute']:
            self.recompute(options['ruleset'])
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

    def recompute(self, only=None):
        """Re-lint the stored rows in place.

        The sync is content-addressed: a file whose git blob sha has not moved is
        not re-read, and its rules keep the lint_codes computed when it last
        changed. That is right for the data and wrong for the verdicts — when the
        lint rules change (as they did on removing 22 false positives), every
        cached verdict is stale and nothing says so. This is the way to move them.
        """
        rulesets = RuleSet.objects.all()
        if only:
            rulesets = rulesets.filter(slug=only)
        changed = 0
        for ruleset in rulesets:
            rules = list(Rule.objects.filter(ruleset=ruleset).order_by('row_index'))
            defects = lint_rows([(r.orth, r.current_ipa) for r in rules])
            for index, rule in enumerate(rules):
                codes = defects.get(index, [])
                if codes != rule.lint_codes:
                    Rule.objects.filter(pk=rule.pk).update(lint_codes=codes)
                    changed += 1
        self.stdout.write(self.style.SUCCESS(f'{changed} row(s) re-linted'))
