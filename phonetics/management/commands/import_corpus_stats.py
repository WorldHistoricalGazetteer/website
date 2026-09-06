"""Load per-rule corpus frequencies and example names from the indexing side.

Reviewer attention is the scarce resource, and corpus reach is the only honest
basis for spending it: the counts span two orders of magnitude, so a row in one
rule set can matter a hundred times more than a row in another. WHG cannot
measure that here — the corpus lives on the indexing host — so this command
imports a file produced there.

Everything it does is additive. It never invents a frequency, and a rule it has
no figure for keeps ``corpus_frequency = NULL``, which the UI renders as "not
measured" and the queue sorts after measured rows. Writing 0 instead would read
as "affects nothing" and quietly bury rows we simply have not counted.

Expected shape (``--path stats.json``)::

    {
      "generated": "2026-09-06T12:00:00Z",
      "generator": "indexing/phonetics/corpus_stats.py @ <commit>",
      "corpus": {"name": "toponyms", "names_total": 1050000},
      "rulesets": {
        "mya-Mymr": {
          "names_total": 79705,
          "conversion_rate": 16.6,
          "rules": {
            "<grapheme>": {
              "frequency": 12345,
              "examples": [
                {"name": "ကရပ်ကွက်", "output": "(က)rp∅ကwက∅", "complete": false}
              ]
            }
          }
        }
      }
    }

Graphemes are matched NFD-normalised, so the producer does not have to agree
with us about normal form — only about the characters.
"""

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from phonetics.models import Rule, RuleSet
from phonetics.validation import nfd


class Command(BaseCommand):
    help = 'Import per-rule corpus frequencies and example names.'

    def add_arguments(self, parser):
        parser.add_argument('--path', required=True)
        parser.add_argument('--dry-run', action='store_true')

    def handle(self, *args, **options):
        path = Path(options['path'])
        if not path.exists():
            raise CommandError(f'{path} not found')
        with path.open(encoding='utf-8') as fh:
            data = json.load(fh)

        matched = missing = unknown_sets = 0
        for code, payload in (data.get('rulesets') or {}).items():
            ruleset = RuleSet.objects.filter(code=code).first()
            if ruleset is None:
                unknown_sets += 1
                self.stderr.write(f'no such rule set here: {code} (sync first?)')
                continue
            if not options['dry_run']:
                ruleset.corpus_name_count = payload.get('names_total')
                ruleset.conversion_rate = payload.get('conversion_rate')
                ruleset.save(update_fields=['corpus_name_count', 'conversion_rate'])
            by_orth = {nfd(k): v for k, v in (payload.get('rules') or {}).items()}
            for rule in Rule.objects.filter(ruleset=ruleset):
                entry = by_orth.get(rule.orth)
                if entry is None:
                    missing += 1
                    continue
                matched += 1
                if options['dry_run']:
                    continue
                rule.corpus_frequency = entry.get('frequency')
                rule.examples = entry.get('examples') or []
                rule.save(update_fields=['corpus_frequency', 'examples'])

        self.stdout.write(self.style.SUCCESS(
            f'{matched} rule(s) given a corpus figure; {missing} left unmeasured'
            f'{" (dry run)" if options["dry_run"] else ""}'))
        if unknown_sets:
            self.stdout.write(self.style.WARNING(
                f'{unknown_sets} rule set(s) in the file are not synced here'))
