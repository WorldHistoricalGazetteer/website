"""Regenerate the bundled ISO 639-3 / 15924 name table.

Development tool. ``pycountry`` is deliberately not in requirements.txt: the
table is committed, so production never needs the library that made it.
"""

import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

# Macrolanguage → the individual code Epitran keys its rule sets by. Without
# these, a browser advertising 'zh' or 'ar' matches no rule set at all.
MACRO = {'zh': 'cmn', 'ms': 'zsm', 'ar': 'arb', 'fa': 'pes', 'sw': 'swh',
         'uz': 'uzn', 'az': 'azj', 'ku': 'ckb', 'no': 'nob'}


class Command(BaseCommand):
    help = 'Regenerate phonetics/data/iso_names.json from pycountry.'

    def handle(self, *args, **options):
        try:
            import pycountry
        except ImportError:
            raise CommandError('pip install pycountry (development only) to regenerate this table.')
        languages = {l.alpha_3: l.name for l in pycountry.languages if hasattr(l, 'alpha_3')}
        scripts = {s.alpha_4: s.name for s in pycountry.scripts}
        alpha2 = {l.alpha_2: l.alpha_3 for l in pycountry.languages if hasattr(l, 'alpha_2')}
        alpha2.update(MACRO)
        path = Path(__file__).resolve().parents[2] / 'data' / 'iso_names.json'
        with path.open('w', encoding='utf-8') as fh:
            json.dump({'languages': languages, 'scripts': scripts,
                       'alpha2_to_alpha3': alpha2}, fh,
                      ensure_ascii=False, indent=0, sort_keys=True)
        self.stdout.write(self.style.SUCCESS(
            f'{len(languages)} languages, {len(scripts)} scripts → {path}'))
