"""Audit the licences of every third-party package WHG actually deploys.

Two things make this worth generating rather than maintaining by hand:

* **Scale.** ~175 Python distributions plus the JavaScript compiled into the
  shipped bundles. A hand-written list cannot be kept true, and a *stale*
  licence page is worse than a vague one — a vague statement is unhelpful,
  a stale specific one is a positive misstatement.
* **The repo is not the deployment.** A developer checkout carries test and
  tooling packages that never ship, and ``node_modules`` carries far more than
  webpack actually bundles. This command therefore reads the **running
  environment** via ``importlib.metadata``, so running it inside the deployed
  container is what makes the answer true.

The JavaScript half comes from ``npm run audit:licenses`` (see
``scripts/audit-js-licenses.js``), which resolves what is genuinely bundled from
webpack's own module list and commits the result. This command merges that file
in; it cannot produce it, because the Django container has no Node.

Usage::

    python manage.py audit_licenses            # write the snapshot
    python manage.py audit_licenses --check     # CI: fail if it would change

Outputs, both committed to the repo:

* ``licensing/data/software_licenses.json`` — the structured snapshot, rendered
  by ``/licenses/software/``.
* ``THIRD_PARTY_LICENSES`` — the same audit as a plain-text notice file at the
  repository root, which is the conventional place for downstream recipients
  (and packagers) to look, and which travels with the release artefacts rather
  than only being reachable over HTTP.
"""
import json
import subprocess
from datetime import date
from importlib import metadata
from pathlib import Path

from django.conf import settings
from django.core.management.base import BaseCommand

# Packages whose metadata declares no licence at all. They all HAVE one — they
# simply don't publish it in a machine-readable field — so omitting them would
# understate the audit and guessing would misstate it. Each value below was read
# from the project's own LICENSE file; recheck on a major version bump.
UNDECLARED_OVERRIDES = {
    'MarkupSafe': 'BSD-3-Clause',
    'PyJWT': 'MIT',
    'Pygments': 'BSD-2-Clause',
    'cachetools': 'MIT',
    'cffi': 'MIT',
    'click': 'BSD-3-Clause',
    'cryptography': 'Apache-2.0 OR BSD-3-Clause',
    'idna': 'BSD-3-Clause',
    'joblib': 'BSD-3-Clause',
    'josepy': 'Apache-2.0',
    'packaging': 'Apache-2.0 OR BSD-2-Clause',
    'pillow': 'MIT-CMU',
    'pip': 'MIT',
    'pycparser': 'BSD-3-Clause',
    'pyparsing': 'MIT',
    'scikit-learn': 'BSD-3-Clause',
    'sentry-sdk': 'MIT',
    'typing_extensions': 'PSF-2.0',
    'urllib3': 'MIT',
}

# Where a package is dual-licensed and WHG relies on one of the options, record
# the ELECTION explicitly. Leaving it implicit invites an aggressive reading of
# the more restrictive option — see `citeproc` below, which ships in every page.
ELECTIONS = {
    'citeproc': {
        'elected': 'CPAL-1.0',
        'offered': 'CPAL-1.0 OR AGPL-1.0',
        'note': 'WHG elects CPAL-1.0. File-level copyleft plus an attribution '
                'notice; it does not reach WHG-authored code. Reached via '
                'citation-js and compiled into the shared bundle.',
    },
    'jszip': {
        'elected': 'MIT',
        'offered': 'MIT OR GPL-3.0-or-later',
        'note': 'WHG elects MIT.',
    },
    'text-unidecode': {
        'elected': 'Artistic-1.0',
        'offered': 'Artistic-1.0 OR GPL-2.0-or-later',
        'note': 'WHG elects the Artistic Licence, so no GPL obligation arises.',
    },
    'odfpy': {
        'elected': 'Apache-2.0',
        'offered': 'Apache-2.0 OR GPL-2.0-or-later OR LGPL-2.1-or-later',
        'note': 'WHG elects Apache-2.0.',
    },
}

# Buckets used to group the page and to make the copyleft picture legible at a
# glance. Order matters: the first pattern that matches wins.
_CATEGORIES = [
    ('copyleft-strong', ('AGPL', 'SSPL')),
    ('copyleft-weak', ('LGPL', 'LPGL', 'MPL', 'Mozilla', 'CPAL', 'EPL', 'CDDL')),
    ('copyleft-strong', ('GPL',)),          # after LGPL/AGPL so it can't swallow them
    ('public-domain', ('CC0', 'Unlicense', 'Public Domain', 'WTFPL')),
    ('permissive', ('MIT', 'BSD', 'Apache', 'ISC', 'PSF', 'Python Software',
                    'Artistic', 'HPND', 'Zlib', 'BlueOak', 'OFL', 'CC-BY',
                    'MIT-CMU', 'AFL', 'Historical Permission')),
]


def categorise(license_str):
    s = (license_str or '')
    for cat, needles in _CATEGORIES:
        if any(n.lower() in s.lower() for n in needles):
            return cat
    return 'unknown'


def _direct_python_requirements():
    """Names listed in requirements.txt — i.e. what WHG chose to depend on, as
    opposed to what those choices dragged in."""
    path = Path(settings.BASE_DIR) / 'requirements.txt'
    names = set()
    if not path.exists():
        return names
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith(('#', '-')):
            continue
        for sep in ('==', '>=', '<=', '~=', '>', '<', '[', ';'):
            line = line.split(sep)[0]
        if line.strip():
            names.add(line.strip().lower().replace('_', '-'))
    return names


def collect_python():
    direct = _direct_python_requirements()
    rows = []
    for dist in metadata.distributions():
        m = dist.metadata
        name = m['Name']
        if not name:
            continue
        classifiers = [c.split('::')[-1].strip()
                       for c in (m.get_all('Classifier') or [])
                       if c.startswith('License ::') and 'OSI Approved' != c.split('::')[-1].strip()]
        declared = (m['License'] or '').strip()
        # Some projects put the entire licence text in the License field.
        if len(declared) > 90:
            declared = ''
        lic = classifiers[0] if classifiers else declared
        election = ELECTIONS.get(name.lower())
        if election:
            lic = election['elected']
        elif not lic:
            lic = UNDECLARED_OVERRIDES.get(name, '')
        rows.append({
            'name': name,
            'version': m['Version'],
            'license': lic or 'Not declared',
            'declared': bool(lic),
            'category': categorise(lic),
            'url': (m['Home-page'] or '').strip(),
            'direct': name.lower().replace('_', '-') in direct,
            'election': election,
        })
    rows.sort(key=lambda r: r['name'].lower())
    return rows


def load_js():
    """The JS half, produced by ``npm run audit:licenses``. Absent on a fresh
    checkout: report that rather than silently emitting an empty list, which
    would read as "WHG ships no third-party JavaScript"."""
    path = Path(settings.BASE_DIR) / 'licensing' / 'data' / 'js_licenses.json'
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    for row in data.get('packages', []):
        election = ELECTIONS.get(row['name'].lower())
        if election:
            row['license'] = election['elected']
            row['election'] = election
        row['category'] = categorise(row.get('license'))
        row['declared'] = bool(row.get('license')) and row['license'] != 'Not declared'
    data['packages'].sort(key=lambda r: r['name'].lower())
    return data


def _git_revision():
    try:
        return subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=settings.BASE_DIR, stderr=subprocess.DEVNULL, text=True).strip()
    except Exception:
        return ''


def _notice_substance(text):
    """The notice minus its volatile header lines, for --check comparison."""
    return "\n".join(
        l for l in text.splitlines()
        if not l.startswith(("Audited:", "Revision:", "App version:"))
    )


def render_notice(snapshot):
    """Render the audit as a plain-text third-party notice file.

    Deliberately a flat, greppable list rather than a pretty table: this file
    exists to be read by people checking compliance and by tooling, and both are
    better served by one package per line than by alignment that breaks the
    moment a name grows long.
    """
    lines = [
        "THIRD-PARTY SOFTWARE NOTICES",
        "World Historical Gazetteer",
        "",
        "This file lists the third-party packages distributed with or deployed as",
        "part of the World Historical Gazetteer, and the licence each is made",
        "available under. It is generated from the running environment by",
        "`python manage.py audit_licenses` — do not edit it by hand.",
        "",
        "Each package remains under its own licence and its own copyright. Nothing",
        "here alters those terms; the WHG licence in LICENSE covers only WHG's own",
        "code. The same audit is published at https://whgazetteer.org/licenses/software/",
        "",
        f"Audited:     {snapshot['audited']}",
        f"Revision:    {snapshot['revision'] or 'unknown'}",
        f"App version: {snapshot['app_version'] or 'unknown'}",
        "",
    ]

    for heading, section in (
        ("PYTHON PACKAGES (server-side)", snapshot["python"]),
        ("JAVASCRIPT PACKAGES (delivered to the browser)", snapshot["javascript"]),
    ):
        packages = section.get("packages", [])
        lines += ["=" * 72, f"{heading} — {len(packages)}", "=" * 72, ""]
        if section.get("missing"):
            lines += ["  (not audited in this environment — run `npm run audit:licenses`)", ""]
            continue
        for row in packages:
            version = row.get("version") or ""
            lines.append(f"{row['name']}{' ' + version if version else ''}")
            lines.append(f"    Licence: {row.get('license') or 'Not declared'}")
            # Where we chose one arm of a multi-licence offer, say which and why:
            # a bare "MIT" against a dual-licensed package looks like an error.
            election = row.get("election")
            if election:
                lines.append(f"    Elected: {election.get('elected')} "
                             f"(offered: {election.get('offered', '')})")
            if row.get("url"):
                lines.append(f"    {row['url']}")
            lines.append("")

    return "\n".join(lines).rstrip("\n") + "\n"


class Command(BaseCommand):
    help = ("Audit third-party package licences in the RUNNING environment and "
            "write licensing/data/software_licenses.json.")

    def add_arguments(self, parser):
        parser.add_argument('--check', action='store_true',
                            help='Exit non-zero if the snapshot is out of date, '
                                 'without writing. For CI.')

    def handle(self, *args, **opts):
        python_rows = collect_python()
        js = load_js()

        snapshot = {
            'audited': date.today().isoformat(),
            'revision': _git_revision(),
            'app_version': getattr(settings, 'APP_VERSION', ''),
            'python': {
                'count': len(python_rows),
                'packages': python_rows,
            },
            'javascript': js or {'count': 0, 'packages': [], 'missing': True},
        }

        out = Path(settings.BASE_DIR) / 'licensing' / 'data' / 'software_licenses.json'
        out.parent.mkdir(parents=True, exist_ok=True)
        rendered = json.dumps(snapshot, indent=1, ensure_ascii=False) + '\n'

        notice = Path(settings.BASE_DIR) / 'THIRD_PARTY_LICENSES'
        notice_text = render_notice(snapshot)

        if opts['check']:
            if not out.exists():
                self.stderr.write(self.style.ERROR('No snapshot; run audit_licenses.'))
                raise SystemExit(1)
            old = json.loads(out.read_text())
            # The date changes on every run, so compare only the substance.
            for d in (old, snapshot):
                d.pop('audited', None)
                d.pop('revision', None)
            if old != snapshot:
                self.stderr.write(self.style.ERROR(
                    'Dependency licences have changed since the last audit — '
                    'run: python manage.py audit_licenses'))
                raise SystemExit(1)
            # The header carries the audit date and revision, which change on
            # every run; compare the substance only, exactly as above.
            if (not notice.exists()
                    or _notice_substance(notice.read_text()) != _notice_substance(notice_text)):
                self.stderr.write(self.style.ERROR(
                    'THIRD_PARTY_LICENSES is missing or stale — '
                    'run: python manage.py audit_licenses'))
                raise SystemExit(1)
            self.stdout.write(self.style.SUCCESS('Licence snapshot is current.'))
            return

        out.write_text(rendered)
        notice.write_text(notice_text)

        undeclared = [r['name'] for r in python_rows if not r['declared']]
        self.stdout.write(self.style.SUCCESS(
            f"Wrote {out.relative_to(settings.BASE_DIR)} and "
            f"{notice.relative_to(settings.BASE_DIR)} — "
            f"{len(python_rows)} Python, {(js or {}).get('count', 0)} JavaScript."))
        if undeclared:
            self.stdout.write(self.style.WARNING(
                f"{len(undeclared)} package(s) still declare no licence and have no "
                f"override: {', '.join(sorted(undeclared))}"))
        if js is None:
            self.stdout.write(self.style.WARNING(
                'No JavaScript audit found — run `npm run audit:licenses` and re-run.'))
