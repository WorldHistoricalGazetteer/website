"""Pull the rule sets from the ``indexing`` repo into the review database.

The CSVs are not ours to edit. They live in ``WorldHistoricalGazetteer/indexing``
and change there — by hand, and by agents processing what reviewers record here.
This module mirrors them; nothing in this app ever writes one back.

**Why a sync and not a fetch per page load.** Three reasons, in order of how
much they would hurt:

1. A review has to name the value it was made against. If the file is re-read on
   every request, a row can change under a reviewer between the page rendering
   and their verdict posting, and the verdict silently attaches to a value they
   never saw. :class:`~phonetics.models.RuleSetVersion` and
   ``Review.reviewed_ipa`` exist to make that impossible; both need a fetch that
   happens at a known moment.
2. 115 files per page view against a third-party API is not a page load.
3. GitHub would rate-limit it within a minute.

So: a scheduled sync (and a staff "sync now" button) writes versions; page loads
read the database. Latency after an upstream edit is bounded by the schedule,
which is the right trade for reviews that can be trusted afterwards.

**Costs almost nothing when nothing changed.** The contents listing returns the
git blob sha of every file, and a blob sha *is* the content — so a file whose
sha we already hold is skipped without being downloaded. A no-op sync of 115
rule sets is two API calls.

**Adoption is detected, not asserted.** When a synced value turns out to equal
a proposal someone made, that review is stamped ``adopted_upstream_at``. Nobody
upstream has to remember to tell us, and the credit is therefore reliable.
"""

import csv
import io
import logging

import requests
from django.conf import settings
from django.db import transaction
from django.utils import timezone

from .iso import language_name, script_name
from .lint import lint_rows
from .models import Posture, Review, Rule, RuleSet, RuleSetVersion, Verdict
from .validation import nfd

logger = logging.getLogger(__name__)

API = 'https://api.github.com'
RAW = 'https://raw.githubusercontent.com'
TIMEOUT = 30


class SyncError(RuntimeError):
    pass


def _sources():
    return getattr(settings, 'PHONETICS_SOURCES', [])


def _headers():
    token = (getattr(settings, 'PHONETICS_GITHUB_TOKEN', '')
             or getattr(settings, 'GITHUB_SNAG_TOKEN', ''))
    headers = {'Accept': 'application/vnd.github+json',
               'User-Agent': 'whg-phonetics-sync'}
    if token:
        headers['Authorization'] = f'Bearer {token}'
    return headers


def _get(url, **kwargs):
    response = requests.get(url, headers=_headers(), timeout=TIMEOUT, **kwargs)
    if response.status_code != 200:
        raise SyncError(f'{response.status_code} from {url}: {response.text[:200]}')
    return response


def list_directory(repo, ref, path):
    """``[{'name', 'sha', 'size'}, …]`` for the CSVs in one source directory.

    A 404 is returned as an empty list rather than raised: a configured source
    that has not been pushed yet is a normal state, not a broken sync.
    """
    url = f'{API}/repos/{repo}/contents/{path}'
    try:
        response = _get(url, params={'ref': ref})
    except SyncError as exc:
        if ' 404 ' in f' {exc} ':
            logger.warning('phonetics sync: %s@%s/%s not found upstream', repo, ref, path)
            return []
        raise
    return [{'name': e['name'], 'sha': e['sha'], 'size': e.get('size', 0)}
            for e in response.json()
            if e.get('type') == 'file' and e['name'].endswith('.csv')]


def head_commit(repo, ref):
    try:
        return _get(f'{API}/repos/{repo}/commits/{ref}').json().get('sha', '')
    except SyncError:
        return ''


def fetch_csv(repo, ref, path, name):
    return _get(f'{RAW}/{repo}/{ref}/{path}/{name}').text


def parse_csv(text):
    """``Orth,Phon`` → ``[(orth, phon), …]``, in file order.

    Values are returned exactly as written; normalisation happens where it is
    compared or stored, so the original spelling stays available to show a
    reviewer what the file actually contains.
    """
    rows = []
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if not header or [h.strip() for h in header[:2]] != ['Orth', 'Phon']:
        raise SyncError(f'unexpected header {header!r}; expected Orth,Phon')
    for row in reader:
        if not row or not row[0]:
            continue
        rows.append((row[0], row[1] if len(row) > 1 else ''))
    return rows


@transaction.atomic
def apply_ruleset(code, rows, *, posture, repo, ref, path, blob_sha, commit_sha):
    """Bring one rule set's rows into line with a freshly fetched file.

    Returns ``(ruleset, version, created_version)``.
    """
    language_code, _, script_code = code.partition('-')
    ruleset, _ = RuleSet.objects.update_or_create(
        code=code,
        defaults={
            'language_code': language_code,
            'script_code': script_code,
            'language_name': language_name(language_code),
            'script_name': script_name(script_code),
            'posture': posture,
            'source_repo': repo,
            'source_ref': ref,
            'source_path': f'{path}/{code}.csv',
            'present_upstream': True,
        })

    existing = ruleset.versions.filter(blob_sha=blob_sha).first()
    if existing:
        # Same bytes: nothing to do beyond confirming this is still current.
        ruleset.versions.exclude(pk=existing.pk).update(is_current=False)
        RuleSetVersion.objects.filter(pk=existing.pk).update(is_current=True)
        return ruleset, existing, False

    ruleset.versions.update(is_current=False)
    version = RuleSetVersion.objects.create(
        ruleset=ruleset, blob_sha=blob_sha, commit_sha=commit_sha,
        row_count=len(rows), is_current=True)

    defects = lint_rows(rows)
    seen_keys = []
    for index, (orth, phon) in enumerate(rows):
        key = nfd(orth)
        if key in seen_keys:
            # A duplicate grapheme cannot become a second Rule — the whole point
            # of the NFD key is that these are one row. It is flagged on the row
            # that already exists, where a reviewer will see it.
            rule = Rule.objects.filter(ruleset=ruleset, orth=key).first()
            if rule:
                codes = sorted(set(rule.lint_codes) | {'duplicate_grapheme'})
                Rule.objects.filter(pk=rule.pk).update(lint_codes=codes)
            continue
        seen_keys.append(key)
        rule, created = Rule.objects.get_or_create(
            ruleset=ruleset, orth=key,
            defaults={'first_version': version})
        rule.orth_source = orth
        rule.current_ipa = nfd(phon)
        rule.current_ipa_source = phon
        rule.row_index = index
        rule.last_version = version
        rule.present_upstream = True
        rule.lint_codes = defects.get(index, [])
        rule.save()
        _reconcile_reviews(rule)

    # Rows that vanished upstream keep their reviews; they are simply marked
    # absent. Deleting them would destroy the record of work done on a row
    # someone removed, which is exactly when that record is most interesting.
    (Rule.objects.filter(ruleset=ruleset)
     .exclude(orth__in=seen_keys)
     .update(present_upstream=False, last_version=version))

    return ruleset, version, True


def _reconcile_reviews(rule):
    """Re-point the review record at a rule whose value may just have changed.

    Two things happen, and both are detections rather than assertions:

    * a standing proposal that now equals the upstream value is stamped as
      adopted — the reviewer's correction was taken up;
    * the stale count is refreshed, so reviews made against a superseded value
      are visibly *about the old value* and are never counted as endorsement of
      the new one.
    """
    adopted = (rule.reviews
               .filter(verdict=Verdict.CORRECT, adopted_upstream_at__isnull=True,
                       proposed_ipa=rule.current_ipa)
               .exclude(reviewed_ipa=rule.current_ipa))
    if adopted.exists():
        adopted.update(adopted_upstream_at=timezone.now())
    rule.recount()


def sync_all(*, only=None):
    """Sync every configured source. Returns a summary dict for the caller to log."""
    summary = {'sources': [], 'rulesets': 0, 'changed': 0, 'errors': []}
    for source in _sources():
        repo, ref, path = source['repo'], source.get('ref', 'main'), source['path']
        posture = source.get('posture', Posture.SHIPPED)
        entries = list_directory(repo, ref, path)
        commit = head_commit(repo, ref)
        found = []
        for entry in entries:
            code = entry['name'][:-4]
            if only and code not in only:
                continue
            found.append(code)
            try:
                text = fetch_csv(repo, ref, path, entry['name'])
                rows = parse_csv(text)
            except SyncError as exc:
                summary['errors'].append(f'{code}: {exc}')
                continue
            _, _, changed = apply_ruleset(
                code, rows, posture=posture, repo=repo, ref=ref, path=path,
                blob_sha=entry['sha'], commit_sha=commit)
            summary['rulesets'] += 1
            summary['changed'] += int(changed)
        if entries and not only:
            (RuleSet.objects.filter(source_repo=repo, source_path__startswith=f'{path}/')
             .exclude(code__in=found).update(present_upstream=False))
        summary['sources'].append({'repo': repo, 'ref': ref, 'path': path,
                                   'posture': posture, 'files': len(entries),
                                   'commit': commit})
    return summary
