# placetypes/management/commands/sync_aat_types.py
"""
Django management command to synchronise the local Type table with the
Getty AAT (Art & Architecture Thesaurus) explicit N-Triples export.

Top-down approach: walks from broad entry points, excludes non-place
subtrees, and assigns multi-valued fclass from a configurable map.

Usage:
    python manage.py sync_aat_types              # download if new, parse, update DB
    python manage.py sync_aat_types --force      # re-download even if unchanged
    python manage.py sync_aat_types --local /path/to/explicit_dir/
    python manage.py sync_aat_types --dry-run    # report counts without writing
    python manage.py sync_aat_types --api        # crawl via JSON API (slow fallback)
"""

import json
import logging
import re
import time
import zipfile
from collections import defaultdict
from pathlib import Path

import requests
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from placetypes.aat_config import (
    AAT_DUMP_CACHE_DIR,
    AAT_DUMP_META_FILE,
    AAT_ENTRY_POINTS,
    AAT_EXCLUDED_SUBTREES,
    AAT_EXPLICIT_DUMP_URL,
    AAT_FCLASS_MAP,
    AAT_NT_HIERARCHICAL_RELS,
    AAT_NT_SCOPE_NOTES,
    AAT_NT_TERMS,
    AAT_SCOPE_NOTE_URI_PREFIX,
    AAT_TERM_URI_PREFIX,
    AAT_URI_PREFIX,
    GVP_BROADER_GENERIC,
    GVP_BROADER_PREFERRED,
    RDF_VALUE,
    SKOS_SCOPE_NOTE,
    SKOSXL_LITERAL_FORM,
    SKOSXL_PREF_LABEL,
)

logger = logging.getLogger(__name__)

# Regex for parsing N-Triples lines.
_NT_LINE_RE = re.compile(
    r'^<([^>]+)>\s+<([^>]+)>\s+'
    r'(?:<([^>]+)>'
    r'|"((?:[^"\\]|\\.)*)"'
    r'(?:@(\w[\w-]*))?'
    r'(?:\^\^<[^>]+>)?'
    r')\s*\.\s*$'
)


def _parse_nt_line(line):
    """Parse a single N-Triples line. Returns (subj, pred, obj_uri, literal, lang) or None."""
    m = _NT_LINE_RE.match(line)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)


# Regex for \uXXXX and \UXXXXXXXX escapes in N-Triples string literals.
_NT_UNICODE_RE = re.compile(r'\\u([0-9A-Fa-f]{4})|\\U([0-9A-Fa-f]{8})')


def _decode_nt_unicode(s):
    """Decode N-Triples \\uXXXX / \\UXXXXXXXX escapes to proper Unicode."""
    if s is None:
        return s
    if '\\u' not in s and '\\U' not in s:
        return s
    return _NT_UNICODE_RE.sub(
        lambda m: chr(int(m.group(1) or m.group(2), 16)), s
    )


def _aat_id_from_uri(uri):
    """Extract integer AAT id from a URI like http://vocab.getty.edu/aat/300008347."""
    if uri and uri.startswith(AAT_URI_PREFIX):
        tail = uri[len(AAT_URI_PREFIX):]
        if tail.isdigit():
            return int(tail)
    return None


class Command(BaseCommand):
    help = "Synchronise the local Type table with the Getty AAT explicit N-Triples dump."

    def add_arguments(self, parser):
        parser.add_argument(
            '--force', action='store_true',
            help='Re-download even if the remote file has not changed.',
        )
        parser.add_argument(
            '--local', type=str, default=None,
            help='Path to a directory containing the extracted .nt files '
                 '(skips download).',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Parse and report counts without writing to the database.',
        )
        parser.add_argument(
            '--api', action='store_true',
            help='Crawl the hierarchy via the Getty Linked Art JSON API '
                 'instead of downloading the bulk N-Triples dump. '
                 'Slower but works when the dump endpoint is unavailable.',
        )

    def handle(self, *args, **options):
        force = options['force']
        local_path = options['local']
        dry_run = options['dry_run']
        use_api = options['api']

        t0 = time.time()

        if use_api:
            place_types = self._crawl_api()
        else:
            # -- Step 1: Obtain the .nt files ------------------------------
            if local_path:
                nt_dir = Path(local_path)
                if not nt_dir.is_dir():
                    raise CommandError(f"Local path is not a directory: {local_path}")
                self.stdout.write(f"Using local directory: {nt_dir}")
            else:
                nt_dir = self._download_if_needed(force)
                if nt_dir is None:
                    raise CommandError(
                        "No cached AAT dump files found and the remote "
                        "returned 304. Use --force to re-download."
                    )

            # -- Step 2: Parse the three files -----------------------------
            t1 = time.time()

            self.stdout.write("Parsing hierarchy edges ...")
            preferred_parent, children, parents = self._parse_hierarchy(
                nt_dir / AAT_NT_HIERARCHICAL_RELS)
            t1a = time.time()
            self.stdout.write(
                f"  -> {sum(len(v) for v in children.values()):,} edges, "
                f"{len(preferred_parent):,} preferred-parent links "
                f"in {t1a - t1:.1f}s")

            self.stdout.write("Parsing labels (SKOS-XL two-hop) ...")
            labels = self._parse_labels(nt_dir / AAT_NT_TERMS)
            t1b = time.time()
            self.stdout.write(
                f"  -> {len(labels):,} English labels in {t1b - t1a:.1f}s")

            self.stdout.write("Parsing scope notes ...")
            notes = self._parse_notes(nt_dir / AAT_NT_SCOPE_NOTES)
            t1c = time.time()
            self.stdout.write(
                f"  -> {len(notes):,} English scope notes in {t1c - t1b:.1f}s")

            # -- Step 3: Walk hierarchy from entry points ------------------
            self.stdout.write("Walking hierarchy from entry points ...")
            place_types = self._walk_hierarchy(
                preferred_parent, children, parents, labels, notes)
            t2 = time.time()
            self.stdout.write(
                f"  -> {len(place_types):,} place-type concepts in {t2 - t1c:.1f}s")

        t_parsed = time.time()
        self.stdout.write(
            f"  -> {len(place_types):,} place-type concepts collected "
            f"in {t_parsed - t0:.1f}s")

        # -- Backfill missing labels from the Getty API --------------------
        gaps = [pt for pt in place_types if re.fullmatch(r'aat:\d+', pt['term'])]
        if gaps:
            self.stdout.write(
                f"  {len(gaps)} concept(s) have no English label "
                f"— backfilling from Getty API ...")
            if not dry_run:
                self._backfill_labels_from_api(gaps)
            else:
                self.stdout.write(self.style.WARNING(
                    "  (skipped API backfill in dry-run mode)"))

        if dry_run:
            self.stdout.write(self.style.WARNING(
                "Dry run -- no database changes made."))
            self._report(place_types)
            return

        # -- Step 4: Upsert ------------------------------------------------
        self.stdout.write("Upserting into Type table ...")
        created, updated = self._upsert(place_types)
        t3 = time.time()
        self.stdout.write(
            f"  -> {created} created, {updated} updated in {t3 - t_parsed:.1f}s")

        # -- Step 5: Invalidate caches -------------------------------------
        from placetypes.aat_utils import invalidate_caches
        invalidate_caches()

        self.stdout.write(self.style.SUCCESS(
            f"Done. {len(place_types)} place types synchronised."
        ))

    # ------------------------------------------------------------------
    # API crawl (fallback)
    # ------------------------------------------------------------------

    _API_BASE = "https://vocab.getty.edu/aat/{aat_id}.json"
    _API_TIMEOUT = 30
    _API_RETRY_WAIT = 2
    _API_MAX_RETRIES = 3

    def _fetch_concept_json(self, aat_id, session):
        url = self._API_BASE.format(aat_id=aat_id)
        for attempt in range(1, self._API_MAX_RETRIES + 1):
            try:
                resp = session.get(url, timeout=self._API_TIMEOUT)
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in (429, 500, 502, 503):
                    time.sleep(self._API_RETRY_WAIT * attempt)
                    continue
                logger.warning("HTTP %s for aat:%s", resp.status_code, aat_id)
                return None
            except requests.RequestException as e:
                logger.warning("Request failed for aat:%s (attempt %d): %s",
                               aat_id, attempt, e)
                time.sleep(self._API_RETRY_WAIT * attempt)
        return None

    def _extract_label_and_note(self, data):
        label = data.get('_label', '')
        for ident in data.get('identified_by', []):
            classes = [c.get('_label', '') for c in ident.get('classified_as', [])]
            lang_list = ident.get('language', [])
            lang_codes = [l.get('_label', '') for l in lang_list]
            if 'preferred term' in classes and 'en' in lang_codes:
                label = ident.get('content', label)
                break
        note = ''
        for subj in data.get('subject_of', []):
            lang_list = subj.get('language', [])
            lang_codes = [l.get('_label', '') for l in lang_list]
            if 'en' in lang_codes and subj.get('content'):
                note = subj['content'][:3000]
                break
        return label, note

    def _crawl_api(self):
        """BFS crawl via JSON API. Returns list of dicts."""
        self.stdout.write("Crawling AAT hierarchy via JSON API ...")
        session = requests.Session()
        session.headers.update({'Accept': 'application/json'})
        result = []
        visited = set()
        fetched = 0
        queue = []
        for ep in AAT_ENTRY_POINTS:
            queue.append((ep, None, str(ep), 0))
        while queue:
            aat_id, parent_id, path, depth = queue.pop(0)
            if aat_id in visited or aat_id in AAT_EXCLUDED_SUBTREES:
                continue
            visited.add(aat_id)
            data = self._fetch_concept_json(aat_id, session)
            fetched += 1
            if fetched % 50 == 0:
                self.stdout.write(f"  ... fetched {fetched} concepts, "
                                  f"{len(queue)} queued")
            if data is None:
                continue
            label, note = self._extract_label_and_note(data)
            if not label:
                label = f"aat:{aat_id}"
            fclasses = sorted(set(
                fc for anc in self._ancestor_chain(aat_id, {})
                if (fc := AAT_FCLASS_MAP.get(anc))
            )) or (list(AAT_FCLASS_MAP.get(aat_id, '')) or [])
            result.append({
                'aat_id': aat_id,
                'parent_id': parent_id,
                'term': label[:100],
                'term_full': label[:100],
                'note': note,
                'fclasses': fclasses,
                'path': path,
                'depth': depth,
                'is_place_type': True,
            })
            for child in sorted(data.get('narrower', []),
                                key=lambda c: c.get('id', '')):
                child_id = _aat_id_from_uri(child.get('id', ''))
                if child_id is not None and child_id not in visited:
                    queue.append((child_id, aat_id, f"{path}.{child_id}",
                                  depth + 1))
        self.stdout.write(f"  ... {fetched} API requests, "
                          f"{len(result)} concepts collected")
        return result

    # ------------------------------------------------------------------
    # Backfill missing labels via Getty API
    # ------------------------------------------------------------------

    def _backfill_labels_from_api(self, gaps):
        """
        For each place-type dict in *gaps* whose ``term`` is a raw AAT
        identifier (``aat:NNNNNNN``), query the Getty Linked Art JSON API
        to obtain the English preferred label and, if still missing, the
        scope note.

        Mutates the dicts in-place so the upsert step writes real labels.
        """
        session = requests.Session()
        session.headers.update({'Accept': 'application/json'})
        filled = 0
        failed = 0

        for i, pt in enumerate(gaps, 1):
            aat_id = pt['aat_id']
            data = self._fetch_concept_json(aat_id, session)
            if data is None:
                failed += 1
                continue
            label, note = self._extract_label_and_note(data)
            if label:
                pt['term'] = label[:100]
                pt['term_full'] = label[:100]
                filled += 1
            else:
                failed += 1
            if note and not pt.get('note'):
                pt['note'] = note[:3000]
            if i % 25 == 0:
                self.stdout.write(
                    f"    ... {i}/{len(gaps)} checked, "
                    f"{filled} filled, {failed} still missing")

        self.stdout.write(
            f"  -> backfilled {filled} label(s); "
            f"{failed} could not be resolved")

    # ------------------------------------------------------------------
    # Download & extract
    # ------------------------------------------------------------------

    def _cache_dir(self):
        d = Path(settings.BASE_DIR) / AAT_DUMP_CACHE_DIR
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _meta_path(self):
        return self._cache_dir() / AAT_DUMP_META_FILE

    def _read_meta(self):
        mp = self._meta_path()
        if mp.exists():
            with open(mp) as f:
                return json.load(f)
        return {}

    def _write_meta(self, meta):
        with open(self._meta_path(), 'w') as f:
            json.dump(meta, f, indent=2)

    def _download_if_needed(self, force):
        meta = self._read_meta()
        headers = {}
        if not force:
            if meta.get('etag'):
                headers['If-None-Match'] = meta['etag']
            if meta.get('last_modified'):
                headers['If-Modified-Since'] = meta['last_modified']

        self.stdout.write(f"Checking {AAT_EXPLICIT_DUMP_URL} ...")
        try:
            resp = requests.head(AAT_EXPLICIT_DUMP_URL, headers=headers,
                                 timeout=30, allow_redirects=True)
        except requests.RequestException as e:
            raise CommandError(f"HEAD request failed: {e}")

        if resp.status_code == 304 and not force:
            cache = self._cache_dir()
            needed = [AAT_NT_HIERARCHICAL_RELS, AAT_NT_TERMS, AAT_NT_SCOPE_NOTES]
            if all((cache / n).exists() for n in needed):
                self.stdout.write(
                    "AAT dump has not changed since last sync "
                    "(skipping download, using cached files)."
                )
                return cache

        if resp.status_code not in (200, 302, 304):
            raise CommandError(
                f"Unexpected status {resp.status_code} from HEAD request")

        self.stdout.write("Downloading AAT explicit dump "
                          "(this may take a few minutes) ...")
        try:
            resp = requests.get(AAT_EXPLICIT_DUMP_URL, stream=True, timeout=600)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise CommandError(f"Download failed: {e}")

        zip_path = self._cache_dir() / "explicit.zip"
        total = int(resp.headers.get('content-length', 0))
        downloaded = 0
        with open(zip_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    self.stdout.write(
                        f"\r  {pct}% ({downloaded // (1024*1024)} MB)",
                        ending='')
        self.stdout.write("")

        new_meta = {
            'etag': resp.headers.get('ETag', ''),
            'last_modified': resp.headers.get('Last-Modified', ''),
            'downloaded_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        }
        self._write_meta(new_meta)

        self.stdout.write("Extracting ...")
        needed = {AAT_NT_HIERARCHICAL_RELS, AAT_NT_TERMS, AAT_NT_SCOPE_NOTES}
        cache = self._cache_dir()
        with zipfile.ZipFile(zip_path, 'r') as zf:
            for name in zf.namelist():
                if name in needed:
                    self.stdout.write(f"  -> {name}")
                    with zf.open(name) as src, \
                            open(cache / name, 'wb') as dst:
                        while True:
                            chunk = src.read(1024 * 1024)
                            if not chunk:
                                break
                            dst.write(chunk)

        for name in needed:
            if not (cache / name).exists():
                raise CommandError(
                    f"Expected file {name} not found inside explicit.zip")

        self.stdout.write(f"Extracted to {cache}")
        return cache

    # ------------------------------------------------------------------
    # Parse: Hierarchy edges
    # ------------------------------------------------------------------

    def _parse_hierarchy(self, nt_path):
        """
        Returns:
            preferred_parent: dict child -> canonical parent
            children:         dict parent -> set of children
            parents:          dict child -> set of ALL parents (for fclass)
        """
        preferred_parent = {}
        children = defaultdict(set)
        parents = defaultdict(set)      # child -> {parent, parent, ...}
        line_count = 0

        with open(nt_path, 'r', encoding='utf-8', errors='replace') as fh:
            for line in fh:
                line_count += 1
                if line_count % 100_000 == 0:
                    self.stdout.write(
                        f"  ... {line_count:,} lines", ending='\r')

                parsed = _parse_nt_line(line)
                if parsed is None:
                    continue

                subj_uri, pred_uri, obj_uri, _literal, _lang = parsed

                if pred_uri not in (GVP_BROADER_PREFERRED, GVP_BROADER_GENERIC):
                    continue
                if not obj_uri:
                    continue

                child_id = _aat_id_from_uri(subj_uri)
                parent_id = _aat_id_from_uri(obj_uri)
                if child_id is None or parent_id is None:
                    continue

                children[parent_id].add(child_id)
                parents[child_id].add(parent_id)

                if pred_uri == GVP_BROADER_PREFERRED:
                    preferred_parent[child_id] = parent_id

        self.stdout.write(
            f"  ... {line_count:,} lines in {AAT_NT_HIERARCHICAL_RELS}")
        return preferred_parent, children, parents

    # ------------------------------------------------------------------
    # Parse: Labels (SKOS-XL two-hop)
    # ------------------------------------------------------------------

    def _parse_labels(self, nt_path):
        concept_to_term = {}
        term_uri_to_literal = {}
        line_count = 0

        with open(nt_path, 'r', encoding='utf-8', errors='replace') as fh:
            for line in fh:
                line_count += 1
                if line_count % 1_000_000 == 0:
                    self.stdout.write(
                        f"  ... {line_count:,} lines", ending='\r')

                parsed = _parse_nt_line(line)
                if parsed is None:
                    continue

                subj_uri, pred_uri, obj_uri, literal, lang = parsed

                if pred_uri == SKOSXL_PREF_LABEL and obj_uri:
                    if not obj_uri.startswith(AAT_TERM_URI_PREFIX):
                        continue
                    suffix = obj_uri[len(AAT_TERM_URI_PREFIX):]
                    dash_idx = suffix.find('-')
                    if dash_idx < 0:
                        continue
                    uri_lang = suffix[dash_idx + 1:]
                    if not uri_lang.startswith('en'):
                        continue
                    is_plain_en = (uri_lang == 'en')
                    concept_id = _aat_id_from_uri(subj_uri)
                    if concept_id is None:
                        continue
                    existing = concept_to_term.get(concept_id)
                    if existing is None or (is_plain_en and not existing[1]):
                        concept_to_term[concept_id] = (obj_uri, is_plain_en)

                elif pred_uri == SKOSXL_LITERAL_FORM and literal:
                    if subj_uri.startswith(AAT_TERM_URI_PREFIX):
                        if lang and lang.startswith('en'):
                            term_uri_to_literal[subj_uri] = _decode_nt_unicode(literal)

        self.stdout.write(f"  ... {line_count:,} lines in {AAT_NT_TERMS}")

        labels = {}
        for concept_id, (term_uri, _) in concept_to_term.items():
            text = term_uri_to_literal.get(term_uri)
            if text:
                labels[concept_id] = text

        return labels

    # ------------------------------------------------------------------
    # Parse: Scope notes (two-hop)
    # ------------------------------------------------------------------

    def _parse_notes(self, nt_path):
        concept_to_note_uris = defaultdict(set)
        note_uri_to_text = {}
        line_count = 0

        with open(nt_path, 'r', encoding='utf-8', errors='replace') as fh:
            for line in fh:
                line_count += 1
                if line_count % 500_000 == 0:
                    self.stdout.write(
                        f"  ... {line_count:,} lines", ending='\r')

                parsed = _parse_nt_line(line)
                if parsed is None:
                    continue

                subj_uri, pred_uri, obj_uri, literal, lang = parsed

                if pred_uri == SKOS_SCOPE_NOTE and obj_uri:
                    concept_id = _aat_id_from_uri(subj_uri)
                    if concept_id is not None:
                        concept_to_note_uris[concept_id].add(obj_uri)

                elif pred_uri == RDF_VALUE and literal:
                    if subj_uri.startswith(AAT_SCOPE_NOTE_URI_PREFIX):
                        if lang == 'en':
                            note_uri_to_text[subj_uri] = _decode_nt_unicode(literal)

        self.stdout.write(
            f"  ... {line_count:,} lines in {AAT_NT_SCOPE_NOTES}")

        notes = {}
        for concept_id, note_uris in concept_to_note_uris.items():
            for uri in sorted(note_uris):
                text = note_uri_to_text.get(uri)
                if text:
                    notes[concept_id] = text[:3000]
                    break

        return notes

    # ------------------------------------------------------------------
    # Compute fclasses for a concept via ancestor walk
    # ------------------------------------------------------------------

    def _compute_fclasses(self, aat_id, parents_map, _cache=None):
        """
        Walk upward through ALL parent edges to find every AAT_FCLASS_MAP
        ancestor.  Returns a sorted list of unique fclass letters.
        """
        if _cache is None:
            _cache = {}
        if aat_id in _cache:
            return _cache[aat_id]

        fcs = set()

        # Check if this node itself is a mapped node
        if aat_id in AAT_FCLASS_MAP:
            fcs.add(AAT_FCLASS_MAP[aat_id])

        # Walk upward through all parents
        visited = {aat_id}
        stack = list(parents_map.get(aat_id, []))
        while stack:
            ancestor = stack.pop()
            if ancestor in visited:
                continue
            visited.add(ancestor)
            if ancestor in AAT_FCLASS_MAP:
                fcs.add(AAT_FCLASS_MAP[ancestor])
            # Keep walking upward
            for grandparent in parents_map.get(ancestor, []):
                if grandparent not in visited:
                    stack.append(grandparent)

        result = sorted(fcs) if fcs else []
        _cache[aat_id] = result
        return result

    # ------------------------------------------------------------------
    # Walk hierarchy from entry points downward
    # ------------------------------------------------------------------

    def _walk_hierarchy(self, preferred_parent, children, parents_map,
                        labels, notes):
        """
        BFS from each entry point downward.  Skips excluded subtrees.
        Computes multi-valued fclasses for each concept.
        """
        result = []
        visited = set()
        fclass_cache = {}

        # Queue: (aat_id, walk_parent, path, depth)
        queue = []
        for ep in AAT_ENTRY_POINTS:
            queue.append((ep, None, str(ep), 0))

        excluded_count = 0

        while queue:
            aat_id, walk_parent_id, path, depth = queue.pop(0)
            if aat_id in visited:
                continue

            # Skip excluded subtrees entirely (don't even visit children)
            if aat_id in AAT_EXCLUDED_SUBTREES:
                excluded_count += 1
                visited.add(aat_id)
                continue

            visited.add(aat_id)

            canonical_parent = preferred_parent.get(aat_id, walk_parent_id)
            term = labels.get(aat_id, f"aat:{aat_id}")
            note = notes.get(aat_id, '')
            fclasses = self._compute_fclasses(aat_id, parents_map, fclass_cache)

            result.append({
                'aat_id': aat_id,
                'parent_id': canonical_parent,
                'term': term[:100],
                'term_full': term[:100],
                'note': note[:3000],
                'fclasses': fclasses,
                'path': path,
                'depth': depth,
                'is_place_type': True,
            })

            for child_id in sorted(children.get(aat_id, [])):
                if child_id not in visited:
                    child_path = f"{path}.{child_id}"
                    queue.append((child_id, aat_id, child_path, depth + 1))

        if excluded_count:
            self.stdout.write(
                f"  (skipped {excluded_count} excluded subtree root(s))")

        return result

    # ------------------------------------------------------------------
    # Upsert into database
    # ------------------------------------------------------------------

    def _upsert(self, place_types):
        from placetypes.models import Type

        created = 0
        updated = 0

        with transaction.atomic():
            Type.objects.update(is_place_type=False)

            for pt in place_types:
                obj, was_created = Type.objects.update_or_create(
                    aat_id=pt['aat_id'],
                    defaults={
                        'parent_id': pt['parent_id'],
                        'term': pt['term'],
                        'term_full': pt['term_full'],
                        'note': pt['note'],
                        'fclasses': pt['fclasses'],
                        'path': pt['path'],
                        'depth': pt['depth'],
                        'is_place_type': True,
                    },
                )
                if was_created:
                    created += 1
                else:
                    updated += 1

        return created, updated

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def _report(self, place_types):
        """Print a summary grouped by fclass."""
        from collections import Counter
        fclass_counts = Counter()
        for pt in place_types:
            for fc in (pt['fclasses'] or []):
                fclass_counts[fc] += 1
        no_fclass = sum(1 for pt in place_types if not pt['fclasses'])

        self.stdout.write("\nBreakdown by fclass (concepts may appear in multiple):")
        for fc in sorted(fclass_counts):
            self.stdout.write(f"  {fc}: {fclass_counts[fc]:,} types")
        if no_fclass:
            self.stdout.write(f"  (no fclass): {no_fclass:,} types")

        multi = sum(1 for pt in place_types if pt['fclasses'] and len(pt['fclasses']) > 1)
        self.stdout.write(f"\nConcepts with multiple fclasses: {multi:,}")

        depths = Counter(pt['depth'] for pt in place_types)
        self.stdout.write("\nBreakdown by depth:")
        for d in sorted(depths):
            self.stdout.write(f"  depth {d}: {depths[d]:,}")

        no_label = [pt for pt in place_types
                    if re.fullmatch(r'aat:\d+', pt['term'])]
        if no_label:
            self.stdout.write(self.style.WARNING(
                f"\nMissing English labels: {len(no_label):,} "
                f"(will be backfilled from Getty API on a live run)"))
            for pt in no_label[:20]:
                self.stdout.write(f"  aat:{pt['aat_id']}")
            if len(no_label) > 20:
                self.stdout.write(f"  ... and {len(no_label) - 20} more")

