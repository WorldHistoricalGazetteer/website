# placetypes/management/commands/sync_aat_types.py
"""
Django management command to synchronise the local Type table with the
Getty AAT (Art & Architecture Thesaurus) bulk N-Triples dump.

Usage:
    python manage.py sync_aat_types              # download if new, parse, update DB
    python manage.py sync_aat_types --force      # re-download even if unchanged
    python manage.py sync_aat_types --local /path/to/AATOut_Full.nt
                                                  # parse a pre-downloaded file
    python manage.py sync_aat_types --dry-run    # report counts without writing

Workflow:
    1. HEAD request to the Getty dump URL to check Last-Modified / ETag.
    2. If changed (or --force), download the .nt.zip and extract.
    3. Stream the N-Triples file in two passes:
       Pass 1 — collect hierarchy edges (child → parent via gvp:broaderGeneric)
                and English prefLabels for every aat: subject.
       Pass 2 — starting from the configured root nodes, walk the hierarchy
                downward to identify which concepts are place types and
                compute materialized paths, depths, and fclass assignments.
    4. Bulk upsert into the Type table.
    5. Invalidate caches.
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
    AAT_FULL_DUMP_URL,
    AAT_URI_PREFIX,
    GVP_BROADER,
    ROOT_AAT_IDS,
    ROOT_TO_FCLASS,
    SKOS_PREF_LABEL,
    SKOS_SCOPE_NOTE,
)

logger = logging.getLogger(__name__)

# Regex for parsing N-Triples lines.  Each line is:
#   <subject> <predicate> <object> .
# Object may be a URI (<...>) or a literal ("..."@lang or "..."^^<type>).
_NT_LINE_RE = re.compile(
    r'^<([^>]+)>\s+<([^>]+)>\s+'      # subject, predicate
    r'(?:<([^>]+)>'                     # object URI
    r'|"([^"]*)"'                       # or literal value
    r'(?:@(\w[\w-]*))?'                 # optional language tag
    r'(?:\^\^<[^>]+>)?'                 # optional datatype (ignored)
    r')\s*\.\s*$'                       # trailing dot
)


def _parse_nt_line(line):
    """Parse a single N-Triples line. Returns (subj, pred, obj_uri, literal, lang) or None."""
    m = _NT_LINE_RE.match(line)
    if not m:
        return None
    return m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)


def _aat_id_from_uri(uri):
    """Extract integer AAT id from a URI like http://vocab.getty.edu/aat/300008347."""
    if uri and uri.startswith(AAT_URI_PREFIX):
        tail = uri[len(AAT_URI_PREFIX):]
        # Only take pure numeric IDs (skip term URIs like aat/term/12345-en)
        if tail.isdigit():
            return int(tail)
    return None


class Command(BaseCommand):
    help = "Synchronise the local Type table with the Getty AAT N-Triples dump."

    def add_arguments(self, parser):
        parser.add_argument(
            '--force', action='store_true',
            help='Re-download even if the remote file has not changed.',
        )
        parser.add_argument(
            '--local', type=str, default=None,
            help='Path to a pre-downloaded .nt file (skips download).',
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Parse and report counts without writing to the database.',
        )

    def handle(self, *args, **options):
        force = options['force']
        local_path = options['local']
        dry_run = options['dry_run']

        # ── Step 1: Obtain the .nt file ──────────────────────────────
        if local_path:
            nt_path = Path(local_path)
            if not nt_path.exists():
                raise CommandError(f"Local file not found: {local_path}")
            self.stdout.write(f"Using local file: {nt_path}")
        else:
            nt_path = self._download_if_needed(force)
            if nt_path is None:
                self.stdout.write(self.style.SUCCESS(
                    "AAT dump has not changed since last sync. Use --force to re-download."
                ))
                return

        # ── Step 2: Parse ─────────────────────────────────────────────
        self.stdout.write("Pass 1: Collecting hierarchy edges and labels …")
        t0 = time.time()
        children, labels, notes = self._parse_pass1(nt_path)
        t1 = time.time()
        self.stdout.write(f"  → {len(children)} parent→children edges, "
                          f"{len(labels)} labelled concepts in {t1 - t0:.1f}s")

        self.stdout.write("Pass 2: Walking hierarchy from root nodes …")
        place_types = self._walk_hierarchy(children, labels, notes)
        t2 = time.time()
        self.stdout.write(f"  → {len(place_types)} place-type concepts in {t2 - t1:.1f}s")

        if dry_run:
            self.stdout.write(self.style.WARNING("Dry run — no database changes made."))
            self._report(place_types)
            return

        # ── Step 3: Upsert ────────────────────────────────────────────
        self.stdout.write("Upserting into Type table …")
        created, updated = self._upsert(place_types)
        t3 = time.time()
        self.stdout.write(f"  → {created} created, {updated} updated in {t3 - t2:.1f}s")

        # ── Step 4: Invalidate caches ─────────────────────────────────
        from placetypes.aat_utils import invalidate_caches
        invalidate_caches()

        self.stdout.write(self.style.SUCCESS(
            f"Done. {len(place_types)} place types synchronised."
        ))

    # ------------------------------------------------------------------
    # Download
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
        """
        Check the remote dump for changes using HEAD (Last-Modified / ETag).
        Download and extract if needed.  Returns the path to the .nt file,
        or None if unchanged and not forced.
        """
        meta = self._read_meta()
        headers = {}
        if not force:
            if meta.get('etag'):
                headers['If-None-Match'] = meta['etag']
            if meta.get('last_modified'):
                headers['If-Modified-Since'] = meta['last_modified']

        self.stdout.write(f"Checking {AAT_FULL_DUMP_URL} …")
        try:
            resp = requests.head(AAT_FULL_DUMP_URL, headers=headers, timeout=30,
                                 allow_redirects=True)
        except requests.RequestException as e:
            raise CommandError(f"HEAD request failed: {e}")

        if resp.status_code == 304 and not force:
            return None  # Not modified

        if resp.status_code not in (200, 302):
            raise CommandError(f"Unexpected status {resp.status_code} from HEAD request")

        # Download the zip
        self.stdout.write("Downloading AAT dump (this may take several minutes) …")
        try:
            resp = requests.get(AAT_FULL_DUMP_URL, stream=True, timeout=600)
            resp.raise_for_status()
        except requests.RequestException as e:
            raise CommandError(f"Download failed: {e}")

        zip_path = self._cache_dir() / "AATOut_Full.nt.zip"
        total = int(resp.headers.get('content-length', 0))
        downloaded = 0
        with open(zip_path, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    self.stdout.write(f"\r  {pct}% ({downloaded // (1024*1024)} MB)", ending='')
        self.stdout.write("")  # newline

        # Save metadata
        new_meta = {
            'etag': resp.headers.get('ETag', ''),
            'last_modified': resp.headers.get('Last-Modified', ''),
            'downloaded_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        }
        self._write_meta(new_meta)

        # Extract .nt from zip
        self.stdout.write("Extracting …")
        nt_path = self._cache_dir() / "AATOut_Full.nt"
        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Find the .nt file inside the zip
            nt_names = [n for n in zf.namelist() if n.endswith('.nt')]
            if not nt_names:
                raise CommandError("No .nt file found inside the zip archive")
            with zf.open(nt_names[0]) as src, open(nt_path, 'wb') as dst:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)

        self.stdout.write(f"Extracted to {nt_path}")
        return nt_path

    # ------------------------------------------------------------------
    # Pass 1: Collect edges and labels
    # ------------------------------------------------------------------

    def _parse_pass1(self, nt_path):
        """
        Stream the N-Triples file and collect:
          - children: dict  parent_aat_id -> set of child_aat_ids
          - labels:   dict  aat_id -> English prefLabel string
          - notes:    dict  aat_id -> scope note string
        """
        children = defaultdict(set)   # parent -> {child, child, ...}
        labels = {}                    # aat_id -> "term"
        notes = {}                     # aat_id -> "scope note"
        line_count = 0

        with open(nt_path, 'r', encoding='utf-8', errors='replace') as fh:
            for line in fh:
                line_count += 1
                if line_count % 5_000_000 == 0:
                    self.stdout.write(f"  … {line_count:,} lines", ending='\r')

                parsed = _parse_nt_line(line)
                if parsed is None:
                    continue

                subj_uri, pred_uri, obj_uri, literal, lang = parsed
                subj_id = _aat_id_from_uri(subj_uri)
                if subj_id is None:
                    continue

                # Hierarchy: <child> gvp:broaderGeneric <parent>
                if pred_uri == GVP_BROADER and obj_uri:
                    parent_id = _aat_id_from_uri(obj_uri)
                    if parent_id is not None:
                        children[parent_id].add(subj_id)

                # English preferred label
                elif pred_uri == SKOS_PREF_LABEL and literal and lang == 'en':
                    labels[subj_id] = literal

                # Scope note (take first English one encountered)
                elif pred_uri == SKOS_SCOPE_NOTE and literal:
                    if subj_id not in notes and (lang == 'en' or lang is None):
                        notes[subj_id] = literal[:3000]

        self.stdout.write(f"  … {line_count:,} lines total")
        return children, labels, notes

    # ------------------------------------------------------------------
    # Pass 2: Walk hierarchy from roots downward
    # ------------------------------------------------------------------

    def _walk_hierarchy(self, children, labels, notes):
        """
        BFS from each root node downward through the children dict.
        Returns a list of dicts ready for upserting:
            [{aat_id, parent_id, term, term_full, note, fclass, path, depth}, ...]
        """
        result = []
        visited = set()

        # Queue items: (aat_id, parent_aat_id_or_None, path_so_far, depth, fclass)
        queue = []
        for root_id in ROOT_AAT_IDS:
            fclass = ROOT_TO_FCLASS[root_id]
            queue.append((root_id, None, str(root_id), 0, fclass))

        while queue:
            aat_id, parent_id, path, depth, fclass = queue.pop(0)
            if aat_id in visited:
                continue
            visited.add(aat_id)

            term = labels.get(aat_id, f"aat:{aat_id}")
            note = notes.get(aat_id, '')

            result.append({
                'aat_id': aat_id,
                'parent_id': parent_id,
                'term': term[:100],
                'term_full': term[:100],
                'note': note[:3000],
                'fclass': fclass,
                'path': path,
                'depth': depth,
                'is_place_type': True,
            })

            # Enqueue children
            for child_id in sorted(children.get(aat_id, [])):
                if child_id not in visited:
                    child_path = f"{path}.{child_id}"
                    queue.append((child_id, aat_id, child_path, depth + 1, fclass))

        return result

    # ------------------------------------------------------------------
    # Upsert into database
    # ------------------------------------------------------------------

    def _upsert(self, place_types):
        from placetypes.models import Type

        created = 0
        updated = 0

        with transaction.atomic():
            # Mark all existing rows as NOT place types; they'll be re-marked
            # if they appear in the new hierarchy walk.
            Type.objects.update(is_place_type=False)

            for pt in place_types:
                obj, was_created = Type.objects.update_or_create(
                    aat_id=pt['aat_id'],
                    defaults={
                        'parent_id': pt['parent_id'],
                        'term': pt['term'],
                        'term_full': pt['term_full'],
                        'note': pt['note'],
                        'fclass': pt['fclass'],
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
        by_fclass = Counter(pt['fclass'] for pt in place_types)
        self.stdout.write("\nBreakdown by fclass:")
        for fc in sorted(by_fclass):
            self.stdout.write(f"  {fc}: {by_fclass[fc]:,} types")

        depths = Counter(pt['depth'] for pt in place_types)
        self.stdout.write("\nBreakdown by depth:")
        for d in sorted(depths):
            self.stdout.write(f"  depth {d}: {depths[d]:,}")

