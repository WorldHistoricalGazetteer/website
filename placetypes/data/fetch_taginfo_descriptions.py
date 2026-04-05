#!/usr/bin/env python3
"""
Fetch missing tag descriptions from the taginfo API and write them
back into osm.json and ohm.json.

The taginfo API endpoint ``/api/4/tag/wiki_pages`` returns pre-extracted
descriptions from the OSM wiki for each ``key=value`` tag, keyed by
language.  We pick the ``lang=="en"`` entry.

This is more reliable (and faster) than scraping the wiki directly,
because taginfo has already parsed the ValueDescription infobox and
returns a clean plaintext description.

Usage:
    cd placetypes/data
    python fetch_taginfo_descriptions.py            # fetch for both files
    python fetch_taginfo_descriptions.py --dry-run   # show what would be fetched
    python fetch_taginfo_descriptions.py --file osm  # osm only

Rate-limited to ~5 req/s to be polite to the taginfo server.
"""

import argparse
import json
import sys
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import URLError

TAGINFO_API = "https://taginfo.openstreetmap.org/api/4"
USER_AGENT = "WHG/3.5 type-mapping-dashboard (https://whgazetteer.org)"
RATE_LIMIT = 0.2  # seconds between requests


def fetch_taginfo_description(key: str, value: str) -> str | None:
    """
    Fetch the English description for ``key=value`` from the taginfo
    wiki_pages endpoint.

    Returns the description string, or None if no English wiki page
    exists.
    """
    url = (
        f"{TAGINFO_API}/tag/wiki_pages"
        f"?key={quote(key, safe='')}"
        f"&value={quote(value, safe='')}"
    )
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except (URLError, json.JSONDecodeError, OSError) as e:
        print(f"  ⚠ HTTP error for {key}={value}: {e}", file=sys.stderr)
        return None

    for item in data.get("data", []):
        if item.get("lang") == "en":
            desc = (item.get("description") or "").strip()
            return desc if desc else None

    return None


def collect_missing(data: dict) -> list[tuple[str, int]]:
    """
    Return a list of (tag_key, value_index) for entries missing a
    description in an osm/ohm JSON structure.
    """
    missing = []
    for tag_key, tag_data in data.items():
        if not isinstance(tag_data, dict):
            continue
        for i, entry in enumerate(tag_data.get("values", [])):
            if not entry.get("description"):
                missing.append((tag_key, i))
    return missing


def main():
    parser = argparse.ArgumentParser(
        description="Fetch missing tag descriptions from the taginfo API."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't write files, just show what would be fetched.",
    )
    parser.add_argument(
        "--file", choices=["osm", "ohm", "both"], default="both",
        help="Which file(s) to process (default: both).",
    )
    parser.add_argument(
        "--max", type=int, default=0,
        help="Max entries to fetch per file (0 = unlimited).",
    )
    args = parser.parse_args()

    data_dir = Path(__file__).parent
    files_to_process = []
    if args.file in ("osm", "both"):
        files_to_process.append(("osm", data_dir / "osm.json"))
    if args.file in ("ohm", "both"):
        files_to_process.append(("ohm", data_dir / "ohm.json"))

    total_fetched = 0
    total_found = 0
    total_missing = 0

    for label, fpath in files_to_process:
        print(f"\n{'='*60}")
        print(f"Processing {label}: {fpath.name}")
        print(f"{'='*60}")

        with open(fpath, encoding="utf-8") as f:
            data = json.load(f)

        missing = collect_missing(data)
        print(f"  Tags missing descriptions: {len(missing)}")

        if not missing:
            continue

        if args.dry_run:
            for tag_key, idx in missing[:20]:
                val = data[tag_key]["values"][idx]["value"]
                print(f"  Would fetch: {tag_key}={val}")
            if len(missing) > 20:
                print(f"  ... and {len(missing) - 20} more")
            continue

        found = 0
        not_found = 0
        limit = args.max if args.max > 0 else len(missing)

        for i, (tag_key, idx) in enumerate(missing):
            if i >= limit:
                break

            entry = data[tag_key]["values"][idx]
            value = entry["value"]

            # Skip values that are clearly not real tag values
            if len(value) > 60:
                not_found += 1
                continue

            desc = fetch_taginfo_description(tag_key, value)
            total_fetched += 1

            if desc:
                entry["description"] = desc
                found += 1
                total_found += 1
                if found <= 10 or found % 50 == 0:
                    print(f"  ✓ {tag_key}={value}: {desc[:80]}")
            else:
                not_found += 1
                total_missing += 1

            # Progress
            if (i + 1) % 100 == 0:
                print(f"  ... processed {i+1}/{min(limit, len(missing))} "
                      f"(found {found}, not found {not_found})")

            time.sleep(RATE_LIMIT)

        print(f"\n  Results for {label}:")
        print(f"    Found: {found}")
        print(f"    Not found: {not_found}")

        # Write back
        with open(fpath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  ✓ Written to {fpath.name}")

    if not args.dry_run:
        print(f"\n{'='*60}")
        print(f"Summary: fetched {total_fetched} pages, "
              f"found {total_found} descriptions, "
              f"{total_missing} not found")
        print(f"{'='*60}")


if __name__ == "__main__":
    main()

