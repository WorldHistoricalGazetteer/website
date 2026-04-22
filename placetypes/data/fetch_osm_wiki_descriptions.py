#!/usr/bin/env python3
"""
Fetch missing tag descriptions from the OSM wiki API and write them
back into osm.json and ohm.json.

The OSM wiki documents tags on pages named ``Tag:<key>=<value>``
(e.g. Tag:place=city).  The ``description`` field from the page's
{{ValueDescription}} infobox template is extracted via the MediaWiki
parse API.

Usage:
    cd placetypes/data
    python fetch_osm_wiki_descriptions.py          # fetch for both files
    python fetch_osm_wiki_descriptions.py --dry-run # show what would be fetched

Rate-limited to ~5 requests/s to be polite to the OSM wiki.
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path
from urllib.parse import quote
from urllib.request import Request, urlopen
from urllib.error import URLError

WIKI_API = "https://wiki.openstreetmap.org/w/api.php"
USER_AGENT = "WHG/3.5 type-mapping-dashboard (https://whgazetteer.org)"
RATE_LIMIT = 0.2  # seconds between requests

# Regex to extract the description= field from wikitext
DESC_RE = re.compile(
    r"""\|\s*description\s*=\s*(.+?)(?:\n|\|)""",
    re.IGNORECASE,
)

# Clean up wikitext markup from description values
WIKITEXT_LINK_RE = re.compile(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]")
WIKITEXT_TAG_RE = re.compile(r"\{\{[^}]*\}\}")
HTML_TAG_RE = re.compile(r"<[^>]+>")


def clean_description(raw: str) -> str:
    """Strip common wikitext/HTML markup from a description string."""
    text = raw.strip()
    # [[Link|display]] → display,  [[simple]] → simple
    text = WIKITEXT_LINK_RE.sub(r"\1", text)
    # Remove {{template}} calls
    text = WIKITEXT_TAG_RE.sub("", text)
    # Remove stray HTML
    text = HTML_TAG_RE.sub("", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Remove leading/trailing punctuation artefacts
    text = text.strip("* ").strip()
    return text


def fetch_wiki_description(key: str, value: str) -> str | None:
    """
    Fetch the description for Tag:<key>=<value> from the OSM wiki.

    Returns the cleaned description string, or None if the page
    doesn't exist or has no description field.
    """
    page = f"Tag:{key}={value}"
    url = (
        f"{WIKI_API}?action=parse"
        f"&page={quote(page, safe='')}"
        f"&prop=wikitext&format=json"
    )
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except (URLError, json.JSONDecodeError, OSError) as e:
        print(f"  ⚠ HTTP error for {page}: {e}", file=sys.stderr)
        return None

    if "error" in data:
        return None  # page doesn't exist

    wikitext = data.get("parse", {}).get("wikitext", {}).get("*", "")
    m = DESC_RE.search(wikitext)
    if not m:
        return None

    raw = m.group(1)
    cleaned = clean_description(raw)
    return cleaned if cleaned else None


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
        description="Fetch missing tag descriptions from the OSM wiki."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Don't write files, just show what would be fetched.",
    )
    parser.add_argument(
        "--file", choices=["osm", "ohm", "both"], default="both",
        help="Which file(s) to process (default: both).",
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
                print(f"  Would fetch: Tag:{tag_key}={val}")
            if len(missing) > 20:
                print(f"  ... and {len(missing) - 20} more")
            continue

        found = 0
        not_found = 0
        errors = 0

        for i, (tag_key, idx) in enumerate(missing):
            entry = data[tag_key]["values"][idx]
            value = entry["value"]

            # Skip values that are clearly not real tag values
            if len(value) > 60 or " " in value and "=" in value:
                not_found += 1
                continue

            desc = fetch_wiki_description(tag_key, value)
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
                print(f"  ... processed {i+1}/{len(missing)} "
                      f"(found {found}, missing {not_found})")

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

