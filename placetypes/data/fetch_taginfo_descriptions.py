#!/usr/bin/env python3
"""
Fetch missing tag descriptions for osm.json and ohm.json using two sources:

1. **taginfo API** — ``/api/4/tag/wiki_pages`` returns pre-extracted
   descriptions from the OSM wiki ``ValueDescription`` infobox template.
   Fast and clean, but many tags have an empty ``description`` field.

2. **OSM wiki parsed HTML** — ``action=parse&prop=text`` renders the full
   wiki page.  The first ``<p>`` paragraph usually contains a usable
   description.  Used as a fallback when taginfo returns nothing.

Usage:
    cd placetypes/data
    python fetch_taginfo_descriptions.py            # fetch for both files
    python fetch_taginfo_descriptions.py --dry-run   # show what would be fetched
    python fetch_taginfo_descriptions.py --file osm  # osm only

Rate-limited to ~5 req/s to be polite to the servers.
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

TAGINFO_API = "https://taginfo.openstreetmap.org/api/4"
WIKI_API = "https://wiki.openstreetmap.org/w/api.php"
USER_AGENT = "WHG/3.5 type-mapping-dashboard (https://whgazetteer.org)"
RATE_LIMIT = 0.2  # seconds between requests

# ── HTML / wikitext cleanup ──────────────────────────────────────────
HTML_TAG_RE = re.compile(r"<[^>]+>")
MULTI_SPACE_RE = re.compile(r"\s+")


def _strip_html(html: str) -> str:
    """Strip HTML tags, entities, and normalise whitespace."""
    text = HTML_TAG_RE.sub("", html)
    # Common HTML entities
    text = text.replace("&nbsp;", " ").replace("&amp;", "&")
    text = text.replace("&lt;", "<").replace("&gt;", ">")
    text = text.replace("&quot;", '"').replace("&#39;", "'")
    text = MULTI_SPACE_RE.sub(" ", text).strip()
    return text


def _truncate_sentence(text: str, max_len: int = 200) -> str:
    """Truncate to the first sentence or max_len characters."""
    for end in (".  ", ". ", ".\n"):
        pos = text.find(end)
        if 0 < pos < max_len:
            return text[: pos + 1].strip()
    if len(text) > max_len:
        cut = text.rfind(" ", 0, max_len)
        if cut > 40:
            return text[:cut].strip() + "…"
        return text[:max_len].strip() + "…"
    return text.strip()


# ── Source 1: taginfo API ────────────────────────────────────────────

def fetch_taginfo_description(key: str, value: str) -> str | None:
    """
    Fetch the English description for ``key=value`` from the taginfo
    wiki_pages endpoint.
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
        print(f"  ⚠ taginfo error for {key}={value}: {e}", file=sys.stderr)
        return None

    for item in data.get("data", []):
        if item.get("lang") == "en":
            desc = (item.get("description") or "").strip()
            return desc if desc else None

    return None


# ── Source 2: OSM wiki parsed HTML ───────────────────────────────────

def fetch_wiki_html_description(key: str, value: str) -> str | None:
    """
    Fetch the rendered HTML of the wiki page ``Tag:<key>=<value>`` and
    extract the first meaningful ``<p>`` paragraph as a description.
    """
    page = f"Tag:{key}={value}"
    url = (
        f"{WIKI_API}?action=parse"
        f"&page={quote(page, safe='')}"
        f"&prop=text&format=json"
        f"&redirects=1"
    )
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
    except (URLError, json.JSONDecodeError, OSError) as e:
        return None

    if "error" in data:
        return None

    html = data.get("parse", {}).get("text", {}).get("*", "")
    if not html:
        return None

    for m in re.finditer(r"<p>(.*?)</p>", html, re.DOTALL):
        text = _strip_html(m.group(1))
        if len(text) < 20:
            continue
        if text.lower().startswith(("see also", "this article", "redirect")):
            continue
        return _truncate_sentence(text)

    return None


# ── Combined fetcher ─────────────────────────────────────────────────

def fetch_description(key: str, value: str) -> str | None:
    """
    Try taginfo first; fall back to wiki HTML parsing.
    """
    desc = fetch_taginfo_description(key, value)
    if desc:
        return desc

    time.sleep(RATE_LIMIT)

    desc = fetch_wiki_html_description(key, value)
    return desc


# ── Helpers ──────────────────────────────────────────────────────────

def collect_missing(data: dict) -> list[tuple[str, int]]:
    """Return (tag_key, value_index) for entries missing a description."""
    missing = []
    for tag_key, tag_data in data.items():
        if not isinstance(tag_data, dict):
            continue
        for i, entry in enumerate(tag_data.get("values", [])):
            if not entry.get("description"):
                missing.append((tag_key, i))
    return missing


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Fetch missing tag descriptions from taginfo + OSM wiki."
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

            if len(value) > 60:
                not_found += 1
                continue

            desc = fetch_description(tag_key, value)
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

            if (i + 1) % 100 == 0:
                print(f"  ... processed {i+1}/{min(limit, len(missing))} "
                      f"(found {found}, not found {not_found})")

            time.sleep(RATE_LIMIT)

        print(f"\n  Results for {label}:")
        print(f"    Found: {found}")
        print(f"    Not found: {not_found}")

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

