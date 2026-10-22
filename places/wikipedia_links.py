"""Wikipedia links WHG's Wikidata records hold, for the Record and Locus pages
(place#325 Stage 5).

The indexing pipeline stores a Wikidata item's sitelinks on its ``wd:`` record as
``links`` entries ``{"type": "seeAlso", "identifier": "https://<lang>.wikipedia.org/wiki/<Title>"}``
(``api.reconcile_helpers.wikipedia_links`` reads the same entries for reconciliation
candidates). This module reads them off group members and nothing else:

* **Only links already held.** A member carries ``wikipedia`` only when the gateway
  response that built it carried ``links`` (``places.record_group.gateway_members``:
  ``POST /api/places``, whose ``PlaceDetail`` model declares ``links``; the Locus page's
  posted hits: the gateway SEARCH model, ``SearchHit``, does NOT declare ``links``, so
  today they never carry any). No Wikipedia URL is ever made from a Q-id, and
  wikipedia.org is never called.
* **Only ``wd:`` members.** ``/api/places`` also merges into a NON-wd record's
  ``links`` the sitelinks of any ``wd:`` record it has a sameAs/exactMatch/closeMatch
  relation to (indexing ``gateway/places.py``, "Step 2.5"). Those are a second step
  beyond the group (one step, no closure), and the response does not say which links
  were inherited, so a non-wd member's Wikipedia links are not read. A group with a
  ``wd:`` member reads that member's own.
* **Visibility.** A link derives from a record, so it obeys the record's visibility:
  by default only members ``mark_offsite`` cleared (an anonymous caller could see them)
  contribute; a beta caller's own view (``include_withheld=True``) also reads the
  members it may see that an anonymous caller may not, as the gloss does.

Output: ``[{"lang", "url", "title", "from"}]``, one entry per distinct URL, English
first then by language code then title, at most ``MAX_WIKIPEDIA_LINKS``; ``from`` is the
``wd:`` member key that held it (the first, in that order, when several did).
"""
from __future__ import annotations

import re
from urllib.parse import unquote

MAX_WIKIPEDIA_LINKS = 300       # a Wikidata item has up to ~330 sitelinks; a group may have several items
MAX_LINKS_READ_PER_MEMBER = 400

# https://<lang>.wikipedia.org/wiki/<Title>; lang as Wikimedia writes it (en, zh-min-nan,
# be-tarask, simple). A mobile host (en.m.) is read as its language. Anything else is not
# a Wikipedia article link and is ignored.
_WIKIPEDIA_URL = re.compile(r"^https?://([a-z][a-z0-9-]{0,19})(?:\.m)?\.wikipedia\.org/wiki/([^?#\s]+)$")


def parse_wikipedia_url(identifier) -> dict | None:
    """``{"lang", "url", "title"}`` for a Wikipedia article URL, else None. The URL is
    normalised to https and the desktop host; the title is the path, unescaped, with
    underscores as spaces (a reading of the URL, not a lookup)."""
    if not isinstance(identifier, str):
        return None
    m = _WIKIPEDIA_URL.match(identifier.strip())
    if not m:
        return None
    lang, path = m.group(1), m.group(2)
    title = " ".join(unquote(path).replace("_", " ").split())
    if not title:
        return None
    return {"lang": lang, "url": f"https://{lang}.wikipedia.org/wiki/{path}", "title": title[:300]}


def member_wikipedia(links) -> list:
    """``[{"lang", "url", "title"}]`` from one record's ``links`` entries (any order kept,
    duplicates dropped). Entries of any type are read: the index writes ``seeAlso``,
    and the URL itself is what makes it a Wikipedia link."""
    out, seen = [], set()
    if not isinstance(links, list):
        return out
    for link in links[:MAX_LINKS_READ_PER_MEMBER]:
        ident = link.get("identifier") if isinstance(link, dict) else None
        w = parse_wikipedia_url(ident)
        if w and w["url"] not in seen:
            seen.add(w["url"])
            out.append(w)
    return out


def is_wikidata_member(m: dict) -> bool:
    key = str(m.get("key") or m.get("place_id") or "")
    ns = (m.get("namespace") or (key.split(":", 1)[0] if ":" in key else "")).lower()
    return ns == "wd"


def _sort_key(w: dict):
    return (w["lang"] != "en", w["lang"], w["title"], w["url"])


def _held(members: list, include_withheld: bool) -> dict:
    """url -> entry over every contributing member (the lowest member key wins a tie)."""
    best: dict[str, dict] = {}
    for m in members or []:
        if not isinstance(m, dict) or not is_wikidata_member(m):
            continue
        if not include_withheld and m.get("offsite") is not True:
            continue
        for w in m.get("wikipedia") or []:
            if not isinstance(w, dict) or not w.get("url"):
                continue
            entry = {"lang": w.get("lang") or "", "url": w["url"], "title": w.get("title") or "",
                     "from": str(m.get("key") or m.get("place_id"))}
            prev = best.get(entry["url"])
            if prev is None or entry["from"] < prev["from"]:
                best[entry["url"]] = entry
    return best


def wikipedia_links(members: list, *, include_withheld: bool = False) -> list:
    """The deduplicated Wikipedia links held on ``members``' ``wikipedia`` lists (see
    the module docstring for which members carry one). Fails closed: a member without
    ``offsite is True`` contributes nothing unless ``include_withheld``."""
    return sorted(_held(members, include_withheld).values(), key=_sort_key)[:MAX_WIKIPEDIA_LINKS]


def wikipedia_payload(members: list, *, include_withheld: bool = False) -> dict:
    """``{"wikipedia": [...], "wikipedia_total": n}`` for a JSON response: ``total`` is
    the distinct count before ``MAX_WIKIPEDIA_LINKS``."""
    return {"wikipedia": wikipedia_links(members, include_withheld=include_withheld),
            "wikipedia_total": len(_held(members, include_withheld))}
