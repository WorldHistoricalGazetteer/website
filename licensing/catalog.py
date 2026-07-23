"""Shared licence-catalogue builder.

Single source of truth for the licence vocabulary as presented to humans —
consumed by the public ``/licenses/`` page, its JSON feed, and the in-Workbench
licence-picker modal, so all three stay in lock-step with ``licensing.License``.
"""
from .models import License

# Creative Commons publishes each licence "deed" (plain-language summary) in many
# languages at ``<canonical-url>deed.<code>`` — e.g.
# https://creativecommons.org/licenses/by/4.0/deed.fr. Every code was verified to
# resolve. CC0 (publicdomain/zero/…) and the IGO ports use the same suffix.
CC_DEED_LANGUAGES = [
    ("es", "Español"),
    ("fr", "Français"),
    ("de", "Deutsch"),
    ("it", "Italiano"),
    ("pt", "Português"),
    ("nl", "Nederlands"),
    ("ru", "Русский"),
    ("uk", "Українська"),
    ("pl", "Polski"),
    ("tr", "Türkçe"),
    ("el", "Ελληνικά"),
    ("ar", "العربية"),
    ("zh-hans", "中文（简体）"),
    ("zh-hant", "中文（繁體）"),
    ("ja", "日本語"),
    ("ko", "한국어"),
    ("id", "Bahasa Indonesia"),
]

# Ordered open → restrictive. Each licence lands in exactly one section via
# ``_category`` (derived from flags / spdx_id — there is no category field).
SECTIONS = [
    ("public_domain", "Public domain & no rights reserved",
     "Anyone may use, share and adapt the work for any purpose. Crediting the "
     "source is welcomed but not legally required."),
    ("attribution", "Attribution — open for any use",
     "Free to use, share and adapt — including commercially — as long as the "
     "original source is credited."),
    ("copyleft", "Attribution + ShareAlike (copyleft)",
     "As open as Attribution, but any adaptations must be released under the "
     "same licence, keeping the data open downstream."),
    ("noncommercial", "Non-commercial use only",
     "Free to use and share for non-commercial purposes, with attribution. "
     "Some variants also forbid adaptations (NoDerivatives)."),
    ("restricted", "Custom & restricted terms",
     "Bespoke terms recorded in WHG. Always consult the rights statement on the "
     "individual dataset, collection or gazetteer record."),
]

_SECTION_TITLES = {key: title for key, title, _ in SECTIONS}


def _category(lic):
    sid = lic.spdx_id
    if "public-domain" in sid or sid.startswith("CC0"):
        return "public_domain"
    if "all-rights-reserved" in sid or "academic" in sid:
        return "restricted"
    if not lic.permits_commercial:
        return "noncommercial"
    if lic.share_alike:
        return "copyleft"
    return "attribution"


def _no_derivatives(lic):
    """True when the licence forbids adaptations (CC …-ND-…); None for custom
    terms, where the answer is bespoke and not encoded in the flags."""
    if lic.custom:
        return None
    return "ND" in lic.spdx_id.split("-")


def _translations(lic):
    """Language-specific deed links for a Creative Commons licence, else []."""
    url = lic.url or ""
    if "creativecommons.org" not in url:
        return []
    base = url if url.endswith("/") else url + "/"
    return [{"code": code, "label": label, "url": "{}deed.{}".format(base, code)}
            for code, label in CC_DEED_LANGUAGES]


def _deed_note(lic, deed_url):
    """Why a custom licence has no external deed to link to (for a tooltip)."""
    if deed_url:
        return ""
    sid = lic.spdx_id
    if "public-domain" in sid:
        return "Public-domain material carries no licence deed — none is required."
    if "all-rights-reserved" in sid:
        return "All rights are reserved by default; there is no licence deed to link to."
    return ("A custom, non-standard designation — there is no external licence text; "
            "see the rights statement on the individual record.")


def license_entry(lic):
    """One enriched, JSON-serialisable dict for a ``License`` row."""
    cat = _category(lic)
    deed_url = lic.url or lic.spdx_url or ""
    return {
        "spdx_id": lic.spdx_id,
        "label": lic.label,
        "deed_url": deed_url,
        "deed_note": _deed_note(lic, deed_url),
        "spdx_url": lic.spdx_url,
        "custom": lic.custom,
        "permits_commercial": lic.permits_commercial,
        "share_alike": lic.share_alike,
        "attribution_required": lic.attribution_required,
        "no_derivatives": _no_derivatives(lic),
        "translations": _translations(lic),
        "notes": lic.notes,
        "category": cat,
        "category_title": _SECTION_TITLES.get(cat, ""),
    }


def license_catalog():
    """The whole vocabulary as ``{"entries": [...], "sections": [...], "count": n}``
    — a flat list (ordered by the section order, then spdx_id) plus section
    metadata. Shared by the page, the JSON feed and the picker modal."""
    licenses = list(License.objects.all())
    by_cat = {}
    for lic in licenses:
        entry = license_entry(lic)
        by_cat.setdefault(entry["category"], []).append(entry)

    sections = []
    entries = []
    for key, title, blurb in SECTIONS:
        bucket = by_cat.get(key, [])
        sections.append({"key": key, "title": title, "blurb": blurb, "licenses": bucket})
        entries.extend(bucket)

    return {"entries": entries, "sections": sections, "count": len(licenses)}
