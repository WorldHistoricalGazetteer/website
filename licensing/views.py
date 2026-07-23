from django.conf import settings
from django.shortcuts import render

from .models import License

# Creative Commons publishes each licence "deed" (the plain-language summary) in
# many languages at ``<canonical-url>deed.<code>`` — e.g.
# https://creativecommons.org/licenses/by/4.0/deed.fr. Every code below was
# verified to resolve. Native language names help non-English contributors find
# their own language. CC0 (publicdomain/zero/…) and the IGO ports use the same
# ``deed.<code>`` suffix, so this table covers every CC licence in the vocabulary.
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

# Ordered open → restrictive. Each licence is bucketed into exactly one section
# by ``_category`` below, derived from the stored boolean flags / SPDX id (there
# is no category field on the model).
_SECTIONS = [
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
    """True when the licence forbids adaptations (CC …-ND-…). ``None`` for custom
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


def licenses_view(request):
    """Public reference page: every licence recorded in the WHG vocabulary,
    grouped by openness, with canonical + non-English deed links. Rendered live
    from ``licensing.License`` (the source of truth) so it can never drift."""
    licenses = list(License.objects.all())
    by_cat = {}
    for lic in licenses:
        deed_url = lic.url or lic.spdx_url or ""
        by_cat.setdefault(_category(lic), []).append({
            "spdx_id": lic.spdx_id,
            "label": lic.label,
            "deed_url": deed_url,
            "spdx_url": lic.spdx_url,
            "custom": lic.custom,
            "permits_commercial": lic.permits_commercial,
            "share_alike": lic.share_alike,
            "attribution_required": lic.attribution_required,
            "no_derivatives": _no_derivatives(lic),
            "translations": _translations(lic),
            "notes": lic.notes,
        })

    sections = [
        {"key": key, "title": title, "blurb": blurb, "licenses": by_cat.get(key, [])}
        for key, title, blurb in _SECTIONS
    ]
    return render(request, "licensing/licenses.html", {
        "sections": sections,
        "license_count": len(licenses),
        "whg_overlay": getattr(settings, "WHG_OVERLAY_LICENSE", None),
    })
