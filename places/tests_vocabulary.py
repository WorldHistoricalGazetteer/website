"""Q9 vocabulary guard (v4): visible text on the Record page and the Link sheet must not use
'attest*', 'accession*' or 'reconcil*'. Code identifiers, CSS classes, data-* attributes, API
field names and comments are unaffected.

The check is deliberately simple: strip // and /* */ comments and import lines from the JS
source, then look at every string/template literal. A literal containing a banned word fails
unless it is on the allowlist below (an identifier-like pattern, with the reason).
"""
import re
from pathlib import Path

from django.test import SimpleTestCase

ROOT = Path(__file__).resolve().parent.parent
FILES = ["whg/webpack/js/atlasRecord.js", "whg/webpack/js/attest.js",
         "whg/webpack/js/enrichment.js"]
BANNED = re.compile(r"attest|accession|reconcil", re.I)

# (regex on the literal, reason). A literal is excused only if, after removing every match of
# these patterns, no banned word is left.
ALLOW = [
    (r"\bdata-attestation-status\b", "data-* attribute name"),
    (r"\bdata-source=\"attestation\"", "data-* attribute value naming the wire source"),
    (r"\bconc-attestation\b", "CSS class"),
    (r"\bdata-attestation\b", "data-* attribute name"),
    (r"^attestation$", "wire value of source"),
    (r"\battestation\b(?=\s*['\"]?\s*$)", "wire value of source"),
    (r"\.attestation\b", "property access on the API entry inside a ${...} expression"),
    (r"\.attested_by\b", "property access on the API entry inside a ${...} expression"),
    (r"attestationProvenanceHtml|attestationItem", "function identifiers inside a ${...} expression"),
]


def strip_comments_and_imports(src):
    out, i, n, quote = [], 0, len(src), None
    while i < n:
        c = src[i]
        if quote:
            out.append(c)
            if c == "\\":
                out.append(src[i + 1]); i += 2; continue
            if c == quote:
                quote = None
            i += 1
        elif c in "'\"`":
            quote = c; out.append(c); i += 1
        elif src.startswith("//", i):
            j = src.find("\n", i); i = n if j < 0 else j
        elif src.startswith("/*", i):
            j = src.find("*/", i + 2); i = n if j < 0 else j + 2
        else:
            out.append(c); i += 1
    text = "".join(out)
    return "\n".join(l for l in text.split("\n") if not re.match(r"\s*(import|export\s+\{[^}]*\}\s+from)\b", l))


def literals(src):
    """Every '..', ".." and `..` literal, template ${...} bodies kept inline (they are code,
    but a banned word inside is still a literal's neighbour; allowlist handles identifiers)."""
    return re.findall(r"'(?:[^'\\\n]|\\.)*'|\"(?:[^\"\\\n]|\\.)*\"|`(?:[^`\\]|\\.)*`", src, re.S)


def violations(src):
    bad = []
    for lit in literals(strip_comments_and_imports(src)):
        if not BANNED.search(lit):
            continue
        rest = lit
        for pat, _why in ALLOW:
            rest = re.sub(pat, "", rest)
        if BANNED.search(rest):
            bad.append(lit[:120])
    return bad


class VocabularyGuardTests(SimpleTestCase):
    def test_no_banned_words_in_visible_strings(self):
        for rel in FILES:
            src = (ROOT / rel).read_text()
            self.assertGreater(len(literals(src)), 50, f"{rel}: read too few literals; guard is not looking")
            self.assertEqual(violations(src), [], rel)

    def test_guard_can_fail(self):
        self.assertTrue(violations("const s = 'No other record is attested to be this place';"))
        self.assertTrue(violations("const t = `<span>Looking up the attestations</span>`;"))
        self.assertEqual(violations("// attested here\nconst s = 'Linked by you'; /* attest */"), [])
