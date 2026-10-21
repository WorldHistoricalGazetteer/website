"""``/ns/whg-context.jsonld``: WHG's minimal, PROVISIONAL JSON-LD context (place#325,
ruling 2026-10-21: "publish a minimal WHG context at a stable URL, marked provisional
until PLATO, reusing schema.org / Dublin Core / LPF terms wherever they exist").

It is the ``@context`` of the Locus (``api.views_locus``) and Record
(``places.v4_landing.record_document``) JSON-LD documents, and defines only the keys
they emit (``api.tests_whg_context`` walks real documents and fails on an undefined
key). The WHG terms it mints are fragments of this same URL, so each term IRI
dereferences to the document that defines it (its ``terms`` map says what each means).

One context for both documents, not one per document: they share most of their terms
(``status``, ``statement``, ``title``, ``minted``/``at`` ...), a client fetches one URL,
and PLATO will replace it in one place.

Served ALWAYS, not behind the v4 release switch: a context reveals nothing. GET/HEAD,
CORS-open (someone else's JSON-LD processor fetches it), ``application/ld+json``, and
a SHORT ``Cache-Control`` because, while provisional, it may change.

The path is free: ``/ns/`` is not otherwise routed in whg3, nginx sends it to Django
(``location /``), and the w3id.org/whg rules rewrite ``/whg/id/...`` only.
"""
from __future__ import annotations

import os

from django.http import HttpResponse
from django.urls import reverse
from django.views.decorators.http import require_safe

CONTEXT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jsonld", "whg-context.jsonld")
# Short while provisional: a change reaches clients within the hour. Lengthen (and
# version the filename) once PLATO fixes the terms.
CONTEXT_MAX_AGE = 3600


def whg_context_url(request) -> str:
    """The absolute URL of the context, for a document's ``@context``."""
    return request.build_absolute_uri(reverse("whg-jsonld-context"))


@require_safe
def whg_context(request):
    with open(CONTEXT_FILE, "rb") as f:
        body = f.read()
    resp = HttpResponse(body, content_type="application/ld+json; charset=utf-8")
    resp["Cache-Control"] = f"public, max-age={CONTEXT_MAX_AGE}"
    resp["Access-Control-Allow-Origin"] = "*"
    resp["X-WHG-Provisional"] = "true"
    return resp

