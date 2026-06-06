# utils/csl_citation_formatter

import json
import re

from django.conf import settings
from nameparser import HumanName

ORCID_PATTERN = re.compile(r'\b\d{4}-\d{4}-\d{4}-\d{4}\b')


def _human_to_csl(person, orcid=None):
    """Build a CSL name dict from a nameparser.HumanName (free-text parsing)."""
    given = f"{person.first} {person.middle}".strip() if person.middle else person.first
    d = {"family": person.last or "Unknown", "given": given or ""}
    if orcid:
        d["ORCID"] = f"https://orcid.org/{orcid}"
    return d


def parse_names(names):
    """Parse a semicolon-separated free-text name string into CSL name dicts.

    Names may carry an ORCiD (``0000-0000-0000-0000``); organisations wrapped
    in [brackets] are emitted as a CSL ``literal``. Module-level so the
    contributor importer can reuse it.
    """
    authors = []
    for name in [n.strip() for n in names.split(';') if n.strip()]:
        if name.startswith('[') and name.endswith(']'):
            authors.append({"literal": name[1:-1]})
        else:
            m = ORCID_PATTERN.search(name)
            orcid = m.group() if m else None
            clean = ORCID_PATTERN.sub('', name).strip()
            authors.append(_human_to_csl(HumanName(clean), orcid=orcid))
    return authors


def _person_to_csl(person):
    """Build a CSL name dict from a structured persons.Person."""
    if person.literal:
        d = {"literal": person.literal}
    else:
        d = {"family": person.family or "Unknown", "given": person.given or ""}
    if person.orcid:
        d["ORCID"] = f"https://orcid.org/{person.orcid}"
    return d


def contributions_csl(obj):
    """CSL author dicts from structured Contribution rows targeting ``obj``,
    ordered by ``order``. Returns [] when there are none, so callers fall back
    to the free-text creator/contributors fields (i.e. existing behaviour is
    unchanged until structured contributions exist)."""
    try:
        from django.contrib.contenttypes.models import ContentType
        from persons.models import Contribution
        ct = ContentType.objects.get_for_model(obj.__class__)
        contribs = (Contribution.objects
                    .filter(content_type=ct, object_id=str(obj.pk))
                    .select_related('person').order_by('order'))
        return [_person_to_csl(c.person) for c in contribs]
    except Exception:
        return []


def csl_citation(self):
    try:
        # If this is a dataset collection, include all related datasets.
        if hasattr(self, 'collection_class') and self.collection_class == 'dataset':
            objects = [self] + list(self.datasets.all())
        else:
            objects = [self]

        authors = []
        for obj in objects:
            # Prefer structured CRediT contributions; fall back to the
            # free-text creator/contributors fields when there are none.
            structured = contributions_csl(obj)
            if structured:
                authors.extend(structured)
                continue
            if hasattr(obj, 'authors') and obj.authors:
                authors.extend(parse_names(obj.authors))
            if hasattr(obj, 'creator') and obj.creator:
                authors.extend(parse_names(obj.creator))
            if hasattr(obj, 'contributors') and obj.contributors:
                authors.extend(parse_names(obj.contributors))

        # Deduplicate authors based on the content of the dictionaries.
        unique_authors = []
        seen_authors = set()
        for author in authors:
            author_tuple = frozenset(author.items())
            if author_tuple not in seen_authors:
                seen_authors.add(author_tuple)
                unique_authors.append(author)

        csl_data = {
            "id": self.label if hasattr(self, 'label') else self.id or "Unknown",
            "type": "dataset",
            "title": self.title or "No Title",
            "author": unique_authors,
            "issued": {
                "date-parts": [[self.create_date.year, self.create_date.month,
                                self.create_date.day]] if self.create_date else []
            },
            "URL": self.webpage or "",
            "DOI": f"{settings.DOI_PREFIX}/whg-{self._meta.model_name}-{self.id}" if self.doi else "",
            "publisher": "World Historical Gazetteer",
            "publisher-place": "Pittsburgh, PA, USA",

            # Custom fields (ignored by CSL processors)
            "description": self.description or "",
            "record_count": self.numrows if hasattr(self, 'numrows') else 0,
            **({"source": self.source} if hasattr(self, 'source') else {}),
            **({"source_citation": self.citation} if hasattr(self, 'citation') else {}),
        }
    except Exception as e:
        csl_data = {"error": str(e)}

    return json.dumps(csl_data)
