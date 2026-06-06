from persons.models import Person


def resolve_person(name, affiliation=None):
    """Reuse-or-create a Person from a ``parse_names`` CSL dict
    (``{family, given[, ORCID]}`` or ``{literal}``).

    Uses ``filter().first()`` so pre-existing duplicate Person rows never raise
    MultipleObjectsReturned. Affiliation is set only when a new Person is created.
    """
    literal = name.get("literal")
    if literal:
        existing = Person.objects.filter(literal=literal).first()
        if existing:
            return existing
        return Person.objects.create(
            literal=literal, **({"affiliation": affiliation} if affiliation else {}))

    orcid = (name.get("ORCID") or "").replace("https://orcid.org/", "") or None
    if orcid:
        existing = Person.objects.filter(orcid=orcid).first()
        if existing:
            return existing

    family = name.get("family") or None
    given = name.get("given") or None
    existing = Person.objects.filter(family=family, given=given).first()
    if existing:
        return existing
    return Person.objects.create(
        family=family, given=given,
        **({"orcid": orcid} if orcid else {}),
        **({"affiliation": affiliation} if affiliation else {}),
    )
