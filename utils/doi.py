import json

import requests
from django.conf import settings
import logging

from collection.models import Collection
from datasets.models import Dataset
from resources.models import Resource

# Set up logger
logger = logging.getLogger('django')  # Or another logger name from your configuration


def format_doi(type, id):
    return f"{settings.DOI_PREFIX}/whg-{type}-{id}"


def format_url(type, id, obj):
    if type == 'dataset':
        return f"{settings.DOI_LANDING_PAGE}datasets/{id}/places"
    elif type == 'collection':
        if obj.collection_class == 'dataset':
            return f"{settings.DOI_LANDING_PAGE}collections/{id}/browse_ds"
        elif obj.collection_class == 'place':
            return f"{settings.DOI_LANDING_PAGE}collections/{id}/browse_pl"
        else:
            return None
    elif type == 'resource':
        return f"{settings.DOI_LANDING_PAGE}resources/{id}/detail"
    else:
        return None


def get_creators(obj):
    try:
        citation_data = json.loads(obj.citation_csl) if isinstance(obj.citation_csl, str) else obj.citation_csl
    except (json.JSONDecodeError, TypeError):
        logger.warning(f"Invalid JSON in citation_csl for object {obj.id}")
        return []

    if not isinstance(citation_data, dict):
        return []

    creators = citation_data.get('author', [])
    valid_creators = []

    for creator in creators:
        # Skip garbage entries
        if not isinstance(creator, dict):
            continue

        # Determine Name
        literal_name = creator.get("literal")
        family_name = creator.get("family")
        given_name = creator.get("given", "")

        # If we have no name source, skip this creator
        if not literal_name and not family_name:
            continue

        creator_entry = {
            "nameType": "Organizational" if literal_name else "Personal",
            "name": literal_name or f"{family_name}, {given_name}".strip(", "),
        }

        # Add Personal specific fields
        if family_name:
            creator_entry["givenName"] = given_name
            creator_entry["familyName"] = family_name

        # Add ORCID if present
        if "ORCID" in creator:
            creator_entry["nameIdentifiers"] = [
                {
                    "nameIdentifier": creator["ORCID"],
                    "nameIdentifierScheme": "ORCID",
                    "schemeURI": "https://orcid.org"
                }
            ]

        valid_creators.append(creator_entry)

    return valid_creators


# CRediT role -> DataCite contributorType (per the JATS4R mapping; roles
# without a clean DataCite equivalent fall back to "Other").
CREDIT_TO_DATACITE = {
    "conceptualization": "Other",
    "data-curation": "DataCurator",
    "formal-analysis": "Researcher",
    "funding-acquisition": "Sponsor",
    "investigation": "Researcher",
    "methodology": "Researcher",
    "project-administration": "ProjectManager",
    "resources": "Other",
    "software": "Other",
    "supervision": "Supervisor",
    "validation": "Researcher",
    "visualization": "Other",
    "writing-original-draft": "Other",
    "writing-review-editing": "Editor",
}


def get_contributors(obj):
    """DataCite ``contributors`` entries from structured Contribution rows
    targeting ``obj``. Empty when there are none (so the DOI payload is
    unchanged for objects without structured CRediT contributions)."""
    try:
        from django.contrib.contenttypes.models import ContentType
        from persons.models import Contribution
        ct = ContentType.objects.get_for_model(obj.__class__)
        contribs = (Contribution.objects
                    .filter(content_type=ct, object_id=str(obj.pk))
                    .select_related("person").order_by("order"))
    except Exception as e:
        logger.warning(f"Could not load contributions for {obj}: {e}")
        return []

    out = []
    for c in contribs:
        p = c.person
        literal = p.literal
        entry = {
            "contributorType": CREDIT_TO_DATACITE.get(c.role, "Other"),
            "nameType": "Organizational" if literal else "Personal",
            "name": literal or f"{p.family or ''}, {p.given or ''}".strip(", "),
        }
        if not literal:
            entry["givenName"] = p.given or ""
            entry["familyName"] = p.family or ""
        if p.orcid:
            entry["nameIdentifiers"] = [{
                "nameIdentifier": f"https://orcid.org/{p.orcid}",
                "nameIdentifierScheme": "ORCID",
                "schemeURI": "https://orcid.org",
            }]
        if p.affiliation:
            entry["affiliation"] = [{"name": p.affiliation}]
        out.append(entry)
    return out


def get_bbox(obj):
    if obj.bbox:
        min_lon, min_lat, max_lon, max_lat = obj.bbox.extent
        return {
            "geoLocationBox": {
                "westBoundLongitude": min_lon,
                "eastBoundLongitude": max_lon,
                "southBoundLatitude": min_lat,
                "northBoundLatitude": max_lat
            }
        }
    else:
        return None


def get_object(type, id):
    model_mapping = {
        'dataset': Dataset,
        'collection': Collection,
        'resource': Resource
    }

    model_class = model_mapping.get(type)

    if model_class:
        return model_class.objects.filter(pk=id).first()
    else:
        return None


def get_rights_list(obj):
    """Build DataCite ``rightsList`` from the object's own recorded licence.

    Returns ``[]`` when no licence is recorded, so that the key is omitted
    entirely. This previously registered a hard-coded CC-BY-NC-4.0 statement
    against every DOI regardless of the object's actual terms; DataCite metadata
    is externally cached and hard to walk back, so asserting nothing is the only
    honest option where we hold nothing. See place#158.

    Note this is the *source* licence — the data's own rights. WHG's
    curation/aggregation overlay is asserted separately and is not registered
    here on a contributor's behalf.
    """
    licence = getattr(obj, 'license', None)
    rights = []

    if licence:
        entry = {"rights": licence.label, "lang": "en"}
        if licence.url:
            entry["rightsURI"] = licence.url
        # A bespoke licence has no SPDX identity, and claiming one would be
        # worse than omitting it.
        if not licence.custom:
            entry["rightsIdentifier"] = licence.spdx_id
            entry["rightsIdentifierScheme"] = "SPDX"
            entry["schemeURI"] = licence.spdx_uri
        rights.append(entry)

    # Extra conditions layered on the licence, or the whole of the terms for a
    # custom one — they cannot be inferred from an SPDX id, so they travel too.
    statement = (getattr(obj, 'rights_statement', '') or '').strip()
    if statement:
        rights.append({"rights": statement, "lang": "en"})

    return rights


def get_doi_metadata(type, id):

    obj = get_object(type, id)

    if not obj:
        return None, None

    # Check for core metadata requirements before building the API payload
    creators_list = get_creators(obj)
    if not creators_list:
        logger.warning(f"DOI Failed: Object {type}:{id} has no valid creators (author field missing/empty in citation_csl).")
        return obj, None

    metadata = {
        'doi': format_doi(type, id),
        'url': format_url(type, id, obj),
        "creators": creators_list,
        **({"contributors": _contributors} if (_contributors := get_contributors(obj)) else {}),
        "titles": [{"title": obj.title or "No title"}],
        "publicationYear": obj.create_date.year if obj.create_date else None,
        "descriptions": [{"description": obj.description or "", "descriptionType": "Abstract"}],
        **({"sizes": [f"{obj.numrows} places"]} if hasattr(obj, 'numrows') else {}),
        **({"geoLocations": [get_bbox(obj)]} if get_bbox(obj) else {}),
        'publisher': {
            "name": "World Historical Gazetteer",
            "publisherIdentifierScheme": "Wikidata",
            "publisherIdentifier": "https://www.wikidata.org/wiki/Q130424771",
            "schemeUri": "https://www.wikidata.org/wiki/",
            "lang": "en"
        },
        'types': {
            "resourceTypeGeneral": "Other" if type =="resource" else "Dataset",
            "resourceType": "Learning Resource" if type =="resource" else "Linked Places Dataset"
        },
        "subjects": [
            {
                "subject": "Historical geography",
                "subjectScheme": "LCSH",
                "schemeURI": "http://id.loc.gov/authorities/subjects"
            },
            {
                "subject": "Place names, History",
                "subjectScheme": "LCSH",
                "schemeURI": "http://id.loc.gov/authorities/subjects"
            },
            {
                "subject": "Geographical names, History",
                "subjectScheme": "LCSH",
                "schemeURI": "http://id.loc.gov/authorities/subjects"
            },
            {
                "subject": "Maps, Historical",
                "subjectScheme": "LCSH",
                "schemeURI": "http://id.loc.gov/authorities/subjects"
            },
            {
                "subject": "Historical regions",
                "subjectScheme": "LCSH",
                "schemeURI": "http://id.loc.gov/authorities/subjects"
            }
        ],
        **({"rightsList": _rights} if (_rights := get_rights_list(obj)) else {}),
    }

    return obj, metadata


def doi(type, id, event='publish'):
    obj, attributes = get_doi_metadata(type, id)

    if not obj or not attributes:
        logger.warning(f"DOI update aborted for '{type}:{id}' due to missing metadata.")
        return None

    # Read `doi` & `public` fields from the object
    doi_exists = hasattr(obj, 'doi') and obj.doi
    public = hasattr(obj, 'public') and obj.public

    # Override event type (e.g., 'draft', 'register', 'publish', 'hide') - default is 'publish'
    attributes['event'] = 'hide' if not public else event

    # Set the headers for the API request
    headers = {
        'Content-Type': 'application/vnd.api+json',
        'authorization': f"Basic {settings.DOI_ENCODED_CREDENTIALS}"
    }

    logger.info(f"Headers: {headers}")
    logger.info(f"Attributes: {attributes}")

    # Send the request to DataCite API: POST for draft, PUT for update
    response = getattr(requests, 'put' if doi_exists else 'post')(
        f"{settings.DOI_API_URL}/{attributes['doi']}" if doi_exists else f"{settings.DOI_API_URL}?publisher=true",
        json={
            "data": {
                "type": "dois",
                "attributes": attributes,
            }
        },
        headers=headers,
    )

    # Check the response status
    if response.status_code in [200, 201]:
        logger.info(f"DOI {'updated' if doi_exists else 'created'} successfully: {response.json()['data']['id']}")
        if not doi_exists:
            # Set `doi` field to True in the object
            obj.doi = True
            obj.save()
        return response.json()  # DOI created/updated successfully
    elif response.status_code == 422:
        # Handle the case where DOI already exists
        response_data = response.json()
        errors = response_data.get('errors', [])

        # Check if the error is specifically about DOI already being taken
        if any('already been taken' in error.get('title', '') for error in errors):
            logger.warning(f"DOI {attributes['doi']} already exists in DataCite, treating as update")

            # Update local database to reflect DOI exists
            if not doi_exists:
                obj.doi = True
                obj.save()

            # Try updating instead with a PUT request
            response = requests.put(
                f"{settings.DOI_API_URL}/{attributes['doi']}",
                json={
                    "data": {
                        "type": "dois",
                        "attributes": attributes,
                    }
                },
                headers=headers,
            )

            if response.status_code in [200, 201]:
                logger.info(f"DOI updated successfully after finding it already exists: {response.json()['data']['id']}")
                return response.json()
            else:
                logger.error(f"Failed to update existing DOI: {response.json()}")
                raise Exception(f"Failed to update existing DOI: {response.json()}")
        else:
            logger.error(f"Failed to create DOI (422 error): {response_data}")
            raise Exception(f"Failed to create DOI: {response_data}")
    else:
        logger.error(f"Failed to create/update DOI: {response.json()}")
        raise Exception(f"Failed to create/update DOI: {response.json()}")


def get_doi_state(type, id):
    doi = format_doi(type, id)
    logger.info(f"Getting state of DOI {doi}...")  # Log the request
    # Set the headers for the API request
    headers = {
        'Content-Type': 'application/vnd.api+json',
        'authorization': f"Basic {settings.DOI_ENCODED_CREDENTIALS}"
    }

    # Send the GET request to DataCite API to retrieve DOI metadata

    response = requests.get(
        f"{settings.DOI_API_URL}/{doi}",
        headers=headers,
    )

    if response.status_code == 200:
        # Parse the JSON response and extract the state
        doi_metadata = response.json()
        state = doi_metadata['data']['attributes'].get('state', 'not_found')
        logger.info(f"DOI {doi} state: {state}")
        return state
    else:
        # Log the error or return the error message
        logger.error(f"Failed to retrieve DOI {doi} state: {response.json()}")
        return response.json()  # Handle errors if the DOI cannot be retrieved
