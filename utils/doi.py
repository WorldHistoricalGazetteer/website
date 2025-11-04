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
    except json.JSONDecodeError:
        logger.error("Error decoding citation_csl JSON.")
        return []

    creators = citation_data.get('author', [])
    return [
        {
            "nameType": "Organizational" if "literal" in creator else "Personal",
            "name": creator.get("literal") or f"{creator['family']}, {creator.get('given', '')}",
            **({"givenName": creator.get("given"), "familyName": creator["family"]} if "family" in creator else {}),
            **({
                "nameIdentifiers": [
                    {
                        "nameIdentifier": creator["ORCID"],
                        "nameIdentifierScheme": "ORCID",
                        "schemeURI": "https://orcid.org"
                    }
                ]
            } if "ORCID" in creator else {})
        }
        for creator in creators
    ]


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


def get_doi_metadata(type, id):

    obj = get_object(type, id)

    if not obj:
        return None, None

    metadata = {
        'doi': format_doi(type, id),
        'url': format_url(type, id, obj),
        "creators": get_creators(obj),
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
        "rightsList": [
            {
                "rights": "Creative Commons Attribution-NonCommercial 4.0 International",
                "rightsURI": "https://creativecommons.org/licenses/by-nc/4.0/",
                "rightsIdentifier": "CC-BY-NC-4.0",
                "rightsIdentifierScheme": "SPDX",
                "schemeURI": "https://spdx.org/licenses/",
                "lang": "en"
            }
        ]
    }

    return obj, metadata


def doi(type, id, event='publish'):
    obj, attributes = get_doi_metadata(type, id)

    if not obj or not attributes:
        logger.error(f"DOI metadata could not be retrieved for type '{type}' and id '{id}'")
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
        logger.info(f"DOI {'updated' if doi_exists else 'created'} successfully: {response.json()['data']['id']}")  # Log success
        if not doi_exists:
            # Set `doi` field to True in the object
            obj.doi = True
            obj.save()
        return response.json()  # DOI created successfully
    else:
        logger.error(f"Failed to create DOI: {response.json()}")  # Log error
        return response.json()  # Handle errors (like invalid metadata)


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
