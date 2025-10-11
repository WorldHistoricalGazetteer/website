# api/schemas.py
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import extend_schema, extend_schema_view, OpenApiParameter, OpenApiResponse, OpenApiExample

from api.querysets import area_owner_queryset, collection_owner_or_public_queryset, dataset_owner_or_public_queryset, \
    place_feature_queryset, place_preview_queryset, period_public_queryset
from api.reconcile_helpers import ReconciliationRequestSerializer
from api.serializers_api import AreaFeatureSerializer, AreaPreviewSerializer, CollectionPreviewSerializer, \
    DatasetPreviewSerializer, PlaceFeatureSerializer, PlacePreviewSerializer, PeriodFeatureSerializer, \
    PeriodPreviewSerializer
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset
from periods.models import Period
from places.models import Place

OBJECT_TYPES = ["place", "dataset", "collection", "area", "period"]

TYPE_MAP = {
    "area": {
        "model": Area,
        "detail_url": "areas:area-update",
        "detail_queryset": area_owner_queryset,
        "feature_serializer": AreaFeatureSerializer,
        "feature_queryset": area_owner_queryset,
        "preview_serializer": AreaPreviewSerializer,
        "preview_queryset": area_owner_queryset,
    },
    "collection": {
        "model": Collection,
        "detail_url": True,  # special case: choose between "ds-collection-browse" and "place-collection-browse"
        "preview_serializer": CollectionPreviewSerializer,
        "preview_queryset": collection_owner_or_public_queryset,
    },
    "dataset": {
        "model": Dataset,
        "detail_url": "datasets:ds_places",
        "preview_serializer": DatasetPreviewSerializer,
        "preview_queryset": dataset_owner_or_public_queryset,
    },
    "period": {
        "model": Period,
        "detail_url": None,  # Will redirect to PeriodO website
        "feature_serializer": PeriodFeatureSerializer,
        "feature_queryset": period_public_queryset,
        "preview_serializer": PeriodPreviewSerializer,
        "preview_queryset": period_public_queryset,
    },
    "place": {
        "model": Place,
        "detail_url": "places:place-portal-pid",
        "feature_serializer": PlaceFeatureSerializer,
        "feature_queryset": place_feature_queryset,
        "preview_serializer": PlacePreviewSerializer,
        "preview_queryset": place_preview_queryset,
    },
}

# Define view metadata including per-method overrides
VIEW_CLASSES = {
    "detail": {
        "methods": {
            "get": {
                "summary": "Render object detail page",
                "description": (
                    "Returns a human-readable detail page for the requested object type. "
                    "Intended for use in the main web application, not as a machine-readable API. "
                    "For structured object data, use the 'feature' endpoint."
                ),
            },
        },
    },
    "feature": {
        "methods": {
            "get": {
                "summary": "Retrieve object as structured API feature",
                "description": "Returns the object in a structured API format (e.g., GeoJSON or JSON feature) suitable for programmatic access.",
            },
        },
    },
    "preview": {
        "methods": {
            "get": {
                "summary": "Preview object HTML snippet",
                "description": "Returns an HTML snippet for previewing an object record.",
            },
        },
    },
    "create": {
        "methods": {
            "post": {
                "summary": "Create a new object",
                "description": "Creates a new object record with the provided data and returns the created object.",
            },
        },
    },
    "update": {
        "methods": {
            "put": {
                "summary": "Replace an existing object",
                "description": "Replaces an existing object record entirely with new data, updating all fields.",
            },
            "patch": {
                "summary": "Update an existing object",
                "description": "Partially updates fields of an existing object record.",
            },
        },
    },
    "delete": {
        "methods": {
            "delete": {
                "summary": "Delete an existing object",
                "description": "Deletes an existing object record permanently from the system.",
            },
        },
    },
}

QUERY_PARAMETERS = """
### Query Parameters

**`query`** *(string)*  
Free-text search string. Required if no spatial or dataset filters are provided.

**`mode`** *(string)*  
Search mode: `exact`, `fuzzy`* (default), `starts`, or `in`. Fuzzy mode can also be specified as `prefix_length|fuzziness` (e.g., `2|1`). **Coming soon**: `phonetic`, and eventually `ner` for LLM-based entity recognition.

**`fclasses`** *(array)*  
Restrict to specific feature classes. Valid values: `A` (Administrative), `H` (Hydrographic), `L` (Landscape), `P` (Populated places), `R` (Roads/routes), `S` (Sites), `T` (Topographic), `X` (unknown - always included). Format: `["A","L"]`.

### Temporal Filtering

**`start`** *(integer)*  
Start year for temporal filtering. Must be a valid year.

**`end`** *(integer)*  
End year for temporal filtering (default: current year). Must be ≥ `start` year (if provided).

### Spatial Filtering

**`countries`** *(array)*  
Restrict results to ISO 3166-1 alpha-2 country codes. Format: `["US","GB"]`.

**`bounds`** *(object)*  
GeoJSON geometry collection for spatial restriction. Ignored if circular search parameters are provided.  
Example: `{"type":"Polygon","coordinates":[[[lon,lat],[lon,lat],...]]})`

**`lat`** *(float)*  
Latitude for circular search (-90 to 90). Must be used with `lng` and `radius`.

**`lng`** *(float)*  
Longitude for circular search (-180 to 180). Must be used with `lat` and `radius`.

**`radius`** *(float)*  
Radius in kilometers for circular search. Must be used with `lat` and `lng`.

**`userareas`** *(array)*  
IDs of user-defined stored areas for spatial filtering. Format: `[123,456]`.

### Dataset Filtering

**`dataset`** *(integer)*  
Restrict results to specific dataset ID. Must be a valid dataset ID to which the user has access.

### Data Completeness

**`unlocated`** *(boolean)*  
Include results with no spatial metadata (default: true).

**`undated`** *(boolean)*  
Include results with no temporal metadata (default: true).

### Response Control

**`size`** *(integer)*  
Maximum results per query (default: 100, max: 1000).
"""


def entity_schema(view_class: str):
    """
    Dynamically builds extend_schema_view for a given view class.
    Supports multiple HTTP methods with per-method summary/description.
    """
    view_config = VIEW_CLASSES.get(view_class, {})
    methods_config = view_config.get("methods", {})
    schema_dict = {}

    # Base parameters applied to all methods
    base_parameters = [
        OpenApiParameter(
            name="token",
            required=True,
            type=str,
            location=OpenApiParameter.QUERY,
            description="API token for authentication",
        ),
        OpenApiParameter(
            name="entity_id",
            required=True,
            type=str,
            location=OpenApiParameter.PATH,
            description=f"The unique identifier of the object, formatted as **\<type\>:\<id\>**. Valid types are {', '.join(OBJECT_TYPES)}. ",
        ),
    ]

    if view_class == "create":
        base_parameters = [param for param in base_parameters if param.name != "entity_id"]

    if view_class in ["create", "update"]:
        base_parameters.append(
            OpenApiParameter(
                name="body",
                required=True,
                type=OpenApiTypes.OBJECT,
                description="The object data in JSON format.",
            )
    )

    # Base responses applied to all methods
    base_responses = {
        400: OpenApiResponse(description="Missing or invalid parameters"),
        403: OpenApiResponse(description="Invalid API token"),
        404: OpenApiResponse(description="Object not found"),
    }

    for method, meta in methods_config.items():
        summary = meta.get("summary", "")
        description = meta.get("description", "")

        # Set standard 200/201/204 response depending on method
        if method in ["post", "put", "patch"]:
            responses = {201: OpenApiResponse(description=description), **base_responses}
        elif method == "delete":
            responses = {204: OpenApiResponse(description="Deleted successfully"), **base_responses}
        else:
            responses = {200: OpenApiResponse(description=description), **base_responses}

        schema_dict[method] = extend_schema(
            tags=["Entity API"],
            summary=summary,
            description=description,
            parameters=base_parameters,
            responses=responses,
        )

    return extend_schema_view(**schema_dict)


def build_schema_view(
        *,
        methods: dict,
        tags: list[str],
        summary: str = "",
        description: str = "",
        parameters: list[OpenApiParameter] = None,
        responses: dict = None,
        request=None,
        examples: list[OpenApiExample] = None
):
    """
    DRY wrapper for extend_schema_view.

    `methods` can be:
    - Simple format: {"get": True, "post": True} (uses shared summary/description)
    - Detailed format: {"get": {"summary": "...", "description": "..."}, "post": {...}}
    """
    schema_dict = {}

    for method, config in methods.items():
        # Handle simple boolean format (backward compatibility)
        if isinstance(config, bool) and config:
            method_summary = summary
            method_description = description
            method_parameters = parameters or []
            method_responses = responses or {}
            method_request = request
            method_examples = examples or []

        # Handle detailed configuration format
        elif isinstance(config, dict):
            method_summary = config.get("summary", summary)
            method_description = config.get("description", description)
            method_parameters = config.get("parameters", parameters or [])
            method_responses = config.get("responses", responses or {})
            method_request = config.get("request", request)
            method_examples = config.get("examples", examples or [])

        else:
            continue  # Skip if config is False or invalid

        schema_dict[method] = extend_schema(
            tags=tags,
            summary=method_summary,
            description=method_description,
            parameters=method_parameters,
            responses=method_responses,
            request=method_request,
            examples=method_examples,
        )

    return extend_schema_view(**schema_dict)


def reconcile_schema():
    """Schema for the Reconciliation API root endpoint"""
    return build_schema_view(
        methods={
            "get": {
                "summary": "Get Reconciliation Service metadata",
                "description": (
                    "Retrieve service metadata including URLs, default types, and preview configuration. "
                    "Returns the service manifest as per Reconciliation Service API v0.2."
                ),
                "responses": {
                    200: OpenApiResponse(description="Service metadata including manifest, default types, and preview configuration"),
                },
            },
            "post": {
                "summary": "Submit Reconciliation queries",
                "description": (
                    "Submit reconciliation queries to match place or period names against the WHG+PeriodO database. "
                    "Supports batch queries and implements the Reconciliation Service API v0.2."
                    f"\n\n{QUERY_PARAMETERS}"
                ),
                "parameters": [
                    OpenApiParameter(
                        name="token",
                        required=False,
                        type=OpenApiTypes.STR,
                        location=OpenApiParameter.QUERY,
                        description="API token for authentication",
                    ),
                    OpenApiParameter(
                        name="queries",
                        type=OpenApiTypes.STR,
                        required=False,
                        location=OpenApiParameter.QUERY,
                        description="JSON object with reconciliation queries",
                    ),
                    OpenApiParameter(
                        name="extend",
                        type=OpenApiTypes.STR,
                        required=False,
                        location=OpenApiParameter.QUERY,
                        description="JSON object with extension request (ids + properties)",
                    ),
                ],
                "request": ReconciliationRequestSerializer,
                "examples": [
                    OpenApiExample(
                        name="Basic Reconciliation Request",
                        description="Example of reconciling place names",
                        value={
                          "queries": {
                            "q0": {
                              "query": "Edinburgh",
                              "type": "https://whgazetteer.org/static/whg_schema.jsonld#Place"
                            },
                            "q1": {
                              "query": "Leeds",
                              "type": "https://whgazetteer.org/static/whg_schema.jsonld#Place"
                            }
                          }
                        }
                    ),
                    OpenApiExample(
                        name="Advanced Reconciliation Request",
                        description="Search with custom fuzziness and with geographic and temporal constraints",
                        value={
                            "queries": {
                                "q0": {
                                    "query": "London",
                                    "type": "https://whgazetteer.org/static/whg_schema.jsonld#Place",
                                    "mode": "3|2",
                                    "countries": ["GB"],
                                    "start": 1800,
                                    "end": 1900,
                                    "lat": 51.5074,
                                    "lng": -0.1278,
                                    "radius": 10,
                                    "fclasses": ["P"],
                                    "size": 10
                                },
                            }
                        }
                    ),
                    OpenApiExample(
                        name="Extend Request",
                        description="Example of extending places with additional properties",
                        value={
                            "extend": {
                                "ids": ["place:6469500", "place:6469512"],
                                "type": "https://whgazetteer.org/static/whg_schema.jsonld#Place",
                                "properties": [
                                    {"id": "whg:ccodes", "name": "Country codes"},
                                    {"id": "whg:geometry", "name": "Geometry (GeoJSON)"}
                                ]
                            }
                        }
                    ),
                ],
                "responses": {
                    200: OpenApiResponse(
                        description="Reconciliation results with candidate matches and scores, or extension data with requested properties"),
                    400: OpenApiResponse(description="Invalid query format or missing required parameters"),
                    401: OpenApiResponse(description="Authentication failed"),
                },
            }
        },
        tags=["Reconciliation API"],
    )


def propose_properties_schema():
    """Schema for /reconcile/properties endpoint"""
    return build_schema_view(
        methods={"get": True},
        tags=["Reconciliation API"],
        summary="Discover extensible properties",
        description="Returns a list of properties that can be extended, as required by the Reconciliation Service API.",
        responses={
            200: OpenApiResponse(description="A list of available properties."),
        },
    )


def suggest_entity_schema():
    """Schema for /reconcile/suggest endpoint"""
    return build_schema_view(
        methods={"get": True},
        tags=["Reconciliation API"],
        summary="Suggest entities by prefix",
        description="Returns a list of suggested entities that match a given prefix.",
        parameters=[
            OpenApiParameter(
                name="prefix",
                type=OpenApiTypes.STR,
                description="The string to match against entity names",
                required=True,
            ),
            OpenApiParameter(
                name="limit",
                type=OpenApiTypes.INT,
                description="Maximum number of suggestions to return",
                default=10,
            ),
            OpenApiParameter(
                name="cursor",
                type=OpenApiTypes.INT,
                description="Number of suggestions to skip for pagination",
                default=0,
            ),
            OpenApiParameter(
                name="exact",
                type=OpenApiTypes.BOOL,
                description="Whether to return exact matches only",
                default=False,
            ),
        ],
        responses={
            200: OpenApiResponse(description="A list of matching entity suggestions."),
            401: OpenApiResponse(description="Authentication failed"),
        },
    )


def suggest_property_schema():
    """Schema for /reconcile/suggest/properties endpoint"""
    return build_schema_view(
        methods={"get": True},
        tags=["Reconciliation API"],
        summary="Suggest properties by prefix",
        description="Returns a list of properties that match a given prefix.",
        parameters=[
            OpenApiParameter(
                name="prefix",
                type=OpenApiTypes.STR,
                description="The string to match against property names",
                required=False,
            ),
            OpenApiParameter(
                name="limit",
                type=OpenApiTypes.INT,
                description="Maximum number of suggestions to return",
                default=10,
            ),
            OpenApiParameter(
                name="cursor",
                type=OpenApiTypes.INT,
                description="Number of suggestions to skip for pagination",
                default=0,
            ),
        ],
        responses={
            200: OpenApiResponse(description="A list of matching property suggestions."),
            401: OpenApiResponse(description="Authentication failed"),
        },
    )
