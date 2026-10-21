# whg/api/root_urls.py

from django.urls import path
from drf_spectacular.utils import extend_schema_view, extend_schema, OpenApiResponse
from drf_spectacular.views import SpectacularAPIView, SpectacularRedocView
from rest_framework.renderers import JSONRenderer, BrowsableAPIRenderer

from .reconcile import ReconciliationView, SuggestEntityView, ExtendProposeView, SuggestPropertyView, \
    AuthorityDatasetsView, DummyView, PeriodSuggestView
from .views_entity import CustomSwaggerUIView, EntityDetailView, EntityFeatureView, EntityPreviewView, \
    EntityUpdateView, EntityDeleteView, EntityCreateView
from .views_context import whg_context
from .views_locus import locus_entity, locus_entity_api
from .views_indexing import (
    ContributorAttestationView,
    GazetteerInventoryView,
    RetentionNotifyView,
)


@extend_schema_view(
    get=extend_schema(
        tags=["Schema"],
        responses={200: OpenApiResponse(description="OpenAPI schema for the API")},
    )
)
class SchemaAPIView(SpectacularAPIView):
    """Forces Swagger UI to display JSON in the Responses tab, even when YAML is requested."""
    renderer_classes = [JSONRenderer, BrowsableAPIRenderer] + SpectacularAPIView.renderer_classes[1:]


SchemaRedocView = extend_schema_view(
    get=extend_schema(tags=["Schema"])
)(SpectacularRedocView)

urlpatterns = [
    # Schema and documentation endpoints
    path("api/schema/", SchemaAPIView.as_view(), name="schema"),
    path("api/schema/swagger-ui/", CustomSwaggerUIView.as_view(), name="swagger-ui"),
    path("api/schema/redoc/", SchemaRedocView.as_view(url_name="schema"), name="redoc"),

    ## Reconciliation API endpoints
    path("reconcile", ReconciliationView.as_view(), name="reconcile"),
    path('reconcile/properties', ExtendProposeView.as_view(), name='extend-propose'),
    path('reconcile/authority-datasets', AuthorityDatasetsView.as_view(), name='authority-datasets'),
    path('reconcile/periods/suggest', PeriodSuggestView.as_view(), name='reconcile-period-suggest'),
    path("suggest/entity", SuggestEntityView.as_view(), name="suggest_entity"),
    path("suggest/property", SuggestPropertyView.as_view(), name="suggest_property"),

    # place#325 (ruling 2026-10-21): the provisional JSON-LD context of the locus and
    # record documents. Always served (not behind the release switch): it reveals nothing.
    path("ns/whg-context.jsonld", whg_context, name="whg-jsonld-context"),

    # place#172 / place#325 Stage 4: /entity/locus:<PID> (content-negotiated) and
    # its /api alias. Listed BEFORE the generic entity routes so the `locus`
    # object type never reaches EntityDetailView; place: etc. are untouched.
    # Off (the v4 release switch, places.v4_landing.v4_landing_enabled) they hand the request to the
    # generic entity views below, so the answer is the pre-route one exactly
    # (anonymous 401, signed-in 404); beta-gated when on.
    path("entity/locus:<str:pid>/", locus_entity, name="entity-locus"),
    path("entity/locus:<str:pid>/api", locus_entity_api, name="entity-locus-api"),

    # Entity API endpoints
    path("entity/<str:entity_id>/", EntityDetailView.as_view(), name="entity-detail"),
    path("entity/<str:entity_id>/api", EntityFeatureView.as_view(), name="entity-api"),
    path("entity/<str:entity_id>/preview", EntityPreviewView.as_view(), name="entity-preview"),
    path("entity/<str:entity_id>/update", EntityUpdateView.as_view(), name="entity-replace"),
    path("entity/<str:entity_id>/delete", EntityDeleteView.as_view(), name="entity-delete"),
    path("entity/<str:entity_id>/create", EntityCreateView.as_view(), name="entity-create"),

    # Dummy endpoint: serves both as a placeholder and as an inert handler for OpenRefine's legacy search calls
    path('search/', DummyView.as_view(), name='dummy_search'),

    # Indexing-rebuild endpoints (see ../indexing/developer/plan-ingestionRebuild.execution.md)
    path("api/registry/inventory", GazetteerInventoryView.as_view(),
         name="indexing-registry-inventory"),
    path("api/retention/notify", RetentionNotifyView.as_view(),
         name="indexing-retention-notify"),
    path("api/links", ContributorAttestationView.as_view(),
         name="indexing-contributor-attestations"),
]
