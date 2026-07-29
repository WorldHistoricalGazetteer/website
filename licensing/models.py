from django.db import models


class License(models.Model):
    """A reusable, SPDX-anchored licence record.

    The single source of truth for licence vocabulary across WHG. Attached by
    FK to Dataset and Collection (Phase 1); GazetteerRegistryEntry follows in a
    later phase. WHG's own curation/aggregation licence is held separately as
    ``settings.WHG_OVERLAY_LICENSE`` and asserted *alongside* — never instead
    of — a source's rights.
    """

    spdx_id = models.CharField(
        max_length=64, unique=True,
        help_text="SPDX identifier, e.g. 'CC-BY-4.0'. "
                  "Use a 'custom-*' id for non-SPDX terms.",
    )
    label = models.CharField(max_length=128)
    url = models.URLField(blank=True, help_text="Canonical licence deed / terms URL.")
    spdx_uri = models.URLField(default="https://spdx.org/licenses/")

    # Tri-state: NULL means "no grant either way is evidenced" — not "forbidden".
    # Some sources (e.g. UN Geospatial data) publish no licence grant at all, and
    # recording that as False would assert a restriction we cannot evidence, just
    # as True would assert a permission we cannot evidence. Consumers must test
    # ``is None`` before treating this as a boolean.
    permits_commercial = models.BooleanField(default=True, null=True)
    share_alike = models.BooleanField(default=False)
    attribution_required = models.BooleanField(default=True)
    # Whether the licence forbids adaptations (CC …-ND-…). Derivable from the
    # SPDX id for standard licences, but bespoke terms have to state it, so it is
    # held rather than computed. NULL = bespoke/unstated ("see terms").
    no_derivatives = models.BooleanField(default=False, null=True)

    custom = models.BooleanField(
        default=False,
        help_text="True for bespoke / non-SPDX terms; "
                  "pair with rights_statement on the object.",
    )
    # False for terms that belong to one specific upstream source (a named
    # institution's EULA, say). They must exist in the vocabulary so that source
    # can be described accurately, but offering them to a WHG contributor as a
    # choice for their OWN data would be meaningless.
    contributor_selectable = models.BooleanField(
        default=True,
        help_text="Offer this licence in the contributor licence picker.",
    )
    notes = models.TextField(blank=True)

    class Meta:
        ordering = ["spdx_id"]

    def __str__(self):
        return self.spdx_id

    @property
    def spdx_url(self):
        """Link to the SPDX licence page, or None for custom (non-SPDX) licences."""
        if self.custom:
            return None
        return f"{self.spdx_uri.rstrip('/')}/{self.spdx_id}.html"
