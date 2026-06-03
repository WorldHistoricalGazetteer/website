# leads/models.py
from django.conf import settings
from django.db import models


class LeadStatus(models.TextChoices):
    SUGGESTED   = 'suggested',   'Suggested'      # public/unreviewed default
    TRIAGE      = 'triage',      'In triage'
    APPROVED    = 'approved',    'Approved'
    IN_PROGRESS = 'in_progress', 'In progress'
    INGESTED    = 'ingested',    'Ingested'
    REJECTED    = 'rejected',    'Rejected'
    PARKED      = 'parked',      'Parked'


class LeadProvenance(models.TextChoices):
    OWN         = 'own',       'Own research'
    COMMUNITY   = 'community', 'Community recommendation'
    PUBLIC_FORM = 'public',    'Public suggestion form'


class ScanStatus(models.TextChoices):
    DOWNLOADABLE = 'downloadable', 'PDF/text downloadable'
    FULLVIEW     = 'fullview',     'Catalogue / full view only'
    NONE         = 'none',         'Not digitised'
    UNKNOWN      = 'unknown',      'Unknown'


class DatasetLead(models.Model):
    # --- bibliographic (spreadsheet columns) ---
    title             = models.CharField(max_length=500)
    volume_example    = models.CharField(max_length=500, blank=True)   # "Volume / district example"
    author_compiler   = models.CharField(max_length=500, blank=True)
    publication_years = models.CharField(max_length=100, blank=True)   # free text e.g. "1877–1896"
    region_covered    = models.TextField(blank=True)
    current_area      = models.CharField(max_length=255, blank=True)   # "Current country/area"
    ccodes            = models.JSONField(default=list, blank=True)     # derived ISO codes for gap-value
    repository        = models.CharField(max_length=255, blank=True)   # "Repository checked"
    source_url        = models.URLField(max_length=1000, blank=True)
    scan_status       = models.CharField(max_length=20, choices=ScanStatus.choices, default=ScanStatus.UNKNOWN)
    tags              = models.JSONField(default=list, blank=True)

    # --- workflow / triage ---
    status            = models.CharField(max_length=20, choices=LeadStatus.choices, default=LeadStatus.SUGGESTED)
    assignee          = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                          on_delete=models.SET_NULL, related_name='assigned_leads')
    next_action       = models.TextField(blank=True)                  # "Suggested next action"
    notes             = models.TextField(blank=True)

    # --- Ruth's two new fields ---
    provenance        = models.CharField(max_length=20, choices=LeadProvenance.choices, default=LeadProvenance.OWN)
    recommended_by    = models.CharField(max_length=255, blank=True)  # free text: who recommended it (public submitter or community member)
    recommender_user  = models.ForeignKey(settings.AUTH_USER_MODEL, null=True, blank=True,
                                          on_delete=models.SET_NULL, related_name='recommended_leads')
    priority_score    = models.PositiveSmallIntegerField(null=True, blank=True)  # 0-100 composite
    gap_value         = models.PositiveSmallIntegerField(null=True, blank=True)  # auto-computed component
    difficulty        = models.PositiveSmallIntegerField(null=True, blank=True)  # manual: cleaning/rights effort

    # --- selection rubric (sheet 2) — keep as a JSON blob to avoid 9 columns ---
    rubric            = models.JSONField(default=dict, blank=True)
    # keys: regionally_focused, authoritative, out_of_copyright, scans_available,
    #       data_richness, gap_value, workflow_fit, ethics_risk  (each "high"/"med"/"low"/note)

    # --- Zotero link (bibliography layer) ---
    zotero_key        = models.CharField(max_length=32, blank=True, db_index=True)  # item key in the group library
    zotero_version    = models.PositiveIntegerField(null=True, blank=True)          # for incremental sync

    created_at        = models.DateTimeField(auto_now_add=True)
    updated_at        = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ['-priority_score', '-created_at']

    def __str__(self):
        return self.title
