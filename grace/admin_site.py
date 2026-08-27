"""A dedicated admin site for GRACE, at ``/grace/admin/``.

Django's stock admin is a developer's tool: it lists every model in the
project, uses Django's branding, and buries the four things that actually
matter to an editor under Auth, Sites, Celery Results and the rest. Someone
administering GRACE should not have to know which of forty apps is theirs.

This is a full ``AdminSite`` of its own, not a skin. It shows **only** GRACE's
models, groups them by register, and leads with the work that needs attention.
Every ``ModelAdmin`` from ``admin.py`` is reused unchanged — the filters, the
bulk actions, the inline interaction log, the autocomplete fields, the
list-editable triage board. Nothing is reimplemented, so nothing can drift out
of step with the default admin.

The stock ``/admin/`` registration is kept as well: superusers debugging a
foreign key still want the everything-view, and keeping both costs one loop.
"""
from django.contrib import admin

#: Order the registers appear in, and which models sit under each. Anything
#: registered but not listed here still shows, under "Other" — so forgetting to
#: add a new model degrades gracefully instead of hiding it.
REGISTERS = [
    ("Pipeline", "Gazetteers on their way in, and the public suggestion queue.",
     ["TrackedGazetteer", "SourceSuggestion"]),
    ("Catalogue", "Who and what we track: people, institutions, projects, sources.",
     ["Contact", "Organisation", "Project", "Source"]),
    ("Engagement", "Correspondence: conversations, tasks and the dated log.",
     ["Engagement", "ActionItem", "Interaction"]),
    ("Content", "Blog posts, newsletter items and talks — the output side.",
     ["Content"]),
]


class GraceAdminSite(admin.AdminSite):
    site_title = "GRACE"
    site_header = "GRACE — editorial tracker"
    index_title = ""
    index_template = "grace/admin_index.html"
    enable_nav_sidebar = False

    #: Django's default is a bare hyphen, which sat next to the em dashes the
    #: Contact columns return and looked like two different kinds of "empty".
    empty_value_display = "—"

    def each_context(self, request):
        context = super().each_context(request)
        # "View site" should lead back to the staff dashboard, which is where
        # someone administering GRACE actually came from.
        context["site_url"] = "/dashboard_admin/"
        # Read by templates/admin/base_site.html to swap in GRACE's branding and
        # stylesheet. The default admin never sees it, so /admin/ is untouched.
        context["is_grace_admin"] = True
        return context

    def index(self, request, extra_context=None):
        """The landing page: what needs attention, then the registers."""
        from .models import Contact, Engagement, SourceSuggestion

        import datetime

        # ~20 cheap COUNT(*)s on an admin landing page. Fine at this scale;
        # if the Catalogue ever gets large, cache them.
        models_by_name = {
            model.__name__: {
                "name": str(model._meta.verbose_name_plural),
                "url": f"/grace/admin/grace/{model._meta.model_name}/",
                "add_url": f"/grace/admin/grace/{model._meta.model_name}/add/",
                "count": model.objects.count(),
            }
            for model in self._registry
            if model._meta.app_label == "grace"
        }

        registers = []
        listed = set()
        for title, blurb, names in REGISTERS:
            rows = [models_by_name[n] for n in names if n in models_by_name]
            listed.update(names)
            if rows:
                registers.append({"title": title, "blurb": blurb, "rows": rows})

        # Anything not placed in a register is a vocabulary: configuration
        # rather than content. Derived by exclusion so a new lookup table needs
        # no bookkeeping here.
        vocabularies = sorted(
            (info for name, info in models_by_name.items()
             if name not in listed),
            key=lambda info: str(info["name"]),
        )

        today = datetime.date.today()
        attention = [
            {
                "label": "untriaged suggestion",
                "count": SourceSuggestion.objects.filter(
                    status__is_untriaged=True).count(),
                "url": "/grace/admin/grace/sourcesuggestion/?triage=untriaged",
                "level": "warn",
                "why": "Someone sent this in and nobody has looked at it yet.",
            },
            {
                "label": "stalled conversation",
                "count": Engagement.objects.filter(
                    stage__is_open=True, next_follow_up__lt=today).count(),
                "url": "/grace/admin/grace/engagement/?stale=stale",
                "level": "bad",
                "why": "Open, and its follow-up date has passed. A stall is the "
                       "absence of a change, so nothing else would show it.",
            },
            {
                "label": "privacy notice due",
                "count": Contact.objects.owed_privacy_notice().count(),
                "url": "/grace/admin/grace/contact/?notice=overdue",
                "level": "bad",
                "why": "GDPR Art. 14 — we hold their details and have not told "
                       "them.",
            },
        ]

        context = {
            **self.each_context(request),
            "title": "",
            "registers": registers,
            "vocabularies": vocabularies,
            "attention": [a for a in attention if a["count"]],
            "all_clear": not any(a["count"] for a in attention),
            **(extra_context or {}),
        }
        return super().index(request, extra_context=context)


grace_admin_site = GraceAdminSite(name="grace_admin")


#: Models GRACE does not own but must be able to autocomplete against.
#: ``autocomplete_fields`` resolves its endpoint on the *same* admin site, so
#: these have to be registered here too or every form referencing them raises
#: admin.E039. They are deliberately left off the index page: reachable when a
#: lookup needs them, not advertised as part of GRACE.
SUPPORT_MODELS = [
    ("api", "gazetteerregistryentry"),
    ("users", "user"),
]


def register_grace_models():
    """Mirror every GRACE ModelAdmin from the default site onto this one.

    Reading the default site's registry rather than re-listing the models keeps
    the two in step automatically: a model registered in ``admin.py`` shows up
    here without anyone remembering to add it in a second place.
    """
    wanted = set(SUPPORT_MODELS)
    for model, model_admin in admin.site._registry.items():
        meta = model._meta
        is_grace = meta.app_label == "grace"
        is_support = (meta.app_label, meta.model_name) in wanted
        if not (is_grace or is_support):
            continue
        if model in grace_admin_site._registry:
            continue
        grace_admin_site.register(model, type(model_admin))
