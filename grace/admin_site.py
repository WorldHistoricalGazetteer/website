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
    ("Pipeline", "Datasets on their way in, their reviews, and the public "
                 "suggestion queue.",
     ["TrackedDataset", "Review", "SourceSuggestion"]),
    ("Catalogue", "Who and what we track: people, institutions, projects, sources.",
     ["Person", "Organisation", "Project", "Source"]),
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
    #: Person columns return and looked like two different kinds of "empty".
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
        """The landing page: what needs attention, the board, then the registers.

        The board is deliberately first-class rather than a link. GRACE was
        asked for "a single view of datasets on their way in… simple enough for
        anyone on the team to navigate without training", and a landing page
        that only counts things is not that.
        """
        from .models import Person, Engagement, Review, SourceSuggestion, \
            TrackedDataset

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
                "label": "review not passed on",
                "count": Review.objects.filter(
                    returned_on__isnull=False,
                    shared_with_author_on__isnull=True).count(),
                "url": "/grace/admin/grace/review/?state=unshared",
                "level": "bad",
                "why": "A reviewer has replied and the contributor has not "
                       "been told. From their side nothing has happened.",
            },
            {
                "label": "privacy notice due",
                "count": Person.objects.owed_privacy_notice().count(),
                "url": "/grace/admin/grace/person/?notice=overdue",
                "level": "bad",
                "why": "GDPR Art. 14 — we hold their details and have not told "
                       "them.",
            },
        ]

        context = {
            **self.each_context(request),
            "title": "",
            "board": self._board(),
            "board_total": self._board_total,
            "board_url": "/grace/admin/grace/trackeddataset/",
            "registers": registers,
            "vocabularies": vocabularies,
            "attention": [a for a in attention if a["count"]],
            "all_clear": not any(a["count"] for a in attention),
            **(extra_context or {}),
        }
        return super().index(request, extra_context=context)


    #: How many rows the landing-page board shows before handing over to the
    #: full changelist. Enough to scan, few enough to stay a summary.
    BOARD_ROWS = 30

    def _board(self):
        """Active datasets, furthest along first, for the landing page.

        Ordering is the point. Everything starts at "on our radar", so sorting
        by stage ascending would fill the top of the board with the backlog and
        bury the four things anyone is actually working on. Descending puts the
        work first and lets the backlog fall off the end, where the full
        changelist picks it up. Terminal stages ("declined", "complete") sort
        after the live ones however far along they are, and a dataset with no
        stage at all goes last rather than first.

        One query plus the prefetch. The read-throughs to the Register and to
        ``datasets.Dataset`` are per-row, so they stay off the board and live
        on the changelist instead.
        """
        from django.db.models import Case, F, IntegerField, Value, When

        from .models import TrackedDataset

        live = (TrackedDataset.objects
                .filter(is_active=True)
                .select_related("stage", "owner", "permission_status",
                                "registry", "project")
                .prefetch_related("reviews")
                .annotate(_shelved=Case(
                    When(stage__is_open=False, then=Value(1)),
                    default=Value(0), output_field=IntegerField()))
                .order_by("_shelved",
                          F("stage__sort_order").desc(nulls_last=True),
                          "title"))
        self._board_total = live.count()
        rows = live[:self.BOARD_ROWS]
        board = []
        for row in rows:
            reviews = list(row.reviews.all())
            if any(r.awaiting_share for r in reviews):
                review = ("author not told", "bad")
            elif any(r.is_outstanding for r in reviews):
                review = ("out for review", "warn")
            elif reviews and reviews[0].recommendation:
                review = (str(reviews[0].recommendation), "")
            else:
                review = ("", "")
            board.append({
                "title": row.title,
                "url": f"/grace/admin/grace/trackeddataset/{row.pk}/change/",
                "stage": row.stage,
                "held": not row.is_prospect,
                "permission": row.permission_status,
                "owner": row.owner,
                "on_radar": row.on_radar_since,
                "review": review[0],
                "review_level": review[1],
                "whg_url": row.whg_url,
            })
        return board


grace_admin_site = GraceAdminSite(name="grace_admin")


#: Models GRACE does not own but must be able to autocomplete against.
#: ``autocomplete_fields`` resolves its endpoint on the *same* admin site, so
#: these have to be registered here too or every form referencing them raises
#: admin.E039. They are deliberately left off the index page: reachable when a
#: lookup needs them, not advertised as part of GRACE.
SUPPORT_MODELS = [
    ("api", "gazetteerregistryentry"),
    ("users", "user"),
    ("licensing", "license"),
]


def register_grace_models():
    """Mirror every GRACE ModelAdmin from the default site onto this one.

    Reading the default site's registry rather than re-listing the models keeps
    the two in step automatically: a model registered in ``admin.py`` shows up
    here without anyone remembering to add it in a second place.
    """
    # Import the support apps' admin modules first. This function runs while
    # Django is autodiscovering admin modules, so whether ``licensing.admin``
    # has been imported yet depends on INSTALLED_APPS order — and if it has
    # not, mirroring silently misses it and every form with a licence
    # autocomplete raises admin.E039. Importing explicitly makes the order
    # irrelevant; the import is idempotent.
    from importlib import import_module
    for app_label, _model_name in SUPPORT_MODELS:
        try:
            import_module(f"{app_label}.admin")
        except ModuleNotFoundError:
            pass

    wanted = set(SUPPORT_MODELS)
    for model, model_admin in admin.site._registry.items():
        meta = model._meta
        is_grace = meta.app_label == "grace"
        is_support = (meta.app_label, meta.model_name) in wanted
        if not (is_grace or is_support):
            continue
        if model in grace_admin_site._registry:
            continue
        grace_admin_site.register(model, _admin_class_for(model, model_admin))


#: Support models whose ``__str__`` is unhelpful in a picker, and the view that
#: relabels their select2 options. The form-side half is handled by
#: ``UserLabelMixin`` and ``formfield_for_*`` in ``admin.py``; this covers the
#: AJAX half, which fetches its options straight from the target model's admin.
RELABELLED_AUTOCOMPLETES = {
    "users.user": "NamedUserAutocompleteView",
    "api.gazetteerregistryentry": "NamedRegistryAutocompleteView",
}


def _admin_class_for(model, model_admin):
    """The ModelAdmin to mirror, with GRACE's relabelling applied.

    A WHG account renders as an ORCID-derived username and a Register entry as
    ``gn (authority)``. Both are right for a developer reading a shell and
    useless in a picker, and changing either ``__str__`` would reach the whole
    site — so the substitution stays here.
    """
    base = type(model_admin)
    view_name = RELABELLED_AUTOCOMPLETES.get(model._meta.label_lower)
    if not view_name:
        return base

    from . import admin_links
    view = getattr(admin_links, view_name)

    def autocomplete_view(self, request):
        return view.as_view(admin_site=self.admin_site)(request)

    return type(f"Grace{base.__name__}", (base,),
                {"autocomplete_view": autocomplete_view})
