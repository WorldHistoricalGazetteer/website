# utils/version_dispatch.py

"""
Thin dispatcher that routes a request to either the *stable* or *beta*
view depending on the version stored in ``request.session['whg_version']``.

Usage in ``urls.py``::

    from utils.version_dispatch import version_dispatch
    from .views import PlaceDetailView
    from .views_beta import PlaceDetailBetaView

    urlpatterns = [
        path('place/<int:pk>/',
             version_dispatch(PlaceDetailView, PlaceDetailBetaView),
             name='place-detail'),
    ]

When the beta is promoted to stable:
  1. Merge ``views_beta.py`` into ``views.py``.
  2. Replace ``version_dispatch(Stable, Beta)`` with just ``Stable.as_view()``
     (or the plain function view).
  3. Set ``WHG_BETA_VERSION=`` in ``.env`` to hide the switcher.

URL *names* never change, so ``{% url %}`` and ``reverse()`` keep working.
"""

from functools import wraps

from django.conf import settings


def _is_beta_session(request) -> bool:
    """Return True if the user has selected the beta version."""
    beta = getattr(settings, "BETA_VERSION", "")
    if not beta:
        return False
    return request.session.get("whg_version") == beta


def version_dispatch(stable_view, beta_view):
    """
    Return a view callable that delegates to *stable_view* or *beta_view*.

    Both arguments may be:
      - a plain function view, or
      - a class-based view **class** (``as_view()`` will be called automatically).
    """
    # Materialise CBVs once at import time so .as_view() is not called per-request.
    stable = stable_view.as_view() if _is_cbv(stable_view) else stable_view
    beta = beta_view.as_view() if _is_cbv(beta_view) else beta_view

    @wraps(stable)
    def dispatcher(request, *args, **kwargs):
        view = beta if _is_beta_session(request) else stable
        return view(request, *args, **kwargs)

    return dispatcher


def _is_cbv(view):
    """Heuristic: class-based views have an ``as_view`` classmethod."""
    return isinstance(view, type) and hasattr(view, "as_view")

