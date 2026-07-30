"""Durably converge embargo auto-release (place#162).

``GazetteerRegistryEntry.visible_to()`` already treats an ``embargoed`` row
whose ``embargo_release_at`` has passed as visible lazily, so nothing is
*broken* if this command never runs — but the row's own ``status`` stays
``embargoed`` until something writes it, which would misreport in the admin
changelist, exports, or any future code path that reads ``status`` directly
rather than through ``visible_to()``. This command is the idempotent
convergence step, safe to run as often as needed (e.g. hourly via Celery
Beat — see ``whg/celery.py``).

Usage::

    python manage.py release_embargoes
"""
from django.core.management.base import BaseCommand

from api.models import GazetteerRegistryEntry


class Command(BaseCommand):
    help = "Flip 'embargoed' GazetteerRegistryEntry rows past their embargo_release_at to 'published'."

    def handle(self, *args, **options):
        released = GazetteerRegistryEntry.release_due_embargoes()
        if released:
            self.stdout.write(self.style.SUCCESS(f"Released {released} embargoed gazetteer(s)."))
        else:
            self.stdout.write("No embargoes due for release.")
