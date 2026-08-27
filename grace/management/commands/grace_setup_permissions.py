"""Create the "GRACE editors" group and put people in it.

Django's admin needs two separate things: ``is_staff`` to get through the door,
and per-model permissions to see anything once inside. A staff account with no
permissions logs in successfully and is shown an empty page, which looks like a
bug and is really a missing group — so this exists to make granting access one
command rather than forty checkboxes clicked by hand.

The group gets every GRACE permission, including the vocabularies: editing
pick-lists without a developer is the whole point of decision 3, and it cannot
happen if the vocabulary tables are not in the grant.

Idempotent — safe to re-run after adding a model, which is exactly when you
want to, since a new model's permissions would otherwise be missing from the
group.

Usage::

    ./manage.py grace_setup_permissions
    ./manage.py grace_setup_permissions --add Ruth-Mostern-0000-0001-8219-7174
    ./manage.py grace_setup_permissions --add alice --add bob --make-staff
"""
from django.apps import apps
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group, Permission
from django.contrib.contenttypes.models import ContentType
from django.core.management.base import BaseCommand

GROUP_NAME = "GRACE editors"


class Command(BaseCommand):
    help = 'Create/refresh the "GRACE editors" group and optionally add users.'

    def add_arguments(self, parser):
        parser.add_argument(
            "--add", action="append", default=[], metavar="USERNAME",
            help="Add this user to the group. Repeatable.")
        parser.add_argument(
            "--make-staff", action="store_true",
            help="Also set is_staff on the users named with --add. Without "
                 "is_staff they cannot reach the admin at all, group or no "
                 "group.")
        parser.add_argument(
            "--list", action="store_true",
            help="Show who is currently in the group and stop.")

    def handle(self, *args, **options):
        group, created = Group.objects.get_or_create(name=GROUP_NAME)
        User = get_user_model()

        if options["list"]:
            members = group.user_set.order_by("username")
            self.stdout.write(f'"{GROUP_NAME}" has {members.count()} member(s):')
            for user in members:
                flag = "" if user.is_staff else "   ⚠ NOT staff — cannot reach the admin"
                self.stdout.write(f"  {user.username}{flag}")
            return

        grace_models = apps.get_app_config("grace").get_models()
        content_types = ContentType.objects.get_for_models(*grace_models).values()
        perms = Permission.objects.filter(content_type__in=content_types)
        group.permissions.set(perms)

        self.stdout.write(self.style.SUCCESS(
            f'{"Created" if created else "Refreshed"} "{GROUP_NAME}" with '
            f'{perms.count()} permissions across {len(list(content_types))} models.'))

        for username in options["add"]:
            user = User.objects.filter(username=username).first()
            if not user:
                self.stdout.write(self.style.ERROR(f"  no such user: {username}"))
                continue
            user.groups.add(group)
            note = ""
            if options["make_staff"] and not user.is_staff:
                user.is_staff = True
                user.save(update_fields=["is_staff"])
                note = " (is_staff set)"
            self.stdout.write(f"  added {username}{note}")
            if not user.is_staff:
                self.stdout.write(self.style.WARNING(
                    f"    ⚠ {username} is not staff, so cannot reach the admin. "
                    f"Re-run with --make-staff."))
