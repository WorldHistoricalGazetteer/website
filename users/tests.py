"""Tests for the custom user manager."""
from django.contrib.auth import get_user_model
from django.test import TestCase

User = get_user_model()


class CreateUserTests(TestCase):
    """``create_user`` validated given_name and surname and then dropped them.

    They are named parameters, so they never reached ``extra_fields``, and
    ``save()`` derives ``name`` from them — so every account created this way
    displayed its username where its name should have been.
    """

    def test_the_name_parts_are_actually_stored(self):
        user = User.objects.create_user(
            username="mostern-0000-0001-8219-7174",
            email="rm@example.org", password="pw",
            given_name="Ruth", surname="Mostern")
        user.refresh_from_db()
        self.assertEqual(user.given_name, "Ruth")
        self.assertEqual(user.surname, "Mostern")

    def test_the_display_name_is_derived_from_them(self):
        user = User.objects.create_user(
            username="someone-1", email="s@example.org", password="pw",
            given_name="Ruth", surname="Mostern")
        self.assertEqual(user.name, "Ruth Mostern")

    def test_a_superuser_keeps_its_name_too(self):
        user = User.objects.create_superuser(
            username="admin-1", email="a@example.org", password="pw",
            given_name="Ada", surname="Lovelace")
        self.assertEqual(user.name, "Ada Lovelace")
        self.assertTrue(user.is_superuser)
