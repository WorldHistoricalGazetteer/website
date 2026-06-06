from django.db import IntegrityError, transaction
from django.test import TestCase

from api.models import GazetteerRegistryEntry
from persons.models import Person, Contribution, CreditRole


class CreditRoleTests(TestCase):
    def test_fourteen_roles(self):
        self.assertEqual(len(CreditRole.choices), 14)

    def test_slug_format(self):
        # Canonical CRediT slugs: lowercase words joined by single hyphens.
        for value, _label in CreditRole.choices:
            self.assertRegex(value, r"^[a-z]+(-[a-z]+)*$")


class ContributionTests(TestCase):
    def setUp(self):
        self.person = Person.objects.create(family="Mostern", given="Ruth")

    def _contrib(self, target, role=CreditRole.DATA_CURATION):
        return Contribution.objects.create(person=self.person, role=role, target=target)

    def test_target_int_pk(self):
        # A second Person stands in for an integer-PK credited object.
        target = Person.objects.create(family="Grossner", given="Karl")
        c = self._contrib(target)
        reloaded = Contribution.objects.get(pk=c.pk)
        self.assertEqual(reloaded.target, target)
        self.assertEqual(reloaded.object_id, str(target.pk))

    def test_target_string_pk(self):
        # GazetteerRegistryEntry has a CharField PK ("gn") — exercises the
        # CharField object_id that lets one generic FK span mixed PK types.
        # Use a sub-namespaced id (and one not seeded by api's data migration)
        # to exercise a non-integer PK in object_id.
        gaz = GazetteerRegistryEntry.objects.create(
            id="whg:credit-test", name="Credit Test", namespace="whg",
            entry_class="dataset",
        )
        c = self._contrib(gaz, role=CreditRole.RESOURCES)
        reloaded = Contribution.objects.get(pk=c.pk)
        self.assertEqual(reloaded.target, gaz)
        self.assertEqual(reloaded.object_id, "whg:credit-test")

    def test_credit_uri(self):
        c = self._contrib(self.person, role=CreditRole.SOFTWARE)
        self.assertEqual(c.credit_uri,
                         "https://credit.niso.org/contributor-roles/software/")

    def test_unique_constraint(self):
        target = Person.objects.create(family="Gadd", given="Stephen")
        self._contrib(target, role=CreditRole.SOFTWARE)
        with self.assertRaises(IntegrityError):
            with transaction.atomic():
                self._contrib(target, role=CreditRole.SOFTWARE)

    def test_same_person_different_role_allowed(self):
        target = Person.objects.create(family="Gadd", given="Stephen")
        self._contrib(target, role=CreditRole.SOFTWARE)
        self._contrib(target, role=CreditRole.DATA_CURATION)  # no error
        self.assertEqual(
            Contribution.objects.filter(object_id=str(target.pk)).count(), 2)
