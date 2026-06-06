import json
from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
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


class CitationWiringTests(TestCase):
    """Phase 2b: CSL + DataCite read structured Contributions, with free-text fallback."""

    def setUp(self):
        from datasets.models import Dataset
        # Dataset post_save fires a DataCite DOI call; mock it out so tests
        # make no external requests.
        doi_patch = patch("datasets.signals.doi")
        doi_patch.start()
        self.addCleanup(doi_patch.stop)
        self.User = get_user_model()
        self.user = self.User.objects.create(username="t", email="t@example.com")
        self.ds = Dataset.objects.create(
            owner=self.user, label="testds", title="T", description="D",
            creator="Ruth Mostern; Karl Grossner (0000-0002-4066-8297)",
            contributors="[University of Pittsburgh]",
        )

    def _csl(self):
        from utils.csl_citation_formatter import csl_citation
        return json.loads(csl_citation(self.ds))

    def test_freetext_fallback_when_no_contributions(self):
        # No structured contributions yet -> author list comes from free text.
        authors = self._csl()["author"]
        families = {a.get("family") for a in authors}
        self.assertIn("Mostern", families)
        self.assertIn("Grossner", families)
        self.assertIn({"literal": "University of Pittsburgh"},
                      [a for a in authors if "literal" in a])

    def test_structured_contributions_preferred(self):
        p = Person.objects.create(family="Mostern", given="Ruth",
                                  orcid="0000-0002-0000-0000")
        Contribution.objects.create(person=p, role=CreditRole.CONCEPTUALIZATION,
                                    target=self.ds, order=0)
        authors = self._csl()["author"]
        # Structured path wins: exactly the one contributor, with ORCID.
        self.assertEqual(authors, [{"family": "Mostern", "given": "Ruth",
                                    "ORCID": "https://orcid.org/0000-0002-0000-0000"}])

    def test_datacite_contributors(self):
        from utils.doi import get_contributors
        p = Person.objects.create(family="Grossner", given="Karl",
                                  orcid="0000-0002-4066-8297",
                                  affiliation="University of Pittsburgh")
        Contribution.objects.create(person=p, role=CreditRole.DATA_CURATION,
                                    target=self.ds)
        contribs = get_contributors(self.ds)
        self.assertEqual(len(contribs), 1)
        c = contribs[0]
        self.assertEqual(c["contributorType"], "DataCurator")
        self.assertEqual(c["familyName"], "Grossner")
        self.assertEqual(c["nameIdentifiers"][0]["nameIdentifier"],
                         "https://orcid.org/0000-0002-4066-8297")
        self.assertEqual(c["affiliation"], [{"name": "University of Pittsburgh"}])

    def test_importer_creates_contributions(self):
        call_command("import_freetext_contributors")
        from django.contrib.contenttypes.models import ContentType
        ct = ContentType.objects.get_for_model(self.ds.__class__)
        contribs = Contribution.objects.filter(content_type=ct, object_id=str(self.ds.pk))
        # 2 creators (Conceptualization) + 1 org contributor (Data curation)
        self.assertEqual(contribs.count(), 3)
        self.assertTrue(Person.objects.filter(orcid="0000-0002-4066-8297").exists())
        self.assertTrue(Person.objects.filter(literal="University of Pittsburgh").exists())
        # idempotent
        call_command("import_freetext_contributors")
        self.assertEqual(contribs.count(), 3)
