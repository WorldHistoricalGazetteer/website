"""Tests for GRACE.

Weighted towards the two things that would be expensive to get wrong: the
no-duplicated-facts rule on Person, and the decision-6 data-protection
machinery. A field that quietly holds a second copy of an address, or an
erasure that takes the engagement history with it, is not the kind of bug that
shows up in a screenshot.
"""
import datetime
from datetime import timedelta

from django.contrib.auth import get_user_model
from django.core.exceptions import ValidationError
from django.core.cache import cache
from django.test import TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from api.models import GazetteerRegistryEntry

from . import privacy
from .models import (
    ActionItem, Engagement, Interaction, Organisation, Person, Review, Source,
    SourceSuggestion, TrackedDataset,
)
from .vocabularies import (
    ActionItemStatus, DiscoverySource, EmailStatus, EngagementStage,
    IntakeStatus, PersonRole, ReviewType, SourceType, Stage,
)

User = get_user_model()


class PersonLinkTests(TestCase):
    """Decision 2: one table, an optional link, and never two copies of a fact."""

    def setUp(self):
        self.user = User.objects.create(
            username="ada-l-1", name="Ada Lovelace",
            email="ada@example.org", affiliation="Analytical Society",
            orcid="https://orcid.org/0000-0002-1825-0097",
        )

    def test_local_copies_cleared_when_account_linked(self):
        c = Person.objects.create(
            name="Ada Lovelace", email="stale@example.org",
            affiliation_text="Somewhere Old", orcid="0000-0000-0000-0000",
            user=self.user,
        )
        c.refresh_from_db()
        # The account owns these now; the local columns must be empty so they
        # cannot drift out of step.
        self.assertIsNone(c.email)
        self.assertEqual(c.affiliation_text, "")
        self.assertEqual(c.orcid, "")

    def test_resolved_accessors_read_through_the_link(self):
        c = Person.objects.create(name="Ada Lovelace", user=self.user)
        self.assertEqual(c.resolved_email, "ada@example.org")
        self.assertEqual(c.resolved_affiliation, "Analytical Society")
        self.assertEqual(c.resolved_orcid, self.user.orcid)

    def test_unlinked_person_keeps_their_own_details(self):
        c = Person.objects.create(name="Grace Hopper",
                                   email="grace@example.org")
        self.assertEqual(c.resolved_email, "grace@example.org")

    def test_organisation_beats_free_text_affiliation(self):
        org = Organisation.objects.create(name="Bodleian Library")
        c = Person.objects.create(name="Anon", organisation=org,
                                   affiliation_text="typed by hand")
        self.assertEqual(c.resolved_affiliation, "Bodleian Library")

    def test_newsletter_consent_follows_the_account_when_linked(self):
        self.user.news_permitted = True
        self.user.save()
        c = Person.objects.create(name="Ada", user=self.user,
                                   news_consent=False)
        # The account flag is the one the person can change themselves.
        self.assertTrue(c.resolved_news_consent)


class ContactEmailLookupTests(TestCase):
    """The address is encrypted, so equality has to go through the HMAC."""

    def test_by_email_finds_an_encrypted_address(self):
        c = Person.objects.create(name="Grace", email="grace@example.org")
        self.assertEqual(Person.objects.by_email("grace@example.org"), c)

    def test_lookup_is_case_and_space_insensitive(self):
        Person.objects.create(name="Grace", email="grace@example.org")
        self.assertIsNotNone(Person.objects.by_email("  GRACE@Example.ORG "))

    def test_direct_filter_on_the_encrypted_column_finds_nothing(self):
        """Documents the trap: this is why by_email exists."""
        Person.objects.create(name="Grace", email="grace@example.org")
        self.assertFalse(Person.objects.filter(email="grace@example.org").exists())

    def test_no_address_means_no_hash(self):
        c = Person.objects.create(name="Nameless")
        self.assertIsNone(c.email_hash)


class ErasureTests(TestCase):
    """Obligation 3: erasure is pseudonymisation, never a cascade delete."""

    def setUp(self):
        self.person = Person.objects.create(
            name="Someone Real", email="real@example.org",
            affiliation_text="A University", notes="private jottings",
        )
        self.engagement = Engagement.objects.create(person=self.person,
                                                    subject="Rights enquiry")
        Interaction.objects.create(
            engagement=self.engagement, person=self.person,
            occurred_on=datetime.date.today(), summary="Asked about licensing",
        )

    def test_identity_is_removed(self):
        self.person.pseudonymise()
        self.person.refresh_from_db()
        self.assertTrue(self.person.is_erased)
        self.assertIsNone(self.person.email)
        self.assertIsNone(self.person.email_hash)
        self.assertEqual(self.person.affiliation_text, "")
        self.assertEqual(self.person.notes, "")
        self.assertNotIn("Someone Real", self.person.name)

    def test_engagement_history_survives(self):
        """The point of the whole design. The record of what happened stays."""
        self.person.pseudonymise()
        self.assertEqual(Engagement.objects.count(), 1)
        interaction = Interaction.objects.get()
        self.assertEqual(interaction.summary, "Asked about licensing")

    def test_erased_people_drop_out_of_live_queries(self):
        self.person.pseudonymise()
        self.assertEqual(Person.objects.live().count(), 0)
        self.assertEqual(Person.objects.count(), 1)

    def test_erasure_does_not_touch_the_linked_account(self):
        user = User.objects.create(username="u1", name="U", email="u@x.org")
        self.person.user = user
        self.person.save()
        self.person.pseudonymise()
        self.assertTrue(User.objects.filter(pk=user.pk).exists())


class RetentionAndNoticeTests(TestCase):
    """Obligations 1 and 4."""

    def test_three_years_quiet_triggers_a_review(self):
        old = Person.objects.create(name="Long Silent")
        Person.objects.filter(pk=old.pk).update(
            created_at=timezone.now() - timedelta(days=365 * 4))
        self.assertIn(old, Person.objects.needing_retention_review())

    def test_a_recent_interaction_resets_the_clock(self):
        c = Person.objects.create(name="Recently Spoken To")
        Person.objects.filter(pk=c.pk).update(
            created_at=timezone.now() - timedelta(days=365 * 4))
        engagement = Engagement.objects.create(person=c)
        Interaction.objects.create(engagement=engagement, person=c,
                                   occurred_on=datetime.date.today(),
                                   summary="Spoke last week")
        self.assertNotIn(c, Person.objects.needing_retention_review())

    def test_retention_period_is_three_years(self):
        self.assertEqual(privacy.RETENTION_REVIEW_YEARS, 3)

    def test_privacy_notice_becomes_overdue_after_a_month(self):
        c = Person.objects.create(name="Not Yet Told")
        self.assertNotIn(c, Person.objects.owed_privacy_notice())
        Person.objects.filter(pk=c.pk).update(
            created_at=timezone.now() - timedelta(days=45))
        self.assertIn(c, Person.objects.owed_privacy_notice())

    def test_sending_the_notice_clears_the_backlog(self):
        c = Person.objects.create(name="Told")
        Person.objects.filter(pk=c.pk).update(
            created_at=timezone.now() - timedelta(days=45),
            privacy_notice_sent_at=timezone.now())
        self.assertNotIn(c, Person.objects.owed_privacy_notice())

    def test_consent_needs_evidence(self):
        c = Person(name="X", news_consent=True)
        with self.assertRaises(ValidationError):
            c.clean()


class TrackedGazetteerTests(TestCase):
    """Decision 1: the Register link, and what 'prospect' means."""

    def test_no_register_link_means_prospect(self):
        g = TrackedDataset.objects.create(title="Something we heard about")
        self.assertTrue(g.is_prospect)
        self.assertIn(g, TrackedDataset.objects.prospects())

    def test_linked_dataset_is_held_and_reads_through(self):
        entry = GazetteerRegistryEntry.objects.create(
            id="test:1", name="Test Authority", namespace="test",
            entry_class="authority", record_count=4242, status="published",
            rights_holder="Some Archive",
        )
        g = TrackedDataset.objects.create(title="Local name", registry=entry)
        self.assertFalse(g.is_prospect)
        self.assertIn(g, TrackedDataset.objects.held())
        # Machine facts are read, never stored.
        self.assertEqual(g.registry_record_count, 4242)
        self.assertEqual(g.registry_rights_holder, "Some Archive")
        self.assertTrue(g.is_published)

    def test_prospect_read_through_is_safe(self):
        g = TrackedDataset.objects.create(title="Prospect")
        self.assertIsNone(g.registry_record_count)
        self.assertIsNone(g.registry_licence)
        self.assertFalse(g.is_published)

    def test_no_machine_fact_is_stored_locally(self):
        """A regression guard: these belong to the Register (review §2)."""
        local = {f.name for f in TrackedDataset._meta.get_fields()}
        for forbidden in ("licence", "license", "record_count",
                          "rights_holder", "citation_text", "h3_coverage"):
            self.assertNotIn(forbidden, local)


class EngagementRuleTests(TestCase):
    """The one-owner rule and the staleness alarm (review §6)."""

    def setUp(self):
        self.open_stage = EngagementStage.objects.create(
            label="Awaiting reply", is_open=True)
        self.closed_stage = EngagementStage.objects.create(
            label="Concluded", is_open=False)
        self.person = Person.objects.create(name="A Correspondent")
        self.owner = User.objects.create(username="owner1", name="Owner",
                                         email="owner@example.org")

    def test_open_conversation_requires_a_follow_up_date(self):
        e = Engagement(person=self.person, stage=self.open_stage)
        with self.assertRaises(ValidationError):
            e.clean()

    def test_closed_conversation_does_not(self):
        e = Engagement(person=self.person, stage=self.closed_stage)
        e.clean()  # must not raise

    def test_responsible_person_is_inherited_from_the_dataset(self):
        g = TrackedDataset.objects.create(title="G", owner=self.owner)
        e = Engagement.objects.create(person=self.person,
                                      dataset=g)
        self.assertEqual(e.effective_responsible, self.owner)

    def test_an_explicit_responsible_person_overrides(self):
        other = User.objects.create(username="other1", name="Other",
                                    email="other@example.org")
        g = TrackedDataset.objects.create(title="G", owner=self.owner)
        e = Engagement.objects.create(person=self.person,
                                      dataset=g, responsible=other)
        self.assertEqual(e.effective_responsible, other)

    def test_a_stalled_conversation_is_detected(self):
        e = Engagement.objects.create(
            person=self.person, stage=self.open_stage,
            next_follow_up=datetime.date.today() - timedelta(days=1))
        self.assertTrue(e.is_stale)

    def test_a_conversation_in_hand_is_not_stale(self):
        e = Engagement.objects.create(
            person=self.person, stage=self.open_stage,
            next_follow_up=datetime.date.today() + timedelta(days=7))
        self.assertFalse(e.is_stale)

    def test_interaction_defaults_to_the_engagements_person(self):
        e = Engagement.objects.create(person=self.person)
        i = Interaction.objects.create(engagement=e, summary="Note")
        self.assertEqual(i.person, self.person)

    def test_overdue_action_item(self):
        todo = ActionItemStatus.objects.create(label="To do", is_open=True)
        done = ActionItemStatus.objects.create(label="Done", is_open=False)
        e = Engagement.objects.create(person=self.person)
        yesterday = datetime.date.today() - timedelta(days=1)
        self.assertTrue(ActionItem.objects.create(
            engagement=e, description="Chase", status=todo,
            due_date=yesterday).is_overdue)
        self.assertFalse(ActionItem.objects.create(
            engagement=e, description="Chased", status=done,
            due_date=yesterday).is_overdue)


class VocabularyTests(TestCase):
    def test_slug_is_derived_once_and_then_left_alone(self):
        s = Stage.objects.create(label="Permission being sought")
        self.assertEqual(s.slug, "permission-being-sought")
        s.label = "Renamed by Palak"
        s.save()
        s.refresh_from_db()
        self.assertEqual(s.slug, "permission-being-sought")

    def test_seed_command_is_idempotent(self):
        from django.core.management import call_command
        from io import StringIO
        call_command("seed_grace_vocabularies", stdout=StringIO())
        first = Stage.objects.count()
        call_command("seed_grace_vocabularies", stdout=StringIO())
        self.assertEqual(Stage.objects.count(), first)

    def test_seed_creates_exactly_one_untriaged_status(self):
        from django.core.management import call_command
        from io import StringIO
        call_command("seed_grace_vocabularies", stdout=StringIO())
        self.assertEqual(
            IntakeStatus.objects.filter(is_untriaged=True).count(), 1)

    def test_seed_creates_the_web_form_discovery_term(self):
        from django.core.management import call_command
        from io import StringIO
        call_command("seed_grace_vocabularies", stdout=StringIO())
        self.assertTrue(
            DiscoverySource.objects.filter(slug="web-form").exists())

    def test_seed_has_no_derived_stage_values(self):
        """published / indexed belong to the Register, not the stage list."""
        from django.core.management import call_command
        from io import StringIO
        call_command("seed_grace_vocabularies", stdout=StringIO())
        labels = {s.label.lower() for s in Stage.objects.all()}
        self.assertNotIn("published", labels)
        self.assertNotIn("indexed", labels)
        self.assertNotIn("not indexed", labels)


@override_settings(CACHES={"default": {
    "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    "LOCATION": "grace-tests",
}})
class SuggestFormTests(TestCase):
    """The public intake door (decision 5).

    Pinned to a local-memory cache. The view's per-IP rate limit lives in the
    cache, and the real one is a *shared* Redis: every test client request comes
    from 127.0.0.1, so on the shared backend the counter accumulates across
    tests and even across separate runs, and once it passes five the form
    silently starts throttling instead of saving. Isolating the cache is the
    fix; clearing it in setUp keeps each test independent of the last.
    """

    def setUp(self):
        cache.clear()
        self.untriaged = IntakeStatus.objects.create(
            label="Untriaged", is_untriaged=True)

    def test_contribute_redirects_to_the_grace_form(self):
        """It used to 302 off-site to Baserow. It must not any more."""
        response = self.client.get(reverse("submit-dataset"))
        self.assertRedirects(response, reverse("grace:suggest"))

    def test_the_form_renders_for_anonymous_visitors(self):
        response = self.client.get(reverse("grace:suggest"))
        self.assertEqual(response.status_code, 200)

    def test_a_valid_submission_lands_untriaged(self):
        response = self.client.post(reverse("grace:suggest"), {
            "title": "Gazetteer of Somewhere",
            "author_compiler": "A Compiler",
            "publication_years": "1890",
            "region_covered": "Somewhere",
            "source_url": "https://example.org/x",
            "notes": "",
            "submitter_name": "A Person",
            "submitter_email": "person@example.org",
            "website": "",
        })
        self.assertRedirects(response, reverse("grace:suggest_thanks"))
        suggestion = SourceSuggestion.objects.get()
        self.assertTrue(suggestion.is_untriaged)
        self.assertEqual(suggestion.status, self.untriaged)

    def test_the_honeypot_rejects_a_bot(self):
        self.client.post(reverse("grace:suggest"), {
            "title": "Spam", "website": "http://spam.example",
        })
        self.assertEqual(SourceSuggestion.objects.count(), 0)

    def test_a_signed_in_user_gets_no_honeypot_and_is_linked(self):
        user = User.objects.create_user(
            username="submitter", email="s@example.org", password="pw",
            given_name="Sub", surname="Mitter", name="Sub Mitter")
        self.client.force_login(user)
        self.client.post(reverse("grace:suggest"), {"title": "A source"})
        suggestion = SourceSuggestion.objects.get()
        self.assertEqual(suggestion.submitter_user, user)

    def test_submitter_address_is_hashed_for_lookup(self):
        self.client.post(reverse("grace:suggest"), {
            "title": "X", "submitter_email": "finder@example.org",
            "website": "",
        })
        suggestion = SourceSuggestion.objects.get()
        self.assertIsNotNone(suggestion.submitter_email_hash)


class SourceProvenanceTests(TestCase):
    """Documents-versus-derived-from (review §4)."""

    def test_the_two_relations_are_distinct(self):
        printed = SourceType.objects.create(label="Printed gazetteer")
        source = Source.objects.create(title="A print gazetteer",
                                       source_type=printed)
        described = TrackedDataset.objects.create(title="Described")
        extracted = TrackedDataset.objects.create(title="Extracted from it")
        source.documents.add(described)
        source.derived_datasets.add(extracted)

        self.assertEqual(list(source.documents.all()), [described])
        self.assertEqual(list(source.derived_datasets.all()), [extracted])
        self.assertEqual(list(extracted.derived_from_sources.all()), [source])
        self.assertEqual(list(described.documented_by.all()), [source])


class ImporterHygieneTests(TestCase):
    """The importer must not undo the encryption it is feeding."""

    def test_a_missing_name_never_becomes_an_email_address(self):
        from grace.management.commands.import_baserow_export import _display_name
        # Person.name is an unencrypted, indexed column. Putting an address
        # there would defeat the point of encrypting Person.email.
        self.assertEqual(_display_name("", "abolen2@unl.edu"), "abolen2")
        self.assertNotIn("@", _display_name("", "abolen2@unl.edu"))

    def test_a_real_name_is_kept(self):
        from grace.management.commands.import_baserow_export import _display_name
        self.assertEqual(_display_name("Werner Stangl", "w@example.org"),
                         "Werner Stangl")

    def test_an_address_in_the_name_column_is_replaced(self):
        from grace.management.commands.import_baserow_export import _display_name
        self.assertEqual(_display_name("a@b.org", "a@b.org"), "a")

    def test_nothing_at_all_still_yields_something_readable(self):
        from grace.management.commands.import_baserow_export import _display_name
        self.assertEqual(_display_name("", None), "(no name recorded)")

    def test_non_breaking_spaces_are_normalised(self):
        from grace.management.commands.import_baserow_export import _clean
        self.assertEqual(_clean("Academy of Korean Studies"),
                         "Academy of Korean Studies")


@override_settings(CACHES={"default": {
    "BACKEND": "django.core.cache.backends.locmem.LocMemCache",
    "LOCATION": "grace-ratelimit-tests",
}})
class RateLimitTests(TestCase):
    """The anonymous rate limit, exercised deliberately rather than by accident."""

    def setUp(self):
        cache.clear()
        IntakeStatus.objects.create(label="Untriaged", is_untriaged=True)

    def _post(self, n):
        for i in range(n):
            self.client.post(reverse("grace:suggest"),
                             {"title": f"Suggestion {i}", "website": ""})

    def test_anonymous_submissions_are_capped(self):
        from grace.views import RATE_LIMIT_MAX
        self._post(RATE_LIMIT_MAX + 3)
        self.assertEqual(SourceSuggestion.objects.count(), RATE_LIMIT_MAX)

    def test_signed_in_users_are_not_rate_limited(self):
        from grace.views import RATE_LIMIT_MAX
        user = User.objects.create_user(
            username="prolific", email="p@example.org", password="pw",
            given_name="Pro", surname="Lific", name="Pro Lific")
        self.client.force_login(user)
        self._post(RATE_LIMIT_MAX + 3)
        self.assertEqual(SourceSuggestion.objects.count(), RATE_LIMIT_MAX + 3)


class OrganisationMergeTests(TestCase):
    """Whitespace variants are one institution, not two."""

    def _normalise(self):
        from grace.management.commands.import_baserow_export import Command
        cmd = Command()
        cmd.stdout = __import__("io").StringIO()
        return cmd._normalise_organisations()

    def test_variant_is_merged_into_the_clean_row(self):
        clean = Organisation.objects.create(name="Academy of Korean Studies")
        variant = Organisation.objects.create(
            name="Academy of Korean Studies")
        person = Person.objects.create(name="Someone", organisation=variant)

        self._normalise()

        self.assertEqual(Organisation.objects.count(), 1)
        person.refresh_from_db()
        self.assertEqual(person.organisation, clean)

    def test_merge_survives_whichever_row_was_created_first(self):
        """The variant existing first must not rename onto the clean name."""
        variant = Organisation.objects.create(name="Trinity College")
        Organisation.objects.create(name="Trinity College")
        Person.objects.create(name="Someone", organisation=variant)

        self._normalise()  # must not raise IntegrityError

        self.assertEqual(Organisation.objects.count(), 1)
        self.assertEqual(Organisation.objects.get().name, "Trinity College")

    def test_a_lone_variant_is_just_cleaned(self):
        Organisation.objects.create(name="Uppsala University")
        self._normalise()
        self.assertEqual(Organisation.objects.get().name, "Uppsala University")


class GraceAdminSiteTests(TestCase):
    """The dedicated admin at /grace/admin/ (not Django's everything-view)."""

    def setUp(self):
        self.staff = User.objects.create_user(
            username="editor", email="editor@example.org", password="pw",
            given_name="Ed", surname="Itor", name="Ed Itor")
        self.staff.is_staff = True
        self.staff.is_superuser = True
        self.staff.save()

    def test_it_requires_a_login(self):
        response = self.client.get("/grace/admin/")
        self.assertNotEqual(response.status_code, 200)

    def test_the_index_lists_the_registers(self):
        self.client.force_login(self.staff)
        response = self.client.get("/grace/admin/")
        self.assertEqual(response.status_code, 200)
        body = response.content.decode()
        for register in ("Pipeline", "Catalogue", "Engagement", "Content"):
            self.assertIn(register, body)

    def test_it_shows_only_grace_models(self):
        """The whole point: no Auth, no Sites, no Celery Results."""
        self.client.force_login(self.staff)
        body = self.client.get("/grace/admin/").content.decode()
        self.assertNotIn("Celery", body)
        self.assertNotIn("Authentication and Authorization", body)

    def test_support_models_are_reachable_but_not_advertised(self):
        """Registered for autocomplete, deliberately absent from the index."""
        from grace.admin_site import grace_admin_site
        from api.models import GazetteerRegistryEntry
        self.assertIn(GazetteerRegistryEntry, grace_admin_site._registry)
        self.client.force_login(self.staff)
        body = self.client.get("/grace/admin/").content.decode()
        self.assertNotIn("Gazetteer registry entries", body)

    def test_every_grace_model_is_mirrored(self):
        from django.contrib import admin as django_admin
        from grace.admin_site import grace_admin_site
        default = {m for m in django_admin.site._registry
                   if m._meta.app_label == "grace"}
        mirrored = {m for m in grace_admin_site._registry
                    if m._meta.app_label == "grace"}
        self.assertEqual(default, mirrored)

    def test_a_changelist_still_works(self):
        self.client.force_login(self.staff)
        response = self.client.get("/grace/admin/grace/trackeddataset/")
        self.assertEqual(response.status_code, 200)

    def test_the_attention_panel_flags_an_untriaged_suggestion(self):
        untriaged = IntakeStatus.objects.create(label="Untriaged",
                                                is_untriaged=True)
        SourceSuggestion.objects.create(title="Something", status=untriaged)
        self.client.force_login(self.staff)
        body = self.client.get("/grace/admin/").content.decode()
        self.assertIn("untriaged suggestion", body)

    def test_all_clear_when_there_is_nothing_to_do(self):
        self.client.force_login(self.staff)
        body = self.client.get("/grace/admin/").content.decode()
        self.assertIn("Nothing needs attention", body)

    def test_vocabulary_names_stay_unambiguous(self):
        """Stripping the register prefix would leave bare 'Statuses'/'Types'."""
        self.client.force_login(self.staff)
        body = self.client.get("/grace/admin/").content.decode()
        self.assertIn("Content types", body)
        self.assertIn("Content statuses", body)


class GraceAdminThemeTests(TestCase):
    """The theme must reach the changelists, not just the landing page."""

    def setUp(self):
        self.staff = User.objects.create_user(
            username="themer", email="themer@example.org", password="pw",
            given_name="The", surname="Mer", name="The Mer")
        self.staff.is_staff = True
        self.staff.is_superuser = True
        self.staff.save()
        self.client.force_login(self.staff)

    def test_the_stylesheet_is_on_a_changelist_too(self):
        """The point of the base_site override — /grace/admin/grace/person/
        was still stock Django until it loaded."""
        body = self.client.get("/grace/admin/grace/person/").content.decode()
        self.assertIn("grace/admin.css", body)

    def test_the_acronym_is_spelled_out_in_the_header(self):
        """Each initial is emboldened, so the words arrive split by markup.

        GRACE keeps its expansion even though the register it names is now
        People: the acronym is the product's name, and "contact engagement" is
        still what the Engagement register records.
        """
        body = self.client.get("/grace/admin/").content.decode()
        for initial, rest in (("G", "azetteer"), ("R", "egister"),
                              ("A", "nd"), ("C", "ontact"), ("E", "ngagement")):
            self.assertIn(f"<b>{initial}</b>{rest}", body)

    def test_the_default_admin_is_left_alone(self):
        """base_site.html is shared, so /admin/ must be untouched."""
        body = self.client.get("/admin/").content.decode()
        self.assertNotIn("grace/admin.css", body)
        self.assertIn("Django administration", body)

    def test_the_browser_title_has_no_leading_pipe(self):
        body = self.client.get("/grace/admin/").content.decode()
        self.assertNotIn("<title>| ", body)

    def test_empty_values_render_consistently(self):
        """Django's default '-' clashed with the em dashes our columns return."""
        from grace.admin_site import grace_admin_site
        self.assertEqual(grace_admin_site.empty_value_display, "—")


# ==========================================================================
# Palak's review of the staging build — points 1 to 7
# ==========================================================================

class InternalRoleTests(TestCase):
    """Point 2: the People register is everyone, ourselves included.

    The reason that is not free: Article 14 covers data obtained from someone
    *other than* the person, so it does not apply to our own staff. Without an
    exemption, adding colleagues would bury the notices genuinely owed under a
    pile of false ones.
    """

    def setUp(self):
        self.outside = PersonRole.objects.create(
            label="Rights holder", is_internal=False)
        self.inside = PersonRole.objects.create(
            label="WHG staff", is_internal=True)
        self.long_ago = timezone.now() - timedelta(
            days=privacy.PRIVACY_NOTICE_DUE_DAYS + 10)

    def _person(self, name, role):
        person = Person.objects.create(name=name, role=role)
        Person.objects.filter(pk=person.pk).update(created_at=self.long_ago)
        return Person.objects.get(pk=person.pk)

    def test_outsiders_are_owed_a_notice(self):
        person = self._person("An Archivist", self.outside)
        self.assertIn(person, Person.objects.owed_privacy_notice())
        self.assertTrue(person.privacy_notice_overdue)

    def test_our_own_people_are_not(self):
        colleague = self._person("A Colleague", self.inside)
        self.assertNotIn(colleague, Person.objects.owed_privacy_notice())
        self.assertFalse(colleague.privacy_notice_overdue)
        self.assertTrue(colleague.is_internal)

    def test_the_exemption_reads_the_flag_not_the_label(self):
        """Renaming the role must not change who is exempt."""
        self.inside.label = "Core team"
        self.inside.save()
        colleague = self._person("A Colleague", self.inside)
        self.assertNotIn(colleague, Person.objects.owed_privacy_notice())


class EmailDeliverabilityTests(TestCase):
    """Point 2, second half: a bounce must not lose the person.

    GRACE is deliberately not the mailing list — the sending platform owns
    subscription state — so all that is held here is whether the address works.
    """

    def setUp(self):
        self.ok = EmailStatus.objects.create(
            label="Deliverable", is_undeliverable=False)
        self.bounced = EmailStatus.objects.create(
            label="Bounced", is_undeliverable=True)

    def test_a_bounce_is_visible_without_deleting_anything(self):
        person = Person.objects.create(
            name="Gone Away", email="gone@example.org",
            email_status=self.bounced)
        self.assertTrue(person.email_is_undeliverable)
        self.assertIn(person, Person.objects.live())

    def test_deliverable_is_not_undeliverable(self):
        person = Person.objects.create(name="Still Here", email_status=self.ok)
        self.assertFalse(person.email_is_undeliverable)

    def test_erasure_clears_the_deliverability_trail(self):
        """It is derived from an address we are erasing, so it must go too."""
        person = Person.objects.create(
            name="Erase Me", email="e@example.org", email_status=self.bounced)
        person.pseudonymise()
        person.refresh_from_db()
        self.assertIsNone(person.email_status_id)
        self.assertIsNone(person.email_status)


class ExpectedVersusRegisterTests(TestCase):
    """Point 4: a prospect has no Register row to read figures from.

    The rule from review §2 stands — machine facts are never copied into GRACE
    — so these fields hold a *different* fact: what someone told us during a
    negotiation. The Register wins the moment there is one.
    """

    def setUp(self):
        self.entry = GazetteerRegistryEntry.objects.create(
            id="tst", name="Test Gazetteer", namespace="tst",
            entry_class="authority", record_count=4200, status="published",
            rights_holder="Test Trust",
        )

    def test_a_prospect_reports_what_we_were_told(self):
        dataset = TrackedDataset.objects.create(
            title="Something promised", expected_record_count=5000,
            expected_rights_holder="A County Society")
        self.assertTrue(dataset.is_prospect)
        self.assertTrue(dataset.figures_are_expectations)
        self.assertEqual(dataset.effective_record_count, 5000)
        self.assertEqual(dataset.effective_rights_holder, "A County Society")

    def test_the_register_wins_once_linked(self):
        dataset = TrackedDataset.objects.create(
            title="Something delivered", registry=self.entry,
            expected_record_count=5000,
            expected_rights_holder="A County Society")
        self.assertFalse(dataset.figures_are_expectations)
        self.assertEqual(dataset.effective_record_count, 4200)
        self.assertEqual(dataset.effective_rights_holder, "Test Trust")

    def test_the_expectation_survives_accession(self):
        """‘They offered X and delivered Y’ is worth keeping."""
        dataset = TrackedDataset.objects.create(
            title="Both", registry=self.entry, expected_record_count=5000)
        dataset.refresh_from_db()
        self.assertEqual(dataset.expected_record_count, 5000)
        self.assertEqual(dataset.registry_record_count, 4200)


class WhgLinkTests(TestCase):
    """The board's ‘WHG’ column, and the reconciliation read-through."""

    def test_a_whg_contribution_links_to_its_dataset_page(self):
        entry = GazetteerRegistryEntry.objects.create(
            id="whg:1234", name="A contribution", namespace="whg",
            entry_class="dataset", record_count=10, status="published")
        dataset = TrackedDataset.objects.create(title="A contribution",
                                                registry=entry)
        self.assertEqual(dataset.whg_dataset_id, 1234)
        self.assertEqual(dataset.whg_url, "/datasets/1234/status")

    def test_an_authority_has_no_page_of_its_own_here(self):
        entry = GazetteerRegistryEntry.objects.create(
            id="gnx", name="An authority", namespace="gnx",
            entry_class="authority", record_count=10, status="published")
        dataset = TrackedDataset.objects.create(title="An authority",
                                                registry=entry)
        self.assertIsNone(dataset.whg_dataset_id)
        self.assertIsNone(dataset.whg_url)
        self.assertIsNone(dataset.reconciliation_status)

    def test_a_prospect_has_neither(self):
        dataset = TrackedDataset.objects.create(title="Only wanted")
        self.assertIsNone(dataset.whg_url)
        self.assertIsNone(dataset.reconciliation_status)


class ReviewTests(TestCase):
    """Point 5. Both failure modes here are absences of an event."""

    def setUp(self):
        self.dataset = TrackedDataset.objects.create(title="Under review")
        self.internal = ReviewType.objects.create(label="Internal editorial")
        self.reviewer = Person.objects.create(name="A Reviewer")

    def test_sent_and_not_back_is_outstanding(self):
        review = Review.objects.create(
            dataset=self.dataset, reviewer=self.reviewer,
            review_type=self.internal, sent_on=datetime.date(2026, 1, 1))
        self.assertTrue(review.is_outstanding)
        self.assertFalse(review.awaiting_share)

    def test_back_and_author_not_told_is_the_worse_one(self):
        """From the contributor's side, nothing has happened at all."""
        review = Review.objects.create(
            dataset=self.dataset, review_type=self.internal,
            sent_on=datetime.date(2026, 1, 1),
            returned_on=datetime.date(2026, 2, 1))
        self.assertFalse(review.is_outstanding)
        self.assertTrue(review.awaiting_share)

    def test_shared_is_finished(self):
        review = Review.objects.create(
            dataset=self.dataset, review_type=self.internal,
            sent_on=datetime.date(2026, 1, 1),
            returned_on=datetime.date(2026, 2, 1),
            shared_with_author_on=datetime.date(2026, 2, 3))
        self.assertFalse(review.is_outstanding)
        self.assertFalse(review.awaiting_share)

    def test_an_outside_reviewer_needs_no_whg_account(self):
        """The People register covers both sides, so one field does."""
        outsider = Person.objects.create(name="External Reader")
        review = Review.objects.create(dataset=self.dataset, reviewer=outsider)
        self.assertIsNone(review.reviewer.user_id)
        self.assertIn(review, self.dataset.reviews.all())


class ConnectionsTests(TestCase):
    """Point 1: open one record and see everything tied to it.

    Django shows the relations a record points at and nothing pointing back,
    which is exactly the half that matters here.
    """

    def setUp(self):
        self.staff = User.objects.create_user(
            username="conn", email="conn@example.org", password="pw",
            given_name="Con", surname="Nection", name="Con Nection")
        self.staff.is_staff = True
        self.staff.is_superuser = True
        self.staff.save()
        self.client.force_login(self.staff)

        self.person = Person.objects.create(name="Well Connected")
        self.dataset = TrackedDataset.objects.create(title="Their dataset")
        self.dataset.people.add(self.person)
        stage = EngagementStage.objects.create(label="In discussion",
                                               is_open=True)
        self.engagement = Engagement.objects.create(
            person=self.person, dataset=self.dataset, stage=stage,
            subject="About their dataset",
            next_follow_up=datetime.date.today() + timedelta(days=7))
        self.source = Source.objects.create(title="A printed gazetteer")
        self.source.people.add(self.person)
        self.source.derived_datasets.add(self.dataset)

    def test_a_person_shows_their_engagements_and_datasets(self):
        body = self.client.get(
            f"/grace/admin/grace/person/{self.person.pk}/change/"
        ).content.decode()
        self.assertIn("Connected records", body)
        self.assertIn("About their dataset", body)
        self.assertIn("Their dataset", body)
        self.assertIn("A printed gazetteer", body)

    def test_a_dataset_shows_its_people_and_sources_back(self):
        body = self.client.get(
            f"/grace/admin/grace/trackeddataset/{self.dataset.pk}/change/"
        ).content.decode()
        self.assertIn("Well Connected", body)
        self.assertIn("A printed gazetteer", body)

    def test_the_panel_does_not_break_the_add_form(self):
        """There is no object yet, so it must say so rather than explode."""
        body = self.client.get(
            "/grace/admin/grace/person/add/").content.decode()
        self.assertEqual(
            self.client.get("/grace/admin/grace/person/add/").status_code, 200)
        self.assertIn("Save this record first", body)


class BoardTests(TestCase):
    """Point 3: a board anyone can scan, on the landing page itself."""

    def setUp(self):
        self.staff = User.objects.create_user(
            username="boarder", email="boarder@example.org", password="pw",
            given_name="Bo", surname="Arder", name="Bo Arder")
        self.staff.is_staff = True
        self.staff.is_superuser = True
        self.staff.save()
        self.client.force_login(self.staff)
        self.stage = Stage.objects.create(label="Making contact", is_open=True)

    def test_active_datasets_appear_on_the_landing_page(self):
        TrackedDataset.objects.create(title="On the board", stage=self.stage)
        body = self.client.get("/grace/admin/").content.decode()
        self.assertIn("Datasets on their way in", body)
        self.assertIn("On the board", body)

    def test_a_shelved_dataset_does_not(self):
        """is_active is how you put something down without deleting it."""
        TrackedDataset.objects.create(title="Shelved", stage=self.stage,
                                      is_active=False)
        body = self.client.get("/grace/admin/").content.decode()
        self.assertNotIn("Shelved", body)

    def test_prospects_and_held_are_told_apart(self):
        TrackedDataset.objects.create(title="Only wanted", stage=self.stage)
        body = self.client.get("/grace/admin/").content.decode()
        self.assertIn("prospect", body)

    def test_an_unshared_review_is_flagged_for_attention(self):
        dataset = TrackedDataset.objects.create(title="Reviewed",
                                                stage=self.stage)
        Review.objects.create(dataset=dataset,
                              sent_on=datetime.date(2026, 1, 1),
                              returned_on=datetime.date(2026, 2, 1))
        body = self.client.get("/grace/admin/").content.decode()
        self.assertIn("review not passed on", body)
        self.assertIn("author not told", body)


class ResponsibleFilterTests(TestCase):
    """Point 3 asked to filter by person; Django's default lists usernames.

    On this project those are ORCID-derived strings, which is not something
    anyone can filter by at a glance.
    """

    def setUp(self):
        self.staff = User.objects.create_user(
            username="filterer", email="filterer@example.org", password="pw",
            given_name="Fil", surname="Terer", name="Fil Terer")
        self.staff.is_staff = True
        self.staff.is_superuser = True
        self.staff.save()
        self.client.force_login(self.staff)
        self.owner = User.objects.create_user(
            username="mostern-0000-0001-8219-7174",
            email="owner@example.org", password="pw",
            given_name="Ruth", surname="Mostern", name="Ruth Mostern")

    def test_the_filter_shows_a_name_not_a_username(self):
        TrackedDataset.objects.create(title="Owned", owner=self.owner)
        body = self.client.get(
            "/grace/admin/grace/trackeddataset/").content.decode()
        self.assertIn(
            f'<a href="?responsible={self.owner.pk}">Ruth Mostern</a>', body)

    def test_the_owner_widget_shows_a_name_too(self):
        """The inline-edit select renders its selected option server-side, so
        the filter alone was not enough."""
        TrackedDataset.objects.create(title="Owned", owner=self.owner)
        body = self.client.get(
            "/grace/admin/grace/trackeddataset/").content.decode()
        self.assertNotIn("mostern-0000-0001-8219-7174", body)

    def test_people_who_own_nothing_are_not_offered(self):
        """A filter listing every account on the site filters nothing."""
        body = self.client.get(
            "/grace/admin/grace/trackeddataset/").content.decode()
        self.assertNotIn("Ruth Mostern", body)

    def test_the_filter_actually_narrows(self):
        TrackedDataset.objects.create(title="Owned", owner=self.owner)
        TrackedDataset.objects.create(title="Unowned")
        body = self.client.get(
            f"/grace/admin/grace/trackeddataset/?responsible={self.owner.pk}"
        ).content.decode()
        self.assertIn("Owned", body)
        self.assertNotIn("Unowned", body)
