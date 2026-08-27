"""Tests for GRACE.

Weighted towards the two things that would be expensive to get wrong: the
no-duplicated-facts rule on Contact, and the decision-6 data-protection
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
    ActionItem, Contact, Engagement, Interaction, Organisation, Source,
    SourceSuggestion, TrackedGazetteer,
)
from .vocabularies import (
    ActionItemStatus, DiscoverySource, EngagementStage, IntakeStatus,
    SourceType, Stage,
)

User = get_user_model()


class ContactLinkTests(TestCase):
    """Decision 2: one table, an optional link, and never two copies of a fact."""

    def setUp(self):
        self.user = User.objects.create(
            username="ada-l-1", name="Ada Lovelace",
            email="ada@example.org", affiliation="Analytical Society",
            orcid="https://orcid.org/0000-0002-1825-0097",
        )

    def test_local_copies_cleared_when_account_linked(self):
        c = Contact.objects.create(
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
        c = Contact.objects.create(name="Ada Lovelace", user=self.user)
        self.assertEqual(c.resolved_email, "ada@example.org")
        self.assertEqual(c.resolved_affiliation, "Analytical Society")
        self.assertEqual(c.resolved_orcid, self.user.orcid)

    def test_unlinked_contact_keeps_its_own_details(self):
        c = Contact.objects.create(name="Grace Hopper",
                                   email="grace@example.org")
        self.assertEqual(c.resolved_email, "grace@example.org")

    def test_organisation_beats_free_text_affiliation(self):
        org = Organisation.objects.create(name="Bodleian Library")
        c = Contact.objects.create(name="Anon", organisation=org,
                                   affiliation_text="typed by hand")
        self.assertEqual(c.resolved_affiliation, "Bodleian Library")

    def test_newsletter_consent_follows_the_account_when_linked(self):
        self.user.news_permitted = True
        self.user.save()
        c = Contact.objects.create(name="Ada", user=self.user,
                                   news_consent=False)
        # The account flag is the one the person can change themselves.
        self.assertTrue(c.resolved_news_consent)


class ContactEmailLookupTests(TestCase):
    """The address is encrypted, so equality has to go through the HMAC."""

    def test_by_email_finds_an_encrypted_address(self):
        c = Contact.objects.create(name="Grace", email="grace@example.org")
        self.assertEqual(Contact.objects.by_email("grace@example.org"), c)

    def test_lookup_is_case_and_space_insensitive(self):
        Contact.objects.create(name="Grace", email="grace@example.org")
        self.assertIsNotNone(Contact.objects.by_email("  GRACE@Example.ORG "))

    def test_direct_filter_on_the_encrypted_column_finds_nothing(self):
        """Documents the trap: this is why by_email exists."""
        Contact.objects.create(name="Grace", email="grace@example.org")
        self.assertFalse(Contact.objects.filter(email="grace@example.org").exists())

    def test_no_address_means_no_hash(self):
        c = Contact.objects.create(name="Nameless")
        self.assertIsNone(c.email_hash)


class ErasureTests(TestCase):
    """Obligation 3: erasure is pseudonymisation, never a cascade delete."""

    def setUp(self):
        self.contact = Contact.objects.create(
            name="Someone Real", email="real@example.org",
            affiliation_text="A University", notes="private jottings",
        )
        self.engagement = Engagement.objects.create(contact=self.contact,
                                                    subject="Rights enquiry")
        Interaction.objects.create(
            engagement=self.engagement, contact=self.contact,
            occurred_on=datetime.date.today(), summary="Asked about licensing",
        )

    def test_identity_is_removed(self):
        self.contact.pseudonymise()
        self.contact.refresh_from_db()
        self.assertTrue(self.contact.is_erased)
        self.assertIsNone(self.contact.email)
        self.assertIsNone(self.contact.email_hash)
        self.assertEqual(self.contact.affiliation_text, "")
        self.assertEqual(self.contact.notes, "")
        self.assertNotIn("Someone Real", self.contact.name)

    def test_engagement_history_survives(self):
        """The point of the whole design. The record of what happened stays."""
        self.contact.pseudonymise()
        self.assertEqual(Engagement.objects.count(), 1)
        interaction = Interaction.objects.get()
        self.assertEqual(interaction.summary, "Asked about licensing")

    def test_erased_contacts_drop_out_of_live_queries(self):
        self.contact.pseudonymise()
        self.assertEqual(Contact.objects.live().count(), 0)
        self.assertEqual(Contact.objects.count(), 1)

    def test_erasure_does_not_touch_the_linked_account(self):
        user = User.objects.create(username="u1", name="U", email="u@x.org")
        self.contact.user = user
        self.contact.save()
        self.contact.pseudonymise()
        self.assertTrue(User.objects.filter(pk=user.pk).exists())


class RetentionAndNoticeTests(TestCase):
    """Obligations 1 and 4."""

    def test_three_years_quiet_triggers_a_review(self):
        old = Contact.objects.create(name="Long Silent")
        Contact.objects.filter(pk=old.pk).update(
            created_at=timezone.now() - timedelta(days=365 * 4))
        self.assertIn(old, Contact.objects.needing_retention_review())

    def test_a_recent_interaction_resets_the_clock(self):
        c = Contact.objects.create(name="Recently Spoken To")
        Contact.objects.filter(pk=c.pk).update(
            created_at=timezone.now() - timedelta(days=365 * 4))
        engagement = Engagement.objects.create(contact=c)
        Interaction.objects.create(engagement=engagement, contact=c,
                                   occurred_on=datetime.date.today(),
                                   summary="Spoke last week")
        self.assertNotIn(c, Contact.objects.needing_retention_review())

    def test_retention_period_is_three_years(self):
        self.assertEqual(privacy.RETENTION_REVIEW_YEARS, 3)

    def test_privacy_notice_becomes_overdue_after_a_month(self):
        c = Contact.objects.create(name="Not Yet Told")
        self.assertNotIn(c, Contact.objects.owed_privacy_notice())
        Contact.objects.filter(pk=c.pk).update(
            created_at=timezone.now() - timedelta(days=45))
        self.assertIn(c, Contact.objects.owed_privacy_notice())

    def test_sending_the_notice_clears_the_backlog(self):
        c = Contact.objects.create(name="Told")
        Contact.objects.filter(pk=c.pk).update(
            created_at=timezone.now() - timedelta(days=45),
            privacy_notice_sent_at=timezone.now())
        self.assertNotIn(c, Contact.objects.owed_privacy_notice())

    def test_consent_needs_evidence(self):
        c = Contact(name="X", news_consent=True)
        with self.assertRaises(ValidationError):
            c.clean()


class TrackedGazetteerTests(TestCase):
    """Decision 1: the Register link, and what 'prospect' means."""

    def test_no_register_link_means_prospect(self):
        g = TrackedGazetteer.objects.create(title="Something we heard about")
        self.assertTrue(g.is_prospect)
        self.assertIn(g, TrackedGazetteer.objects.prospects())

    def test_linked_gazetteer_is_held_and_reads_through(self):
        entry = GazetteerRegistryEntry.objects.create(
            id="test:1", name="Test Authority", namespace="test",
            entry_class="authority", record_count=4242, status="published",
            rights_holder="Some Archive",
        )
        g = TrackedGazetteer.objects.create(title="Local name", registry=entry)
        self.assertFalse(g.is_prospect)
        self.assertIn(g, TrackedGazetteer.objects.held())
        # Machine facts are read, never stored.
        self.assertEqual(g.registry_record_count, 4242)
        self.assertEqual(g.registry_rights_holder, "Some Archive")
        self.assertTrue(g.is_published)

    def test_prospect_read_through_is_safe(self):
        g = TrackedGazetteer.objects.create(title="Prospect")
        self.assertIsNone(g.registry_record_count)
        self.assertIsNone(g.registry_licence)
        self.assertFalse(g.is_published)

    def test_no_machine_fact_is_stored_locally(self):
        """A regression guard: these belong to the Register (review §2)."""
        local = {f.name for f in TrackedGazetteer._meta.get_fields()}
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
        self.contact = Contact.objects.create(name="A Correspondent")
        self.owner = User.objects.create(username="owner1", name="Owner",
                                         email="owner@example.org")

    def test_open_conversation_requires_a_follow_up_date(self):
        e = Engagement(contact=self.contact, stage=self.open_stage)
        with self.assertRaises(ValidationError):
            e.clean()

    def test_closed_conversation_does_not(self):
        e = Engagement(contact=self.contact, stage=self.closed_stage)
        e.clean()  # must not raise

    def test_responsible_person_is_inherited_from_the_gazetteer(self):
        g = TrackedGazetteer.objects.create(title="G", owner=self.owner)
        e = Engagement.objects.create(contact=self.contact,
                                      tracked_gazetteer=g)
        self.assertEqual(e.effective_responsible, self.owner)

    def test_an_explicit_responsible_person_overrides(self):
        other = User.objects.create(username="other1", name="Other",
                                    email="other@example.org")
        g = TrackedGazetteer.objects.create(title="G", owner=self.owner)
        e = Engagement.objects.create(contact=self.contact,
                                      tracked_gazetteer=g, responsible=other)
        self.assertEqual(e.effective_responsible, other)

    def test_a_stalled_conversation_is_detected(self):
        e = Engagement.objects.create(
            contact=self.contact, stage=self.open_stage,
            next_follow_up=datetime.date.today() - timedelta(days=1))
        self.assertTrue(e.is_stale)

    def test_a_conversation_in_hand_is_not_stale(self):
        e = Engagement.objects.create(
            contact=self.contact, stage=self.open_stage,
            next_follow_up=datetime.date.today() + timedelta(days=7))
        self.assertFalse(e.is_stale)

    def test_interaction_defaults_to_the_engagements_contact(self):
        e = Engagement.objects.create(contact=self.contact)
        i = Interaction.objects.create(engagement=e, summary="Note")
        self.assertEqual(i.contact, self.contact)

    def test_overdue_action_item(self):
        todo = ActionItemStatus.objects.create(label="To do", is_open=True)
        done = ActionItemStatus.objects.create(label="Done", is_open=False)
        e = Engagement.objects.create(contact=self.contact)
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
        described = TrackedGazetteer.objects.create(title="Described")
        extracted = TrackedGazetteer.objects.create(title="Extracted from it")
        source.documents.add(described)
        source.derived_gazetteers.add(extracted)

        self.assertEqual(list(source.documents.all()), [described])
        self.assertEqual(list(source.derived_gazetteers.all()), [extracted])
        self.assertEqual(list(extracted.derived_from_sources.all()), [source])
        self.assertEqual(list(described.documented_by.all()), [source])


class ImporterHygieneTests(TestCase):
    """The importer must not undo the encryption it is feeding."""

    def test_a_missing_name_never_becomes_an_email_address(self):
        from grace.management.commands.import_baserow_export import _display_name
        # Contact.name is an unencrypted, indexed column. Putting an address
        # there would defeat the point of encrypting Contact.email.
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
        contact = Contact.objects.create(name="Someone", organisation=variant)

        self._normalise()

        self.assertEqual(Organisation.objects.count(), 1)
        contact.refresh_from_db()
        self.assertEqual(contact.organisation, clean)

    def test_merge_survives_whichever_row_was_created_first(self):
        """The variant existing first must not rename onto the clean name."""
        variant = Organisation.objects.create(name="Trinity College")
        Organisation.objects.create(name="Trinity College")
        Contact.objects.create(name="Someone", organisation=variant)

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
        response = self.client.get("/grace/admin/grace/trackedgazetteer/")
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
        self.assertIn("Content: types", body)
        self.assertIn("Content: statuses", body)
