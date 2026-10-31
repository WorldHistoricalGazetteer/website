"""The HTML screens for the v4 dataset lifecycle (place#334 Phase 3): the owner's panel on
the dataset status page and the editors' review page. Every absence asserted here is paired
with the same page, for the right person or state, showing the thing (a check that can fail).

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test datasets.tests_lifecycle_screens \
        --settings=whg.settings_localtest
"""
import re
from pathlib import Path

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.template.loader import render_to_string
from django.urls import reverse

from datasets.models import DatasetLifecycleEvent, DatasetUser
from datasets.templatetags.lifecycle_tags import panel_context
from datasets.tests_lifecycle import LifecycleBase
from places.tests_vocabulary import BANNED, literals, violations

User = get_user_model()
ACTIONS = ("lifecycle-start", "lifecycle-submit", "lifecycle-publish", "lifecycle-withdraw", "lifecycle-sensitivity")
QUEUE_ACTIONS = ("review-start", "review-accept", "review-request-changes", "review-decline")
ROOT = Path(__file__).resolve().parent


def visible_text(html):
    """Text a reader sees: tags, attribute values and scripts removed, except the confirm
    questions (they are shown in the dialog), which are kept."""
    confirms = " ".join(re.findall(r'data-confirm="([^"]*)"', html))
    html = re.sub(r"<script.*?</script>", " ", html, flags=re.S)
    return re.sub(r"<[^>]+>", " ", html) + " " + confirms


def testids(html, names):
    return {n for n in names if f'data-testid="{n}"' in html}


class ScreenBase(LifecycleBase):
    def setUp(self):
        super().setUp()
        User.objects.filter(pk=self.owner.pk).update(role="beta_tester")
        self.owner.refresh_from_db()

    def status_page(self, user=None):
        self.client.force_login(user or self.owner)
        r = self.client.get(reverse("datasets:ds_status", args=[self.ds.pk]))
        self.assertEqual(r.status_code, 200)
        return r.content.decode()

    def queue(self):
        self.client.force_login(self.editor)
        r = self.client.get("/datasets/review-queue/")
        self.assertEqual(r.status_code, 200)
        return r.content.decode()

    def panel(self, html):
        m = re.search(r'<div id="ds_lifecycle".*?<script', html, re.S)
        return m.group(0) if m else ""


class OwnerPanelGateTests(ScreenBase):
    def test_null_lifecycle_beta_owner_gets_only_the_start_control(self):
        """Ruling 2026-10-30: a v3-era (NULL) private dataset offers its beta owner one thing,
        the explicit opt-in; viewing the page changes nothing."""
        self.set_state(None)
        html = self.status_page()
        self.assertIn('data-state="none"', html)
        self.assertEqual(testids(html, ACTIONS), {"lifecycle-start"})
        self.assertIsNone(self.state())
        confirm = re.search(r'data-testid="lifecycle-start"[^>]*data-confirm="([^"]+)"', html, re.S)
        self.assertIsNotNone(confirm)
        self.assertIn("published only after that review", confirm.group(1))
        url = re.search(r'data-testid="lifecycle-start"\s+data-url="([^"]+)"', html).group(1)
        self.assertEqual(url, reverse("datasets:lifecycle-start", args=[self.ds.pk]))
        with self.captureOnCommitCallbacks(execute=True):
            r = self.client.post(url, {}, content_type="application/json")
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(self.state(), "draft")
        self.assertEqual(testids(self.status_page(), ACTIONS),
                         {"lifecycle-submit", "lifecycle-sensitivity"})

    def test_null_lifecycle_shows_nothing_new_to_anyone_else(self):
        self.set_state(None)
        self.assertIn('data-testid="lifecycle-start"', self.status_page())
        # a public v3 dataset: the review process is for unpublished datasets
        self.set_state(None, public=True)
        html = self.status_page()
        self.assertNotIn('data-testid="lifecycle-panel"', html)
        self.assertNotIn("lifecycle.js", html)
        self.assertNotIn("/lifecycle/", html)
        self.set_state(None, public=False)
        # staff: in the circle, not an owner
        self.assertNotIn("/lifecycle/", self.status_page(self.editor))
        # the owner without beta
        User.objects.filter(pk=self.owner.pk).update(role="normal")
        html = self.status_page(User.objects.get(pk=self.owner.pk))
        self.assertNotIn('data-testid="lifecycle-panel"', html)
        self.assertNotIn("/lifecycle/", html)
        self.assertIsNone(self.state())

    def test_owner_without_beta_sees_nothing(self):
        self.set_state("draft")
        self.assertIn('data-testid="lifecycle-panel"', self.status_page())
        User.objects.filter(pk=self.owner.pk).update(role="normal")
        self.assertNotIn('data-testid="lifecycle-panel"',
                         self.status_page(User.objects.get(pk=self.owner.pk)))

    def test_only_owners_see_the_panel(self):
        """Staff are in every circle (and may load the status page) but are not owners;
        a co-owner is."""
        self.set_state("accepted")
        self.assertNotIn('data-testid="lifecycle-panel"', self.status_page(self.editor))
        User.objects.filter(pk=self.stranger.pk).update(role="beta_tester")
        co = User.objects.get(pk=self.stranger.pk)
        collab = DatasetUser.objects.create(dataset_id=self.ds, user_id=co, role="member")
        self.assertNotIn('data-testid="lifecycle-panel"', self.status_page(co))  # in the circle
        collab.role = "owner"
        collab.save()
        html = self.status_page(co)
        self.assertIn('data-testid="lifecycle-panel"', html)
        self.assertEqual(testids(html, ACTIONS), {"lifecycle-publish"})

    def test_panel_context_gate_directly(self):
        self.set_state("draft")
        self.assertTrue(panel_context(self.ds, self.owner)["show"])
        for user in (None, self.stranger, self.editor):
            self.assertFalse(panel_context(self.ds, user)["show"], user)


class OwnerPanelStateTests(ScreenBase):
    # state, public, exactly the action forms offered
    EXPECTED = [
        ("uploaded", False, set()),
        ("validation_failed", False, set()),
        ("draft", False, {"lifecycle-submit", "lifecycle-sensitivity"}),
        ("submitted", False, set()),
        ("in_review", False, set()),
        ("changes_requested", False, {"lifecycle-submit", "lifecycle-sensitivity"}),
        ("declined", False, {"lifecycle-submit", "lifecycle-sensitivity"}),  # may be submitted again (2026-10-31)
        ("accepted", False, {"lifecycle-publish"}),
        ("published", True, {"lifecycle-withdraw"}),
        ("withdrawn", False, {"lifecycle-submit", "lifecycle-sensitivity"}),
    ]

    def test_only_the_actions_allowed_now(self):
        offered_somewhere = set()
        for state, public, expected in self.EXPECTED:
            with self.subTest(state=state):
                self.set_state(state, public=public)
                html = self.status_page()
                self.assertIn(f'data-state="{state}"', html)
                self.assertEqual(testids(html, ACTIONS), expected)
                offered_somewhere |= expected
        # every button but "start" (NULL only, tested above) can appear
        self.assertEqual(offered_somewhere, set(ACTIONS) - {"lifecycle-start"})

    def test_buttons_post_to_the_endpoints_that_accept_them(self):
        """The URL a button posts to, taken from the page, accepts the request (the endpoint
        is what the JS calls, with the same JSON)."""
        self.set_state("draft")
        html = self.status_page()
        url = re.search(r'data-testid="lifecycle-submit"\s+data-url="([^"]+)"', html).group(1)
        self.assertEqual(url, reverse("datasets:lifecycle-submit", args=[self.ds.pk]))
        with self.captureOnCommitCallbacks(execute=True):
            r = self.client.post(url, {"note": "ready"}, content_type="application/json")
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(self.state(), "submitted")
        html = self.status_page()
        self.assertEqual(testids(html, ACTIONS), set())
        self.assertIn("usually reply by", html)

    def test_publish_and_withdraw_confirmations_say_what_happens(self):
        self.set_state("accepted")
        p = self.panel(self.status_page())
        confirm = re.search(r'data-testid="lifecycle-publish"[^>]*data-confirm="([^"]+)"', p, re.S)
        self.assertIsNotNone(confirm)
        self.assertIn("public", confirm.group(1))
        self.assertIn("search index", confirm.group(1))
        self.set_state("published", public=True)
        p = self.panel(self.status_page())
        confirm = re.search(r'data-testid="lifecycle-withdraw"[^>]*data-confirm="([^"]+)"', p, re.S)
        self.assertIsNotNone(confirm)
        self.assertIn("removed from the", confirm.group(1))
        self.assertIn("search index", confirm.group(1))

    def test_changes_requested_shows_the_editors_note_and_submit_again(self):
        self.set_state("changes_requested")
        DatasetLifecycleEvent.objects.create(
            dataset=self.ds, dataset_pk=self.ds.pk, dataset_label=self.ds.label,
            from_state="in_review", to_state="changes_requested", actor=self.editor,
            reason="Please add a licence note.", payload={})
        p = self.panel(self.status_page())
        self.assertIn("Please add a licence note.", p)
        self.assertIn("Submit again for review", p)

    def test_owner_sees_link_counts(self):
        self.set_state("draft")
        self.assertIn("Records with links", self.panel(self.status_page()))


class ReviewQueueTests(ScreenBase):
    url = "/datasets/review-queue/"

    def test_anonymous_goes_to_login(self):
        r = self.client.get(self.url)
        self.assertEqual(r.status_code, 302)
        self.assertIn("login", r["Location"])

    def test_only_editors_with_beta(self):
        self.client.force_login(self.editor)
        self.assertEqual(self.client.get(self.url).status_code, 200)
        self.client.force_login(self.owner)  # beta, not an editor
        self.assertEqual(self.client.get(self.url).status_code, 404)
        # in the editorial group but without beta access
        Group.objects.get_or_create(name="editorial")[0].user_set.add(self.stranger)
        self.client.force_login(self.stranger)
        self.assertEqual(self.client.get(self.url).status_code, 404)
        User.objects.filter(pk=self.stranger.pk).update(role="beta_tester")
        self.assertEqual(self.client.get(self.url).status_code, 200)

    def test_lists_only_datasets_waiting_for_review(self):
        listed = lambda html: f'data-dataset="{self.ds.pk}"' in html
        for state, expected in [("submitted", True), ("in_review", True), ("draft", False),
                                ("accepted", False), ("changes_requested", False), (None, False)]:
            with self.subTest(state=state):
                self.set_state(state)
                self.assertEqual(listed(self.queue()), expected)

    def test_buttons_per_state(self):
        self.set_state("submitted")
        self.assertEqual(testids(self.queue(), QUEUE_ACTIONS), {"review-start"})
        self.set_state("in_review")
        html = self.queue()
        self.assertEqual(testids(html, QUEUE_ACTIONS),
                         {"review-accept", "review-request-changes", "review-decline"})
        for name in ("changes", "decline"):
            self.assertRegex(html, rf'id="{name}_{self.ds.pk}"[^>]*required')
        self.assertNotRegex(html, rf'id="accept_{self.ds.pk}"[^>]*required')

    def test_editor_report_has_no_link_counts(self):
        """Q11: the counts the owner's panel shows are absent from the editors' page."""
        self.set_state("submitted")
        self.assertNotIn("Records with links", self.queue())
        self.assertIn('data-check="records"', self.queue())

    def test_through_the_page_to_a_decision(self):
        self.set_state("draft")
        self.client.force_login(self.owner)
        with self.captureOnCommitCallbacks(execute=True):
            self.client.post(reverse("datasets:lifecycle-submit", args=[self.ds.pk]),
                             {"note": "First go"}, content_type="application/json")
        html = self.queue()
        self.assertIn("First go", html)
        start = re.search(r'data-testid="review-start"\s+data-url="([^"]+)"', html).group(1)
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(self.client.post(start, {}, content_type="application/json")
                             .status_code, 200)
        html = self.queue()
        conclude = re.search(r'data-testid="review-request-changes"\s+data-url="([^"]+)"',
                             html).group(1)
        with self.captureOnCommitCallbacks(execute=True):
            r = self.client.post(conclude, {"outcome": "request_changes", "comments": ""},
                                 content_type="application/json")
        self.assertEqual(r.status_code, 400)  # the note is required by the endpoint too
        with self.captureOnCommitCallbacks(execute=True):
            r = self.client.post(conclude, {"outcome": "request_changes",
                                            "comments": "Add dates"},
                                 content_type="application/json")
        self.assertEqual(r.status_code, 200, r.content)
        self.assertEqual(self.state(), "changes_requested")
        self.assertNotIn(f'data-dataset="{self.ds.pk}"', self.queue())
        self.assertIn("Add dates", self.panel(self.status_page()))


class VocabularyTests(ScreenBase):
    """Q9: no 'attest', 'accession' or 'reconcile' in what these screens show."""

    def test_rendered_screens(self):
        self.set_state("in_review")
        queue_html = self.queue()
        m = re.search(r'<div class="container mt-3" id="review_queue".*?<script', queue_html, re.S)
        self.assertIsNotNone(m)
        self.assertIn("Request changes", visible_text(m.group(0)))
        self.assertIsNone(BANNED.search(visible_text(m.group(0))))
        for state, public in ((None, False), ("draft", False), ("withdrawn", False), ("changes_requested", False),
                              ("accepted", False), ("published", True)):
            self.set_state(state, public=public)
            p = self.panel(self.status_page())
            self.assertTrue(p, state)
            self.assertIsNone(BANNED.search(visible_text(p)), state)

    def test_template_sources_and_js(self):
        for name in ("_lifecycle_panel.html", "_lifecycle_checks.html", "review_queue.html"):
            src = (ROOT / "templates" / "datasets" / name).read_text()
            self.assertIsNone(BANNED.search(visible_text(src)), name)
        src = (ROOT / "static" / "datasets" / "lifecycle.js").read_text()
        self.assertGreater(len(literals(src)), 10)
        self.assertEqual(violations(src), [])

    def test_guard_can_fail(self):
        self.assertIsNotNone(BANNED.search(visible_text("<p>Request accessioning</p>")))
        self.assertIsNotNone(BANNED.search(visible_text('<form data-confirm="Reconcile now?">')))


class ReviewQueueNavLinkTests(ScreenBase):
    """The user menu's "Review queue" link: shown to exactly those the page admits."""
    LINK = 'data-testid="nav-review-queue"'

    def page(self, user):
        self.client.force_login(user)
        r = self.client.get(reverse("licenses"))  # any page on the base template
        self.assertEqual(r.status_code, 200)
        return r.content.decode()

    def test_link_follows_the_page_gate(self):
        self.assertIn(self.LINK, self.page(self.editor))  # staff: beta + editor
        self.assertIn("/datasets/review-queue/", self.page(self.editor))
        self.assertNotIn(self.LINK, self.page(self.owner))  # beta, not an editor
        Group.objects.get_or_create(name="editorial")[0].user_set.add(self.stranger)
        stranger = User.objects.get(pk=self.stranger.pk)
        self.assertNotIn(self.LINK, self.page(stranger))  # editor, no beta
        User.objects.filter(pk=self.stranger.pk).update(role="beta_tester")
        self.assertIn(self.LINK, self.page(User.objects.get(pk=self.stranger.pk)))


class DashboardWaitingTests(ScreenBase):
    """The editors' dashboard line: 'N datasets waiting for review', linking to the queue."""
    MARK = 'data-testid="dashboard-review-waiting"'

    def dash(self, user, name="dashboard-user"):
        self.client.force_login(user)
        r = self.client.get(reverse(name))
        self.assertEqual(r.status_code, 200)
        return r.content.decode()

    def test_count_matches_the_review_page_and_links_to_it(self):
        self.set_state("submitted")
        html = self.dash(self.editor)
        m = re.search(self.MARK + r'><a href="([^"]+)">(\d+) datasets? waiting for review</a>', html)
        self.assertIsNotNone(m, "line missing for an editor with a dataset waiting")
        self.assertEqual(m.group(1), "/datasets/review-queue/")
        self.assertEqual(m.group(2), "1")
        self.assertIn("1 dataset waiting for review", html)
        self.assertEqual(self.queue().count('data-testid="review-row"'), 1)  # agrees with the page
        self.set_state("in_review")
        self.assertIn("1 dataset waiting", self.dash(self.editor))
        # a second dataset: both pages say 2
        from datasets.models import Dataset
        Dataset.objects.create(owner=self.owner, label="lc334b", title="B", description="D", creator="X",
                               public=False, uri_base="https://example.org/b/", lifecycle_state="submitted")
        self.assertIn("2 datasets waiting for review", self.dash(self.editor))
        self.assertEqual(self.queue().count('data-testid="review-row"'), 2)

    def test_nothing_when_none_waiting_or_for_anyone_else(self):
        self.set_state("submitted")
        self.assertIn(self.MARK, self.dash(self.editor))               # presence, same data
        for state in ("draft", "accepted", "changes_requested", None):
            self.set_state(state)
            self.assertNotIn(self.MARK, self.dash(self.editor), state)
        self.set_state("submitted")
        self.assertNotIn(self.MARK, self.dash(self.owner))             # beta, not an editor
        Group.objects.get_or_create(name="editorial")[0].user_set.add(self.stranger)
        self.assertNotIn(self.MARK, self.dash(User.objects.get(pk=self.stranger.pk)))  # editor, no beta
        User.objects.filter(pk=self.stranger.pk).update(role="beta_tester")
        # editorial but outside the dataset's circle: the queue lists nothing, so the line is absent too
        self.assertNotIn(self.MARK, self.dash(User.objects.get(pk=self.stranger.pk)))
        self.assertEqual(self.queue_for(User.objects.get(pk=self.stranger.pk)).count('data-testid="review-row"'), 0)
        Group.objects.get_or_create(name="whg_admins")[0].user_set.add(self.stranger)  # in every circle
        self.assertIn(self.MARK, self.dash(User.objects.get(pk=self.stranger.pk)))

    def queue_for(self, user):
        self.client.force_login(user)
        return self.client.get("/datasets/review-queue/").content.decode()

    def test_admin_dashboard_too(self):
        self.set_state("submitted")
        Group.objects.get_or_create(name="whg_admins")[0].user_set.add(self.editor)
        self.assertIn(self.MARK, self.dash(User.objects.get(pk=self.editor.pk), "dashboard-admin"))


class PublicV3HintTests(ScreenBase):
    HINT = "To put this dataset through the review process, make it private first."

    def test_hint_for_beta_owner_of_public_null_dataset_only(self):
        self.set_state(None, public=True)
        html = self.status_page()
        self.assertIn(self.HINT, html)
        self.assertEqual(testids(html, ACTIONS), set())
        self.assertNotIn("/lifecycle/", html)
        self.assertIsNone(BANNED.search(visible_text(html[html.index(self.HINT) - 200:][:400])))
        # co-owner too
        User.objects.filter(pk=self.stranger.pk).update(role="beta_tester")
        co = User.objects.get(pk=self.stranger.pk)
        DatasetUser.objects.create(dataset_id=self.ds, user_id=co, role="owner")
        self.assertIn(self.HINT, self.status_page(co))
        # negatives, each beside the presence above
        self.assertNotIn(self.HINT, self.status_page(self.editor))              # staff, not an owner
        User.objects.filter(pk=self.owner.pk).update(role="normal")
        self.assertNotIn(self.HINT, self.status_page(User.objects.get(pk=self.owner.pk)))  # no beta
        self.set_state(None, public=False)                                       # private: the Start control instead
        User.objects.filter(pk=self.owner.pk).update(role="beta_tester")
        html = self.status_page(User.objects.get(pk=self.owner.pk))
        self.assertNotIn(self.HINT, html)
        self.assertIn('data-testid="lifecycle-start"', html)
        for state, public in (("draft", False), ("published", True), ("withdrawn", False)):
            self.set_state(state, public=public)
            html = self.status_page(User.objects.get(pk=self.owner.pk))
            self.assertIn('data-testid="lifecycle-panel"', html)
            self.assertNotIn(self.HINT, html, state)
