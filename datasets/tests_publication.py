"""Publishing an accepted dataset, and making it private again (place#334 Phase 3).

The pending -> active flip must save one row at a time: the gateway push is queued by
``post_save`` (api/signals.py) and a ``QuerySet.update()`` fires none, so a bulk flip would
look right in Postgres and leave the overlay knowing nothing. The tests count the Celery
task calls per row (the task objects are patched, as api/tests_attestations.py does), prove
the flip is idempotent, and show a withdrawn dataset's links leaving the anonymous
Concordance in the same request. Each absence is paired with the presence it replaces.

    GDAL_LIBRARY_PATH=... venv/bin/python manage.py test datasets.tests_publication \
        --settings=whg.settings_localtest
"""
from unittest import mock

from django.contrib.auth.models import AnonymousUser
from django.core.cache import cache

from api import attestations
from api.models import ContributorAttestation
from datasets import lifecycle
from datasets.models import Dataset
from datasets.tests_lifecycle import LifecycleBase
from places.concordance import build_concordance, cached_concordance


class PublicationBase(LifecycleBase):
    def setUp(self):
        super().setUp()
        from places.models import Place
        with mock.patch("datasets.signals.doi"):
            self.other_public = Dataset.objects.create(owner=self.stranger, label="lc334b",
                                                       title="Other public", description="D",
                                                       public=True)
            self.other_private = Dataset.objects.create(owner=self.stranger, label="lc334c",
                                                        title="Other private", description="D",
                                                        public=False)
        self.a1 = Place.objects.create(title="Alpha", src_id="a1", dataset=self.ds, ccodes=[])
        self.a2 = Place.objects.create(title="Alpha two", src_id="a2", dataset=self.ds, ccodes=[])
        self.b1 = Place.objects.create(title="Beta", src_id="b1", dataset=self.other_public, ccodes=[])
        self.c1 = Place.objects.create(title="Gamma", src_id="c1", dataset=self.other_private,
                                       ccodes=[])
        self.set_state("accepted")
        cache.clear()

    def gid(self, place):
        return f"whg:{place.dataset.pk}:{place.src_id}"

    def row(self, a, b, status="pending", relation="closeMatch", user=None, a_id=None):
        """An attestation between two places (or a place and an authority id), stored as
        the write path stores it: gateway ids in canonical order, Place pks beside them."""
        sides = []
        for x, override in ((a, a_id), (b, None)):
            if isinstance(x, str):
                sides.append((x, None))
            else:
                sides.append((override or self.gid(x), x.pk))
        first_ds = next(x.dataset.pk for x in (a, b) if not isinstance(x, str))
        sides.sort()
        return ContributorAttestation.objects.create(
            user=user or self.owner, dataset_id=f"whg:{first_ds}", relation_type=relation,
            place_a=sides[0][0], place_b=sides[1][0], place_a_pk=sides[0][1],
            place_b_pk=sides[1][1], status=status)

    def status(self, row):
        return ContributorAttestation.objects.values_list("status", flat=True).get(pk=row.pk)

    def publish(self):
        self.m["post"].reset_mock()
        self.m["revoke"].reset_mock()
        self.run_on_commit(lifecycle.publish, self.ds, self.owner)

    def posted(self):
        return sorted(c.args[0] for c in self.m["post"].delay.call_args_list)


class FlipTests(PublicationBase):
    def test_each_flipped_row_is_forwarded_exactly_once(self):
        to_public = self.row(self.a1, self.b1)
        to_authority = self.row(self.a1, "wd:Q90")
        to_private = self.row(self.a1, self.c1)            # the other side is still private
        withdrawn = self.row(self.a2, self.b1, status="withdrawn")
        unrelated = self.row(self.b1, "wd:Q1", status="active")
        self.publish()
        self.assertEqual(self.status(to_public), "active")
        self.assertEqual(self.status(to_authority), "active")
        self.assertEqual(self.status(to_private), "pending")
        self.assertEqual(self.status(withdrawn), "withdrawn")
        self.assertEqual(self.status(unrelated), "active")
        self.assertEqual(self.posted(), sorted([to_public.pk, to_authority.pk]))
        self.m["revoke"].delay.assert_not_called()
        event = self.ds.lifecycle_events.get(to_state="published")
        self.assertEqual((event.payload["attestations_activated"],
                          event.payload["attestations_still_pending"]), (2, 1))

    def test_the_flip_is_idempotent(self):
        row = self.row(self.a1, "wd:Q90")
        self.publish()
        self.assertEqual(self.posted(), [row.pk])
        with self.captureOnCommitCallbacks(execute=True):
            again = attestations.activate_for_dataset(self.ds.pk)
        self.assertEqual(again["activated"], 0)
        self.assertEqual(self.m["post"].delay.call_count, 1)  # still the one push
        # and publishing an already published dataset is refused, so cannot re-forward
        with self.assertRaises(lifecycle.LifecycleRefused):
            lifecycle.publish(self.ds, self.owner)

    def test_a_row_resolves_by_its_stored_place_pk(self):
        """N2: the src_id written when the link was made may have changed since."""
        row = self.row(self.a1, "wd:Q90", a_id=f"whg:{self.ds.pk}:old-src")
        self.assertEqual(row.place_b, f"whg:{self.ds.pk}:old-src")
        self.publish()
        row.refresh_from_db()
        self.assertEqual((row.status, row.place_b, row.place_b_pk),
                         ("active", self.gid(self.a1), self.a1.pk))

    def test_an_ambiguous_source_id_stays_pending(self):
        from places.models import Place
        row = self.row(self.a1, "wd:Q90")
        control = self.row(self.a2, "wd:Q90")
        Place.objects.create(title="Alpha again", src_id="a1", dataset=self.ds, ccodes=[])
        self.publish()
        self.assertEqual(self.status(row), "pending")
        self.assertEqual(self.status(control), "active")
        event = self.ds.lifecycle_events.get(to_state="published")
        self.assertEqual(event.payload["attestations_unresolved"],
                         [[row.pk, "source id is not unique in its dataset"]])

    def test_the_other_dataset_publishing_later_flips_the_rest(self):
        to_private = self.row(self.a1, self.c1)
        self.publish()
        self.assertEqual(self.status(to_private), "pending")
        Dataset.objects.filter(pk=self.other_private.pk).update(public=True)
        with self.captureOnCommitCallbacks(execute=True):
            out = attestations.activate_for_dataset(self.other_private.pk)
        self.assertEqual((out["activated"], self.status(to_private)), (1, "active"))


class WithdrawTests(PublicationBase):
    def test_withdraw_returns_rows_to_pending_and_revokes_each_once(self):
        r1 = self.row(self.a1, self.b1)
        r2 = self.row(self.a1, "wd:Q90")
        unrelated = self.row(self.b1, "wd:Q1", status="active")
        self.publish()
        self.assertEqual({self.status(r1), self.status(r2)}, {"active"})
        self.run_on_commit(lifecycle.withdraw, self.ds, self.owner, "not ready")
        self.assertEqual({self.status(r1), self.status(r2)}, {"pending"})
        self.assertEqual(self.status(unrelated), "active")
        revoked = sorted((c.args[0]["place_a"], c.args[0]["place_b"])
                         for c in self.m["revoke"].delay.call_args_list)
        self.assertEqual(revoked, sorted([(r1.place_a, r1.place_b), (r2.place_a, r2.place_b)]))
        self.assertFalse(Dataset.objects.get(pk=self.ds.pk).public)

    def test_withdrawn_links_leave_the_anonymous_concordance_in_the_same_request(self):
        self.row(self.a1, self.b1)
        anon = AnonymousUser()

        def attested(conc):
            return [e for e in conc["outbound"] + conc["inbound"]
                    if e.get("source") == "attestation"]

        # pending: not on the public page of the other record
        self.assertEqual(attested(build_concordance(self.b1, user=None, use_gateway=False)), [])
        self.publish()
        with mock.patch("places.concordance.gateway_namespaces", return_value=set()):
            published = cached_concordance(self.b1, user=anon)        # also warms the cache
        self.assertEqual([e["identifier"] for e in attested(published)], [self.gid(self.a1)])
        self.assertEqual(len(attested(build_concordance(self.b1, user=None, use_gateway=False))), 1)
        self.run_on_commit(lifecycle.withdraw, self.ds, self.owner)
        # No task has run and no TTL has passed: the same process, straight after.
        self.assertEqual(attested(build_concordance(self.b1, user=None, use_gateway=False)), [])
        with mock.patch("places.concordance.gateway_namespaces", return_value=set()):
            self.assertEqual(attested(cached_concordance(self.b1, user=anon)), [])


class DeleteStillWithdrawsTests(PublicationBase):
    """Hazard (f): deleting a dataset withdraws its attestations, published or not."""

    def test_delete_path_withdraws_active_and_pending_rows(self):
        active = self.row(self.a1, "wd:Q90")
        self.publish()
        pending = self.row(self.a2, self.c1)
        self.assertEqual((self.status(active), self.status(pending)), ("active", "pending"))
        with self.captureOnCommitCallbacks(execute=True):
            self.assertEqual(attestations.withdraw_for_dataset(self.ds.pk), 2)
        self.assertEqual((self.status(active), self.status(pending)), ("withdrawn", "withdrawn"))
