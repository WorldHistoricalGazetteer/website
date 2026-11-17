"""Cohort B backfill (place#349 C1): non-public pre-2024-07-15 datasets only."""
import datetime
from io import StringIO

from unittest.mock import patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.test import TestCase
from django.utils import timezone

from datasets.models import Dataset, DatasetFile
from licensing.models import License

User = get_user_model()


class CohortBBackfillTests(TestCase):
    def setUp(self):
        License.objects.get_or_create(spdx_id='CC-BY-4.0', defaults={'label': 'CC BY 4.0'})
        p = patch('whgmail.messaging.zulip_notification', return_value=True)
        p.start()
        self.addCleanup(p.stop)
        self.owner = User.objects.create_user(username='b-owner', email='b@example.org', password='x' * 20,
                                              given_name='B', surname='Owner')

    def _ds(self, label, public, year, with_file=True):
        ds = Dataset.objects.create(owner=self.owner, label=label, title=label, description='d', public=public)
        Dataset.objects.filter(pk=ds.pk).update(create_date=datetime.datetime(year, 3, 1, tzinfo=datetime.timezone.utc))
        if with_file:
            DatasetFile.objects.create(dataset_id=ds, rev=1, file='x.csv', format='delimited',
                                       delimiter='\t', df_status='uploaded', numrows=1, upload_date=timezone.now())
        return ds

    def test_only_nonpublic_pre_acceptance_with_file_and_no_licence(self):
        priv = self._ds('b_priv', False, 2023)
        pub = self._ds('b_pub', True, 2023)
        nofile = self._ds('b_nofile', False, 2023, with_file=False)
        recent = self._ds('a_priv', False, 2025)
        call_command('backfill_legacy_licenses', '--cohort', 'b', '--commit', stdout=StringIO())
        for d in (priv, pub, nofile, recent):
            d.refresh_from_db()
        self.assertEqual(priv.license.spdx_id, 'CC-BY-4.0')
        self.assertEqual(priv.license_source, 'legacy_notice')
        self.assertIsNone(pub.license)
        self.assertIsNone(nofile.license)
        self.assertIsNone(recent.license)

    def test_dry_run_writes_nothing(self):
        priv = self._ds('b_priv2', False, 2022)
        call_command('backfill_legacy_licenses', '--cohort', 'b', stdout=StringIO())
        priv.refresh_from_db()
        self.assertIsNone(priv.license)
