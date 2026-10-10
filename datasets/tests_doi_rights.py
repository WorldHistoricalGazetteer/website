"""``update_doi_rights`` (place#158, C4) with the DataCite client mocked.

The command may send only a ``rightsList``, only for a DOI that exists, only
when it differs from what is registered, and only with ``--commit``. It must
refuse before any network call when a selected dataset has no licence, and
must never print the credentials.
"""
import json
import tempfile
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

from django.contrib.auth import get_user_model
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings

from datasets.models import Dataset
from licensing.models import License

User = get_user_model()

SECRET = "dGVzdDpzZWNyZXQ="   # a stand-in for settings.DOI_ENCODED_CREDENTIALS
API = "https://api.test.datacite.org/dois"
PREFIX = "10.5072"

# What DataCite returns for a DOI minted under the old hard-coded statement.
OLD_NC = [{
    "rights": "Creative Commons Attribution Non Commercial 4.0 International",
    "rightsUri": "https://creativecommons.org/licenses/by-nc/4.0/legalcode",
    "schemeUri": "https://spdx.org/licenses/",
    "rightsIdentifier": "cc-by-nc-4.0",
    "rightsIdentifierScheme": "SPDX",
    "lang": "en",
}]


def _resp(status, attrs=None):
    r = MagicMock()
    r.status_code = status
    r.json.return_value = {"data": {"attributes": attrs or {}}}
    return r


@override_settings(DOI_API_URL=API, DOI_PREFIX=PREFIX, DOI_ENCODED_CREDENTIALS=SECRET)
class UpdateDoiRightsTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.owner = User.objects.create_user(
            username="doi_owner", email="doi@example.org", password="pw",
            given_name="Dee", surname="Owner", role="normal")
        cls.cc_by = License.objects.get(spdx_id="CC-BY-4.0")
        cls.cc0 = License.objects.get(spdx_id="CC0-1.0")
        with patch("datasets.signals.doi"):
            def ds(label, license=None, doi=True):
                return Dataset.objects.create(
                    owner=cls.owner, label=label, title=f"Dataset {label}", description="D",
                    public=True, license=license, doi=doi,
                    license_source="legacy_notice" if license else None, ds_status="indexed")
            cls.by = ds("doi_by", cls.cc_by)
            cls.zero = ds("doi_zero", cls.cc0)
            cls.bare = ds("doi_bare")
            cls.nodoi = ds("doi_none", cls.cc_by, doi=False)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        p = patch("datasets.management.commands.update_doi_rights.requests")
        self.requests = p.start()
        self.addCleanup(p.stop)
        # ``except requests.RequestException`` needs a real exception class on the mock.
        import requests as real_requests
        self.requests.RequestException = real_requests.RequestException
        self.requests.get.return_value = _resp(200, {"rightsList": OLD_NC})
        self.requests.put.return_value = _resp(200)

    def _run(self, *args):
        out = StringIO()
        call_command("update_doi_rights", *args, stdout=out, stderr=out)
        return out.getvalue()

    def _ids(self, *dss):
        return "--ids", ",".join(str(d.id) for d in dss)

    def _doi(self, ds):
        return f"{PREFIX}/whg-dataset-{ds.id}"

    # --- refusals before any network call ----------------------------------
    def test_refuses_an_unlicensed_dataset_without_calling_datacite(self):
        with self.assertRaisesMessage(CommandError, "record no licence"):
            self._run(*self._ids(self.by, self.bare), "--commit")
        self.requests.get.assert_not_called()
        self.requests.put.assert_not_called()

    def test_refuses_unknown_ids(self):
        with self.assertRaisesMessage(CommandError, "do not exist"):
            self._run("--ids", "999999")
        self.requests.get.assert_not_called()

    # --- dry run -------------------------------------------------------------
    def test_dry_run_fetches_compares_and_sends_nothing(self):
        out = self._run(*self._ids(self.by))
        self.requests.get.assert_called_once()
        self.assertEqual(self.requests.get.call_args.args[0], f"{API}/{self._doi(self.by)}")
        self.requests.put.assert_not_called()
        self.assertIn(f"{self._doi(self.by)}", out)
        self.assertIn("cc-by-nc-4.0 -> CC-BY-4.0", out)
        self.assertIn("to update 1", out)
        self.assertIn("Dry run", out)

    def test_credentials_are_never_printed(self):
        out = self._run(*self._ids(self.by), "--commit")
        self.assertNotIn(SECRET, out)
        # ...but they are sent, in the header, on both calls.
        for call in (self.requests.get.call_args, self.requests.put.call_args):
            self.assertEqual(call.kwargs["headers"]["authorization"], f"Basic {SECRET}")

    # --- commit --------------------------------------------------------------
    def test_commit_puts_only_the_rights_list(self):
        out = self._run(*self._ids(self.by), "--commit")
        self.requests.put.assert_called_once()
        call = self.requests.put.call_args
        self.assertEqual(call.args[0], f"{API}/{self._doi(self.by)}")
        attrs = call.kwargs["json"]["data"]["attributes"]
        self.assertEqual(list(attrs.keys()), ["rightsList"])
        self.assertNotIn("event", attrs)
        self.assertEqual(attrs["rightsList"][0]["rightsIdentifier"], "CC-BY-4.0")
        self.assertEqual(attrs["rightsList"][0]["rightsIdentifierScheme"], "SPDX")
        self.assertIn("Sent 1 of 1", out)

    def test_commit_sends_each_dataset_its_own_licence(self):
        self._run(*self._ids(self.by, self.zero), "--commit")
        sent = {c.args[0].rsplit("/", 1)[1]: c.kwargs["json"]["data"]["attributes"]["rightsList"][0]["rightsIdentifier"]
                for c in self.requests.put.call_args_list}
        self.assertEqual(sent, {f"whg-dataset-{self.by.id}": "CC-BY-4.0",
                                f"whg-dataset-{self.zero.id}": "CC0-1.0"})

    # --- idempotence ---------------------------------------------------------
    def test_already_matching_rights_are_not_resent(self):
        """DataCite does not echo our entry verbatim: it lower-cases a known
        SPDX id, rewrites the text and URI from the SPDX list, and uses its own
        key casing (verified against a live record). That must still read as
        equal, or the command would re-send every DOI on every run."""
        self.requests.get.return_value = _resp(200, {"rightsList": [{
            "rights": "Creative Commons Attribution 4.0 International",
            "rightsUri": "https://creativecommons.org/licenses/by/4.0/legalcode",
            "schemeUri": "https://spdx.org/licenses/",
            "rightsIdentifier": "cc-by-4.0",
            "rightsIdentifierScheme": "SPDX",
            "lang": "en",
        }]})
        out = self._run(*self._ids(self.by), "--commit")
        self.requests.put.assert_not_called()
        self.assertIn("unchanged: CC-BY-4.0", out)
        self.assertIn("Sent 0 of 0", out)

    def test_a_different_version_of_the_same_family_is_a_change(self):
        """Identity comparison must not collapse CC BY 3.0 into CC BY 4.0."""
        self.requests.get.return_value = _resp(200, {"rightsList": [{
            "rightsIdentifier": "cc-by-3.0", "rightsIdentifierScheme": "SPDX"}]})
        out = self._run(*self._ids(self.by))
        self.assertIn("cc-by-3.0 -> CC-BY-4.0", out)
        self.assertIn("to update 1", out)

    def test_custom_licence_compares_by_text(self):
        pd = License.objects.get(spdx_id="custom-public-domain")
        with patch("datasets.signals.doi"):
            Dataset.objects.filter(id=self.zero.id).update(license=pd)
        self.requests.get.return_value = _resp(200, {"rightsList": [
            {"rights": pd.label.upper(), "lang": "en"}]})
        out = self._run(*self._ids(self.zero))
        self.requests.put.assert_not_called()
        self.assertIn("unchanged", out)

    # --- skips ---------------------------------------------------------------
    def test_dataset_without_doi_is_skipped_not_fetched(self):
        out = self._run(*self._ids(self.nodoi), "--commit")
        self.requests.get.assert_not_called()
        self.requests.put.assert_not_called()
        self.assertIn(f"skipped, no DOI: [{self.nodoi.id}]", out)

    def test_doi_missing_at_datacite_is_reported_and_not_minted(self):
        self.requests.get.return_value = _resp(404)
        out = self._run(*self._ids(self.by), "--commit")
        self.requests.put.assert_not_called()
        self.requests.post.assert_not_called()
        self.assertIn("not registered at DataCite", out)
        self.assertIn("not registered 1", out)

    def test_a_failed_update_is_reported_and_the_rest_continue(self):
        self.requests.put.side_effect = [_resp(500), _resp(200)]
        with self.assertRaisesMessage(CommandError, "1 DOI(s) failed"):
            self._run(*self._ids(self.by, self.zero), "--commit")
        self.assertEqual(self.requests.put.call_count, 2)

    # --- default scope: the proposals file ----------------------------------
    def test_default_scope_is_the_approved_proposal_rows(self):
        path = Path(self.tmp.name) / "p.json"
        path.write_text(json.dumps({"proposals": [
            {"dataset_id": self.by.id, "label": self.by.label, "spdx_id": "CC-BY-4.0",
             "license_source": "legacy_notice", "approved": True,
             "approved_by": "SG", "approved_on": "2026-10-10"},
            {"dataset_id": self.zero.id, "label": self.zero.label, "spdx_id": "CC0-1.0",
             "license_source": "upstream_terms", "approved": False,
             "approved_by": None, "approved_on": None},
        ]}), encoding="utf-8")
        out = self._run("--proposals", str(path))
        self.assertIn("approved proposal rows (1)", out)
        self.requests.get.assert_called_once()
        self.assertIn(self._doi(self.by), self.requests.get.call_args.args[0])

    def test_default_scope_refuses_an_approved_row_not_yet_applied(self):
        """The proposals scope exists to follow apply_licence_proposals; a row
        whose dataset still has no licence means that has not happened."""
        path = Path(self.tmp.name) / "p.json"
        path.write_text(json.dumps({"proposals": [
            {"dataset_id": self.bare.id, "label": self.bare.label, "spdx_id": "CC-BY-4.0",
             "license_source": "legacy_notice", "approved": True,
             "approved_by": "SG", "approved_on": "2026-10-10"},
        ]}), encoding="utf-8")
        with self.assertRaisesMessage(CommandError, "record no licence"):
            self._run("--proposals", str(path), "--commit")
        self.requests.get.assert_not_called()

    def test_all_licensed_scope(self):
        out = self._run("--all-licensed")
        self.assertIn("--all-licensed", out)
        fetched = sorted(c.args[0].rsplit("-", 1)[1] for c in self.requests.get.call_args_list)
        self.assertEqual(fetched, sorted(str(d.id) for d in (self.by, self.zero)))
