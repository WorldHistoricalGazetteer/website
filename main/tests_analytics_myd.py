"""The Map-your-Data panel on /dashboard_admin/analytics/ (main.views.plausible_analyser_view).

Map your Data is moving to PLATO Tools; the panel counts only WHG's built-in (beta) version, says
so, states who could have used it, and hides step percentages when the importing base is too small
for a percentage to mean anything. Plausible is stubbed: no network.
"""
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from main import views


def _stub(events):
    """A _plausible_get stand-in: the event:name breakdown returns `events` ({name: (visitors,
    events)}); every other call returns an empty, successful result."""
    def fake(path, params, site_id):
        if path == 'breakdown' and params.get('property') == 'event:name':
            return {'results': [{'name': n, 'visitors': v, 'events': e}
                                for n, (v, e) in events.items()]}, None
        if path == 'aggregate':
            return {'results': {}}, None
        return {'results': []}, None
    return fake


class MydPanelTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        User = get_user_model()

        def mk(name, **kw):
            return User.objects.create_user(username=name, email=f'{name}@example.org', password='x',
                                            given_name='An', surname=name, **kw)
        cls.staff = mk('an_staff', is_staff=True)
        mk('an_beta', role='beta_tester')
        mk('an_plain')
        mk('an_gone', role='beta_tester', is_active=False)

    def _panel(self, events):
        self.client.force_login(self.staff)
        with mock.patch.object(views, '_plausible_get', side_effect=_stub(events)):
            resp = self.client.get(reverse('plausible-analyser'), {'period': '30d'})
        self.assertEqual(resp.status_code, 200)
        html = resp.content.decode()
        start = html.find('id="an-myd"')
        self.assertGreater(start, -1, 'Map your Data panel not rendered at all')
        end = html.find('Via the Plausible Stats API', start)
        self.assertGreater(end, start)
        return resp, html[start:end]

    def test_panel_is_labelled_in_whg_version_and_links_plato_tools(self):
        _, panel = self._panel({'MyD: import': (1, 1)})
        self.assertIn('in-WHG version', panel)
        self.assertIn('being moved to PLATO Tools', panel)
        self.assertIn(f'href="{views.PLATO_TOOLS_URL}"', panel)
        self.assertIn(f'href="{views.PLATO_TOOLS_GUIDE_URL}"', panel)
        self.assertIn('not</strong> measured here', panel)

    def test_small_base_shows_counts_with_denominator_and_no_percentages(self):
        resp, panel = self._panel({'MyD: import': (1, 1), 'MyD: reconcile': (1, 1),
                                   'MyD: reconcile result': (1, 1), 'MyD: export': (1, 1)})
        # Presence first: the steps rendered, with their denominator.
        self.assertIn('Exported results', panel)
        self.assertIn('of 1 importing visitor ', panel)
        self.assertIn('id="an-myd-small-n"', panel)
        # Then the absence: the old page printed "100.0%" for one visitor.
        self.assertNotIn('100.0%', panel)
        self.assertNotIn('an-bar-fill', panel)
        self.assertTrue(all(s['pct'] is None and s['drop'] is None for s in resp.context['funnel']))

    def test_large_base_keeps_percentages_and_bars(self):
        n = views.MYD_PCT_MIN_BASE * 2
        resp, panel = self._panel({'MyD: import': (n, n), 'MyD: reconcile': (n // 2, n)})
        self.assertNotIn('id="an-myd-small-n"', panel)
        self.assertIn('an-bar-fill', panel)
        self.assertIn('50.0%', panel)
        self.assertIn(f'of {n} importing visitors', panel)
        steps = {s['name']: s for s in resp.context['funnel']}
        self.assertEqual(steps['MyD: reconcile']['drop'], 50.0)

    def test_states_how_many_accounts_can_open_it(self):
        resp, panel = self._panel({'MyD: import': (1, 1)})
        # staff + active beta_tester; the plain user and the inactive beta tester do not count.
        self.assertEqual(resp.context['myd_beta_accounts'], 2)
        self.assertIn('2 active accounts can open it', panel)

    def test_extract_column_event_is_listed(self):
        _, panel = self._panel({'MyD: import': (1, 1), 'MyD: extract column': (1, 3)})
        self.assertIn('MyD: extract column', panel)

    def test_no_imports_message(self):
        _, panel = self._panel({})
        self.assertIn('No <code>MyD: import</code> events from the in-WHG version', panel)
