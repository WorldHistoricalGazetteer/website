"""'API calls by origin' panel on /dashboard_admin/analytics/ (main.views._api_origin_panel).

Plausible is stubbed: no network. The panel reads the API_Call event's `origin` and `client` props, states
its denominator (all API_Call events in the period), shows an honest empty state before data arrives, and a
failing breakdown for these props must not raise the page-wide error banner.
"""
from unittest import mock

from django.contrib.auth import get_user_model
from django.test import TestCase
from django.urls import reverse

from main import views


def _stub(total=100, origin_rows=(), client_rows=(), props_error=None):
    def fake(path, params, site_id):
        prop = params.get('property', '')
        if path == 'breakdown' and prop == 'event:name':
            return {'results': [{'name': 'API_Call', 'visitors': 5, 'events': total}]}, None
        if path == 'breakdown' and prop == 'event:props:origin':
            return (None, props_error) if props_error else ({'results': list(origin_rows)}, None)
        if path == 'breakdown' and prop == 'event:props:client':
            return (None, props_error) if props_error else ({'results': list(client_rows)}, None)
        if path == 'aggregate':
            return {'results': {}}, None
        return {'results': []}, None
    return fake


class ApiOriginPanelTests(TestCase):
    @classmethod
    def setUpTestData(cls):
        cls.staff = get_user_model().objects.create_user(
            username='ao_staff', email='ao@example.org', password='x', given_name='A', surname='O', is_staff=True)

    def _panel(self, **kw):
        self.client.force_login(self.staff)
        with mock.patch.object(views, '_plausible_get', side_effect=_stub(**kw)):
            resp = self.client.get(reverse('plausible-analyser'), {'period': '30d'})
        self.assertEqual(resp.status_code, 200)
        html = resp.content.decode()
        start = html.find('id="an-api-origin"')
        self.assertGreater(start, -1, 'API origin panel not rendered')
        return resp, html[start:html.find('Via the Plausible Stats API', start)]

    def test_empty_state_before_any_tagged_events(self):
        _, panel = self._panel(total=1380)
        self.assertIn('id="an-api-origin-empty"', panel)
        self.assertIn('No origin data yet', panel)
        self.assertIn('1380', panel)                         # the earlier events are acknowledged, not hidden

    def test_rows_percentages_use_all_api_call_events_as_denominator(self):
        _, panel = self._panel(total=200, origin_rows=[
            {'origin': 'plato-tools', 'events': 50, 'visitors': 3},
            {'origin': 'none', 'events': 30, 'visitors': 2},
            {'origin': '(none)', 'events': 120, 'visitors': 9}],      # events without the prop
            client_rows=[{'client': 'openrefine', 'events': 30, 'visitors': 2}])
        self.assertIn('PLATO Tools (plato.pelagios.org)', panel)
        self.assertIn('25.0%', panel)                        # 50 / 200 of ALL API_Call events
        self.assertIn('15.0%', panel)                        # 30 / 200
        self.assertIn('OpenRefine', panel)
        self.assertIn('Before origin tracking began', panel)  # 200 - 80 tagged = 120 untagged
        self.assertIn('>120<', panel)
        self.assertNotIn('id="an-api-origin-empty"', panel)

    def test_unknown_label_is_shown_as_given_not_invented(self):
        _, panel = self._panel(total=10, origin_rows=[{'origin': 'zzz-new', 'events': 4, 'visitors': 1}])
        self.assertIn('zzz-new', panel)

    def test_props_error_does_not_trigger_page_error_banner(self):
        resp, panel = self._panel(total=10, props_error='Plausible Stats API error: 400 Bad Request')
        self.assertNotIn('400 Bad Request', resp.content.decode())
        self.assertIn('id="an-api-origin-empty"', panel)

    def test_other_errors_still_reported(self):
        self.client.force_login(self.staff)
        def fake(path, params, site_id):
            if path == 'aggregate':
                return None, 'Plausible Stats API error: 401 Unauthorized'
            return {'results': []}, None
        with mock.patch.object(views, '_plausible_get', side_effect=fake):
            html = self.client.get(reverse('plausible-analyser')).content.decode()
        self.assertIn('401 Unauthorized', html)
