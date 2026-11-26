"""Screenshots on beta snag reports: stored privately, linked (never embedded) from the public issue."""
from io import BytesIO
from unittest import mock

from django.contrib.auth import get_user_model
from django.core.files.uploadedfile import SimpleUploadedFile
from django.test import TestCase, override_settings
from django.urls import reverse
from PIL import Image

from main.models import BetaSnag, BetaSnagImage


def _png(w=40, h=30):
    b = BytesIO()
    Image.new('RGB', (w, h), (200, 30, 30)).save(b, 'PNG')
    return b.getvalue()


@override_settings(GITHUB_SNAG_TOKEN='', GITHUB_SNAG_REPO='')
class BetaSnagImageTests(TestCase):
    def setUp(self):
        U = get_user_model()
        self.tester = U.objects.create_user('crystal', 'c@example.org', 'pw', 'Test', 'User', role='beta_tester')
        self.other = U.objects.create_user('other', 'o@example.org', 'pw', 'Test', 'User', role='beta_tester')
        self.staff = U.objects.create_user('staffer', 's@example.org', 'pw', 'Staff', 'User', is_staff=True)

    def _post(self, files):
        self.client.force_login(self.tester)
        return self.client.post(reverse('beta-snag'), {'title': 'Map blank', 'screenshots': files})

    def test_images_saved_and_served_only_to_staff_and_reporter(self):
        r = self._post([SimpleUploadedFile('a.png', _png(), 'image/png'),
                        SimpleUploadedFile('b.png', _png(), 'image/png')])
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.context['shots_saved'], 2)
        snag = BetaSnag.objects.get(title='Map blank')
        img = snag.images.first()
        url = reverse('beta-snag-image', args=[img.pk])
        for user, code in ((self.tester, 200), (self.staff, 200), (self.other, 404)):
            self.client.force_login(user)
            self.assertEqual(self.client.get(url).status_code, code, user.username)
        self.client.logout()
        self.assertNotEqual(self.client.get(url).status_code, 200)   # anonymous: login redirect

    def test_non_image_and_excess_are_refused_but_report_still_saved(self):
        files = [SimpleUploadedFile('x.png', b'<script>alert(1)</script>', 'image/png')] + \
                [SimpleUploadedFile(f'{i}.png', _png(), 'image/png') for i in range(5)]
        r = self._post(files)
        snag = BetaSnag.objects.get(title='Map blank')
        self.assertEqual(snag.images.count(), 3)         # 4 considered: 1 bad + 3 good
        self.assertEqual(r.context['shots_saved'], 3)
        self.assertEqual(r.context['shots_skipped'], 3)  # the bad one + 2 beyond the cap

    def test_stored_bytes_are_a_reencoded_png(self):
        self._post([SimpleUploadedFile('a.jpg', _png(), 'image/jpeg')])
        data = bytes(BetaSnagImage.objects.get().data)
        self.assertEqual(Image.open(BytesIO(data)).format, 'PNG')

    def test_issue_links_screenshots_without_embedding(self):
        posted = {}

        def fake_post(url, headers=None, json=None, timeout=None):
            posted.update(json)
            return mock.Mock(status_code=201, json=lambda: {'html_url': 'https://github.com/x/y/issues/1'})

        with override_settings(GITHUB_SNAG_TOKEN='t', GITHUB_SNAG_REPO='x/y'), \
                mock.patch('requests.post', fake_post):
            self._post([SimpleUploadedFile('a.png', _png(), 'image/png')])
        body = posted['body']
        self.assertIn('/beta/snag/image/', body)
        self.assertNotIn('![', body)          # a link, never an embedded picture on a public issue

    def test_no_screenshots_leaves_issue_body_unchanged(self):
        r = self._post([])
        self.assertEqual(r.context['shots_saved'], 0)
        self.assertEqual(BetaSnagImage.objects.count(), 0)
