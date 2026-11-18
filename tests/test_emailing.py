# ./manage.py test --settings=tests.settings tests.test_emailing.ContactFormTestCase
# ./manage.py test --settings=tests.settings tests.test_emailing.NewUserTestCase
# ./manage.py test --settings=tests.settings tests.test_emailing.DatasetSignalTestCase

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.gis.geos import Point
from django.core import mail
from django.db import transaction
from django.test import TestCase
from django.urls import reverse
from datasets.models import Dataset
from places.models import Place, PlaceGeom
from unittest.mock import patch
import sys
import pdb

User = get_user_model()
# from datasets.tasks import align_idx
# from datasets.tasks import align_wdlocal

# CASES for new_emailer()
# ======================
# /welcome, new_user: user.email_confirmed = True; name, id
# /contact_form, contact_reply: contact form submitted

# DATASET SIGNALS
# ===============
# /new_dataset: new dataset created
# /dataset_published: public=True
# /dataset_unpublished: public=False
# /wikidata_complete: ds_status='wd-complete' -> to editorial
# /dataset_indexed: ds_status='indexed'

# TASKS
# =====
# not testable at this time (can't simulate recon task); but they work
# recon_task_success: reconciliation task SUCCESS (wdlocal or idx)
# recon_task_failed: reconciliation task FAILURE (wdlocal or idx)
# recon_task_test: test index reconciliation task SUCCESS


class NewUserTestCase(TestCase):
  def setUp(self):
    """ Create a user and dataset to test with."""
    self.user = User.objects.create_user(username='testuser',
                                         given_name='Test', surname='User',
                                         email='test@example.com',
                                         password='testpassword')
    self.user.save()

    self.dataset = Dataset.objects.create(
      title='Test Dataset',
      label='test_dataset',
      description='Test description',
      owner=self.user,
      ds_status='uploaded'
    )
    self.dataset.save()
    ds = self.dataset
    print('user created in setUp', self.user.email)
    print('dataset created in setUp', self.dataset.label)

  def test_new_user_emails(self):
    """ When a user's email is verified they get ONE welcome email; the admin notice goes to Zulip
    (it used to be a second email, but admin notifications moved to Zulip)."""
    mail.outbox = []
    self.user.email_confirmed = True
    with patch('whgmail.messaging.zulip_notification') as zulip:
      self.user.save()

    self.assertEqual([m.subject for m in mail.outbox], ['Welcome to WHG'])
    self.assertIn(self.user.email, mail.outbox[0].to)
    topics = [c.kwargs.get('topic') for c in zulip.call_args_list]
    self.assertEqual(topics.count('New User Registered'), 1)

    self.assertIn('Thank you for registering', mail.outbox[0].body)

    mail.outbox.clear()

class ContactFormTestCase(TestCase):
    def setUp(self):
        self.user = User.objects.create_user(username='testuser', email='testuser@example.com',
                                             password='testpass', given_name='Test', surname='User')
        self.client.login(username='testuser', password='testpass')

    def test_contact_form_goes_to_zulip(self):
      """The contact form posts to Zulip (stream website-contact), not by email, and an authenticated
      user does not face the Turnstile check. (This used to be two emails behind a django-captcha field.)"""
      data = {
          'name': 'Test User',
          'username': 'testuser',
          'subject': 'Test Subject',
          'from_email': 'testuser@example.com',
          'message': 'Test Message',
      }
      with patch('main.views.zulip_notification') as zulip:
        response = self.client.post(reverse('contact-modal'), data=data)

      self.assertEqual(response.status_code, 200)
      self.assertEqual(response.json(), {'success': True})
      self.assertEqual(len(mail.outbox), 0)
      zulip.assert_called_once()
      self.assertEqual(zulip.call_args.kwargs['stream'], 'website-contact')
      self.assertEqual(zulip.call_args.kwargs['topic'], 'Test Subject')
      self.assertIn('Test Message', zulip.call_args.args[0])


class DatasetSignalTestCase(TestCase):
    def setUp(self):
      with transaction.atomic():
        # Create a user, dataset, place and placegeom for testing
        self.user = User.objects.create_user(username='testuser', given_name='Test', surname='User', email='testuser@example.com',
                                             password='testpass')
        self.dataset = Dataset.objects.create(owner=self.user, title='Test Dataset',
                                              label='test_dataset', ds_status=None)
        print('dataset id', self.dataset.id)
        print('settings.CELERY_ALWAYS_EAGER', settings.CELERY_ALWAYS_EAGER == True)

        self.place = Place.objects.create(
          src_id='abc123',
          dataset=self.dataset,
          title='Test Place',
          ccodes=['TH'],
        )
        self.place_geom = PlaceGeom.objects.create(
          place=self.place,
          geom=Point(0, 0),
        )
        print(self.place_geom.geom)

    def test_send_new_dataset_email(self):
        # The admin notice for a newly uploaded dataset goes to Zulip now, not to email.
        with patch('whgmail.messaging.zulip_notification') as zulip:
            self.dataset.ds_status = 'uploaded'
            self.dataset.save()

        topics = [c.kwargs.get('topic') for c in zulip.call_args_list]
        self.assertEqual(topics.count('New Dataset Created'), 1)
        notice = zulip.call_args_list[topics.index('New Dataset Created')].args[0]
        self.assertIn(self.dataset.label, notice)
        self.assertEqual(len(mail.outbox), 0)

        # Clear the outbox
        mail.outbox = []

    def test_handle_public_true(self):
      # Clear the outbox
      mail.outbox = []

      # Change the public status of the dataset
      self.dataset.public = True
      self.dataset.save()
      print('dataset id', self.dataset.id)
      # pdb.set_trace()

      # Check if an email was sent
      self.assertEqual(len(mail.outbox), 1)

      # Check the subject of the email
      self.assertEqual(mail.outbox[0].subject, 'Your WHG dataset has been published')

      # Check recipient is owner
      self.assertIn(self.user.email, mail.outbox[0].to)

    def test_handle_public_false(self):
      # Clear the outbox
      mail.outbox = []

      # set public True, clear outbox, then set False
      self.dataset.public = True
      self.dataset.save()
      mail.outbox = []
      self.dataset.public = False
      self.dataset.save()

      # Check if an email was sent
      self.assertEqual(len(mail.outbox), 1)

      # Check the subject of the email
      self.assertEqual(mail.outbox[0].subject, 'Your WHG dataset has been unpublished')

      # Check recipient is owner
      self.assertIn(self.user.email, mail.outbox[0].to)

    def test_handle_wdcomplete(self):
      # Clear the outbox
      mail.outbox = []

      # Change the ds_status of the dataset
      self.dataset.ds_status = 'wd-complete'
      self.dataset.save()

      # Check if an email was sent
      self.assertGreaterEqual(len(mail.outbox), 1)

      # Check the subject of the email
      self.assertEqual(mail.outbox[0].subject, 'WHG reconciliation review complete')

      # The owner is told, with the editorial address on bcc
      self.assertEqual(mail.outbox[0].to, [self.user.email])
      self.assertIn(settings.DEFAULT_FROM_EDITORIAL, mail.outbox[0].bcc)

    # def test_handle_indexed(self):
    #   # Clear the outbox
    #   mail.outbox = []
    #
    #   # Change the ds_status of the dataset
    #   self.dataset.ds_status = 'indexed'
    #   self.dataset.save()
    #
    #   # Check if an email was sent
    #   self.assertGreaterEqual(len(mail.outbox), 1)
    #
    #   # Check the subject of the email
    #   self.assertEqual(mail.outbox[0].subject, 'Your WHG dataset is fully indexed')
    #
    #   # Check recipient is owner
    #   self.assertIn(self.user.email, mail.outbox[0].to)

