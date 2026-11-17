from django.db import transaction
from django.test import TestCase
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.urls import reverse
from django.test import Client

from collection.models import Collection, CollectionGroup, CollectionGroupUser
from datasets.models import Dataset
from bs4 import BeautifulSoup

# test gui changes in response to actions
class CollectionGroupScenarioTestCase(TestCase):
    def setUp(self):
        # Create a User instance
        self.user = get_user_model().objects.create_user(
            username='testuser',
            email='testuser@somewhere.com',
            given_name='Test', surname='User',
            password='12345')

        # Create a group leader user
        self.group_leader = get_user_model().objects.create_user(
            username='group_leader',
            email='testleader@somewhere.com',
            given_name='Group', surname='Leader',
            password='12345')

        # create 'group_leaders' group, add group_leader to it
        group_leaders, created = Group.objects.get_or_create(name='group_leaders')
        print('group_leaders', group_leaders)
        self.group_leader.groups.add(group_leaders)

        # Create a (place) Collection instance
        self.collection = Collection.objects.create(
            owner=self.user,
            title='Test Place Collection',
            collection_class='place',
            description='Test Place Collection Description')

        # Create a CollectionGroup instance
        self.collection_group = CollectionGroup.objects.create(
            owner=self.group_leader,
            title='Test Collection Group',
            type='class',
            description='Test Collection Group Description')

        self.collection.group = self.collection_group
        self.collection.save()

        # the sample dataset the builder adds to non-superusers' dropdowns (optional now; see collection/tests_sample_dataset.py)
        self.dataset = Dataset.objects.create(
            owner=self.user,
            title='Test Dataset',
            label='owt10_foo',  # PlaceCollectionUpdateView adds this dataset to every non-superuser's dropdown
            description='Test Dataset Description')

        # add the user to the group
        CollectionGroupUser.objects.create(
            collectiongroup=self.collection_group, user=self.user, role='member')

        # Log in the user
        self.member = Client()
        self.member.login(username=self.user.username, password='12345')

        # log in the group leader
        self.leader = Client()
        self.leader.login(username=self.group_leader.username, password='12345')

    def test_submit_collection_to_group(self):
        print('status before', self.collection.status)

        """
        member submits collection to the CollectionGroup
        """
        response = self.member.post(reverse('collection:group-connect'), {
            'action': 'submit',
            'coll': self.collection.id,
            'group': self.collection_group.id
        })
        self.collection.refresh_from_db()

        # Check collection is now part of the CollectionGroup's collections
        self.assertIn(self.collection, self.collection_group.collections.all())
        self.assertEqual(self.collection.status, 'group')

        # Get the HTML of the CollectionGroup update page
        response_group = self.leader.get(reverse('collection:collection-group-update',
                                           args=[self.collection_group.id]))
        soup_group = BeautifulSoup(response_group.content, 'html.parser')

        # Get the HTML of the user's Collection update page
        response_user = self.member.get(reverse('collection:place-collection-update',
                                           args=[self.collection.id]))

        # Check that the user's builder page shows the group status and links to the group's info page
        # (it used to print 'Submitted to class: <title>'; the builder was redesigned since)
        self.assertContains(response_user, 'Status</span>: &nbsp;group</p>')
        self.assertContains(response_user, reverse('collection:collection-group-detail',
                                                   args=[self.collection_group.id]))

        # Check that collection appears in "#coll_list" on leader's CollectionGroup update page
        coll_list = soup_group.find(id='coll_list')
        self.assertIn(self.collection.title, str(coll_list))
        self.assertIn(self.collection.owner.name, str(coll_list))

        # Check previously hidden 'reviewed' checkbox now shown
        self.assertIn('reviewed', str(coll_list))


        """
        leader checks 'reviewed' checkbox
        """
        # JS collection_status() function changes the status to 'reviewed'
        response = self.leader.post(reverse('collection:status-update'), {
            'status': 'reviewed',
            'coll': self.collection.id,
        })
        self.collection.refresh_from_db()
        self.assertEqual(self.collection.status, 'reviewed')

        response_user = self.member.get(reverse('collection:place-collection-update',
                                           args=[self.collection.id]))
        self.assertContains(response_user, 'Status</span>: &nbsp;reviewed</p>')


        """
        leader unchecks 'reviewed' checkbox
        """
        # JS collection_status() function changes the status to 'group'
        response = self.leader.post(reverse('collection:status-update'), {
            'status': 'group',
            'coll': self.collection.id,
        })
        self.collection.refresh_from_db()
        self.assertEqual(self.collection.status, 'group')

        response_user = self.member.get(reverse('collection:place-collection-update',
                                           args=[self.collection.id]))
        self.assertContains(response_user, 'Status</span>: &nbsp;group</p>')


        """
        leader checks 'nominated' checkbox
        """
        # A group leader cannot set 'nominated' through status-update any more (only 'group'/'reviewed'):
        # it is refused and nothing changes...
        response = self.leader.post(reverse('collection:status-update'), {
            'status': 'nominated',
            'coll': self.collection.id,
        })
        self.assertEqual(response.status_code, 400)
        self.collection.refresh_from_db()
        self.assertEqual(self.collection.status, 'group')

        # ...nominating goes through the dedicated `nominator` endpoint
        response = self.leader.post(reverse('collection:nominator'), {
            'nominated': 'true',
            'coll': self.collection.id,
        })
        self.assertEqual(response.status_code, 200)
        self.collection.refresh_from_db()

        # collection status to 'nominated'
        self.assertEqual(self.collection.status, 'nominated')

        # check status on member's collection update page
        response_user = self.member.get(reverse('collection:place-collection-update',
                                           args=[self.collection.id]))
        self.assertContains(response_user, 'Status</span>: &nbsp;nominated</p>')


        """
        leader unchecks 'nominated' checkbox
        """
        # JS collection_status() function changes the status to 'reviewed'
        response = self.leader.post(reverse('collection:status-update'), {
            'status': 'reviewed',
            'coll': self.collection.id,
        })
        self.collection.refresh_from_db()

        # collection status back to 'reviewed'
        self.assertEqual(self.collection.status, 'reviewed')

        # check status on member's collection update page
        response_user = self.member.get(reverse('collection:place-collection-update',
                                           args=[self.collection.id]))
        self.assertContains(response_user, 'Status</span>: &nbsp;reviewed</p>')

    def test_withdraw_collection_from_group(self):
        response = self.member.post(reverse('collection:group-connect'), {
            'action': 'withdraw',
            'coll': self.collection.id,
            'group': self.collection_group.id
        })
        self.collection.refresh_from_db()

        # Get the HTML of the CollectionGroup update page
        response_group = self.leader.get(reverse('collection:collection-group-update',
                                           args=[self.collection_group.id]))
        soup_group = BeautifulSoup(response_group.content, 'html.parser')

        # Check that the collection is no longer part of the CollectionGroup's collections
        self.assertNotIn(self.collection, self.collection_group.collections.all())
        # Check that the status of the collection is 'sandbox'
        self.assertEqual(self.collection.status, 'sandbox')
        # Check that the collection title and owner do not appear on a list in the div "#coll_list"
        coll_list = soup_group.find(id='coll_list')
        self.assertNotIn(self.collection.title, str(coll_list))


    # Add similar tests for other actions (withdraw, review, nominate)


# all OK, 2024-01-03
class CollectionGroupTestCase(TestCase):
    def setUp(self):
        # Create a User instance
        User = get_user_model()
        self.user = User.objects.create_user(
            username='testuser',
            given_name='Test', surname='User',
            email='testuser@foo.com',
            password='12345')

        # Create a group leader user
        self.group_leader = User.objects.create_user(
            username='group_leader',
            given_name='Group', surname='Leader',
            email='testleader@foo.com',
            password='12345')

        # Create the 'group_leaders' group if it does not exist
        group_leaders, created = Group.objects.get_or_create(name='group_leaders')

        # Add group_leader to the 'group_leaders' group
        group_leaders = Group.objects.get(name='group_leaders')
        self.group_leader.groups.add(group_leaders)

        # Create a Collection instance
        self.collection = Collection.objects.create(
            owner=self.user,
            title='Test Collection',
            description='Test Collection Description',
            collection_class='place',
            status='sandbox'
        )

        # Create a CollectionGroup instance
        self.collection_group = CollectionGroup.objects.create(
            owner=self.group_leader,
            title='Test Collection Group',
            description='Test Collection Group Description'
        )

    ########################################
    # unit tests
    ########################################
    # ok 2023-01-03
    def test_only_group_leader_can_create_collection_group(self):
        # Create an ordinary user
        # ordinary_user = User.objects.create_user(username='ordinaryuser', password='12345')

        # Try to create a CollectionGroup with the ordinary user as the owner
        try:
            CollectionGroup.objects.create(
                owner=self.user,
                title='Test Collection Group 2',
                description='Test Collection Group Description 2'
            )
            self.fail("Should have thrown an error because ordinary users can't create CollectionGroup objects")
        except Exception as e:
            # If an error is thrown, the test passes
            pass

    # ok 2023-01-03
    def test_add_user_to_collection_group(self):
        # Use the ordinary user created in setUp
        new_user = self.user

        # Create a CollectionGroupUser instance
        CollectionGroupUser.objects.create(
            collectiongroup=self.collection_group, user=new_user, role='member')

        # Check that the new user is now a member of the CollectionGroup
        self.assertTrue(
            CollectionGroupUser.objects.filter(
                collectiongroup=self.collection_group, user=new_user).exists())

    # ok 2023-01-03
    def test_submit_collection_to_group(self):
        # Use the ordinary user and the collection created in setUp
        user = self.user
        collection = self.collection

        # Submitting needs the requester to be a member of the target group
        CollectionGroupUser.objects.create(
            collectiongroup=self.collection_group, user=user, role='member')

        # Log in the user
        self.client = Client()
        self.client.login(username=user.username, password='12345')

        # Submit the collection to the CollectionGroup
        response = self.client.post(reverse('collection:group-connect'), {
            'action': 'submit',
            'coll': collection.id,
            'group': self.collection_group.id
        })

        # Check that the collection is now part of the CollectionGroup's collections
        self.collection_group.refresh_from_db()
        self.assertIn(collection, self.collection_group.collections.all())
        collection.refresh_from_db()
        self.assertEqual(collection.status, 'group')

    # ok 2023-01-03
    def test_withdraw_collection_from_group(self):
        # Use the ordinary user and the collection created in setUp
        user = self.user
        collection = self.collection

        # Log in the user
        self.client = Client()
        self.client.login(username=user.username, password='12345')

        # Withdraw the collection from the CollectionGroup
        response = self.client.post(reverse('collection:group-connect'), {
            'action': 'withdraw',
            'coll': collection.id,
            'group': self.collection_group.id
        })

        # Check that the collection is no longer part of the CollectionGroup's collections
        self.collection_group.refresh_from_db()
        self.assertNotIn(collection, self.collection_group.collections.all())

        # Refresh the collection object to get the latest data from the database
        collection.refresh_from_db()

        # Check that the status of the collection is 'sandbox'
        self.assertEqual(collection.status, 'sandbox')

