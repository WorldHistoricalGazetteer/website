from django.test import TestCase, Client
from django.contrib.auth import get_user_model
from collection.models import Collection, CollPlace, TraceAnnotation
from datasets.models import Dataset, Place
from django.urls import reverse

# all OK, 2024-01-09
class CollectionTestCase(TestCase):
    def setUp(self):
        # Create a User
        self.user = get_user_model().objects.create_user(username='testuser', given_name='Test', surname='User',
                                                         email='testuser@foo.com',
                                                         password='12345')

        # Create a test Dataset
        self.dataset = Dataset.objects.create(title='Test Dataset',
                                              label='test_dataset',
                                              description='Test Dataset Description',
                                              owner=self.user)

        # Create a few Place records with FK to that Dataset
        self.place1 = Place.objects.create(dataset=self.dataset,
                                           title='Place 1', src_id='12345', ccodes=['US', 'CA'],)
        self.place2 = Place.objects.create(dataset=self.dataset,
                                           title='Place 2', src_id='67890', ccodes=['US', 'CA'],)

        # Create a new Place Collection
        self.collection = Collection.objects.create(title='Test Collection',
                                                    description='Test Collection Description',
                                                    collection_class='place',
                                                    owner=self.user)

        # Create a test client
        self.client = Client()

    def test_add_places(self):
        # Login the test client
        self.client.login(username='testuser', password='12345')

        # Call the add_places() function
        response = self.client.post(reverse('collection:collection-add-places'),
                                    {'collection': self.collection.id, 'place_list': [self.place1.id]})

        # Confirm appropriate TraceAnnotation and CollPlace records are created
        self.assertTrue(TraceAnnotation.objects.filter(collection=self.collection, place=self.place1).exists())
        self.assertTrue(CollPlace.objects.filter(collection=self.collection, place=self.place1).exists())


    def test_add_dataset_places(self):
        # Login the test client
        self.client.login(username='testuser', password='12345')

        response = self.client.post(reverse('collection:add-dsplaces',
                                            kwargs={'coll_id': self.collection.id, 'ds_id': self.dataset.id}))

        # Confirm appropriate TraceAnnotation and CollPlace records are created
        self.assertTrue(TraceAnnotation.objects.filter(collection=self.collection,
                                                       place__in=[self.place1, self.place2]).exists())
        self.assertTrue(CollPlace.objects.filter(collection=self.collection,
                                                 place__in=[self.place1, self.place2]).exists())

    def test_add_and_remove_dataset_places(self):
        # Login the test client
        self.client.login(username='testuser', password='12345')

        # Call the add_dataset_places() function
        response = self.client.post(reverse('collection:add-dsplaces',
                                            kwargs={'coll_id': self.collection.id, 'ds_id': self.dataset.id}))

        # Confirm appropriate TraceAnnotation and CollPlace records are created
        self.assertTrue(TraceAnnotation.objects.filter(collection=self.collection,
                                                       place__in=[self.place1, self.place2]).exists())
        self.assertTrue(CollPlace.objects.filter(collection=self.collection,
                                                 place__in=[self.place1, self.place2]).exists())

        # Modify one of the TraceAnnotation records to make it not 'blank'
        trace_annotation = TraceAnnotation.objects.filter(collection=self.collection, place=self.place1).first()
        trace_annotation.note = "Test note"
        trace_annotation.save()

        # Call the remove_dataset() function
        response = self.client.post(reverse('collection:remove-ds',
                                            kwargs={'coll_id': self.collection.id, 'ds_id': self.dataset.id}))

        # Confirm appropriate TraceAnnotation and CollPlace records are deleted or archived
        self.assertFalse(TraceAnnotation.objects.filter(collection=self.collection,
                                                        place=self.place2, archived=False).exists())
        self.assertTrue(TraceAnnotation.objects.filter(collection=self.collection,
                                                       place=self.place1, archived=True).exists())
        self.assertFalse(CollPlace.objects.filter(collection=self.collection,
                                                  place__in=[self.place1, self.place2]).exists())
