# basic write and read models
# ensure req. fields conform to current
from django.contrib.auth import get_user_model
User = get_user_model()
from django.test import TestCase
from django.shortcuts import get_object_or_404
from datasets.models import Dataset
from places.models import Place

# OK, 2024-01-09
class DatasetAndPlaceModelsTest(TestCase):
    
    @classmethod
    def setUpTestData(cls):
        user = User.objects.create_user(username='tempy', email='user@tempy.com', password='tempy',
                                        given_name='Tempy', surname='User')
        print("setUpTestData(): Run once to set up non-modified data for all class methods.")
        cls.ds = Dataset.objects.create(
            owner=user,
            label='ds_testy',
            title='Test Dataset',
            description='I could go on and on...',
            uri_base='https://fubar.edu',
            datatype='place',
            public=True
        )
        # (don't assume pk=1: TestCase rolls back rows but not the id sequences)
        cls.place = Place.objects.create(
            dataset=cls.ds,
            title='Timbuktu',
            src_id='foo123',
            ccodes=['AR','CA']
        )

    def test_dataset(self):
        ds = Dataset.objects.get(id=self.ds.id)
        field_label = ds.label
        #field_title = ds._meta.get_field('title').verbose_name
        field_title = ds.title
        print('made database '+field_label)
        self.assertEqual(field_label, 'ds_testy')
        self.assertEqual(field_title, 'Test Dataset')
        
    def test_place(self):
        p = Place.objects.get(id=self.place.id)
        #field_title = p._meta.get_field('title').verbose_name
        field_title = p.title
        field_srcid = p.src_id
        field_ccodes = p.ccodes
        print('made place '+field_title+' ('+field_srcid+')')
        self.assertEqual(field_title, 'Timbuktu')
        self.assertEqual(field_srcid, 'foo123')
        self.assertEqual(field_ccodes,['AR','CA'])
        self.assertIn('AR',str(field_ccodes))
        
    #def test_false_is_false(self):
        #print("Method: test_false_is_false.")
        #self.assertFalse(False)

    #def test_false_is_true(self):
        #print("Method: test_false_is_true.")
        #self.assertTrue(False)

    #def test_one_plus_one_equals_two(self):
        #print("Method: test_one_plus_one_equals_two.")
        #self.assertEqual(1 + 1, 3)