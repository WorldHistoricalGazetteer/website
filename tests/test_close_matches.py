from unittest.mock import patch, MagicMock
from django.test import TestCase
from places.models import Place, CloseMatch
from datasets.models import Dataset
from datasets.tasks import maxID
from datasets.services import indexMatch
from django_celery_results.models import TaskResult
from django.conf import settings
from django.contrib.auth import get_user_model

User = get_user_model()

class IndexMatchTestCase(TestCase):

    def setUp(self):
        # Ensure the user is created
        self.user, _ = User.objects.get_or_create(
            username='test_user',
            defaults={
                'email': 'user@useme.com',
                'given_name': 'Test',
                'password': 'testpassword',
                'surname': 'User'
            }
        )
        self.task = TaskResult.objects.create(task_name='test_task')

        self.test_dataset = Dataset.objects.create(
            title='Test Dataset',
            owner=self.user,
            label='test_dataset',
            description='Test description',
            uri_base='http://example.com'
        )

    def tearDown(self):
        # Clean up any created objects (if necessary)
        TaskResult.objects.all().delete()
        Dataset.objects.all().delete()
        Place.objects.all().delete()
        CloseMatch.objects.all().delete()
        User.objects.all().delete()

    def test_index_match(self):
        # Create a mock place
        place = Place.objects.create(
            src_id='abc123',
            dataset=self.test_dataset,
            title='Test Place',
            ccodes=['BR']
        )
        place_id = place.id
        parent = Place.objects.create(src_id='parent1', dataset=self.test_dataset, title='Parent Place', ccodes=['BR'])
        parent_id = parent.id
        print(f"Created place with ID: {place_id}")

        # Mock Elasticsearch response for place_id
        es_response_no_hits = {
            'hits': {
                'total': {'value': 0},
                'hits': []
            }
        }

        es_response_hit = {
            'hits': {
                'total': {'value': 1},
                'hits': [{
                    '_id': str(parent_id),
                    '_source': {
                        'relation': {'name': 'parent'},
                        'place_id': parent_id,
                        'children': []
                    }
                }]
            }
        }

        # Mock the maxID function
        def mock_maxID(es, idx):
            return 1000

        # Correct the way the mock values are returned
        def mock_search(index, body):
            print(f"Mock search query: {body}")
            if body['query']['bool']['must'][0]['match'].get('place_id') == place_id:
                return es_response_no_hits
            elif body['query']['bool']['must'][0]['match'].get('place_id') == parent_id:
                return es_response_hit
            return es_response_no_hits

        # indexMatch() lives in datasets.services, which does `from whg import settings` (the settings
        # MODULE, not django.conf.settings), so that is the object to patch.
        es = MagicMock()
        es.search.side_effect = mock_search
        es.index.return_value = {'result': 'created'}
        es.update.return_value = {'result': 'updated'}
        with patch('datasets.services.settings.ES_CONN', es), \
             patch('datasets.services.settings.ES_WHG', 'test_index'), \
             patch('datasets.tasks.maxID', side_effect=mock_maxID):

            # Test case: No match found, create new parent
            indexMatch(place_id, user=self.user, task=self.task)

            # Check that the Place object has been indexed as a parent
            self.assertTrue(Place.objects.filter(id=place_id).exists())

            # Check that no CloseMatch records have been created
            self.assertEqual(CloseMatch.objects.filter(place_a_id=place_id).count(), 0)

            # Test case: Match found, create new child and update parent
            another_place = Place.objects.create(
                src_id='def456',
                dataset=self.test_dataset,
                title='Another Place',
                ccodes=['BR']
            )
            another_place_id = another_place.id
            print(f"Created another place with ID: {another_place_id}")

            # Call indexMatch with the correct hit_pid
            indexMatch(another_place_id, hit_pid=parent_id, user=self.user, task=self.task)

            # Check that the Place object has been indexed as a child
            self.assertTrue(Place.objects.filter(id=another_place_id).exists())

            # Debug: Print all CloseMatch records
            close_matches = CloseMatch.objects.all()
            for match in close_matches:
                print(f"CloseMatch: place_a_id={match.place_a_id}, place_b_id={match.place_b_id}")

            # Check that the CloseMatch record has been created
            # (update_close_matches stores the pair with the lower id first)
            a, b = sorted([parent_id, another_place_id])
            self.assertTrue(CloseMatch.objects.filter(place_a_id=a, place_b_id=b).exists())

    print('All tests passed!')

# To run the tests
if __name__ == '__main__':
    from django.core.management import call_command
    call_command('test', __name__)
