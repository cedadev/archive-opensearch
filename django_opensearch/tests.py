from django.test import TestCase
from django.test import Client
from django.test import override_settings
import xmltodict


# Create your tests here.
@override_settings(ELASTICSEARCH_CONNECTION_PARAMS={
    'timeout': 30,
    'hosts': ['https://elasticsearch.ceda.ac.uk'],
    'ca_certs': None
})
class OpensearchTestCase(TestCase):
    REQUEST_BASE = '/opensearch/request'

    DESCRIPTION_BASE = '/opensearch/description.xml'

    @classmethod
    def setUpClass(cls):
        cls.client = Client()

        super().setUpClass()

    @classmethod
    def get_url(cls, base, **kwargs):
        qs = ''

        if kwargs:
            if base == cls.REQUEST_BASE and not kwargs.get('httpAccept'):
                kwargs['httpAccept'] = 'application/geo%2Bjson'
            query_list = [f'{k}={v}' for k, v in kwargs.items()]

            qs = f'?{"&".join(query_list)}'

        return f'{base}{qs}'


class PageSizeTest(OpensearchTestCase):

    def test_response_page_size(self):
        response = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='aa09603e91b44f3cb1573c9dd415e8a8',
                maximumRecords=40
            )
        )

        response_json = response.json()
        self.assertEqual(len(response_json['features']), 40)


class PaginationTestCase(OpensearchTestCase):

    @staticmethod
    def get_page_ids(features):
        """
        Convert a list of features into a list of ids

        :param features: Opensearch response feature list
        :return: list of ids from those features
        """
        ids = []

        for feature in features:
            id = feature['properties']['identifier']
            ids.append(id)

        return ids

    @staticmethod
    def chunk(iterator, size):
        """
        Split list into chunks of given size
        :param iterator: Input list (list|tuple)
        :param size: window size (int)

        """
        for i in range(0, len(iterator), size):
            yield (iterator[i:i + size])

    @classmethod
    def setUpTestData(cls):

        # Get first 40 results
        response = cls.client.get(
            cls.get_url(
                cls.REQUEST_BASE,
                parentIdentifier='aa09603e91b44f3cb1573c9dd415e8a8',
                maximumRecords=40
            )
        )

        results = response.json()

        cls.pages = {}

        for i, page in enumerate(cls.chunk(results['features'], 10), start=1):
            cls.pages[i] = cls.get_page_ids(page)

    def get_page_feature_ids(self, page_number=1):

        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='aa09603e91b44f3cb1573c9dd415e8a8',
                startPage=page_number
            )
        )

        features = results.json()['features']
        self.assertEqual(len(features), 10)

        return self.get_page_ids(features)

    def test_page_1(self):

        page_ids = self.get_page_feature_ids()
        self.assertListEqual(page_ids, self.pages[1])

    def test_page_2(self):

        page_ids = self.get_page_feature_ids(2)
        self.assertListEqual(page_ids, self.pages[2])

    def test_page_3(self):
        page_ids = self.get_page_feature_ids(3)
        self.assertListEqual(page_ids, self.pages[3])

    def test_page_4(self):
        page_ids = self.get_page_feature_ids(4)
        self.assertListEqual(page_ids, self.pages[4])

    def test_page1to2(self):
        self.test_page_1()
        self.test_page_2()

    def test_page2to3(self):
        self.test_page_2()
        self.test_page_3()

    def test_page3to4(self):
        self.test_page_3()
        self.test_page_4()

    def test_page4to3(self):
        self.test_page_4()
        self.test_page_3()

    def test_page_3to3(self):
        self.test_page_3()
        self.test_page_3()

    def test_page1_2_3_4(self):
        self.test_page_1()
        self.test_page_2()
        self.test_page_3()
        self.test_page_4()

    def test_page4_3_2_1(self):
        self.test_page_4()
        self.test_page_3()
        self.test_page_2()
        self.test_page_1()

    def test_page3_2_2(self):
        self.test_page_3()
        self.test_page_2()
        self.test_page_2()

    def test_page2_3_3(self):
        self.test_page_3()
        self.test_page_2()
        self.test_page_2()

    def test_page_past_10k(self):
        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='42f7230ab55641cdac1bba84eabd446a',
                startPage=99,
                maximumRecords=100
            )
        )
        self.assertEqual(results.status_code, 200)

        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='42f7230ab55641cdac1bba84eabd446a',
                startPage=100,
                maximumRecords=100
            )
        )
        self.assertEqual(results.status_code, 200)

        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='42f7230ab55641cdac1bba84eabd446a',
                startPage=101,
                maximumRecords=100
            )
        )
        self.assertEqual(results.status_code, 200)


class DateRangeTestCase(OpensearchTestCase):

    @classmethod
    def setUpTestData(cls):
        description = cls.client.get(
            cls.get_url(
                cls.DESCRIPTION_BASE,
                parentIdentifier='4eb4e801424a47f7b77434291921f889'
            )
        )

        description = xmltodict.parse(description.content)
        params = description['OpenSearchDescription']['Url'][1]['param:Parameter']

        cls.start_date = [param for param in params if param['@name'] == 'startDate'][0]['@minInclusive']
        cls.end_date = [param for param in params if param['@name'] == 'endDate'][0]['@maxInclusive']

    def test_start_date_query(self):
        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='4eb4e801424a47f7b77434291921f889',
                startDate=self.start_date
            )
        )

        self.assertEqual(results.status_code, 200)

        results = results.json()

        self.assertGreater(results['totalResults'], 10)

    def test_end_date_query(self):
        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='4eb4e801424a47f7b77434291921f889',
                endDate=self.end_date
            )
        )

        self.assertEqual(results.status_code, 200)

        results = results.json()

        self.assertGreater(results['totalResults'], 10)



class ReturnCodeTestCase(OpensearchTestCase):

    def test_404_response_from_request(self):

        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier= 'asdas'
            )
        )

        self.assertEqual(results.status_code, 404)

    def test_404_response_from_description(self):
        results = self.client.get(
            self.get_url(
                self.DESCRIPTION_BASE,
                parentIdentifier='asdas'
            )
        )

        self.assertEqual(results.status_code, 404)

        
class JSONResponseTestCase(OpensearchTestCase):

    def test_subtitle(self):
        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='cci'
            )
        )

        self.assertEqual(results.status_code, 200)
        results = results.json()

        feature_id = results['features'][0]['properties']['identifier']

        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='cci',
                uuid=feature_id
            )
        )
        self.assertEqual(results.status_code, 200)
        results = results.json()
        self.assertEqual(results['subtitle'], 'Showing 1 - 1 of 1')


class ResponseTreeTestCase(OpensearchTestCase):

    def test_root_description(self):
        results = self.client.get(
            self.get_url(
                self.DESCRIPTION_BASE
            )
        )
        self.assertEqual(results.status_code, 200)

        content = xmltodict.parse(results.content)
        desc = content['OpenSearchDescription']['Description']
        self.assertEqual(desc, "Opensearch interface to the CEDA archive")

    def test_filter_collection(self):
        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                httpAccept='application/geo%2Bjson'
            )
        )
        self.assertEqual(results.status_code, 200)
        results = results.json()
        ids = PaginationTestCase.get_page_ids(results['features'])
        self.assertEqual(ids, ['cci'])

    def test_collection_response(self):
        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier='cci',
                httpAccept='application/geo%2Bjson'
            )
        )
        self.assertEqual(results.status_code, 200)

    def test_granule_response(self):
        results = self.client.get(
            self.get_url(
                self.REQUEST_BASE,
                parentIdentifier="2e656d34d016414c8d6bced18634772c",
                httpAccept="application/geo%2Bjson"
            )
        )
        self.assertEqual(results.status_code, 200)

