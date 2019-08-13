# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '08 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from unittest import TestCase
from ..granule import Granule
from django.http.request import QueryDict

class TestGranule(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.path = '/badc/cmip5/data'
        cls.kwargs = {
            'uri': 'http://localhost',
            'start_index': 0,
            'max_results': 10
        }
        cls.collection_id = '1'

    def test_search_query(self):

        params = QueryDict(f'parentIdentifier={self.collection_id}&query=water')
        count, results = Granule().search(params, **self.kwargs)

        self.assertEqual(955672, count)
        self.assertEqual(10, len(results))

    def test_search_uuid(self):

        params = QueryDict(f'parentIdentifier={self.collection_id}&uuid=0b5abdcaf3b8690108beab1b08eb1beba2be7abc')
        count, results = Granule().search(params, **self.kwargs)

        self.assertEqual(1, count)
        self.assertEqual(1, len(results))

    def test_get_facet_set(self):
        facet_set = Granule(self.path).get_facet_set()

        self.assertEqual(18, len(facet_set))

    def test_get_example_queries(self):

        granule = Granule(self.path)
        granule.get_facet_set()
        example_qs = granule.get_example_queries()
        self.assertEqual(10, len(example_qs))