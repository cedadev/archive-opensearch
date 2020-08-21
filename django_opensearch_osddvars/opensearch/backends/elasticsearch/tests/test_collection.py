# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '08 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from unittest import TestCase
from ..collection import Collection


class TestCollection(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.collection = Collection()
        cls.kwargs = {
            'uri': 'http://localhost'
        }

    def test_search_by_id(self):
        params = {
            'parentIdentifier': '1'
        }

        result_count, results = self.collection.search(params, **self.kwargs)

        self.assertEqual(1, result_count)

    def test_search_by_query(self):

        result_count, results = self.collection.search({'query':'cool'}, **self.kwargs)
        self.assertEqual(2, result_count)

        result_count, results = self.collection.search({'query':'cci'}, **self.kwargs)
        self.assertEqual(1, result_count)

    def test_get_path(self):

        path = self.collection.get_path('1')
        self.assertEqual(path, '/badc/cmip5/data')



