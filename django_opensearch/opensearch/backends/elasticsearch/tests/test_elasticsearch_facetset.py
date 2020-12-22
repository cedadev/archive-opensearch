# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '09 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from unittest import TestCase
from ..facets.base import ElasticsearchFacetSet
from ..facets import CMIP5Facets
from ..facets.elasticsearch_connection import ElasticsearchConnection
from django.http.request import QueryDict


class TestElasticsearchFacetSet(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.efc = CMIP5Facets('/badc/cmip5/data')

    def test__extract_bbox(self):
        spatial = {
            "coordinates": {
                "coordinates": [
                    [
                        0.05,
                        89.742
                    ],
                    [
                        179.99,
                        -78.394
                    ]
                ],
                "type": "envelope"
            }
        }

        coordinates = ElasticsearchFacetSet._extract_bbox(spatial)

        self.assertListEqual(coordinates, [[0.05, -78.394], [179.99, 89.742]])

    def test__extract_time_range(self):
        temporal = {
            "start_time": "1880-01-16T12:00:00",
            "end_time": "1889-12-16T12:00:00"
        }

        time_range = ElasticsearchFacetSet._extract_time_range(temporal)

        self.assertEqual('1880-01-16T12:00:00/1889-12-16T12:00:00', time_range)
        
    def test__isodate(self):
        
        dates = {
            '30/05/2015': '2015-05-30T00:00:00',
            '30-05-2015': '2015-05-30T00:00:00',
            '2015-05-30': '2015-05-30T00:00:00',
            '2015/05/30': '2015-05-30T00:00:00'
        }

        for date, isodate in dates.items():
            self.assertEqual(ElasticsearchFacetSet._isodate(date), isodate)
        

    def run_query(self, params, extra_kwargs={}):

        kwargs = {
            'uri': 'http://localhost',
            'start_index': 0,
            'max_results': 10
        }

        kwargs.update(extra_kwargs)

        query = self.efc.build_query(params, **kwargs)

        return ElasticsearchConnection().search(query)

    def test__build_query_single_query(self):

        params = QueryDict('query=water')
        response = self.run_query(params)

        self.assertGreater(response['hits']['total'], 0)


    def test__build_query_single_start_date(self):

        params = QueryDict('startDate=13/05/2015')
        response = self.run_query(params)

        self.assertGreater(response['hits']['total'], 0)

    def test__build_query_single_end_date(self):

        params = QueryDict('endDate=13/05/2015')
        response = self.run_query(params)

        self.assertGreater(response['hits']['total'], 0)

    def test__build_query_single_bbox(self):

        params = QueryDict('bbox=-180,90,180,-90')
        response = self.run_query(params)

        self.assertGreater(response['hits']['total'], 0)

    def test__build_query_single_facets(self):

        params = QueryDict('product=output')
        response = self.run_query(params)

        self.assertGreater(response['hits']['total'], 0)


    def test__build_query_mutiple_facets(self):

        output_response = self.run_query(QueryDict('product=output'))
        output2_response = self.run_query(QueryDict('product=output2'))

        expected_total = output_response['hits']['total'] + output2_response['hits']['total']

        params = QueryDict('product=output&product=output2')
        multiple_response = self.run_query(params)

        self.assertEqual(expected_total, multiple_response['hits']['total'])

    def test_start_index(self):

        kwargs = {
            'start_index': 3
        }

        base_response = self.run_query({})

        response = self.run_query({}, extra_kwargs=kwargs)

        base_hits = base_response['hits']['hits']

        hits = response['hits']['hits']

        uuid3 = base_hits[2]['_id']
        self.assertEqual(uuid3, hits[0]['_id'])

