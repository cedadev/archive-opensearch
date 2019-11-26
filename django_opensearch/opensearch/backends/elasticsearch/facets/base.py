# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '21 Mar 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .collection_map import COLLECTION_MAP
from pydoc import locate
import os
from django_opensearch.constants import DEFAULT
from django_opensearch.opensearch.backends import NamespaceMap, Param, FacetSet
from django_opensearch import settings
from .elasticsearch_connection import ElasticsearchConnection
import copy
from dateutil.parser import parse as date_parser
from django_opensearch.opensearch.utils import NestedDict


class ElasticsearchFacetSet(FacetSet):
    """
    Class to provide opensearch URL template with facets and parameter options
    """

    base_query = {
        'query': {
            'bool': {
                'must': [
                    {
                        'exists': {
                            'field':f'projects.{settings.APPLICATION_ID}'

                        }
                    }
                ],
                'should': [],
                'filter': []
            }
        }
    }

    agg_query = {

    }

    facet_values = {}

    # List of facets to exclude from value aggregation
    exclude_list = ['uuid', 'bbox', 'startDate', 'endDate','title','parentIdentifier']


    @staticmethod
    def _extract_bbox(coordinates):
        coordinates = coordinates['coordinates']['coordinates']

        sw = [coordinates[0][0], coordinates[1][1]]
        ne = [coordinates[1][0], coordinates[0][1]]
        return [sw,ne]

    @staticmethod
    def _extract_time_range(temporal):
        return f"{temporal['start_time']}/{temporal['end_time']}"

    @staticmethod
    def _isodate(date):
        return date_parser(date).isoformat()

    @staticmethod
    def _get_es_path(facet, param):
        """
        Return the path to the target in elasticsearch index. Extracted
        to method to allow subclasses to modify the behaviour.
        :param facet: facet path
        :param param: parameter
        :return str: path to item in elasticsearch index
        """
        return f'projects.{settings.APPLICATION_ID}.{param}' if facet is DEFAULT else facet

    @staticmethod
    def get_date_field(key):
        """
        Date field for the target index
        :param key: one of 'start'|'end'|'range'
        :return: field name attached to key
        """
        date_fields = {
            'start': 'info.temporal.start_time',
            'end': 'info.temporal.end_time',
            'range': 'info.temporal.time_frame'
        }

        return date_fields[key]

    def get_handler(self):
        return HandlerFactory().get_handler(self.path)

    def _build_query(self, params, **kwargs):

        # Deep copy to avoid aliasing
        query = copy.deepcopy(self.base_query)

        if kwargs.get('start_index'):
            query['from'] = (kwargs['start_index'] - 1) if kwargs['start_index'] > 0 else 0

        if kwargs.get('max_results'):
            # Set number of results
            query['size'] = kwargs['max_results']

        for param in params:

            if param == 'query' and params[param]:
                query['query']['bool']['must'].append({
                    'simple_query_string': {
                        'query': params[param]
                    }
                })

            elif param == 'bbox':

                coordinates = params[param].split(',')

                # Coordinates supplied top-left (lon,lat), bottom-right (lon,lat)
                query['query']['bool']['filter'].append({
                    'geo_bounding_box': {
                        self.facets.get(param): {
                            'top_left': {
                                'lat': coordinates[1],
                                'lon': coordinates[0]
                            },
                            'bottom_right': {
                                'lat': coordinates[3],
                                'lon': coordinates[2]
                            }
                        }
                    }
                })

            else:
                facet = self.facets.get(param)

                if facet and param not in self.exclude_list:

                    es_path = self._get_es_path(facet=facet, param=param)
                    search_terms = params.getlist(param)

                    if search_terms:

                        if len(search_terms) == 1:
                            # Equal to AND query
                            query['query']['bool']['must'].append({
                                'match_phrase': {
                                    es_path: search_terms[0]
                                }
                            })

                        else:
                            # Equal to AND (a OR b) query where there should be at least one match
                            andor = {'bool':{'should':[], 'minimum_should_match': 1}}

                            for search_term in search_terms:
                                andor['bool']['should'].append(
                                    {
                                        'match_phrase': {
                                            es_path: search_term
                                        }
                                    })

                            query['query']['bool']['must'].append(andor)

        # Add date filter
        date_filter = NestedDict()

        if 'startDate' in params:
            date_filter.nested_put([
                'range',self.get_date_field('range'),'gte'],
                self._isodate(params['startDate'])
            )

        if 'endDate' in params:
            date_filter.nested_put([
                'range', self.get_date_field('range'), 'lte'],
                self._isodate(params['endDate'])
            )

        if date_filter:
            query['query']['bool']['filter'].append(
                date_filter.data
                )


        return query

    def _query_elasticsearch(self, query):
        return ElasticsearchConnection().search(query)

    @staticmethod
    def _process_aggregations(aggs):
        """
        Process the aggregations and return the facet values

        :param aggs: elasticsearch query response
        :return: {} Dict of values with the facet as the key
        """
        values = {}

        if aggs.get('aggregations'):

            for result in aggs['aggregations']:

                # Start and end date buckets have a different format so
                # handle them differently
                if result == 'startDate':

                    # Reject null values
                    if aggs['aggregations'][result].get('value_as_string'):
                        values['startDate'] = {'extra_kwargs': {
                            'minInclusive': aggs['aggregations'][result].get('value_as_string'),
                            'pattern': '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$'
                        }}

                elif result == 'endDate':

                    # Reject null values
                    if aggs['aggregations'][result].get('value_as_string'):
                        values['endDate'] = {'extra_kwargs': {
                            'maxInclusive': aggs['aggregations'][result].get('value_as_string'),
                            'pattern': '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$'
                        }}

                else:
                    values[result] = {
                        'values': [
                            {
                                'label': f"{bucket['key']} ({bucket['doc_count']})",
                                'value': bucket['key']
                            } for bucket in aggs['aggregations'][result]['buckets']
                        ]
                    }

        return values

    def get_facet_values(self, search_params):
        """
        Perform aggregations to get the range of possible values
        for each facet to put in the description document.
        :return dict: List of values for each facet
        """

        query = self._build_query(search_params)

        query.update({
            'aggs': {},
            'size': 0
        })

        for facet in self.facets:
            if facet not in self.exclude_list:
                # Get the path to the facet data
                value = self.facets[facet]

                query['aggs'][facet] = {
                    'terms': {
                        'field': f'{facet}.keyword',
                        'size': 1000
                    }
                }

        # Get start and end time ranges
        query['aggs']['startDate'] = {
            "min": {"field": self.get_date_field('start')}
        }

        query['aggs']['endDate'] = {
            "max": {"field": self.get_date_field('end')}
        }

        aggs = self._query_elasticsearch(query)

        values = self._process_aggregations(aggs)

        self.facet_values = values


    def search(self, params, **kwargs):
        """
        Search interface to elasticsearch
        :param params: Opensearch parameters
        :param kwargs:
        :return:
        """

        query = self._build_query(params, **kwargs)

        es_search = ElasticsearchConnection().search(query)

        hits = es_search['hits']['hits']

        results = self.build_representation(hits, params, **kwargs)

        return es_search['hits']['total'], results

    def build_representation(self, hits, params, **kwargs):

        results = []
        base_url = kwargs['uri']

        for hit in hits:

            source = hit['_source']

            entry = {
                'type': 'Feature',
                'id': f'{base_url}?parentIdentifier={params["parentIdentifier"]}&uuid={ hit["_id"] }',
                'properties': {
                    'title': source['info']['name'],
                    'identifier': hit["_id"],
                    'updated': source['info']['last_modified']
                }
            }

            if source['info'].get('temporal'):
                entry['properties']['date'] = self._extract_time_range(source['info']['temporal'])

            if source['info'].get('spatial'):
                # SW - NE (lon,lat)
                entry['bbox'] = self._extract_bbox(source['info']['spatial'])

            results.append(entry)

        return results

class HandlerFactory:

    def __init__(self):
        self.map = COLLECTION_MAP

    def get_handler(self, path):
        """
        Takes a system path and returns the file extensions to look for and
        the correct handler for the collection.

        :param path:
        :return: granule extension, handler class
        """

        collection, root_path = self.get_collection_map(path)
        if collection is not None:
            handler = locate(collection['handler'])
            return handler(path)

    def get_collection_map(self, path):
        """
        Takes an arbitrary path and returns a collection path
        :param path: Path to the data of interest
        :return: The value from the map object
        """

        while path not in self.map and path != '/':
            path = os.path.dirname(path)

        # No match has been found
        if path == '/':
            return None

        return self.map[path], path
