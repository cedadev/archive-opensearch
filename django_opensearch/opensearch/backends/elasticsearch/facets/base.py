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
    exclude_list = ['uuid', 'bbox', 'startDate', 'endDate']


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

    def _build_query(self, params, **kwargs):

        # Deep copy to avoid aliasing
        query = copy.deepcopy(self.base_query)

        query['query']['bool']['must'].append({
            'match_phrase_prefix': {
                'info.directory.analyzed': self.path
            }
        })

        query['from'] = (kwargs['start_index'] - 1) if kwargs['start_index'] > 0 else 0

        # Set number of results
        query['size'] = kwargs['max_results']

        for param in params:

            if param == 'query':
                query['query']['bool']['must'].append({
                    'multi_match': {
                        'query': params[param],
                        'fields': ['info.phenomena.names', 'info.phenomena.var_id']
                    }
                })
            elif param == 'startDate':

                query['query']['bool']['filter'].append({
                    'range': {
                        self.facets.get(param): {
                            'gte': self._isodate(params[param])
                        }
                    }
                })

            elif param == 'endDate':

                query['query']['bool']['filter'].append({
                    'range': {
                        self.facets.get(param): {
                            'lte': self._isodate(params[param])
                        }
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

                if facet:
                    es_path = f'projects.{settings.APPLICATION_ID}.{param}' if facet is DEFAULT else facet
                    search_terms = params.getlist(param)

                    if search_terms:
                        query['query']['bool']['minimum_should_match'] = 1

                    for search_term in params.getlist(param):

                        query['query']['bool']['should'].append({
                            'match_phrase': {
                                es_path: search_term
                            }
                        })

        return query

    def get_facet_values(self):
        """
        Perform aggregations to get the range of possible values
        for each facet to put in the description document.
        :return dict: List of values for each facet
        """

        values = {}

        query = {
            'aggs': {},
            'size': 0
        }

        for facet in self.facets:
            if facet not in self.exclude_list:

                # Get the path to the facet data
                value = self.facets[facet]

                query['aggs'][facet] = {
                    'terms': {
                        'field': f'projects.{settings.APPLICATION_ID}.{facet}.keyword' if value is DEFAULT else value,
                        'size': 1000
                    }
                }

        aggs = ElasticsearchConnection().search(query)

        for result in aggs['aggregations']:
            values[result] = [{'label': f"{bucket['key']} ({bucket['doc_count']})", 'value': bucket['key']} for bucket
                              in aggs['aggregations'][result]['buckets']]

        self.facet_values = values

    def search(self, params, **kwargs):
        """
        Search interface to elasticsearch
        :param params: Opensearch parameters
        :param kwargs:
        :return:
        """

        results = []

        query = self._build_query(params, **kwargs)

        es_search = ElasticsearchConnection().search(query)

        hits = es_search['hits']['hits']

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

        return es_search['hits']['total'], results


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
            return handler(root_path)

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
