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
from abc import abstractmethod
from django_opensearch import settings
import os
from django_opensearch.constants import DEFAULT
from django_opensearch.opensearch.backends import NamespaceMap, Param, FacetSet
from .elasticsearch_connection import ElasticsearchConnection


class ElasticsearchFacetSet(FacetSet):
    """
    Class to provide opensearch URL template with facets and parameter options
    """

    default_facets = {
        'query': DEFAULT,
        'maximumRecords': DEFAULT,
        'startPage': DEFAULT,
        'startRecord': DEFAULT,
        'startDate': DEFAULT,
        'endDate': DEFAULT,
    }

    base_query = {
        'query': {
            'bool': {
                'must': [
                    {
                        'term': {
                            'projects.application_id.keyword': settings.APPLICATION_ID
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

    def __init__(self, path):
        self.path = path

    @property
    @abstractmethod
    def facets(self):
        return {}

    @staticmethod
    def _extract_bbox(coordinates):
        coordinates = coordinates['coordinates']['coordinates']

        sw = [coordinates[0][0], coordinates[1][1]]
        ne = [coordinates[1][0], coordinates[0][1]]
        return [sw,ne]

    @staticmethod
    def _extract_time_range(temporal):
        return f"{temporal['start_time']}/{temporal['end_time']}"

    @abstractmethod
    def _build_query(self, params, **kwargs):
        pass

    def _get_facet_set(self):
        """
        Turns facets into parameter objects
        :return:
        """

        # Merge the facet dictionaries into one
        facets = {**self.default_facets, **self.facets}

        return [Param(*NamespaceMap.get_namespace(facet)) for facet in facets]

    def get_facet_set(self):
        """
        Used to build the description document. Get available facets
        for this collection and add values where possible.
        :return list: List of parameter object for each facet
        """

        # Returns list of parameter objects
        facet_set = self._get_facet_set()
        facet_set_with_vals = []

        # Get the aggregated values for each facet
        self.get_facet_values()

        for param in facet_set:
            values_list = self.facet_values.get(param.name)

            # Add the values list to the parameter if it exists
            if values_list is not None:
                param.value_list = values_list

            facet_set_with_vals.append(param)

        return facet_set_with_vals

    def get_example_queries(self):
        examples = []
        for facet in self.facets:
            values_list = self.facet_values.get(facet)
            if values_list is not None:
                examples.append({facet:values_list[0]['value']})

        return examples

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
                        'field': f'projects.{facet}.keyword' if value is DEFAULT else value,
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
        Search interface to the CMIP5 collection
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
                'id': f'{base_url}?collectionId={params["collectionId"]}&uuid={ hit["_id"] }',
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

    @staticmethod
    def get_facet(facet_list, key):

        for facet in facet_list:
            if facet[0] == key:

                if len(facet) == 1:
                    return facet[0], None

                return facet


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
