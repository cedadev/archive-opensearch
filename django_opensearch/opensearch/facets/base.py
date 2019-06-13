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
from django_opensearch.opensearch.elasticsearch_connection import ElasticsearchConnection
from django_opensearch.constants import DEFAULT


class Param:

    def __init__(self, name, value, required=False, value_list=[], **kwargs):
        self.name = name
        self.value = value
        self.required = required
        self.value_list = value_list

        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def val(self):
        return f'{{{self.value}}}'


class NamespaceMap:
    map = {
        'q': {'name': 'searchTerms'},
        'maximumRecords': dict(name='count'),
        'collectionId': dict(name='uid', namespace=settings.GEO_NAMESPACE_TAG),
        'startDate': dict(name='start', namespace=settings.TIME_NAMESPACE_TAG),
        'endDate': dict(name='end', namespace=settings.TIME_NAMESPACE_TAG),
        'uuid': dict(name='uid', namespace=settings.CEDA_NAMESPACE_TAG),
        'bbox': dict(name='box', namespace=settings.GEO_NAMESPACE_TAG)
    }

    @classmethod
    def get_namespace(cls, key):

        mapping = cls.map.get(key)

        if mapping is not None:
            namespace = mapping.get('namespace')
            if namespace is not None:
                value = f'{namespace}:{mapping["name"]}'
            else:
                value = mapping['name']

            return key, value

        return key, key


class FacetSet:
    """
    Class to provide opensearch URL template with facets and parameter options
    """

    default_facets = {
        'q': DEFAULT,
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

    # List of facets to exclude from value aggregation
    exclude_list = ['uuid', 'bbox', 'startDate', 'endDate']

    def __init__(self, path):
        self.path = path

    @property
    @abstractmethod
    def facets(self):
        return {}

    def get_facet_set(self):
        """
        Turns facets into parameter objects
        :return:
        """

        # Merge the facet dictionaries into one
        facets = {**self.default_facets, **self.facets}

        return [Param(*NamespaceMap.get_namespace(facet)) for facet in facets]

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

        return values

    @abstractmethod
    def search(self, params):
        pass

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
