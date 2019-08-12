# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '06 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django_opensearch.opensearch.backends import NamespaceMap, Param, FacetSet
from .collection_map import COLLECTION_MAP
from ..solr_connection import SolrConnection
import os
from pydoc import locate
from django_opensearch.constants import DEFAULT
from ..util import pairwise
import copy


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


class SolrFacetSet(FacetSet):
    """
    Class to provide opensearch URL template with facets and parameter options
    """

    facet_values = {}

    # List of facets to exclude from value aggregation
    exclude_list = ['uuid', 'bbox', 'startDate', 'endDate']

    base_query = {
        'q': '*:*',
        'fq': []
    }

    def _build_query(self, params, **kwargs):
        query = copy.deepcopy(self.base_query)

        # Starting index
        query['offset'] = (kwargs['start_index'] - 1) if kwargs['start_index'] > 0 else 0

        # Maximum records per page
        query['limit'] = kwargs['max_results']

        # Search parameters
        for param in params:
            if param == 'query':
                query['q'] = params[param]

            elif param == 'startDate':
                pass
            elif param == 'endDate':
                pass
            elif param == 'bbox':
                pass
            else:
                facet = self.facets.getlist(param)

                if facet:
                    field_path = param if facet is DEFAULT else facet

                    query['fq'].append(
                        f'{field_path}:{params[param]}'
                    )

        return query

    def _build_properties(self, hits, params, base_url):
        results = []

        for hit in hits:

            entry = {
                'type': 'Feature',
                'id': f'{base_url}?collectionId={params["collectionId"]}&uuid={hit["id"]}',
                'properties': {
                    'title': hit['title'],
                    'identifier': hit['id'],
                    'updated': hit['_timestamp'],
                    'data_node': hit['data_node'],
                    'version': hit['version'],
                    'number_of_files': hit['number_of_files'],
                }
            }

            results.append(entry)

        return results

    def get_facet_values(self):
        """
        Perform aggregations to get the range of possible values
        for each facet to put in the description document.
        :return dict: List of values for each facet
        """

        field_to_facet_map = {}

        values = {}
        query = {
            'fq': f'project:{self.path}',
            'facet.field': [],
            'rows': 0
        }

        for facet in self.facets:
            if facet not in self.exclude_list:
                # Get the field name for the facet data
                value = self.facets[facet]

                # If field name is same as facet, add facet to query
                if value is DEFAULT:
                    query['facet.field'].append(facet)

                # If field name is different to facet, add field name and create reverse mapping
                else:
                    query['facet.field'].append(value)
                    field_to_facet_map[value] = facet


                query['facet.field'].append(facet if value is DEFAULT else value)

        results = SolrConnection().search(**query)

        for facet_field in results.facets['facet_fields']:

            f2f_map = field_to_facet_map.get(facet_field)

            facet_name = facet_field if f2f_map is None else f2f_map

            values[facet_name] = [
                {
                    'label': f'{field} ({count})',
                    'value': f'{field}'
                }
                for field, count in pairwise(results.facets['facet_fields'][facet_field])]

            self.facet_values = values

    def search(self, params, **kwargs):
        """
        Search interface to the CMIP5 collection
        :param params: Opensearch parameters
        :param kwargs:
        :return:
        """

        query = self._build_query(params, **kwargs)

        response = SolrConnection().search(**query)

        hits = response.docs

        results = self._build_properties(hits, params=params, base_url=kwargs['uri'])

        return response.hits, results
