# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jul 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .facets.base import ElasticsearchFacetSet
from .facets.elasticsearch_connection import ElasticsearchConnection
from .facets.base import HandlerFactory
from django_opensearch.constants import DEFAULT
from django.http import Http404
from django_opensearch.opensearch.utils.aggregation_tools import get_thredds_aggregation, get_aggregation_capabilities


def collection_search(search_params):
    """
    Function to determine whether the current search is a collection
    or a granule

    :param search_params: URL params
    :type search_params: <class 'django.http.request.QueryDict'>

    :return bool:
    """

    if 'parentIdentifier' not in search_params:
        return True

    if 'parentIdentifier' in search_params:
        query = {
            "query": {
                "term": {
                    "parent_identifier": search_params['parentIdentifier']
                }
            }
        }

        return bool(ElasticsearchConnection().es.count(index='opensearch-collections', body=query)['count'])

    return False


class Collection(ElasticsearchFacetSet):
    """
    Class to handle the collection search and response

    :attr facets:
    :attr base_query:
    """

    facets = {
        'parentIdentifier': DEFAULT,
        'title': DEFAULT
    }

    base_query = {
        'query': {
            'bool': {
                'must': [],
                'should': [],
                'filter': [],
                'must_not': []
            }
        }
    }

    @staticmethod
    def get_es_path(facet_path, facet_name):
        """
        Return the path to the target in elasticsearch index. Extracted
        to method to allow subclasses to modify the behaviour.

        :param facet_path: route to facet in the target index
        :type facet_path: str

        :param facet_name: name of the facet being processed
        :type facet_name: str

        :return: path to item in elasticsearch index
        :rtype: str
        """

        return f'{facet_name}' if facet_path is DEFAULT else facet_path

    @staticmethod
    def get_date_field(key):
        """
        Date field for the target index

        :param key: one of 'start'|'end'|'range'
        :type key: str

        :return: field name attached to key
        :rtype: str
        """

        date_fields = {
            'start': 'start_date',
            'end': 'end_date',
            'range': 'time_frame'
        }

        return date_fields[key]

    def query_elasticsearch(self, query):
        """
        Execute the query
        :param query: Elasticsearch query dict
        :type query: dict

        :return: elasticsearch response
        :rtype: dict
        """

        return ElasticsearchConnection().search_collections(query)

    def build_query(self, params, **kwargs):
        """
        Helper method to build the elasticsearch query
        :param params: Search parameters
        :type params: dict

        :param kwargs:

        :return: elasticsearch query
        :rtype: dict
        """

        if params.get('bbox'):
            self.facets['bbox'] = 'bbox.coordinates'

        query = super().build_query(params, **kwargs)

        pid = params.get('parentIdentifier')

        if pid:

            query['query']['bool']['must'].append({
                'term': {
                    'parent_identifier': pid
                }
            })

        else:
            query['query']['bool']['must_not'].append({
                'exists': {
                    'field': 'parent_identifier'
                }
            })

        return query

    def search(self, params, **kwargs):
        """
        Search interface to elasticsearch

        :param params: Opensearch parameters
        :param kwargs:

        :return: search results
        :rtype: SearchResults
        """

        if self.path:
            handler = HandlerFactory().get_handler(self.path)
            self.facets.update(handler.facets)
            kwargs['handler'] = handler

        query = self.build_query(params, **kwargs)

        es_search = ElasticsearchConnection().search_collections(query)

        hits = es_search['hits']['hits']

        results = self.build_representation(hits, params, **kwargs)

        # Use the response from the query to get the total unless > 10k
        # In this case will need to query size directly
        if es_search['hits']['total']['relation'] == 'eq':
            total_hits = es_search['hits']['total']['value']
        else:
            # Size keys are not compatible with the count query
            query.pop('size')
            total_hits = ElasticsearchConnection().count_collections(query)['count']

        return total_hits, results

    def build_representation(self, hits, params, **kwargs):
        """
        Build the dict representation of the granule and return the
        result list

        :param hits: Elasticsearch query hits
        :param params: url params
        :param kwargs:

        :return: Result list
        :rtype: list
        """

        base_url = kwargs['uri']
        handler = kwargs.pop('handler', None)

        results = []

        for hit in hits:
            if handler:
                entry = handler.build_collection_entry(hit, params, base_url)
            else:
                entry = self.build_collection_entry(hit, params, base_url)
            results.append(entry)

        return results

    @staticmethod
    def get_path(collection_id):
        """
        Return the root filepath for the given collection ID

        :param collection_id: Collection ID
        :type collection_id: str

        :return: filepath
        :rtype: str
        :raises Http404: Collection not found
        """

        query = {
            'query': {
                'term': {
                    'collection_id': collection_id
                }
            },
            'size': 1
        }

        result = ElasticsearchConnection().search_collections(query)
        if result['hits']['hits']:
            return result['hits']['hits'][0]['_source']['path']

        else:
            raise Http404(f'Collection not found with id: {collection_id}')
