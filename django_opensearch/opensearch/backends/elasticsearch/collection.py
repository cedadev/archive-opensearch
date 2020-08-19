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
from django.conf import settings


def collection_search(search_params):
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

        return bool(ElasticsearchConnection().es.count(index=settings.ELASTICSEARCH_COLLECTION_INDEX, body=query)['count'])

    return False


class Collection(ElasticsearchFacetSet):
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
    def _get_es_path(facet_path, facet_name):
        return f'{facet_name}' if facet_path is DEFAULT else facet_path

    @staticmethod
    def get_date_field(key):
        """
        Date field for the target index
        :param key: one of 'start'|'end'|'range'
        :return: field name attached to key
        """

        date_fields = {
            'start': 'start_date',
            'end': 'end_date',
            'range': 'time_frame'
        }

        return date_fields[key]

    def _query_elasticsearch(self, query):
        return ElasticsearchConnection().search_collections(query)

    def _build_query(self, params, **kwargs):
        if params.get('bbox'):
            self.facets['bbox'] = 'bbox.coordinates'

        query = super()._build_query(params, **kwargs)

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

        if self.path:
            handler = HandlerFactory().get_handler(self.path)
            self.facets.update(handler.facets)
            kwargs['handler'] = handler

        query = self._build_query(params, **kwargs)

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

        query = {
            'query': {
                'term': {
                    'collection_id.keyword': collection_id
                }
            },
            'size': 1
        }

        result = ElasticsearchConnection().search_collections(query)
        if result['hits']['hits']:
            return result['hits']['hits'][0]['_source']['path']

        else:
            raise Http404(f'Collection not found with id: {collection_id}')
