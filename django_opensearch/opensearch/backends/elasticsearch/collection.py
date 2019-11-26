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

        return bool(ElasticsearchConnection().es.count(index='opensearch-collections', body=query)['count'])

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
    def _get_es_path(facet, param):
        return f'{param}' if facet is DEFAULT else facet

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

    # def get_facet_values(self, search_params):
    #     """
    #     Perform aggregations to get the range of possible values
    #     for each facet to put in the description document.
    #     :return dict: List of values for each facet
    #     """
    #
    #     query = self._build_query(search_params)
    #
    #     query.update({
    #         'aggs': {},
    #         'size': 0
    #     })
    #
    #     for facet in self.facets:
    #         if facet not in self.exclude_list:
    #             # Get the path to the facet data
    #             value = self.facets[facet]
    #
    #             query['aggs'][facet] = {
    #                 'terms': {
    #                     'field': f'{facet}.keyword',
    #                     'size': 1000
    #                 }
    #             }
    #
    #     # Get start and end time ranges
    #     query['aggs']['startDate'] = {
    #         "min": {"field": self.get_date_field('start')}
    #     }
    #
    #     query['aggs']['endDate'] = {
    #         "max": {"field": self.get_date_field('end')}
    #     }
    #
    #     aggs = self._query_elasticsearch(query)
    #
    #     values = self._process_aggregations(aggs)
    #
    #     self.facet_values = values

    def search(self, params, **kwargs):
        results = []

        if self.path:
            handler = HandlerFactory().get_handler(self.path)
            self.facets.update(handler.facets)

        query = self._build_query(params, **kwargs)

        es_search = ElasticsearchConnection().search_collections(query)

        hits = es_search['hits']['hits']

        base_url = kwargs['uri']

        for hit in hits:
            source = hit['_source']
            entry = {
                'type': 'FeatureCollection',
                'id': f'{base_url}/request?uuid={ hit["_id"] }',
                'properties': {
                    'title': source['title'],
                    'identifier': source["collection_id"],
                    'links': {
                        'search': [
                            {
                                'title': 'Opensearch Description Document',
                                'href': f'{base_url}/description.xml?parentIdentifier={source["collection_id"]}',
                                'type': 'application/xml'}
                        ]
                    }
                }
            }

            if source.get('start_date'):
                entry['properties']['date'] = f"{source['start_date']}/{source['end_date']}"

            if source.get('collection_id') != 'cci':
                entry['properties']['links']['describedby'] = [
                            {
                                'title': 'ISO19115',
                                'href': f'https://catalogue.ceda.ac.uk/export/xml/{source["collection_id"]}.xml'
                            }
                        ]

            results.append(entry)

        return es_search['hits']['total'], results

    def build_representation(self, hits, **kwargs):
        base_url = kwargs['uri']

    @staticmethod
    def get_path(collection_id):

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
