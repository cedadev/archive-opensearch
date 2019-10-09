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
        'parentIdentifier': 'parent_identifier.keyword',
        'title': 'title.keyword'
    }

    base_query = {
        'query': {
            'bool': {
                'must': [],
                'should': [],
                'filter': [],
                'must_not':[]
            }
        }
    }

    def __init__(self):

        self.data = [
            {
                'parentIdentifier': '1',
                'title': 'CMIP5',
                'description': 'cmip5 is very cool',
                'path': '/badc/cmip5/data',
                'startDate': '01-01-1583',
                'endDate': '01-01-5091'
            },
            {
                'parentIdentifier': '2',
                'title': 'CCI',
                'description': 'cci is very cool',
                'path': '/neodc/esacci',
                'startDate': '20-01-2019',
                'endDate': '21-03-2019'
            }
        ]

    def _query_elasticsearch(self, query):
        return ElasticsearchConnection().search_collections(query)

    def _build_query(self, params, **kwargs):
        query = super()._build_query(params, **kwargs)

        if params.get('parentIdentifier') is None:
            query['query']['bool']['must_not'].append({
                'exists': {
                    'field': 'parent_identifier'
                }
            })

        return query

    def search(self, params, **kwargs):
        results = []

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

            results.append(entry)

        return es_search['hits']['total'], results

    def get_path(self, collection_id):

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

