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
from django_opensearch.opensearch.utils.aggregation_tools import get_thredds_aggregation, get_aggregation_capabilities


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

        results = []

        for hit in hits:
            source = hit['_source']
            entry = {
                'type': 'FeatureCollection',
                'id': f'{base_url}/request?uuid={hit["_id"]}',
                'properties': {
                    'title': source['title'],
                    'identifier': source["collection_id"],
                    'links': {
                        'search': [
                            {
                                'title': 'Opensearch Description Document',
                                'href': f'{base_url}/description.xml?parentIdentifier={source["collection_id"]}',
                                'type': 'application/xml'
                            }
                        ],
                        'related': [
                            {
                                'title': 'ftp',
                                'href': f'ftp://anon-ftp.ceda.ac.uk{source["path"]}',
                            }
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
                    },
                    {
                        'title': 'Dataset Information',
                        'href': f'https://catalogue.ceda.ac.uk/uuid/{source["collection_id"]}'
                    }
                ]

            if source.get('aggregations'):
                entry['properties']['aggregations'] = []

                for aggregation in source.get('aggregations'):

                    agg = {
                        'id': aggregation['id'],
                        'type': 'Feature',
                        'properties': {
                            'links': {
                                'described_by': [
                                    {
                                        'title': 'THREDDS Catalog',
                                        'href': get_thredds_aggregation(aggregation['id'], format='html')
                                    }
                                ],
                                'related': get_aggregation_capabilities(aggregation)
                            }
                        }
                    }

                    entry['properties']['aggregations'].append(agg)

            if params.get('parentIdentifier'):
                entry['id'] = f'{base_url}/request?parentIdentifier={params["parentIdentifier"]}&uuid={hit["_id"]}'

            if source.get('variables'):
                entry['properties']['variables'] = self._extract_variables(source['variables'])

            results.append(entry)

        return results

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
