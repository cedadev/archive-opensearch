# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '21 Mar 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import FacetSet
from django_opensearch.opensearch.elasticsearch_connection import ElasticsearchConnection
from django_opensearch import settings
import copy


class CMIP5Facets(FacetSet):
    """

    """
    path = '/badc/cmip5/data'

    facets = {
        'project': 'default',
        'product': 'default',
        'institute': 'default',
        'model': 'default',
        'experiment': 'default',
        'timeFrequency': 'default',
        'realm': 'default',
        'cmipTable': 'default',
        'ensemble': 'default',
        'version': 'default',
        'uuid': '_id'
    }

    def get_facet_set(self):
        facet_set = super().get_facet_set()
        facet_set_with_vals = []
        values = self.get_facet_values()

        for param in facet_set:
            values_list = values.get(param.name)

            # Add the values list if it exists
            if values_list is not None:
                param.value_list = values[param.name]

            facet_set_with_vals.append(param)

        return facet_set_with_vals

    def get_facet_values(self):

        values = {}

        query = {
            'aggs': {},
            'size': 0
        }

        for facet in self.facets:
            if facet not in ['uuid']:
                query['aggs'][facet] = {
                    'terms': {
                        'field': f'projects.{facet}.keyword',
                        'size': 1000
                    }
                }

        aggs = ElasticsearchConnection().search(query)

        for result in aggs['aggregations']:
            values[result] = [{'label': f"{bucket['key']} ({bucket['doc_count']})", 'value': bucket['key']} for bucket
                              in aggs['aggregations'][result]['buckets']]

        return values

    def search(self, params, **kwargs):
        results = []

        query = self._build_query(params, **kwargs)

        es_search = ElasticsearchConnection().search(query)

        hits = es_search['hits']['hits']

        for hit in hits:
            entry = {}

            entry['entry_id'] = f'collectionId={params["collectionId"]}&uuid={ hit["_id"] }'
            entry['title'] = hit['_source']['info']['name']
            entry['updated'] = hit['_source']['info']['last_modified']
            results.append(entry)

        return es_search['hits']['total'], results

    def _build_query(self, params, **kwargs):

        query = copy.deepcopy(self.base_query)

        query['query']['bool']['must'].append({
            'match_phrase_prefix': {
                'info.directory.analyzed': self.path
            }
        })

        query['from'] = kwargs['start_index'] -1 if kwargs['start_index'] > 0 else 0

        for param in params:

            if param == 'q':
                query['query']['bool']['must'].append({
                    'match': {
                        'info.phenomena.names': params[param]
                    }
                })

            else:
                facet = self.facets.get(param)

                if facet is not None:
                    es_path = f'projects.{param}' if facet is 'default' else facet

                    query['query']['bool']['must'].append({
                        'match_phrase': {
                            es_path: params[param]
                        }
                    })

        return query
