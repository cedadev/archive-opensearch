# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '29 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import ElasticsearchFacetSet
from django_opensearch.constants import DEFAULT
from django_opensearch import settings
from .elasticsearch_connection import ElasticsearchConnection


class CCIFacets(ElasticsearchFacetSet):
    """

    """

    facets = {
        'ecv': DEFAULT,
        'frequency': f'projects.{settings.APPLICATION_ID}.time_coverage_resolution.keyword',
        'institute': f'projects.{settings.APPLICATION_ID}.institution.keyword',
        'processingLevel': f'projects.{settings.APPLICATION_ID}.processing_level.keyword',
        'productString': f'projects.{settings.APPLICATION_ID}.product_string.keyword',
        'productVersion': f'projects.{settings.APPLICATION_ID}.product_version.keyword',
        'dataType': f'projects.{settings.APPLICATION_ID}.data_type.keyword',
        'sensor': DEFAULT,
        'platform': DEFAULT

    }

    def get_datasets(self, query, **kwargs):
        """
        Retrieve all datasets from aggregations
        :param query:
        :return:
        """

        datasets = []

        # Add the aggregation onto the standard query
        query['aggs'] = {
            'datasets': {
                'composite': {
                    'size': 100,
                    'sources': [
                        {
                            'dataset': {
                                'terms': {
                                    'field': 'projects.opensearch.dataset_id.keyword'
                                }
                            }
                        }
                    ]
                }
            }
        }

        es = ElasticsearchConnection()

        results = es.search(query)

        # Extract the first batch
        datasets.extend(results['aggregations']['datasets']['buckets'])

        # Append the after key
        query['aggs']['datasets']['composite']['after'] = results['aggregations']['datasets']['after_key']

        # Scroll aggregations
        while results['aggregations']['datasets']['buckets']:

            # Get the next batch
            results = es.search(query)

            # Extract the datasets
            datasets.extend(results['aggregations']['datasets']['buckets'])

            # Append the after key
            try:
                query['aggs']['datasets']['composite']['after'] = results['aggregations']['datasets']['after_key']

            except KeyError:
                break

        # Sort results based on count
        sorted_datasets = sorted(datasets, key=lambda k: k['doc_count'], reverse=True)

        # Calculate page
        start_index = (kwargs['start_index'] -1) if kwargs['start_index'] >0 else 0
        max_records = kwargs['max_results']

        end_index = start_index + max_records

        # start inclusive end exclusive
        return len(sorted_datasets), sorted_datasets[start_index:end_index]

    def _build_query(self, params, **kwargs):
        """
        Filter file results by path
        :param params:
        :param kwargs:
        :return:
        """

        query = super()._build_query(params, **kwargs)

        query['query']['bool']['must'].append({
            'match_phrase_prefix': {
                'info.directory.analyzed': self.path
            }
        })

        print(query)

        return query

    # def search(self, params, **kwargs):
    #     """
    #     Search interface to elasticsearch
    #     :param params: Opensearch parameters
    #     :param kwargs:
    #     :return:
    #     """
    #
    #     results = []
    #
    #     query = self._build_query(params, **kwargs)
    #
    #     total_results, datasets = self.get_datasets(query, **kwargs)
    #
    #     for dataset in datasets:
    #         entry = {
    #             'type': 'Feature',
    #             'id': '',
    #             'properties': {
    #                 'title': dataset['key']['dataset'],
    #                 'identifier': dataset['key']['dataset'],
    #                 'file_count': dataset['doc_count']
    #             }
    #         }
    #         results.append(entry)
    #
    #
    #
    #
    #     es_search = ElasticsearchConnection().search(query)
    #
    #     hits = es_search['hits']['hits']
    #
    #     base_url = kwargs['uri']
    #
    #     for hit in hits:
    #         source = hit['_source']
    #         entry = {
    #             'type': 'Feature',
    #             'id': f'{base_url}?parentIdentifier={params["parentIdentifier"]}&uuid={ hit["_id"] }',
    #             'properties': {
    #                 'title': source['info']['name'],
    #                 'identifier': hit["_id"],
    #                 'updated': source['info']['last_modified']
    #             }
    #         }
    #
    #         if source['info'].get('temporal'):
    #             entry['properties']['date'] = self._extract_time_range(source['info']['temporal'])
    #
    #         if source['info'].get('spatial'):
    #             # SW - NE (lon,lat)
    #             entry['bbox'] = self._extract_bbox(source['info']['spatial'])
    #         results.append(entry)
    #
    #     return total_results, results


