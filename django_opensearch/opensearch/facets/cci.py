# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '29 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import FacetSet
from django_opensearch.opensearch.elasticsearch_connection import ElasticsearchConnection
import copy
from django_opensearch.constants import DEFAULT

class CCIFacets(FacetSet):
    """

    """

    facets = {
        'ecv': DEFAULT,
        'frequency': 'projects.time_coverage_resolution.keyword',
        'institute': 'projects.institution.keyword',
        'processingLevel': 'projects.processing_level.keyword',
        'productString': 'projects.product_string.keyword',
        'productVersion': 'projects.product_version.keyword',
        'dataType': 'projects.data_type.keyword',
        'sensor': DEFAULT,
        'platform': DEFAULT

    }

    def get_facet_set(self):
        """
        Used to build the description document. Get available facets
        for this collection and add values where possible.
        :return list: List of parameter object for each facet
        """

        # Returns list of parameter objects
        facet_set = super().get_facet_set()
        facet_set_with_vals = []

        # Get the aggregated values for each facet
        values = self.get_facet_values()

        for param in facet_set:
            values_list = values.get(param.name)

            # Add the values list to the parameter if it exists
            if values_list is not None:
                param.value_list = values_list

            facet_set_with_vals.append(param)

        return facet_set_with_vals

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

        for hit in hits:
            entry = {}

            entry['entry_id'] = f'collectionId={params["collectionId"]}&uuid={ hit["_id"] }'
            entry['title'] = hit['_source']['info']['name']
            entry['updated'] = hit['_source']['info']['last_modified']
            results.append(entry)

        return es_search['hits']['total'], results

    def _build_query(self, params, **kwargs):

        # Deep copy to avoid aliasing
        query = copy.deepcopy(self.base_query)

        query['query']['bool']['must'].append({
            'match_phrase_prefix': {
                'info.directory.analyzed': self.path
            }
        })

        query['from'] = (kwargs['start_index'] - 1) if kwargs['start_index'] > 0 else 0

        # Set number of results
        query['size'] = kwargs['max_results']

        for param in params:

            if param == 'q':
                query['query']['bool']['must'].append({
                    'multi_match': {
                        'query': params[param],
                        'fields': ['info.phenomena.names', 'info.phenomena.var_id']
                    }
                })
            elif param == 'startDate':

                query['query']['bool']['filter'].append({
                    'range': {
                        self.facets.get(param): {
                            'gte': params[param]
                        }
                    }
                })

            elif param == 'endDate':

                query['query']['bool']['filter'].append({
                    'range': {
                        self.facets.get(param): {
                            'lte': params[param]
                        }
                    }
                })

            elif param == 'bbox':

                coordinates = params[param].split(',')

                # Coordinates supplied top-left (lon,lat), bottom-right (lon,lat)
                query['query']['bool']['filter'].append({
                    'geo_bounding_box': {
                        self.facets.get(param): {
                            'top_left': {
                                'lat': coordinates[1],
                                'lon': coordinates[0]
                            },
                            'bottom_right': {
                                'lat': coordinates[3],
                                'lon': coordinates[2]
                            }
                        }
                    }
                })

            else:
                facet = self.facets.get(param)

                if facet is not None:
                    es_path = f'projects.{param}' if facet is DEFAULT else facet

                    query['query']['bool']['must'].append({
                        'match_phrase': {
                            es_path: params[param]
                        }
                    })

        return query
