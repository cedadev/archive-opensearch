# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '06 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import SolrFacetSet
from django_opensearch.constants import DEFAULT


class ESGFFacets(SolrFacetSet):
    facets = {
        # Standard
        'uuid': 'id',
        'bbox': DEFAULT,
        'startDate': 'datetime_start',
        'endDate': 'datetime_stop',

        # ESGF Facets
        'institute': DEFAULT,
        'model': DEFAULT,
        'experiment': DEFAULT,
        'timeFrequency': 'time_frequency',
        'project': DEFAULT,
        'product': DEFAULT,
        'ensemble': DEFAULT,
        'realm': DEFAULT,
        'cmorTable': 'cmor_table',
        'cfStandardName': 'cf_standard_name',
        'variableLongName': 'variable_long_name',
        'dataNode': 'data_node',
        'variable': DEFAULT,
        'version': DEFAULT,
    }

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
                    'date': f'{hit["datetime_start"]}/{hit["datetime_stop"]}',
                    'abstract': hit['description'][0],
                    'data_node': hit['data_node'],
                    'version': hit['version'],
                    'number_of_files': hit['number_of_files'],
                }
            }

            results.append(entry)

        return results