# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jul 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django_opensearch.opensearch.backends import FacetSet
from django_opensearch.constants import DEFAULT
from .solr_connection import SolrConnection


def pairwise(l):
    i = 0
    while i < len(l):
        yield l[i], l[i + 1]
        i += 2


COLLECTION_METADATA = {
    'CMIP5': {
        'description': 'Coupled Model Intercomparison Project Phase 5.'
                       'Includes related MIP data from EUCLIPSE, GeoMIP, LUCID, PMIP3 and TAMIP'
    },
    'CORDEX': {
        'description': 'Coordinated Regional Climate Downscaling Experiment'
    },
    'GeoMIP': {
    },
    'PMIP3': {
    },
    'TAMIP': {
    },
    'clipc': {
        'title': 'CLIPC',
        'description': 'CLIPC: Climate Information Platform for Copernicus'
    },
    'eucleia': {
        'title': 'EUCLEIA',
        'description': 'EUCLEIA: EUropean CLimate and weather Events: Interpretation and Attribution'
    },
    'obs4MIPS': {
        'description': 'Observations for Climate Model Intercomparisons'
    },
    'specs': {
        'title': 'SPECS',
        'description': 'Seasonal-to-decadal climate Prediction for the improvement of European Climate Services'
    },
}


class Collection(FacetSet):

    facets = {
        'collectionId': DEFAULT
    }

    def get_facet_values(self):

        values = {}
        query = {
            'facet.field': [],
            'rows': 0
        }

        for facet in self.facets:
            if facet not in self.exclude_list:
                # Get the field name for the facet data
                value = self.facets[facet]

                query['facet.field'].append(facet if value is DEFAULT else value)

        results = SolrConnection().search(**query)

        for facet_field in results.facets['facet_fields']:
            values[facet_field] = [
                {
                    'label': f'{field} ({count})',
                    'value': f'{field}'
                }
                for field, count in pairwise(results.facets['facet_fields'][facet_field])]

            self.facet_values = values

    def search(self, params, **kwargs):

        results = []

        extra_params = {
            'rows': 0,
            'facet.field': ['project']
        }

        q = None

        for param in params:

            if param == 'query':
                q = params[param]

        # Execute search
        if q:
            resp = SolrConnection().search(q, **extra_params)
        else:
            resp = SolrConnection().search(**extra_params)

        for collection, count in pairwise(resp.facets['facet_fields']['project']):
            pass



