# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '06 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import SolrFacetSet
from archive_opensearch.django_opensearch.constants import DEFAULT


class CORDEXFacets(SolrFacetSet):
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

        # Cordex specific
        'domain': DEFAULT,
        'rcmName': 'rcm_name',
        'rcmVersion': 'rcm_version'
    }

