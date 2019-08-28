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


class CCIFacets(ElasticsearchFacetSet):
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


