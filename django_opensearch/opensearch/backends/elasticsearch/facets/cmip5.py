# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '21 Mar 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import ElasticsearchFacetSet
from django_opensearch.constants import DEFAULT


class CMIP5Facets(ElasticsearchFacetSet):
    """

    """

    facets = {
        'project': DEFAULT,
        'product': DEFAULT,
        'institute': DEFAULT,
        'model': DEFAULT,
        'experiment': DEFAULT,
        'timeFrequency': DEFAULT,
        'realm': DEFAULT,
        'cmipTable': DEFAULT,
        'ensemble': DEFAULT,
        'version': DEFAULT,
        'uuid': '_id',
        'bbox': 'info.spatial.coordinates.coordinates',
        'startDate': 'info.temporal.start_time',
        'endDate': 'info.temporal.end_time',
    }

