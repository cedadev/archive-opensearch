# encoding: utf-8
"""
Map from archive file path to collection handlers
"""
__author__ = 'Richard Smith'
__date__ = '02 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

COLLECTION_MAP = {
    '/badc/cmip5/data': dict(handler='django_opensearch.opensearch.backends.elasticsearch.facets.CMIP5Facets'),
    '/neodc/esacci': dict(handler='django_opensearch.opensearch.backends.elasticsearch.facets.CCIFacets')
}

DEFAULT_COLLECTION = '/neodc/esacci'