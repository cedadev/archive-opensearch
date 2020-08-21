# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '06 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


COLLECTION_MAP = {
    'CMIP5': dict(handler='django_opensearch.opensearch.backends.solr.facets.ESGFFacets'),
    'GeoMIP': dict(handler='django_opensearch.opensearch.backends.solr.facets.ESGFFacets'),
    'PMIP3': dict(handler='django_opensearch.opensearch.backends.solr.facets.ESGFFacets'),
    'TAMIP': dict(handler='django_opensearch.opensearch.backends.solr.facets.ESGFFacets'),
    'clipc': dict(handler='django_opensearch.opensearch.backends.solr.facets.ESGFFacets'),
    'eucleia': dict(handler='django_opensearch.opensearch.backends.solr.facets.ESGFFacets'),
    'CORDEX': dict(handler='django_opensearch.opensearch.backends.solr.facets.CORDEXFacets'),
    'obs4MIPs': dict(handler='django_opensearch.opensearch.backends.solr.facets.Obs4MIPsFacets'),
    'specs': dict(handler='django_opensearch.opensearch.backends.solr.facets.SPECSFacets'),
}