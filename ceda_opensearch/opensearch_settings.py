# encoding: utf-8
"""
Constants for the specific opensearch implementation
"""
__author__ = 'Richard Smith'
__date__ = '20 Mar 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

SHORT_NAME = 'CEDA Opensearch'
LONG_NAME = 'CEDA Opensearch'
DESCRIPTION = 'Opensearch interface to the CEDA archive'
TAGS = ['CEDA','NERC']
DEVELOPER = 'CEDA'

ELASTICSEARCH_INDEX = 'opensearch-files'
ELASTICSEARCH_COLLECTION_INDEX = 'opensearch-collections'
APPLICATION_ID = 'opensearch'
ELASTICSEARCH_CONNECTION_PARAMS = {
    'timeout': 30
}

THREDDS_HOST = 'https://data.cci.ceda.ac.uk'

SOLR_HOST = 'https://esgf-index1.ceda.ac.uk/solr'
SOLR_CORE = 'datasets'

OPENSEARCH_BACKEND = 'elasticsearch'

DATA_BRIDGE_URL = 'https://eo-data-bridge.ceda.ac.uk'

EXTERNAL_DATA_SOURCES = ['https://wui.cmsaf.eu/s']
