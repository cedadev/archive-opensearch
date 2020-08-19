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

ELASTICSEARCH_INDEX = 'opensearch-files-double-nested'
ELASTICSEARCH_COLLECTION_INDEX = 'opensearch-collections-double-nested'
APPLICATION_ID = 'opensearch'

THREDDS_HOST = 'data.cci.ceda.ac.uk'

SOLR_HOST = 'https://esgf-index1.ceda.ac.uk/solr'
SOLR_CORE = 'datasets'

OPENSEARCH_BACKEND = 'elasticsearch'
