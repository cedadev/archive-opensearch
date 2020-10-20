# encoding: utf-8
"""
Default settings for the django_opensearch application. These can be overridden by
settings in the project level django settings.py file.
"""

__author__ = 'Richard Smith'
__date__ = '19 Mar 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

SHORT_NAME = 'Django Opensearch'
LONG_NAME = 'Django Opensearch'
DESCRIPTION = 'Opensearch application for Django'
TAGS = []
DEVELOPER = ''
SYNDICATION_RIGHT = 'open'
ADULT_CONTENT = 'false'
LANGUAGE = 'en-uk'
INPUT_ENCODING = 'UTF-8'
OUTPUT_ENCODING = 'UTF-8'

RESPONSE_TYPES = ['application/geo+json','application/atom+xml']
DEFAULT_RESPONSE_TYPE = 'application/atom+xml'

MAX_RESULTS_PER_PAGE = 10
DEFAULT_START_PAGE = 1
DEFAULT_START_RECORD = 1

OS_NAMESPACE = 'http://a9.com/-/spec/opensearch/1.1/'
OS_PREFIX = 'os'
OS_ROOT_TAG = 'OpenSearchDescription'

GEO_NAMESPACE_TAG = 'geo'
EO_NAMESPACE_TAG = 'eo'
TIME_NAMESPACE_TAG = 'time'
CEDA_NAMESPACE_TAG = 'ceda'
DUBLIN_CORE_NAMESPACE_TAG = 'dc'
