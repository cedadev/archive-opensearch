# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '26 Jul 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from pysolr import Solr
from django.conf import settings


class SolrConnection:

    def __init__(self):
        self.solr = Solr(f'{settings.SOLR_HOST}/{settings.SOLR_CORE}')

    def search(self, q='*:*', **kwargs):
        return self.solr.search(q, **kwargs)


"""
https://lucene.apache.org/solr/guide/7_6/json-request-api.html

# Perhaps need to build a light wrapper to allow python JSON search with solr

Example:

import requests

r = requests.post(url, json={"query":"*:*", "limit":0,"facet":{"projects":{"terms":{"field":"project","limit":2, "offset":1}}}})
r.json()
{
    'responseHeader': {
        'status': 0,
        'QTime': 5,
        'params': {
            'json': '{"query": "*:*", "limit": 0, "facet": {"projects": {"terms": {"field": "project", "limit": 2, "offset": 1}}}}'
        }
    },
    'response': {
        'numFound': 481614,
        'start': 0,
        'docs': []
    },
    'facets': {
        'count': 481614,
        'projects': {
            'buckets': [
                {
                    'val': 'CMIP5',
                    'count': 48143
                },
                {
                    'val': 'CORDEX',
                    'count': 2611
                }
            ]
        }
    }
}
"""

