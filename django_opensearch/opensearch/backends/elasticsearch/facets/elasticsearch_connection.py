# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '01 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


from elasticsearch import Elasticsearch
from django.conf import settings

class ElasticsearchConnection:

    def __init__(self, index=settings.ELASTICSEARCH_INDEX, host=settings.ELASTICSEARCH_HOST):
        self.index = index
        self.collection_index = settings.ELASTICSEARCH_COLLECTION_INDEX
        self.es = Elasticsearch([host])

    def search(self, query):
        return self.es.search(index=self.index, body=query)

    def search_collections(self, query):
        return self.es.search(index=self.collection_index, body=query)









