# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '01 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


from django.conf import settings
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient


class ElasticsearchConnection:

    def __init__(self, index=settings.ELASTICSEARCH_INDEX):
        self.index = index
        self.collection_index = settings.ELASTICSEARCH_COLLECTION_INDEX
        self.es = CEDAElasticsearchClient(**settings.ELASTICSEARCH_CONNECTION_PARAMS)

    def search(self, query):
        return self.es.search(index=self.index, body=query)

    def search_collections(self, query):
        return self.es.search(index=self.collection_index, body=query)

    def count(self, query):
        return self.es.count(index=self.index, body=query)

    def count_collections(self, query):
        return self.es.count(index=self.collection_index, body=query)








