# encoding: utf-8
"""
Handles the elasticsearch connection and wraps some of the search functionality
"""
__author__ = 'Richard Smith'
__date__ = '01 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


from django.conf import settings
from elasticsearch import Elasticsearch


class ElasticsearchConnection:
    """
    Elasticsearch Connection class. Uses `CEDAElasticsearchClient <https://github.com/cedadev/ceda-elasticsearch-tools>`_

    :param index: files index (default: settings.ELASTICSEARCH_INDEX)
    :type index: str

    """

    def __init__(self, index=settings.ELASTICSEARCH_INDEX):
        self.index = index
        self.collection_index = settings.ELASTICSEARCH_COLLECTION_INDEX
        self.es = Elasticsearch(
            hosts=settings.ELASTICSEARCH_HOSTS,
            headers={'x-api-key':settings.ES_API_KEY},
            **settings.ELASTICSEARCH_CONNECTION_PARAMS)

    def search(self, query):
        """
        Search the files index

        :param query: Elasticsearch file query
        :type query: dict

        :return: Elasticsearch response
        :rtype: dict
        """
        return self.es.search(index=self.index, body=query)

    def search_collections(self, query):
        """
        Search the collections index

        :param query: Elasticsearch collection query
        :type query: dict

        :return: Elasticsearch response
        :rtype: dict
        """
        return self.es.search(index=self.collection_index, body=query)

    def count(self, query):
        """
        Return the hit count from the current file query

        :param query: Elasticsearch file query
        :type query: dict

        :return: Elasticsearch count response
        :rtype: dict
        """
        return self.es.count(index=self.index, body=query)

    def count_collections(self, query):
        """
        Return the hit count from the current collection query

        :param query: Elasticsearch collection query
        :type query: dict

        :return: Elasticsearch count response
        :rtype: dict
        """
        return self.es.count(index=self.collection_index, body=query)








