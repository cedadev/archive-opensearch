# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jul 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from archive_opensearch.django_opensearch.opensearch.backends.elasticsearch.facets.collection_map import (
    DEFAULT_COLLECTION,
)

from .facets.base import HandlerFactory


class Granule:
    """
    Class to handle the granule level search and response

    :param path: Filepath of collection:
    :type path: str
    """

    def __init__(self, path=None):
        """
        :param path: filepath
        :type path: str
        """
        if path:
            self.handler = HandlerFactory().get_handler(path)
        if self.handler is None:
            self.handler = HandlerFactory().get_handler(DEFAULT_COLLECTION)

    def get_facet_set(self, search_params):
        """
        Used to build the description document. Get available facets
        for this collection and add values where possible.

        :return list: List of parameter object for each facet
        :rtype: list
        """

        return self.handler.get_facet_set(search_params)

    def get_example_queries(self):
        """
        Generate example queries as part of the description document from the
        facets available in the current context.

        :return: List of examples
        :rtype: list
        """
        return self.handler.get_example_queries()

    def search(self, params, **kwargs):
        """
        Search the collection based on the query parameters

        :param params: URL parameters
        :type params: <class 'django.http.request.QueryDict'>
        :param kwargs:

        :return: search results
        :rtype: SearchResults
        """
        return self.handler.search(params, **kwargs)