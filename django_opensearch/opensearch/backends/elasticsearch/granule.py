# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jul 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .facets.base import HandlerFactory
from .collection import Collection
from django_opensearch import settings

class Granule:

    def __init__(self, path=None):
        if path:
            self.handler = HandlerFactory().get_handler(path)

    def get_facet_set(self):
        return self.handler.get_facet_set()

    def get_example_queries(self):
        return self.handler.get_example_queries()

    def search(self, params, **kwargs):
        collection = Collection(settings.TOP_LEVEL_COLLECTION)

        path = collection.get_path(params.get('collectionId'))

        handler = HandlerFactory().get_handler(path)

        return handler.search(params, **kwargs)