# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '31 Jul 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import BaseLookupHandler
from django.core.cache import cache
from cci_tagger.facets import Facets
from django.conf import settings
import json
import re

CAMEL_PATTERN = re.compile(r'(?<!^)(?=[A-Z])')
LABEL_PATTERN = re.compile(r'(?P<label>.+)\s\((?P<count>\d+)')


def camel_to_snake(value):
    """
    Convert camel case to snake case
    :param value: String to convert
    :return: converted string
    """
    return CAMEL_PATTERN.sub('_', value).lower()


class CCILookupHandler(BaseLookupHandler):
    MAPPABLE_FACETS = [
        'ecv',
        'processingLevel',
        'dataType',
    ]

    def __init__(self):

        self.facets = Facets.from_json(cache.get_or_set('cci_vocabs', self._load_facets, timeout=43000))

    @staticmethod
    def _load_facets():

        with open(settings.VOCAB_CACHE_FILE) as reader:
            data = json.load(reader)
        return data

    def lookup_values(self, facet, value_list):
        """
        Perform term lookup and conversion
        :param facet:
        :param value_list:
        :return:
        """

        # Reduce calls to lookup
        if facet in self.MAPPABLE_FACETS:

            for value in value_list:
                pref_label = self.facets.get_pref_label_from_alt_label(camel_to_snake(facet), value['value'])
                if pref_label:
                    count = value['label'].split()[-1]
                    value['label'] = f'{pref_label.title()} {count}'

        return value_list
