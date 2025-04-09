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
import logging

CAMEL_PATTERN = re.compile(r'(?<!^)(?=[A-Z])')
LABEL_PATTERN = re.compile(r'(?P<label>.+)\s\((?P<count>\d+)')

logger = logging.getLogger(__name__)


def camel_to_snake(value):
    """
    Convert camel case to snake case

    :param value: String to convert
    :return: converted string
    """
    return CAMEL_PATTERN.sub('_', value).lower()


class CCILookupHandler(BaseLookupHandler):
    """
    Class to handle lookups from the extracted terms to the preferred lables
    in the SKOS vocabulary.

    :attr MAPPABLE_FACETS: Facets to perform the lookup on. Others are ignored to
    reduce the overhead of lookups.

    """
    MAPPABLE_FACETS = [
        'ecv',
        'processingLevel',
        'dataType',
    ]

    def __init__(self):

        # Retrieve cached values, cache lasts for 24 hours as vocab server doesn't change much
        #self.facets = Facets.from_json(cache.get_or_set('cci_vocabs', self._load_facets, timeout=86400))

        # Emergency fix for this specific version.
        import requests
        self.facets = Facets.from_json(requests.get('https://raw.githubusercontent.com/cedadev/archive-opensearch/refs/heads/project_facet/ceda_opensearch/facets_json.json').json())

    @staticmethod
    def _load_facets():
        """
        Load the facets from the server

        :return: vocab mapping
        :rtype: dict
        """

        data = {}
        if settings.READ_FROM_VOCAB:
            try:
                data = Facets().to_json()
            except Exception as e:
                logger.error(f'Failed to get vocabs from vocab server: {e}')

        # If vocabs successfully retrieved, save to disk
        if data:
            try:
                with open(settings.VOCAB_CACHE_FILE, 'w') as writer:
                    json.dump(data, writer)
            except Exception as e:
                logger.warning(f'Failed to save vocab mapping: {e}')

        # If vocabs not successfully retrieved from live server. Try
        # to retrieve the disk cache from the last successful attempt
        if not data:
            try:
                with open(settings.VOCAB_CACHE_FILE) as reader:
                    data = json.load(reader)

            except Exception as e:
                logger.critical('Unable to retrieve vocab cache from live server or disk: {e}')

        return data

    def lookup_values(self, facet, value_list):
        """
        Perform term lookup and conversion

        :param facet: the facet you wish to perform the lookup on
        :type facet: str
        :param value_list: the list of values for that facet
        :type value_list: list(dict)

        :return: Modified value list
        """

        # Reduce calls to lookup
        if facet in self.MAPPABLE_FACETS:

            for value in value_list:
                pref_label = self.facets.get_pref_label_from_alt_label(camel_to_snake(facet), value['value'])
                print('CAMEL: ',facet, pref_label)
                if pref_label:
                    count = value['label'].split()[-1]
                    value['label'] = f'{pref_label.title()} {count}'

        return value_list
