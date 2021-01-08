# encoding: utf-8
"""


Django setting required

ELASTICSEARCH_CACHE_PAGESIZE
ELASTICSEARCH_RESULT_LIMIT
CACHE_GENERATING_FLAG
"""
__author__ = 'Richard Smith'
__date__ = '06 Jan 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

# Django Imports
from django.core.cache import cache

# Third-party imports
from elasticsearch.helpers import scan

# Python imports
from hashlib import sha1
import itertools
from collections import namedtuple
import copy


def chunks(iterable, size=10):
    """
    Chunks an iterable into given size chunks
    :param iterable:
    :param size:
    :return:
    """
    iterator = iter(iterable)
    for first in iterator:
        yield itertools.chain([first], itertools.islice(iterator, size - 1))


CacheState = namedtuple('CacheState', ('id', 'cache_value'))

ELASTICSEARCH_CACHE_PAGESIZE = 1000

class DjangoElasticsearchPaginationCache:
    ELASTICSEARCH_RESULT_LIMIT = 10000
    CACHE_GENERATING_FLAG = 'GENERATING'

    def __init__(self, es_client, source_index):
        self.es = es_client
        self.source_index = source_index

    @staticmethod
    def get_cache_id(params):
        """
        Return the cache ID given the search parameters
        :param params: Search parameters
        :type params: QueryDict

        :return: id hash
        :rtype: string
        """
        excluded_keys = ['httpAccept', 'maximumRecords', 'startPage', 'startRecord']

        # Initiate empty hash
        id = sha1()

        # Iterate keys and build the hash
        for key in params:
            if key not in excluded_keys:
                value = params.getlist(key)
                param_string = f'{key}:{",".join(value)}'
                id.update(param_string.encode('utf8'))

        return id.hexdigest()

    def request_exceeds_limit(self, max_results, start_index=1, **kwargs):
        """
        Quick check to see if the current request is going to exceed the
        elasticsearch result limit for random access.

        :param max_results: int
        :param start_index: int
        :param kwargs: additional kwargs

        :return: bool
        """
        return start_index + max_results >= self.ELASTICSEARCH_RESULT_LIMIT

    def get_cache_state(self, params):
        """
        Get the state of the request in the cache

        The request can be:
        - In the cache
        - In the cache but content not yet generated
        - Not in the cache

        :return: CacheState
        Attr:
            - id
            - cache_value

        :rtype: CacheState
        """
        # Generate cache ID
        id = self.get_cache_id(params)

        # Check if cache record present
        cache_response = cache.get(id)

        if cache_response:
            print('in the cache')
            # print(cache_response)
        else:
            print('not in the cache')
            # If cache not present, set a flag to denote the cache is generating
            cache.set(id, self.CACHE_GENERATING_FLAG)

        cached_state = CacheState(id, cache_response)

        return cached_state

    def get_cached_results(self, cache_state, query, max_results, start_index=1, **kwargs):
        """
        Get the required data from the cache and return the requested
        slice

        :param cache_state: response from the cache
        :param query: Elasticsearch query
        :param max_results: Maximum number of results per page
        :param start_index: Start point for results

        :return: List of elasticsearch response dicts
        :rtype: List
        """

        # if cache is present and generating flag set wait and poll cache
        if cache_state.cache_value == self.CACHE_GENERATING_FLAG:
            print('i would wait for a response now')

        # if cache is not present generate cache
        if not cache_state.cache_value:
            self.generate_cache(cache_state, query)

        # Get requested results from cache
        results = self._select_pages(max_results, start_index, cache_state.cache_value)

        # return results
        return results

    def generate_cache(self, cache_state, query):
        """
        Retrieve all records for the given ID and save in cache with pages of
        ELASTICSEARCH_LOCALCACHE_PAGESIZE

        :param client: Elasticsearch Client
        :param cache_state: CacheState object. Contains the id and cache_value for the current request
        :param query: elasticsearch query

        :return: page_dict as stored in cache
        """

        # Modify the query for the scan call
        query = copy.deepcopy(query)
        query['size'] = ELASTICSEARCH_CACHE_PAGESIZE

        # scroll all results for given query
        results = scan(self.es, query=query, index=self.source_index, preserve_order=True)

        # separate into pages
        pages = self._generate_pages(results)

        # store in cache
        id = cache_state.id
        cache.set(id, pages)

        return pages

    @staticmethod
    def _generate_pages(results):
        """
        Generate the pages for the cache from the elasticsearch scan result

        :param results: Elasticsearch Scan Results
        :type results: Generator

        :return: Dictionary containing the pages for the request grouped by ELASTICSEARCH_CACHE_PAGESIZE
        """
        page_dict = {}

        for i, chunk in enumerate(chunks(results, ELASTICSEARCH_CACHE_PAGESIZE)):
            print(f'processing page {i}')
            page_dict[i] = list(chunk)

        return page_dict

    @staticmethod
    def _get_page_indices(max_results, start_index, cache_page_size=ELASTICSEARCH_CACHE_PAGESIZE):
        """
        Get the page indices to retrieve the correct documents from the cache

        :param max_results: int
        :param start_index: int
        :param cache_page_size: int

        :return: list
        """

        end_index = start_index + max_results

        # Round down
        start_page = start_index // cache_page_size

        # Round up
        end_page = -(-end_index // cache_page_size)

        return list(range(start_page, end_page))

    def _select_pages(self, max_results, start_index, cache_value, cache_page_size=ELASTICSEARCH_CACHE_PAGESIZE):
        """
        Select the correct pages from the cache and retrive the requested result subset

        :param max_results: int
        :param start_index: int
        :param cache_value: dict
        :param cache_page_size: int
        :return: list
        """

        # Figure out which pages to retieve from cache
        page_indices = self._get_page_indices(max_results, start_index, cache_page_size)

        results = []

        for index in page_indices:
            page = cache_value.get(index)
            if page:
                results.extend(page)

        result_start = start_index - (page_indices[0] * cache_page_size)
        result_end = result_start + max_results

        return results[result_start:result_end]
