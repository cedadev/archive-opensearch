# encoding: utf-8
"""
Cache primer for large datasets as it takes too long in real-time data access
"""
__author__ = 'Richard Smith'
__date__ = '07 Jan 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient

from elasticsearch.helpers import scan

from django.core.management.base import BaseCommand, CommandError
from django.core.cache import cache
from django.conf import settings

import itertools
from datetime import datetime
import asyncio


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


ELASTICSEARCH_CACHE_PAGESIZE = 1000


class Command(BaseCommand):
    help = 'Downloads vocabs from vocab server to json file'

    def handle(self, *args, **options):

        es = CEDAElasticsearchClient()

        query= {'query': {'bool': {'must': [{'exists': {'field': 'projects.opensearch'}}, {'exists': {'field': 'info'}}, {'bool': {'should': [{'match_phrase': {'projects.opensearch.ecv': 'SST'}}, {'match_phrase': {'projects.opensearch.ecv': 'OC'}}], 'minimum_should_match': 1}}, {'term': {'projects.opensearch.datasetId': '908017206d085b33b12789dcdea6cc92cac586dc'}}], 'should': [], 'filter': []}}, 'sort': [{'info.directory': {'order': 'asc'}}, {'info.name': {'order': 'asc'}}], 'size': ELASTICSEARCH_CACHE_PAGESIZE}

        start = datetime.now()
        results = scan(es, query=query, index=settings.ELASTICSEARCH_INDEX, preserve_order=False)

        page_dict = {}

        for i, chunk in enumerate(chunks(results, ELASTICSEARCH_CACHE_PAGESIZE)):
            print(f'processing page {i}')
            if i > 0 and i % 10 == 0:
                interim = datetime.now()
                print(f'{i} pages took: {interim-start}' )
            page_dict[i] = list(chunk)

        end = datetime.now()
        cache.set('89d2962171c53d7d58939127dc17c70875a3e23a', page_dict, 3600)

        print(f'Total scan Processing Time {end-start}')

        start = datetime.now()
        loop = asyncio.get_event_loop()
        search_page_dict = loop.run_until_complete(self.generate_cache_object(es, query))
        # search_page_dict = self.generate_cache_object(es, query)
        end = datetime.now()
        print(f'Total search_after Processing Time {end-start}')

    def get_page(self,es, query):

        print('processing page')
        results = es.search(body=query, index=settings.ELASTICSEARCH_INDEX)
        hits = results['hits']['hits']
        # print(hits[0]['_id'])

        search_after = hits[-1]['sort']

        return hits, search_after

    def generate_cache_object(self, es, query):

        total = 59724

        first_page, search_after = self.get_page(es, query)

        cache_object = {0: first_page}

        for i in range(1,total//ELASTICSEARCH_CACHE_PAGESIZE):
            query['search_after'] = search_after
            page, search_after = self.get_page(es, query)

            cache_object[i] = page
            query['search_after'] = search_after

        return cache_object