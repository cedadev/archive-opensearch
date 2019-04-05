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


def parse(input_d, pre=None):
    pre = pre[:] if pre else []

    if isinstance(input_d, dict):
        for key, value in input_d.items():
            if isinstance(value, dict):
                for d in parse(value, [key] + pre):
                    yield d
            elif isinstance(value, list) or isinstance(value, tuple):
                for v in value:
                    for d in parse(v, [key] + pre):
                        yield d
            else:
                yield pre + [key, value]
    else:
        yield input_d

class ElasticsearchConnection:

    def __init__(self, type='file', index=settings.ELASTICSEARCH_INDEX, host=settings.ELASTICSEARCH_HOST):
        self.index = index
        self.es = Elasticsearch([host])

    def search(self, query):
        return self.es.search(index=self.index, body=query)

    def get_fields(self):

        mapping = self.es.indices.get_mapping(index=self.index)

        a = parse(mapping)

        for i in a:
            print(i)










