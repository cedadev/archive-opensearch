# encoding: utf-8
"""
Constants for the specific opensearch implementation
"""
__author__ = "Richard Smith"
__date__ = "20 Mar 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import os

import yaml
from cci_tag_scanner.facets import Facets
from cci_tag_scanner.utils.elasticsearch import es_connection_kwargs
from elasticsearch import Elasticsearch


def get_from_conf(env):
    """
    Get elasticsearch index value from config file
    if present.
    """

    deploy_settings = "/etc/django/settings.d/20-runtime-settings.yaml"

    if not os.path.isfile(deploy_settings):
        return None

    with open(deploy_settings) as f:
        conf = yaml.safe_load(f)
    try:
        for prop in env.split("."):
            conf = conf.get(prop, {})
        return conf
    except:
        raise ValueError("Invalid config provided")


SHORT_NAME = "CEDA Opensearch"
LONG_NAME = "CEDA Opensearch"
DESCRIPTION = "Opensearch interface to the CEDA archive"
TAGS = ["CEDA", "NERC"]
DEVELOPER = "CEDA"

ELASTICSEARCH_INDEX = get_from_conf("elasticsearch.index") or "opensearch-files"
ELASTICSEARCH_COLLECTION_INDEX = "opensearch-collections"
APPLICATION_ID = "opensearch"
ELASTICSEARCH_CONNECTION_PARAMS = {"timeout": 30}
ELASTICSEARCH_HOSTS = ["https://elasticsearch.ceda.ac.uk"]

OPENEO_HOST = "https://api.stac.164.30.69.113.nip.io"

READ_FROM_VOCAB = False

THREDDS_HOST = "https://data.cci.ceda.ac.uk"

SOLR_HOST = "https://esgf-index1.ceda.ac.uk/solr"
SOLR_CORE = "datasets"

OPENSEARCH_BACKEND = "elasticsearch"

DATA_BRIDGE_URL = "https://eo-data-bridge.ceda.ac.uk"

EXTERNAL_DATA_SOURCES = ["https://wui.cmsaf.eu/s"]

FACETS = Facets()


class ElasticsearchConnection:
    """
    Elasticsearch Connection class. Uses `CEDAElasticsearchClient <https://github.com/cedadev/ceda-elasticsearch-tools>`_

    :param index: files index (default: settings.ELASTICSEARCH_INDEX)
    :type index: str

    """

    def __init__(self, api_key, index=ELASTICSEARCH_INDEX):
        self.index = index
        self.collection_index = ELASTICSEARCH_COLLECTION_INDEX
        self.es = Elasticsearch(
            **es_connection_kwargs(hosts=ELASTICSEARCH_HOSTS, api_key=api_key, **ELASTICSEARCH_CONNECTION_PARAMS)
        )

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
