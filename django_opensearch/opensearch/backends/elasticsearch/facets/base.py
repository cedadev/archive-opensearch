# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '21 Mar 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

# Application Imports
from django_opensearch.constants import DEFAULT
from django_opensearch.opensearch.backends import NamespaceMap, Param, FacetSet
from django_opensearch import settings
from django_opensearch.opensearch.utils import NestedDict
from django_opensearch.opensearch.utils.geo_point import Point, Envelope
from django_opensearch.opensearch.utils.aggregation_tools import get_thredds_aggregation, get_aggregation_capabilities, \
    get_aggregation_search_link
from django_opensearch.opensearch.backends.base import HandlerFactory
from .elasticsearch_connection import ElasticsearchConnection
from django_opensearch.opensearch.backends.elasticsearch.pagination import DjangoElasticsearchPaginationCache

# Django Imports
from django.conf import settings

# Third-party imports
from dateutil.parser import parse as date_parser

# Python Imports
import copy
from collections import namedtuple


class PagingError(Exception):
    """
    Raised when the requested page is not possible
    """
    pass


SearchResults = namedtuple('SearchResults', ('total', 'results', 'search_before', 'search_after'))


class ElasticsearchFacetSet(FacetSet):
    """
    Class to provide opensearch URL template with facets and parameter options

    :attr base_query: Base Elasticsearch Query
    :attr agg_query: Base elasticsearch aggregation query (default: {})
    :attr exclude_list: List of facets to exclude from value aggregation
    """

    base_query = {
        'query': {
            'bool': {
                'must': [
                    {
                        'exists': {
                            'field': f'projects.{settings.APPLICATION_ID}'

                        }
                    },
                    {
                        'exists': {
                            'field': 'info'

                        }
                    }
                ],
                'should': [],
                'filter': []
            }
        },
        'sort': [
            {
                'info.directory': {
                    'order': 'asc'
                }
            },
            {
                'info.name': {
                    'order': 'asc'
                }
            }
        ]
    }

    agg_query = {}

    # List of facets to exclude from value aggregation
    exclude_list = ['uuid', 'bbox', 'startDate', 'endDate', 'title', 'parentIdentifier']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.depcache = DjangoElasticsearchPaginationCache(
            es_client= ElasticsearchConnection().get_client(),
            source_index= settings.ELASTICSEARCH_INDEX
        )

    @staticmethod
    def _extract_bbox(coordinates):
        """
        Extract the bounding box from the elasticsearch response::

            {
                "coordinates" : {
                    "coordinates" : [
                        [
                            -179.979,
                            89.979
                        ],
                        [
                            -0.021,
                            -89.979
                        ]
                ],
                "type" : "envelope"
                }
            }

        :param coordinates: spatial attribute of file from FBI index
        :type coordinates: dict

        :return: object based on input type
        """
        type = coordinates['coordinates']['type']
        coordinates = coordinates['coordinates']['coordinates']

        if type == 'envelope':
            envelope = Envelope(coordinates)
            return envelope.bbox()

        elif type == 'Point':
            point = Point(coordinates)
            return point.bbox()

    @staticmethod
    def _extract_time_range(temporal):
        """
        Extract the time rage from the elasticsearch response::

            {
                "start_time" : "1985-06-25T10:01:26",
                "time_range" : {
                    "gte" : "1985-06-25T10:01:26",
                    "lte" : "1985-06-25T10:01:26"
                },
                "end_time" : "1985-06-25T10:01:26"
            }

        :param temporal: Temporal attribute of file from FBI index
        :type temporal: dict

        :return: ISO 8601 formatted date range
        :rtype: str
        """

        return f"{temporal['start_time']}/{temporal['end_time']}"

    @staticmethod
    def _extract_variables(phenomena):
        """
        Extract the variables from the elasticsearch response::

            [
                {
                    "standard_name" : "time",
                    "long_name" : "reference time",
                    "var_id" : "time",
                    "names" : [
                        "\"reference time\"",
                        "\"time\""
                    ],
                    "best_name" : "reference time",
                "   agg_string" : "\"long_name\":\"reference time\",\"names\":\"reference time\";\"time\",\"standard_name\":\"time\",\"var_id\":\"time\""
                },...
            ]

        Returns::

            [
                {
                    "standard_name" : "time",
                    "long_name" : "reference time",
                    "var_id" : "time",
                    "best_name" : "reference time",
                },...
            ]



        :param phenomena: Phenomena attribute of file from FBI index
        :type phenomena: list

        :return: List of variables creating a dict of the variable attributes
        """
        variables = []
        for variable in phenomena:
            variables.append({key: value for key, value in variable.items() if key not in ['names', 'agg_string']})

        return variables

    @staticmethod
    def _isodate(date):
        """
        Convert a given date string to isoformat

        :param date: date string as extracted from elasticsearch
        :type date: str

        :return: ISO Formatted date
        :rtype: str
        """
        return date_parser(date).isoformat()

    @staticmethod
    def get_es_path(facet_path, facet_name):
        """
        Return the path to the target in elasticsearch index. Extracted
        to method to allow subclasses to modify the behaviour.

        :param facet_path: route to facet in the target index
        :type facet_path: str

        :param facet_name: name of the facet being processed
        :type facet_name: str

        :return: path to item in elasticsearch index
        :rtype: str
        """
        return f'projects.{settings.APPLICATION_ID}.{facet_name}' if facet_path is DEFAULT else facet_path

    @staticmethod
    def get_date_field(key):
        """
        Date field for the target index

        :param key: one of 'start'|'end'|'range'
        :type key: str

        :return: field name attached to key
        :rtype: str
        """
        date_fields = {
            'start': 'info.temporal.start_time',
            'end': 'info.temporal.end_time',
            'range': 'info.temporal.time_range'
        }

        return date_fields[key]

    def get_handler(self):
        """
        Get the handler for the given collection

        :return: The correct handler for the requested collection
        :rtype: ElasticsearchFacetSet
        """
        return HandlerFactory().get_handler(self.path)

    def build_query(self, params, **kwargs):
        """
        Helper method to build the elasticsearch query
        :param params: Search parameters
        :type params: dict

        :param kwargs:

        :return: elasticsearch query
        :rtype: dict
        """

        # Deep copy to avoid aliasing
        query = copy.deepcopy(self.base_query)

        # Get parameters from kwargs
        search_after = kwargs.get('search_after')
        reverse = kwargs.get('reverse')
        start_index = kwargs.get('start_index', 1)
        page_size = kwargs.get('max_results')

        # If search after key use this
        if search_after:
            query['search_after'] = search_after

            if reverse:
                # reverse the ordering of the sort to go back a page
                for sort_key in query['sort']:
                    for key in sort_key:
                        sort_key[key]['order'] = 'desc'

        # If there is no search after
        elif start_index != 1:

            if (start_index + page_size) < 10000:
                query['from'] = start_index

            # If window is > 10,000 raise error
            # else:
            #     raise PagingError(f"Result window is too large, from + size must be"
            #                       f" less than or equal to [10000] but was [{start_index + page_size}].")

        # Set number of results
        if kwargs.get('max_results'):
            query['size'] = kwargs['max_results']

        # Loop search parameters
        for param in params:

            if param == 'query' and params[param]:
                query['query']['bool']['must'].append({
                    'simple_query_string': {
                        'query': params[param],
                        'default_operator': 'and'
                    }
                })

            elif param == 'uuid':
                query['query']['bool']['must'].append({
                    'term': {
                        '_id': params[param]
                    }
                })

            elif param == 'bbox':

                coordinates = params[param].split(',')

                # Coordinates supplied west, south, east, north
                query['query']['bool']['filter'].append({
                    'geo_bounding_box': {
                        self.facets.get(param): {
                            'top_left': {
                                'lat': coordinates[3],
                                'lon': coordinates[0]
                            },
                            'bottom_right': {
                                'lat': coordinates[1],
                                'lon': coordinates[2]
                            }
                        }
                    }
                })

            # Handle all other search facets
            else:
                facet_path = self.facets.get(param)

                if facet_path and param not in self.exclude_list:

                    es_path = self.get_es_path(facet_path, param)
                    search_terms = params.getlist(param)

                    if search_terms:

                        if len(search_terms) == 1:
                            # Equal to AND query
                            query['query']['bool']['must'].append({
                                'match_phrase': {
                                    es_path: search_terms[0]
                                }
                            })

                        else:
                            # Equal to AND (a OR b) query where there should be at least one match
                            andor = {'bool': {'should': [], 'minimum_should_match': 1}}

                            for search_term in search_terms:
                                andor['bool']['should'].append(
                                    {
                                        'match_phrase': {
                                            es_path: search_term
                                        }
                                    })

                            query['query']['bool']['must'].append(andor)

        # Add date filter
        date_filter = NestedDict()

        if 'startDate' in params:
            date_filter.nested_put([
                'range', self.get_date_field('range'), 'gte'],
                self._isodate(params['startDate'])
            )

        if 'endDate' in params:
            date_filter.nested_put([
                'range', self.get_date_field('range'), 'lte'],
                self._isodate(params['endDate'])
            )

        if date_filter:
            query['query']['bool']['filter'].append(
                date_filter.data
            )

        return query

    def query_elasticsearch(self, query):
        """
        Execute the query
        :param query: Elasticsearch query dict
        :type query: dict

        :return: elasticsearch response
        :rtype: dict
        """
        return ElasticsearchConnection().search(query)

    @staticmethod
    def _process_aggregations(aggs):
        """
        Process the aggregations and return the facet values

        :param aggs: elasticsearch query response
        :type aggs: dict

        :return: {} Dict of values with the facet as the key
        :rtype: dict
        """
        values = {}

        if aggs.get('aggregations'):

            for result in aggs['aggregations']:

                # Start and end date buckets have a different format so
                # handle them differently
                if result == 'startDate':

                    # Reject null values
                    if aggs['aggregations'][result].get('value_as_string'):
                        values['startDate'] = {'extra_kwargs': {
                            'minInclusive': aggs['aggregations'][result].get('value_as_string'),
                            'pattern': '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$'
                        }}

                elif result == 'endDate':

                    # Reject null values
                    if aggs['aggregations'][result].get('value_as_string'):
                        values['endDate'] = {'extra_kwargs': {
                            'maxInclusive': aggs['aggregations'][result].get('value_as_string'),
                            'pattern': '^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$'
                        }}

                else:
                    values[result] = {
                        'values': [
                            {
                                'label': f"{bucket['key']} ({bucket['doc_count']})",
                                'value': bucket['key']
                            } for bucket in aggs['aggregations'][result]['buckets']
                        ]
                    }

        return values

    def get_facet_values(self, search_params):
        """
        Perform aggregations to get the range of possible values
        for each facet to put in the description document.

        result is set as self.facet_values
        """

        query = self.build_query(search_params)

        query.update({
            'aggs': {},
            'size': 0
        })

        for facet in self.facets:
            if facet not in self.exclude_list:
                # Get the path to the facet data
                facet_path = self.facets[facet]

                query['aggs'][facet] = {
                    'terms': {
                        'field': f'{self.get_es_path(facet_path, facet)}.keyword',
                        'size': 1000
                    }
                }

        # Get start and end time ranges
        query['aggs']['startDate'] = {
            "min": {"field": self.get_date_field('start')}
        }

        query['aggs']['endDate'] = {
            "max": {"field": self.get_date_field('end')}
        }

        aggs = self.query_elasticsearch(query)

        values = self._process_aggregations(aggs)

        #TODO: Move self.facet_values to __init__ and return the values dict for setting elsewhere
        self.facet_values = values

    def _search(self, params, **kwargs):
        """

        :param params:
        :param kwargs:
        :return:
        """

        query = self.build_query(params, **kwargs)

        # Get the explicit count. This is needed because, elasticsearch
        # will approximate any number > 10k
        total_hits = ElasticsearchConnection().count(query)['count']

        if total_hits > 10000:

            cache_state = self.depcache.get_cache_state(params)

            if not self.depcache.request_exceeds_limit(**kwargs):
                hits = self.depcache.get_cached_results(
                    cache_state,
                    query,
                    **kwargs
                )

            else:
                es_search = self.query_elasticsearch(query)

                hits = es_search['hits']['hits']

                # reverse = kwargs.get('reverse')
                # print(reverse)
                #
                # if reverse:
                #     hits.reverse()

        return hits, total_hits

    def search(self, params, **kwargs):
        """
        Search interface to elasticsearch

        :param params: Opensearch parameters
        :param kwargs:

        :return: search results
        :rtype: SearchResults
        """

        hits, total_hits = self._search(params, **kwargs)

        results = self.build_representation(hits, params, **kwargs)

        after_key = hits[-1]['sort'] if hits else None
        before_key = hits[0]['sort'] if hits else None

        return SearchResults(total_hits, results, before_key, after_key)

    def build_representation(self, hits, params, **kwargs):
        """
        Build the dict representation of the granule and return the
        result list

        :param hits: Elasticsearch query hits
        :param params: url params
        :param kwargs:

        :return: Result list
        :rtype: list
        """

        results = []
        base_url = kwargs['uri']

        for hit in hits:
            entry = self.build_entry(hit, params, base_url)

            results.append(entry)

        return results

    def build_collection_entry(self, hit, params, base_url):
        """
        Build individual entries at the collection level

        :param hit: Elasticsearch query hits
        :type hit: dict

        :param params: url params
        :type params: django.http.request.QueryDict

        :param base_url: base_url for service
        :type base_url: str

        :return: entry
        :rtype: dict
        """

        source = hit['_source']
        entry = {
            'type': 'FeatureCollection',
            'id': f'{base_url}/request?uuid={hit["_id"]}',
            'properties': {
                'title': source['title'],
                'identifier': source["collection_id"],
                'links': {
                    'search': [
                        {
                            'title': 'Opensearch Description Document',
                            'href': f'{base_url}/description.xml?parentIdentifier={source["collection_id"]}',
                            'type': 'application/opensearchdescription+xml'
                        }
                    ],
                    'related': [
                        {
                            'title': 'ftp',
                            'href': f'ftp://anon-ftp.ceda.ac.uk{source["path"]}',
                            'type': 'text/html'
                        }
                    ]
                }
            }
        }

        if source.get('start_date'):
            entry['properties']['date'] = f"{source['start_date']}/{source['end_date']}"

        if source.get('aggregations'):
            entry['properties']['aggregations'] = []

            for aggregation in source.get('aggregations'):
                agg = {
                    'id': aggregation['id'],
                    'type': 'Feature',
                    'properties': {
                        'links': {
                            'described_by': [
                                {
                                    'title': 'THREDDS Catalog',
                                    'href': get_thredds_aggregation(aggregation['id'], format='html')
                                }
                            ],
                            'search': [
                                {
                                    'title': 'Files',
                                    'href': get_aggregation_search_link(
                                        base_url,
                                        source['collection_id'],
                                        aggregation['id'],
                                        params.get('httpAccept', 'application/geo+json')
                                    )
                                }
                            ],
                            'related': get_aggregation_capabilities(aggregation)
                        }
                    }
                }

                entry['properties']['aggregations'].append(agg)

        if params.get('parentIdentifier'):
            entry['id'] = f'{base_url}/request?parentIdentifier={params["parentIdentifier"]}&uuid={hit["_id"]}'

        if source.get('variable'):
            entry['properties']['variables'] = self._extract_variables(source['variable'])

        return entry

    def build_entry(self, hit, params, base_url):
        """
        Build individual entries at the granule level

        :param hit: elasticsearch response hit
        :type hit: dict

        :param params: URL Params
        :type params: django.http.request.QueryDict

        :param base_url: url of running opensearch service
        :type param: str

        :return: entry
        :rtype: dict
        """
        source = hit['_source']

        entry = {
            'type': 'Feature',
            'id': f'{base_url}?parentIdentifier={params["parentIdentifier"]}&uuid={hit["_id"]}',
            'properties': {
                'title': source['info']['name'],
                'identifier': hit["_id"],
                'updated': source['info']['last_modified'],
                'filesize': source['info']['size'],
                'links': {}
            }
        }

        if source['info'].get('temporal'):
            entry['properties']['date'] = self._extract_time_range(source['info']['temporal'])

        if source['info'].get('spatial'):
            # SW - NE (lon,lat)
            bbox = self._extract_bbox(source['info']['spatial'])

            if bbox:
                entry['bbox'] = bbox

        return entry


