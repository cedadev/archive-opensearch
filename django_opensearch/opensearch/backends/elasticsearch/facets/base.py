# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '21 Mar 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .collection_map import COLLECTION_MAP
from pydoc import locate
import os
from django_opensearch.constants import DEFAULT
from django_opensearch.opensearch.backends import NamespaceMap, Param, FacetSet
from django_opensearch import settings
from .elasticsearch_connection import ElasticsearchConnection
import copy
from dateutil.parser import parse as date_parser
from django_opensearch.opensearch.utils import NestedDict
from django_opensearch.opensearch.utils.geo_point import Point, Envelope
from collections import namedtuple
from django_opensearch.opensearch.utils.aggregation_tools import get_thredds_aggregation, get_aggregation_capabilities, \
    get_aggregation_search_link


class PagingError(Exception):
    """
    Raised when the requested page is not possible
    """
    pass


SearchResults = namedtuple('SearchResults', ('total', 'results', 'search_before', 'search_after'))


class ElasticsearchFacetSet(FacetSet):
    """
    Class to provide opensearch URL template with facets and parameter options
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

    @staticmethod
    def _extract_bbox(coordinates):
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
        return f"{temporal['start_time']}/{temporal['end_time']}"

    @staticmethod
    def _extract_variables(phenomena):
        variables = []
        for variable in phenomena:
            variables.append({key: value for key, value in variable.items() if key not in ['names', 'agg_string']})

        return variables

    @staticmethod
    def _isodate(date):
        return date_parser(date).isoformat()

    @staticmethod
    def _get_es_path(facet_path, facet_name):
        """
        Return the path to the target in elasticsearch index. Extracted
        to method to allow subclasses to modify the behaviour.
        :param facet: facet path
        :param param: parameter
        :return str: path to item in elasticsearch index
        """
        return f'projects.{settings.APPLICATION_ID}.{facet_name}' if facet_path is DEFAULT else facet_path

    @staticmethod
    def get_date_field(key):
        """
        Date field for the target index
        :param key: one of 'start'|'end'|'range'
        :return: field name attached to key
        """
        date_fields = {
            'start': 'info.temporal.start_time',
            'end': 'info.temporal.end_time',
            'range': 'info.temporal.time_range'
        }

        return date_fields[key]

    def get_handler(self):
        return HandlerFactory().get_handler(self.path)

    def _build_query(self, params, **kwargs):

        # Deep copy to avoid aliasing
        query = copy.deepcopy(self.base_query)

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

            # Set start index to 0 if below 0 or -1 for zero indexing
            start_index = start_index if start_index > 0 else 0

            if start_index > 0 and (start_index + page_size) < 10000:
                query['from'] = start_index

            # If window is > 10,000 raise error
            else:
                raise PagingError(f"Result window is too large, from + size must be"
                                  f" less than or equal to [10000] but was [{start_index + page_size}].")

        if kwargs.get('max_results'):
            # Set number of results
            query['size'] = kwargs['max_results']

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

                # Coordinates supplied top-left (lon,lat), bottom-right (lon,lat)
                query['query']['bool']['filter'].append({
                    'geo_bounding_box': {
                        self.facets.get(param): {
                            'top_left': {
                                'lat': coordinates[1],
                                'lon': coordinates[0]
                            },
                            'bottom_right': {
                                'lat': coordinates[3],
                                'lon': coordinates[2]
                            }
                        }
                    }
                })

            else:
                facet_path = self.facets.get(param)

                if facet_path and param not in self.exclude_list:

                    es_path = self._get_es_path(facet_path, param)
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

    def _query_elasticsearch(self, query):
        return ElasticsearchConnection().search(query)

    @staticmethod
    def _process_aggregations(aggs):
        """
        Process the aggregations and return the facet values

        :param aggs: elasticsearch query response
        :return: {} Dict of values with the facet as the key
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
        :return dict: List of values for each facet
        """

        query = self._build_query(search_params)

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
                        'field': f'{self._get_es_path(facet_path, facet)}.keyword',
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

        aggs = self._query_elasticsearch(query)

        values = self._process_aggregations(aggs)

        self.facet_values = values

    def search(self, params, **kwargs):
        """
        Search interface to elasticsearch
        :param params: Opensearch parameters
        :param kwargs:
        :return:
        """

        query = self._build_query(params, **kwargs)

        es_search = ElasticsearchConnection().search(query)

        hits = es_search['hits']['hits']

        reverse = kwargs.get('reverse')

        if reverse:
            hits.reverse()

        results = self.build_representation(hits, params, **kwargs)

        # Use the response from the query to get the total unless > 10k
        # In this case will need to query size directly
        if es_search['hits']['total']['relation'] == 'eq':
            total_hits = es_search['hits']['total']['value']
        else:
            # Some keys are not compatible with the count query
            for key in ['sort', 'size', 'from', 'search_after']:
                query.pop(key, None)

            total_hits = ElasticsearchConnection().count(query)['count']

        after_key = hits[-1]['sort'] if hits else None
        before_key = hits[0]['sort'] if hits else None

        return SearchResults(total_hits, results, before_key, after_key)

    def build_representation(self, hits, params, **kwargs):

        results = []
        base_url = kwargs['uri']

        for hit in hits:
            entry = self.build_entry(hit, params, base_url)

            results.append(entry)

        return results

    def build_collection_entry(self, hit, params, base_url):
        """
        Build individual entries at the collection level
        :param hit: elasticsearch response
        :param params: url params
        :param base_url: base_url for service
        :return: entry
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

        :param hit:
        :param params:
        :param base_url:
        :return:
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


class HandlerFactory:

    def __init__(self):
        self.map = COLLECTION_MAP

    def get_handler(self, path):
        """
        Takes a system path and returns the file extensions to look for and
        the correct handler for the collection.

        :param path:
        :return: granule extension, handler class
        """

        collection, root_path = self.get_collection_map(path)
        if collection is not None:
            handler = locate(collection['handler'])
            return handler(path)

    def get_collection_map(self, path):
        """
        Takes an arbitrary path and returns a collection path
        :param path: Path to the data of interest
        :return: The value from the map object
        """

        while path not in self.map and path != '/' and path:
            path = os.path.dirname(path)

        # No match has been found
        if not path or path == '/':
            return None, None

        return self.map[path], path
