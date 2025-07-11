from django_opensearch import settings
import math
from django_opensearch.opensearch.backends import NamespaceMap
from importlib import import_module
from django_opensearch.opensearch.backends.elasticsearch.facets.collection_map import COLLECTION_MAP

# Import required backend
backend = import_module(f'django_opensearch.opensearch.backends.{settings.OPENSEARCH_BACKEND}')
Collection = getattr(backend, 'Collection')
Granule = getattr(backend, 'Granule')


class OpensearchDescription:
    """
    Class to generate an opensearch Description document and handle boiler plate
    """
    OS_NAMESPACE = settings.OS_NAMESPACE
    OS_PREFIX = settings.OS_PREFIX
    OS_ROOT_TAG = settings.OS_ROOT_TAG

    def __init__(self, request):
        self.short_name = settings.SHORT_NAME
        self.long_name = settings.LONG_NAME
        self.description = settings.DESCRIPTION
        self.tags = settings.TAGS
        self.developer = settings.DEVELOPER
        self.syndication_right = settings.SYNDICATION_RIGHT
        self.adult_content = settings.ADULT_CONTENT
        self.language = settings.LANGUAGE
        self.input_encoding = settings.INPUT_ENCODING
        self.output_encoding = settings.OUTPUT_ENCODING
        self.url_sections = []
        self.example_queries = []
        self.host = request._current_scheme_host

        search_params = request.GET

        response_types = settings.RESPONSE_TYPES

        if not search_params.get('parentIdentifier'):
            default_path = '/neodc/esacci'
            # Get top level collection description
            params = Collection(path=default_path).get_facet_set(search_params)

            for response in response_types:
                self.generate_url_section(response, params)

        else:

            parentID = search_params.get('parentIdentifier')
            collection_path = Collection.get_path(parentID)

            if backend.collection.collection_search(search_params):
                params = Collection(path=collection_path).get_facet_set(search_params)

            else:

                granule = Granule(collection_path)

                # Get granule level params
                params = granule.get_facet_set(search_params)

                # Get example queries
                self.example_queries = granule.get_example_queries()

            for response in response_types:
                self.generate_url_section(response, params)

    def generate_url_template(self, response_type, params):

        base_url = f'{self.host}/opensearch/request?httpAccept={response_type}&'

        for i, param in enumerate(params, 1):
            required = '' if param.required else '?'
            line_end = '&' if i != len(params) else ''

            base_url += f'{param.name}={{{param.value}{required}}}{line_end}'

        return base_url

    def generate_url_section(self, response, params):
        """
        Generate the data for the URL section of the description doc
        :return:
        """

        url_section = {
            'url_template': self.generate_url_template(response, params),
            'type': response,
            'params': params
        }

        self.url_sections.append(url_section)


class OpensearchResponse:
    """
    Base class for all the common attributes of an opensearch response
    """
    type = "FeatureCollection"
    title = "Opensearch Response"

    def __init__(self, request):
        search_params = request.GET
        full_uri = request.build_absolute_uri('/opensearch/request')

        self.totalResults = 0
        self.itemsPerPage = int(search_params.get('maximumRecords', settings.MAX_RESULTS_PER_PAGE))
        self.startRecord = int(search_params.get('startRecord', settings.DEFAULT_START_RECORD))
        self.startPage = int(search_params.get('startPage', settings.DEFAULT_START_PAGE))
        self.queries = {}
        self.features = []
        self.links = {}
        self.request = request

        if all(value in search_params for value in ['startRecord', 'startPage']) or search_params.get('startPage'):
            search_index = (self.startPage - 1) * self.itemsPerPage
        else:
            search_index = self.startRecord

        search_index = search_index if search_index > 1 else 1

        search_after, reverse = self.check_scrolling()

        search_after = search_after or search_params.get('searchAfter',None)

        search_next = self._generate_responses(search_params, start_index=search_index, max_results=self.itemsPerPage, uri=full_uri, search_after=search_after, reverse=reverse)

        if search_index + self.itemsPerPage -1 > self.totalResults:
            end_of_page = self.totalResults
        else:
            end_of_page = search_index + self.itemsPerPage -1

        self.subtitle = f'Showing {search_index} - {end_of_page}' \
                        f' of {self.totalResults}'
        
        if search_next is not None and self.startPage == settings.DEFAULT_START_PAGE:
            self.links['next'] = [
                {
                    'href': f'{full_uri}?{self._stitch_query_params(search_params)}&searchAfter={search_next}',
                    'title':'next'
                }
            ]
            if search_after is not None:
                self.subtitle = f'Showing {end_of_page} results after {search_after}' \
                                f' ({self.totalResults} total)'
            return

        if self.totalResults > self.itemsPerPage:
            # Generate paging links
            if self.startPage > 1:

                self.links['first'] = [
                        {
                            'href': self._generate_navigation_url(full_uri, search_params, 'first'),
                            'title': 'first',
                        }
                    ]
                self.links['previous'] = [
                        {
                            'href': self._generate_navigation_url(full_uri, search_params, 'prev'),
                            'title': 'prev',
                        }
                    ]

            if self.startPage < self.totalResults / self.itemsPerPage:
                self.links['next'] = [
                        {
                            'href': self._generate_navigation_url(full_uri, search_params, 'next'),
                            'title': 'next',
                        }
                    ]

                self.links['last'] = [
                            {
                                'href': self._generate_navigation_url(full_uri, search_params, 'last'),
                                'title': 'last',
                            }
                        ]

    def __getstate__(self):
        state = self.__dict__.copy()
        del state['request']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)

    @staticmethod
    def _stitch_query_params(query_params):

        query_list = [f'{param}={value.replace("+","%2B")}' for param, value in query_params.items() if param != 'startPage']
        return '&'.join(query_list)

    def _generate_navigation_url(self, url, search_params, link_type):
        """
        Generate the navigation urls
        :param url:
        :param search_params:
        :param link_type: first|last|next|prev
        :return:
        """

        if link_type == 'first':
            return f'{url}?{self._stitch_query_params(search_params)}&startPage=1'
            
        elif link_type == 'last':
            last_page = f'startPage={math.ceil(self.totalResults/self.itemsPerPage)}'
            return f'{url}?{self._stitch_query_params(search_params)}&{last_page}'

        else:
            if link_type == 'prev':
                page = f'startPage={self.startPage - 1}'
            else:
                page = f'startPage={self.startPage + 1}'

            return f'{url}?{self._stitch_query_params(search_params)}&{page}'

    def _generate_responses(self, search_params, **kwargs):

        self._generate_request_query(search_params)

        collection_path = None
        next_search = None

        if search_params.get('parentIdentifier'):
            parentID = search_params.get('parentIdentifier')
            collection_path = Collection.get_path(parentID)

        if backend.collection.collection_search(search_params):
            # Search for collections
            self.totalResults, self.features = Collection(path=collection_path).search(search_params, **kwargs)

        else:
            results, next_search = Granule(collection_path).search(search_params, **kwargs)
            self.totalResults = results.total
            self.features = results.results
            #self.set_search_after(results)

        return next_search

    def _generate_request_query(self, search_params):

        request = {}
        for param in search_params:
            _, value = NamespaceMap.get_namespace(param)

            request[value] = search_params[param]

        self.queries['request'] = [request]

    def check_scrolling(self):
        """
        Retrieve the search_after key if the request is the same with a page within 1 of the previous search
        :param request:
        :return:
        """

        session = self.request.session

        search_params = self.request.GET
        start_page = int(search_params.get('startPage',1))
        last_query = session.get('last_query')

        # Only need to check if we are looking at a page higher than the first page
        if start_page > 1:

            # Check if we have a last_query in the session
            if last_query:

                last_query_page = int(last_query.get('startPage', 1))

                # If the page is not within 1 we can't use search after
                if not abs(start_page - last_query_page) <= 1:
                    return None, None

                # Check that the search parameters are the same
                ignored_params = ('startPage', 'httpAccept')
                current_search = {key for key in search_params if key not in ignored_params}
                last_search = {key for key in last_query if key not in ignored_params}

                # If searches are not the same, cannot use last query
                if not current_search == last_search:
                    return None, None

                # Check if values for search facets are the same
                for key in current_search:
                    if not search_params[key] == last_query[key]:
                        return None, None

                # Page is  +/-1, search params are the same and values for them
                # are the same. Retrieve the search_after key
                diff = start_page - last_query_page

                if diff == -1:
                    search_after = session.get('before_key')
                elif diff == 0:
                    search_after = session.get('current_key')
                else:
                    search_after = session.get('after_key')

                reverse = any((diff < 0, session.get('downpage')))

                return search_after, reverse

        return None, None

    def set_search_after(self, results):

        before_key = results.search_before
        after_key = results.search_after

        session = self.request.session
        search_params = self.request.GET

        last_query = session.get('last_query')
        start_page = int(search_params.get('startPage', 1))

        # Set the last query
        session['last_query'] = search_params

        if start_page > 1:
            if last_query:
                last_query_page = int(last_query.get('startPage', 1))

                diff = start_page - last_query_page

                if diff == -1:
                    session['current_key'] = session['before_key']
                    session['downpage'] = True

                elif diff == 1:
                    session['current_key'] = session['after_key']
                    session['downpage'] = False

        session['before_key'] = before_key
        session['after_key'] = after_key


