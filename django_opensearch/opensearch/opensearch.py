from django_opensearch import settings
import math
from django_opensearch.opensearch.backends.solr import Collection, Granule
from django_opensearch.opensearch.backends import NamespaceMap

def collection_search(search_params):
    len_params = len(search_params)

    if 'collectionId' in search_params:
        if len_params == 1:
            return True
        if len_params == 2:
            if 'httpAccept' in search_params:
                return True

    return False

class OpensearchDescription:
    """
    Class to generate an opensearch Description document and handle boiler plate
    """
    OS_NAMESPACE = settings.OS_NAMESPACE
    OS_PREFIX = settings.OS_PREFIX
    OS_ROOT_TAG = settings.OS_ROOT_TAG

    def __init__(self, collectionId=None):
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

        response_types = settings.RESPONSE_TYPES

        collection = Collection()

        if not collectionId:

            # Get top level collection description
            params = collection.get_facet_set()

            for response in response_types:
                self.generate_url_section(response, params)

        else:
            collection_path = collection.get_path(collectionId)

            granule = Granule(collection_path)

            # Get granule level params
            params = granule.get_facet_set()

            # Get example queries
            self.example_queries = granule.get_example_queries()

            for response in response_types:
                self.generate_url_section(response, params)

    def generate_url_template(self, response_type, params):

        base_url = f'http://{settings.OPENSEARCH_HOST}/opensearch/request?httpAccept={response_type}&'

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
        full_uri = request.build_absolute_uri('?')

        self.totalResults = 0
        self.itemsPerPage = int(search_params.get('maximumRecords', settings.MAX_RESULTS_PER_PAGE))
        self.startRecord = int(search_params.get('startRecord', settings.DEFAULT_START_RECORD))
        self.startPage = int(search_params.get('startPage', settings.DEFAULT_START_PAGE))
        self.queries = {}
        self.features = []
        self.links = []

        if all(value in search_params for value in ['startRecord', 'startPage']) or search_params.get('startPage'):
            search_index = (self.startPage - 1) * self.itemsPerPage
        else:
            search_index = self.startRecord

        search_index = search_index if search_index > 1 else 1

        self._generate_responses(search_params, start_index=search_index, max_results=self.itemsPerPage, uri=full_uri)

        self.subtitle = f'Showing {search_index} - {search_index + self.itemsPerPage -1}' \
                        f' of {self.totalResults}'

        if self.totalResults > self.itemsPerPage:
            # Generate paging links
            if self.startPage > 1:
                self.links.append({
                    'first': [
                        {
                            'href': self._generate_navigation_url(full_uri, search_params, 'first'),
                            'title': 'first',
                        }
                    ]
                })
                self.links.append({
                    'previous': [
                        {
                            'href': self._generate_navigation_url(full_uri, search_params, 'prev'),
                            'title': 'prev',
                        }
                    ]
                })

            if self.startPage + 1 < self.totalResults / self.itemsPerPage:
                self.links.append({
                    'next': [
                        {
                            'href': self._generate_navigation_url(full_uri, search_params, 'next'),
                            'title': 'next',
                        }
                    ]
                })

                self.links.append(
                    {
                        'last': [
                            {
                                'href': self._generate_navigation_url(full_uri, search_params, 'last'),
                                'title': 'last',
                            }
                        ]
                    }
                )

    @staticmethod
    def _stitch_query_params(query_params):

        query_list = [f'{param}={value}' for param, value in query_params.items() if param != 'startPage']
        return '&'.join(query_list)

    def _generate_navigation_url(self, url, search_params, link_type):
        """
        Generate the navigation urls
        :param url:
        :param search_params:
        :param link_type: first|last|next|prev
        :return:
        """
        nav_url = None

        if link_type is 'first':
            nav_url = f'{url}?{self._stitch_query_params(search_params)}&startPage=1'

        elif link_type is 'last':
            last_page = f'startPage={math.ceil(self.totalResults/self.itemsPerPage)}'
            nav_url = f'{url}?{self._stitch_query_params(search_params)}&{last_page}'

        elif link_type is 'prev':
            prev_page = f'startPage={self.startPage - 1}'
            nav_url = f'{url}?{self._stitch_query_params(search_params)}&{prev_page}'

        elif link_type is 'next':
            next_page = f'startPage={self.startPage + 1}'
            nav_url = f'{url}?{self._stitch_query_params(search_params)}&{next_page}'

        return nav_url

    def _generate_responses(self, search_params, **kwargs):

        self._generate_request_query(search_params)

        if 'collectionId' not in search_params:
            # Search for collections
            self.totalResults, self.features = Collection().search(search_params, **kwargs)

        elif collection_search(search_params):
            # Search for collections
            coll = Collection()
            self.totalResults, self.features = coll.search(search_params, **kwargs)

        else:
            self.totalResults, self.features = Granule().search(search_params, **kwargs)

    def _generate_request_query(self, search_params):

        request = {}
        for param in search_params:
            _, value = NamespaceMap.get_namespace(param)

            request[value] = search_params[param]

        self.queries['request'] = [request]


