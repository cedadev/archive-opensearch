from django_opensearch import settings
from .elasticsearch_connection import ElasticsearchConnection
from .facets.base import FacetSet, NamespaceMap, HandlerFactory


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

        response_types = settings.RESPONSE_TYPES

        collection = Collection(settings.TOP_LEVEL_COLLECTION)

        if not collectionId:

            # Get top level collection description
            params = collection.get_facet_set()

            for response in response_types:
                self.generate_url_section(response, params)

        else:
            collection_path = collection.get_path(collectionId)

            # Get granule level params
            params = Granule().get_facet_set(collection_path)

            for response in response_types:
                self.generate_url_section(response, params)

    def generate_url_template(self, response_type, params):

        base_url = f'localhost:8000/opensearch/{response_type}?'

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


class Collection(FacetSet):
    facets = {
        'collectionId': 'default',
        'title': 'default'
    }

    def __init__(self, collection):
        self.data = collection

    def search(self, params):
        """
        Search through the dictionary for key, value matches.
        Only matches strings.
        :param params:
        :return:
        """

        results = []

        # Loop through the collection list of dicts
        for d in self.data:

            # Check all the parameters in the query string with the
            # dictionary keys
            for param in params:
                val = d.get(param)

                # If key exists in dictionary and values match
                # return result
                if val is not None and params[param] in val:
                    # Add description document
                    d['links'] = [{
                        'href': f'localhost:8000/opensearch/description.xml?collectionId={d["collectionId"]}',
                        'args': 'rel="search" type="application/xml"'
                    }]

                    d['entry_id'] = f'collectionId={ d["collectionId"] }'

                    results.append(d)

        return len(results), results

    def get_path(self, collection_id):
        for d in self.data:
            val = d.get('collectionId')
            if val == collection_id:
                return d.get('path')


class Granule:

    def get_facet_set(self, path):
        handler = HandlerFactory().get_handler(path)

        return handler().get_facet_set()

    def search(self, params, **kwargs):
        collection = Collection(settings.TOP_LEVEL_COLLECTION)

        path = collection.get_path(params.get('collectionId'))

        handler = HandlerFactory().get_handler(path)

        return handler().search(params, **kwargs)


class OpensearchResponse:
    """
    Base class for all the common attributes of an opensearch response
    """

    def __init__(self, search_params):

        self.totalResults = 0
        self.itemsPerPage = int(search_params.get('maximumRecords', settings.MAX_RESULTS_PER_PAGE))
        self.startRecord = int(search_params.get('startRecord', settings.DEFAULT_START_RECORD))
        self.startPage = int(search_params.get('startPage', settings.DEFAULT_START_PAGE))

        if all(value in search_params for value in ['startRecord', 'startPage']) or search_params.get('startPage'):
            search_index = (self.startPage - 1) * self.itemsPerPage
        else:
            search_index = self.startRecord

        search_index = search_index if search_index > 1 else 1

        self._generate_responses(search_params, start_index=search_index)

        self.pagination_string = f'Showing {search_index} - {search_index + self.itemsPerPage -1}' \
                                 f' of {self.totalResults}'

    def _generate_responses(self, search_params, **kwargs):

        self._generate_query_string(search_params)

        if 'collectionId' not in search_params:
            # Search for collections
            self.totalResults, self.responses = Collection(settings.TOP_LEVEL_COLLECTION).search(search_params)

        elif 'collectionId' in search_params and len(search_params) == 1:
            # Search for collections
            self.totalResults, self.responses = Collection(settings.TOP_LEVEL_COLLECTION).search(search_params)

        else:
            self.totalResults, self.responses = Granule().search(search_params, **kwargs)

    def _generate_query_string(self, search_params):

        query_string = ''

        for param in search_params:
            _, value = NamespaceMap.get_namespace(param)

            query_string += f'{value}="{search_params[param]}" '

        self.query = query_string
