# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jul 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


from django_opensearch import settings
from django_opensearch.constants import DEFAULT

class Param:

    def __init__(self, name, value, required=False, value_list=[], **kwargs):
        self.name = name
        self.value = value
        self.required = required
        self.value_list = value_list
        self.extra_kwargs = {}

        for k, v in kwargs.items():
            setattr(self, k, v)

    @property
    def val(self):
        return f'{{{self.value}}}'

    def __str__(self):
        return self.name


class NamespaceMap:
    map = {
        'query': {'name': 'searchTerms'},
        'maximumRecords': dict(name='count'),
        'parentIdentifer': dict(name='parentIdentifier', namespace=settings.GEO_NAMESPACE_TAG),
        'startDate': dict(name='start', namespace=settings.TIME_NAMESPACE_TAG),
        'endDate': dict(name='end', namespace=settings.TIME_NAMESPACE_TAG),
        'uuid': dict(name='uid', namespace=settings.GEO_NAMESPACE_TAG),
        'bbox': dict(name='box', namespace=settings.GEO_NAMESPACE_TAG),
        'identifier': dict(name='identifier', namespace=settings.DUBLIN_CORE_NAMESPACE_TAG)
    }

    @classmethod
    def get_namespace(cls, key):

        mapping = cls.map.get(key)

        if mapping is not None:
            namespace = mapping.get('namespace')
            if namespace is not None:
                value = f'{namespace}:{mapping["name"]}'
            else:
                value = mapping['name']

            return key, value

        return key, key


class FacetSet:
    """
    Class to provide opensearch URL template with facets and parameter options
    """

    default_facets = {
        'uuid': DEFAULT,
        'query': DEFAULT,
        'maximumRecords': DEFAULT,
        'startPage': DEFAULT,
        'startRecord': DEFAULT,
        'startDate': DEFAULT,
        'endDate': DEFAULT,
        'parentIdentifier': DEFAULT,
    }

    facet_values = {}

    # List of facets to exclude from value aggregation
    exclude_list = ['uuid', 'bbox', 'startDate', 'endDate']

    def __init__(self, path=None):
        self.path = path

    @property
    def facets(self):
        raise NotImplementedError

    @property
    def all_facets(self):
        """
        Merge the facet dictionaries into one
        :return: dict
        """
        return {**self.default_facets, **self.facets}

    def _build_query(self, params, **kwargs):
        raise NotImplementedError

    def _get_facet_set(self):
        """
        Turns facets into parameter objects
        :return:
        """

        return [Param(*NamespaceMap.get_namespace(facet)) for facet in self.all_facets]

    def get_handler(self):
        raise NotImplementedError

    def get_facet_set(self, search_params):
        """
        Used to build the description document. Get available facets
        for this collection and add values where possible.
        :return list: List of parameter object for each facet
        """

        # Returns list of parameter objects
        if self.path:
            handler = self.get_handler()

            self.facets.update(handler.facets)

            facet_set = handler._get_facet_set()

        else:
           facet_set = self._get_facet_set()

        # facet_set = self._get_facet_set()
        facet_set_with_vals = []

        # Get the aggregated values for each facet
        self.get_facet_values(search_params)

        for param in facet_set:
            facet_data = self.facet_values.get(param.name)

            # Add the values list to the parameter if it exists
            if facet_data is not None:

                if facet_data.get('values'):
                    param.value_list = facet_data.get('values')

                if facet_data.get('extra_kwargs'):
                    param.extra_kwargs = facet_data.get('extra_kwargs')

            facet_set_with_vals.append(param)

        return facet_set_with_vals

    def get_example_queries(self):
        examples = []
        for facet in self.facets:
            values_list = self.facet_values.get(facet).get('values')
            if values_list:
                examples.append({facet:values_list[0]['value']})

        return examples

    def get_facet_values(self, search_params):
        """
        Perform aggregations to get the range of possible values
        for each facet to put in the description document.
        :return dict: List of values for each facet
        """
        raise NotImplementedError

    def search(self, params, **kwargs):
        """
        Search interface to the CMIP5 collection
        :param params: Opensearch parameters
        :param kwargs:
        :return:
        """

        raise NotImplementedError

    def build_representation(self, data):
        raise NotImplementedError
