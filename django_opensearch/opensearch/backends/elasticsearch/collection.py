# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jul 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .facets.base import ElasticsearchFacetSet

class Collection(ElasticsearchFacetSet):
    facets = {
        'parentIdentifier': 'default',
        'title': 'default'
    }

    def __init__(self):

        self.data = [
            {
                'parentIdentifier': '1',
                'title': 'CMIP5',
                'description': 'cmip5 is very cool',
                'path': '/badc/cmip5/data',
                'startDate': '01-01-1583',
                'endDate': '01-01-5091'
            },
            {
                'parentIdentifier': '2',
                'title': 'CCI',
                'description': 'cci is very cool',
                'path': '/neodc/esacci',
                'startDate': '20-01-2019',
                'endDate': '21-03-2019'
            }
        ]

    def search(self, params, **kwargs):
        """
        Search through the dictionary for key, value matches.
        Only matches strings.
        :param params:
        :return:
        """

        base_url = kwargs['uri'].split('/opensearch')[0]

        results = []

        # Loop through the collection list of dicts
        for d in self.data:

            # Check all the parameters in the query string with the
            # dictionary keys
            for param in params:
                entry = {
                    'type': 'FeatureCollection',
                    'properties': {}
                }

                if param == 'query':
                    if any([x in d['description'] for x in params['query'].split(',')]):
                        # Add description document
                        entry['properties']['links'] = {
                            'search': [
                                {
                                    'title': 'Opensearch Description Document',
                                    'href': f'{base_url}/opensearch/description.xml?parentIdentifier={d["parentIdentifier"]}',
                                    'type': 'application/xml'}
                            ]
                        }

                        entry['id'] = f'parentIdentifier={ d["parentIdentifier"] }'
                        entry['properties']['identifier'] = d["parentIdentifier"]
                        entry['properties']['title'] = d['title']
                        entry['properties']['date'] = f'{d["startDate"]}/{d["endDate"]}'

                        results.append(entry)


                else:
                    val = d.get(param)

                    # If key exists in dictionary and values match
                    # return result
                    if val is not None and params[param] in val:
                        # Add description document
                        entry['properties']['links'] = [{
                            'search': [
                                {
                                    'title': 'Opensearch Description Document',
                                    'href': f'{base_url}/opensearch/description.xml?parentIdentifier={d["parentIdentifier"]}',
                                    'type': 'application/xml'}
                            ]
                        }]

                        entry['id'] = f'parentIdentifier={ d["parentIdentifier"] }'
                        entry['properties']['identifier'] = d["parentIdentifier"]
                        entry['properties']['title'] = d['title']
                        entry['properties']['date'] = f'{d["startDate"]}/{d["endDate"]}'

                        results.append(entry)

        return len(results), results

    def get_path(self, collection_id):
        for d in self.data:
            val = d.get('parentIdentifier')
            if val == collection_id:
                return d.get('path')


