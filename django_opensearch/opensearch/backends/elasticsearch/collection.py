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
        'collectionId': 'default',
        'title': 'default'
    }

    def __init__(self, collection):
        super().__init__(path=None)

        self.data = collection

    def _build_query(self, params, **kwargs):
        pass

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
                        entry['properties']['links'] = [{
                            'search': [
                                {
                                    'title': 'Opensearch Description Document',
                                    'href': f'{base_url}/opensearch/description.xml?collectionId={d["collectionId"]}',
                                    'type': 'application/xml'}
                            ]
                        }]

                        entry['id'] = f'collectionId={ d["collectionId"] }'
                        entry['properties']['identifier'] = d["collectionId"]
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
                                    'href': f'{base_url}/opensearch/description.xml?collectionId={d["collectionId"]}',
                                    'type': 'application/xml'}
                            ]
                        }]

                        entry['id'] = f'collectionId={ d["collectionId"] }'
                        entry['properties']['identifier'] = d["collectionId"]
                        entry['properties']['title'] = d['title']
                        entry['properties']['date'] = f'{d["startDate"]}/{d["endDate"]}'

                        results.append(entry)

        return len(results), results

    def get_path(self, collection_id):
        for d in self.data:
            val = d.get('collectionId')
            if val == collection_id:
                return d.get('path')


