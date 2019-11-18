# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '29 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import ElasticsearchFacetSet
from django_opensearch.constants import DEFAULT
from django_opensearch import settings

class CCIFacets(ElasticsearchFacetSet):
    """

    """

    facets = {
        'ecv': DEFAULT,
        'frequency': DEFAULT,
        'institute': DEFAULT,
        'processingLevel': DEFAULT,
        'productString': DEFAULT,
        'productVersion': DEFAULT,
        'dataType': DEFAULT,
        'sensor': DEFAULT,
        'platform': DEFAULT
    }

    def _build_query(self, params, **kwargs):
        """
        Filter file results by path
        :param params:
        :param kwargs:
        :return:
        """

        query = super()._build_query(params, **kwargs)

        pid = params.get('parentIdentifier')

        if pid:

            query['query']['bool']['must'].append({
                'term': {
                    f'projects.{settings.APPLICATION_ID}.datasetId': pid
                }
            })
        return query

    def build_representation(self, hits, params, **kwargs):

        base_url = kwargs['uri']

        results = []

        for hit in hits:

            source = hit['_source']

            entry = {
                'type': 'Feature',
                'id': f'{base_url}?parentIdentifier={params["parentIdentifier"]}&uuid={ hit["_id"] }',
                'properties': {
                    'title': source['info']['name'],
                    'identifier': hit["_id"],
                    'updated': source['info']['last_modified'],
                    'links': {
                            'related': [
                                {
                                    'title': 'Download',
                                    'href': f'http://dap.ceda.ac.uk/thredds/fileServer{source["info"]["directory"]}/{source["info"]["name"]}',
                                },
                                {
                                    'title': 'Opendap',
                                    'href': f'http://dap.ceda.ac.uk/thredds/dodsC/dap/{source["info"]["directory"]}/{source["info"]["name"]}',
                                }
                            ]
                    }
                }
            }

            if source['info'].get('temporal'):
                entry['properties']['date'] = self._extract_time_range(source['info']['temporal'])

            if source['info'].get('spatial'):
                # SW - NE (lon,lat)
                entry['bbox'] = self._extract_bbox(source['info']['spatial'])

            results.append(entry)

        return results


