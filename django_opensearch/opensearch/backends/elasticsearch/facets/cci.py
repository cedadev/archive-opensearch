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
from django_opensearch.opensearch.utils import thredds_path
import os


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
        'platform': DEFAULT,
        'fileFormat': 'info.type',
        'bbox': 'info.spatial.coordinates.coordinates',
        'drsId': DEFAULT
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

        if pid.endswith('.all'):
            pid = pid[:-4]

        if pid:
            query['query']['bool']['must'].append({
                'term': {
                    f'projects.{settings.APPLICATION_ID}.datasetId': pid
                }
            })
        return query

    def build_collection_entry(self, hit, params, base_url):
        source = hit['_source']
        entry = super().build_collection_entry(hit, params, base_url)

        if source['path'].startswith('http'):
            # Clear the search links from default entry
            entry['properties']['links'].pop('search', None)

            # Overwrite the related links
            entry['properties']['links']['related'] = [
                {
                    'title': 'Dataset',
                    'href': source['path']
                }
            ]

        if source.get('collection_id') != 'cci':
            entry['properties']['links']['describedby'] = [
                {
                    'title': 'ISO19115',
                    'href': f'https://catalogue.ceda.ac.uk/export/xml/{source["collection_id"]}.xml'
                },
                {
                    'title': 'Dataset Information',
                    'href': f'https://catalogue.ceda.ac.uk/uuid/{source["collection_id"]}'
                }
            ]

        return entry

    def build_entry(self, hit, params, base_url):
            source = hit['_source']
            file_path = os.path.join(source["info"]["directory"], source["info"]["name"])

            entry = super().build_entry(hit, params, base_url)
            entry['properties']['links']['related'] = [
                {
                    'title': 'Download',
                    'href': f'http://{thredds_path("http", file_path)}',
                }
            ]

            # Add opendap link to netCDF files
            if source['info'].get('format') == 'NetCDF':
                entry['properties']['links']['related'].append(
                    {
                        'title': 'Opendap',
                        'href': f'http://{thredds_path("opendap", file_path)}',
                    }
                )

            return entry

