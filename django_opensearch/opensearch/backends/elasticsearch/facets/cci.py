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
from django.urls import reverse


class CCIFacets(ElasticsearchFacetSet):
    """
    Class to handle the representation of the CCI collection

    :attr LOOKUP_HANDLER: Handler for vocab lookups
    :attr facets: Facet map from facet term to path in the elasticsearch index
    """

    LOOKUP_HANDLER = 'django_opensearch.opensearch.lookup.cci_lookup.CCILookupHandler'

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

    def build_query(self, params, **kwargs):
        """
        Filter file results by path

        :param params: URL Params
        :type params: <class 'django.http.request.QueryDict'>

        :param kwargs:

        :return: Elasticsearch query
        :rtuype: dict
        """

        query = super().build_query(params, **kwargs)

        pid = params.get('parentIdentifier')

        if pid:
            query['query']['bool']['must'].append({
                'term': {
                    f'projects.{settings.APPLICATION_ID}.datasetId': pid
                }
            })
        return query

    def build_collection_entry(self, hit, params, base_url):
        """
        Build individual entries at the collection level

        :param hit: Elasticsearch query hits
        :type hit: dict

        :param params: url params
        :type params: <class 'django.http.request.QueryDict'>

        :param base_url: base_url for service
        :type base_url: str

        :return: entry
        :rtype: dict
        """

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
                    'href': f'https://catalogue.ceda.ac.uk/export/xml/gemini2.3/{source["collection_id"]}.xml',
                    'type': 'application/xml'
                },
                {
                    'title': 'Dataset Information',
                    'href': f'https://catalogue.ceda.ac.uk/uuid/{source["collection_id"]}',
                    'type': 'text/html'
                }
            ]

            if source.get('manifest'):
                via = entry['properties']['links'].get('via',[])
                via.append(
                    {
                        'title': 'Dataset Manifest',
                        'href': f"{base_url.rstrip('/opensearch')}{reverse('manifest:get_manifest', kwargs={'uuid': source['collection_id']})}"
                    }
                )

                entry['properties']['links']['via'] = via

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
        file_path = os.path.join(source["info"]["directory"], source["info"]["name"])

        entry = super().build_entry(hit, params, base_url)
        entry['properties']['links']['related'] = [
            {
                'title': 'Download',
                'href': thredds_path("http", file_path),
                'type': 'application/octet-stream'
            }
        ]

        # Add opendap link to netCDF files
        if source['info'].get('format') == 'NetCDF':
            # Check if any of the values are of int64 type. Dap cannot serve int64
            int64 = any([phenom.get('dtype') for phenom in source['info'].get('phenomena', [])])

            if not int64:
                entry['properties']['links']['related'].append(
                    {
                        'title': 'Opendap',
                        'href': thredds_path("opendap", file_path),
                        'type': 'application/octet-stream'
                    }
                )

        return entry
