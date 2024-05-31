# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '29 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import ElasticsearchFacetSet
from ceda_opensearch.opensearch_settings import EXTERNAL_DATA_SOURCES
from django_opensearch.constants import DEFAULT
from django_opensearch import settings
from django_opensearch.opensearch.utils import thredds_path
import os
from django.urls import reverse
import requests


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

        external_data_source_found = False
        for external_data_source in EXTERNAL_DATA_SOURCES:
            if source['path'].startswith(external_data_source):
                external_data_source_found = True
                # Overwrite the related links
                entry['properties']['links']['related'] = [
                    {
                        'title': 'Dataset',
                        'href': source['path']
                    }
                ]

        if not external_data_source_found and source['path'].startswith('http'):
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
                    'type': 'application/vnd.iso.19139+xml'
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

        relationships = self.get_relationships(source.get('collection_id'))
        if relationships is not None:
                entry["relationships"] = relationships

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
            int64 = any([phenom.get('dtype') == 'int64' for phenom in source['info'].get('phenomena', [])])

            if not int64:
                entry['properties']['links']['related'].append(
                    {
                        'title': 'Opendap',
                        'href': thredds_path("opendap", file_path),
                        'type': 'application/octet-stream'
                    }
                )

                # Check if character array. Dap converts these into strings
                for phenom in source["info"].get("phenomena", []):
                    if (
                        phenom is not None
                        and phenom.get("dtype") == "bytes8"
                        and len(phenom.get("dimensions", [])) == 1
                    ):
                        # add a flag for the toolbox
                        entry['properties']['links']['related'][-1]['opendap_fully_compatible'] = False
                        break

        if source['info'].get('kerchunk_location') is not None:
            kerchunk_location = source['info'].get('kerchunk_location')
            entry['properties']['links']['related'].append(
                {
                    'title': 'Kerchunk',
                    'href': thredds_path("http", kerchunk_location),
                    'type': 'application/octet-stream'
                }
            )

        return entry

    def get_relationships(self, uid):
        """
        Call out to the data bridge service to find related datasets.

        @param uid (str): the uid of dataset

        @return a list of related datasets

        """
        url_string = f"{settings.DATA_BRIDGE_URL}/dataset/https://catalogue.ceda.ac.uk/uuid/{uid}?format=json"

        try:
            response = requests.get(url_string, verify=False)
            if response.status_code != 200:
                return None
            return response.json()[0]["relationships"]

        except Exception as ex:
            print(f"ERROR {ex}")
            print(f"ERROR {url_string}")
            return None




x = {'projects': {'opensearch': {'productString': ['MERIS_ENVISAT'], 'platformGroup': [], 'productVersion': ['2.2'], 'processingLevel': ['L3C'], 'drsId': 'esacci.AEROSOL.day.L3C.AOD.MERIS.Envisat.MERIS_ENVISAT.2-2.r1', 'dataType': ['AOD'], 'ecv': ['AEROSOL'], 'datasetId': '11c5f6df1abc41968d0b28fe36393c9d', 'institute': ['ICARE', 'HYGEOS'], 'sensor': ['MERIS'], 'platform': ['Envisat'], 'frequency': ['day']}}, 'info': {'name_auto': '20080220-ESACCI-L3C_AEROSOL-AOD-MERIS_ENVISAT-ALAMO-fv2.2.nc', 'kerchunk_location': '/neodc/esacci/aerosol/metadata/kerchunk/MERIS_ALAMO/L3/v2.2/DAILY/2008/20080101-20081231-ESACCI-L3C_AEROSOL-AOD-MERIS_ENVISAT-ALAMO-fv2.2_kr1.0.json', 'format': 'NetCDF', 'read_status': 'Successful', 'type': '.nc', 'directory': '/neodc/esacci/aerosol/data/MERIS_ALAMO/L3/v2.2/DAILY/2008/02', 'size': 590150, 'is_link': False, 'name': '20080220-ESACCI-L3C_AEROSOL-AOD-MERIS_ENVISAT-ALAMO-fv2.2.nc', 'location': 'on_disk', 'spatial': {'coordinates': {'coordinates': [[-180.0, 90.0], [180.0, -90.0]], 'type': 'envelope'}}, 'user': 'badc', 'last_modified': '2016-05-07T02:33:16', 'spot_name': 'spot-2240-esacci_aerosol', 'phenomena': [{'best_name': 'Latitude', 'names': ['"Latitude"', '"latitude"'], 'shape': [180], 'size': '180', 'agg_string': '"long_name":"Latitude","names":"Latitude";"latitude","standard_name":"latitude","units":"degree_north","var_id":"latitude"', '_FillValue': '-9999.0', 'dtype': 'float32', 'standard_name': 'latitude', 'units': 'degree_north', 'var_id': 'latitude', 'long_name': 'Latitude', 'dimensions': ['latitude']}, {'best_name': 'Longitude', 'names': ['"Longitude"', '"longitude"'], 'shape': [360], 'size': '360', 'agg_string': '"long_name":"Longitude","names":"Longitude";"longitude","standard_name":"longitude","units":"degree_east","var_id":"longitude"', '_FillValue': '-9999.0', 'dtype': 'float32', 'standard_name': 'longitude', 'units': 'degree_east', 'var_id': 'longitude', 'long_name': 'Longitude', 'dimensions': ['longitude']}, {'best_name': 'Mean Effective Aerosol Optical Thickness at 550nm over ocean - Best solution', 'names': ['"Mean Effective Aerosol Optical Thickness at 550nm over ocean - Best solution"', '"atmosphere_optical_thickness_due_to_ambient_aerosol"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Mean Effective Aerosol Optical Thickness at 550nm over ocean - Best solution","names":"Mean Effective Aerosol Optical Thickness at 550nm over ocean - Best solution";"atmosphere_optical_thickness_due_to_ambient_aerosol","standard_name":"atmosphere_optical_thickness_due_to_ambient_aerosol","units":"1","var_id":"AOD550"', '_FillValue': 'nan', 'dtype': 'float32', 'standard_name': 'atmosphere_optical_thickness_due_to_ambient_aerosol', 'units': '1', 'var_id': 'AOD550', 'long_name': 'Mean Effective Aerosol Optical Thickness at 550nm over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Standard Deviation of Effective Aerosol Optical Thickness at 550nm over ocean - Best solution', 'names': ['"Standard Deviation of Effective Aerosol Optical Thickness at 550nm over ocean - Best solution"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Standard Deviation of Effective Aerosol Optical Thickness at 550nm over ocean - Best solution","names":"Standard Deviation of Effective Aerosol Optical Thickness at 550nm over ocean - Best solution","units":"1","var_id":"AOD550_std"', '_FillValue': 'nan', 'dtype': 'float32', 'units': '1', 'var_id': 'AOD550_std', 'long_name': 'Standard Deviation of Effective Aerosol Optical Thickness at 550nm over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Mean Effective Aerosol Optical Thickness at 865nm over ocean - Best solution', 'names': ['"Mean Effective Aerosol Optical Thickness at 865nm over ocean - Best solution"', '"atmosphere_optical_thickness_due_to_ambient_aerosol"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Mean Effective Aerosol Optical Thickness at 865nm over ocean - Best solution","names":"Mean Effective Aerosol Optical Thickness at 865nm over ocean - Best solution";"atmosphere_optical_thickness_due_to_ambient_aerosol","standard_name":"atmosphere_optical_thickness_due_to_ambient_aerosol","units":"1","var_id":"AOD865"', '_FillValue': 'nan', 'dtype': 'float32', 'standard_name': 'atmosphere_optical_thickness_due_to_ambient_aerosol', 'units': '1', 'var_id': 'AOD865', 'long_name': 'Mean Effective Aerosol Optical Thickness at 865nm over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Standard Deviation of Effective Aerosol Optical Thickness at 865nm over ocean - Best solution', 'names': ['"Standard Deviation of Effective Aerosol Optical Thickness at 865nm over ocean - Best solution"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Standard Deviation of Effective Aerosol Optical Thickness at 865nm over ocean - Best solution","names":"Standard Deviation of Effective Aerosol Optical Thickness at 865nm over ocean - Best solution","units":"1","var_id":"AOD865_std"', '_FillValue': 'nan', 'dtype': 'float32', 'units': '1', 'var_id': 'AOD865_std', 'long_name': 'Standard Deviation of Effective Aerosol Optical Thickness at 865nm over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Mean Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution', 'names': ['"Mean Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution"', '"atmosphere_optical_thickness_due_to_ambient_aerosol"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Mean Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution","names":"Mean Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution";"atmosphere_optical_thickness_due_to_ambient_aerosol","standard_name":"atmosphere_optical_thickness_due_to_ambient_aerosol","units":"1","var_id":"fAOD550"', '_FillValue': 'nan', 'dtype': 'float32', 'standard_name': 'atmosphere_optical_thickness_due_to_ambient_aerosol', 'units': '1', 'var_id': 'fAOD550', 'long_name': 'Mean Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Standard Deviation of Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution', 'names': ['"Standard Deviation of Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Standard Deviation of Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution","names":"Standard Deviation of Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution","units":"1","var_id":"fAOD550_std"', '_FillValue': 'nan', 'dtype': 'float32', 'units': '1', 'var_id': 'fAOD550_std', 'long_name': 'Standard Deviation of Fine Mode Aerosol Optical Thickness at 550nm over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Mean Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution', 'names': ['"Mean Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution"', '"atmosphere_optical_thickness_due_to_ambient_aerosol"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Mean Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution","names":"Mean Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution";"atmosphere_optical_thickness_due_to_ambient_aerosol","standard_name":"atmosphere_optical_thickness_due_to_ambient_aerosol","units":"1","var_id":"fAOD865"', '_FillValue': 'nan', 'dtype': 'float32', 'standard_name': 'atmosphere_optical_thickness_due_to_ambient_aerosol', 'units': '1', 'var_id': 'fAOD865', 'long_name': 'Mean Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Standard Deviation of Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution', 'names': ['"Standard Deviation of Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Standard Deviation of Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution","names":"Standard Deviation of Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution","units":"1","var_id":"fAOD865_std"', '_FillValue': 'nan', 'dtype': 'float32', 'units': '1', 'var_id': 'fAOD865_std', 'long_name': 'Standard Deviation of Fine Mode Aerosol Optical Thickness at 865nm over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Mean Effective Radius over ocean - Best solution', 'names': ['"Mean Effective Radius over ocean - Best solution"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Mean Effective Radius over ocean - Best solution","names":"Mean Effective Radius over ocean - Best solution","units":"m","var_id":"R_eff"', '_FillValue': 'nan', 'dtype': 'float32', 'units': 'm', 'var_id': 'R_eff', 'long_name': 'Mean Effective Radius over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Standard Deviation of Effective Radius over ocean - Best solution', 'names': ['"Standard Deviation of Effective Radius over ocean - Best solution"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Standard Deviation of Effective Radius over ocean - Best solution","names":"Standard Deviation of Effective Radius over ocean - Best solution","units":"m","var_id":"R_eff_std"', '_FillValue': 'nan', 'dtype': 'float32', 'units': 'm', 'var_id': 'R_eff_std', 'long_name': 'Standard Deviation of Effective Radius over ocean - Best solution', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Mean Aerosol Altitude', 'names': ['"Mean Aerosol Altitude"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Mean Aerosol Altitude","names":"Mean Aerosol Altitude","units":"m","var_id":"Aerosol_Altitude"', '_FillValue': 'nan', 'dtype': 'float32', 'units': 'm', 'var_id': 'Aerosol_Altitude', 'long_name': 'Mean Aerosol Altitude', 'dimensions': ['latitude', 'longitude']}, {'best_name': 'Standard Deviation of Aerosol Altitude', 'names': ['"Standard Deviation of Aerosol Altitude"'], 'shape': [180, 360], 'size': '64800', 'agg_string': '"long_name":"Standard Deviation of Aerosol Altitude","names":"Standard Deviation of Aerosol Altitude","units":"m","var_id":"Aerosol_Altitude_std"', '_FillValue': 'nan', 'dtype': 'float32', 'units': 'm', 'var_id': 'Aerosol_Altitude_std', 'long_name': 'Standard Deviation of Aerosol Altitude', 'dimensions': ['latitude', 'longitude']}], 'temporal': {'start_time': '2008-02-20T00:00:00+00:00', 'time_range': {'gte': '2008-02-20T00:00:00+00:00', 'lte': '2008-02-21T00:00:00+00:00'}, 'end_time': '2008-02-21T00:00:00+00:00'}, 'group': 'open'}}
