# encoding: utf-8
""" """
__author__ = "Richard Smith"
__date__ = "29 Apr 2019"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import os

import requests
from django.urls import reverse

from ceda_opensearch.opensearch_settings import EXTERNAL_DATA_SOURCES
from django_opensearch import settings
from django_opensearch.constants import DEFAULT
from django_opensearch.opensearch.utils import thredds_path

from .base import ElasticsearchFacetSet


class CCIFacets(ElasticsearchFacetSet):
    """
    Class to handle the representation of the CCI collection

    :attr LOOKUP_HANDLER: Handler for vocab lookups
    :attr facets: Facet map from facet term to path in the elasticsearch index
    """

    LOOKUP_HANDLER = "django_opensearch.opensearch.lookup.cci_lookup.CCILookupHandler"

    facets = {
        "ecv": DEFAULT,
        "frequency": DEFAULT,
        "institute": DEFAULT,
        "processingLevel": DEFAULT,
        "productString": DEFAULT,
        "productVersion": DEFAULT,
        "project": DEFAULT,
        "dataType": DEFAULT,
        "sensor": DEFAULT,
        "platform": DEFAULT,
        "fileFormat": "info.type",
        "bbox": "info.spatial.coordinates.coordinates",
        "drsId": DEFAULT,
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

        pid = params.get("parentIdentifier")

        if pid:
            query["query"]["bool"]["must"].append({"term": {f"projects.{settings.APPLICATION_ID}.datasetId": pid}})
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

        source = hit["_source"]
        entry = super().build_collection_entry(hit, params, base_url)

        external_data_source_found = False
        for external_data_source in EXTERNAL_DATA_SOURCES:
            if source["path"].startswith(external_data_source):
                external_data_source_found = True
                # Overwrite the related links
                entry["properties"]["links"]["related"] = [{"title": "Dataset", "href": source["path"]}]

        if not external_data_source_found and source["path"].startswith("http"):
            # Clear the search links from default entry
            entry["properties"]["links"].pop("search", None)

            # Overwrite the related links
            entry["properties"]["links"]["related"] = [{"title": "Dataset", "href": source["path"]}]

        if source.get("collection_id") != "cci":
            entry["properties"]["links"]["describedby"] = [
                {
                    "title": "ISO19115",
                    "href": f'https://catalogue.ceda.ac.uk/export/xml/gemini2.3/{source["collection_id"]}.xml',
                    "type": "application/vnd.iso.19139+xml",
                },
                {
                    "title": "Dataset Information",
                    # Replace with ESA branded page link?
                    "href": f'https://catalogue.ceda.ac.uk/uuid/{source["collection_id"]}',
                    "type": "text/html",
                },
            ]

            if source.get("manifest"):
                via = entry["properties"]["links"].get("via", [])
                via.append(
                    {
                        "title": "Dataset Manifest",
                        "href": f"{base_url.removesuffix('/opensearch')}{reverse('manifest:get_manifest', kwargs={'uuid': source['collection_id']})}",
                    }
                )

                entry["properties"]["links"]["via"] = via

        relationships = self.get_relationships(source.get("collection_id"))
        if relationships is not None:
            entry["relationships"] = entry.get("relationships", []) + relationships

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

        source = hit["_source"]
        file_path = os.path.join(source["info"]["directory"], source["info"]["name"])

        entry = super().build_entry(hit, params, base_url)
        entry["properties"]["links"]["related"] = [
            {"title": "Download", "href": thredds_path("http", file_path), "type": "application/octet-stream"}
        ]

        # Add opendap link to netCDF files
        if source["info"].get("format") == "NetCDF":
            # Check if any of the values are of int64 type. Dap cannot serve int64
            possibles = []
            for phenom in source["info"].get("phenomena", []):
                if isinstance(phenom, dict):
                    possibles.append(phenom.get("dtype") == "int64")
            int64 = any(possibles)

            if not int64:
                entry["properties"]["links"]["related"].append(
                    {"title": "Opendap", "href": thredds_path("opendap", file_path), "type": "application/octet-stream"}
                )

                # Check if character array. Dap converts these into strings
                for phenom in source["info"].get("phenomena", []):
                    if isinstance(phenom, dict):
                        if phenom.get("dtype") == "bytes8" and len(phenom.get("dimensions", [])) == 1:
                            # add a flag for the toolbox
                            entry["properties"]["links"]["related"][-1]["opendap_fully_compatible"] = False
                            break

        # Multiple kerchunk locations are permissible.
        if source["info"].get("kerchunk_location") is not None:
            kerchunk_location = source["info"].get("kerchunk_location")
            if not isinstance(kerchunk_location, list):
                kerchunk_location = [kerchunk_location]
            for location in kerchunk_location:
                entry["properties"]["links"]["related"].append(
                    {"title": "Kerchunk", "href": thredds_path("http", location), "type": "application/octet-stream"}
                )

        # Multiple zarr locations are permissible (although not expected to be required)
        if source["info"].get("zarr_location") is not None:
            zarr_location = source["info"].get("zarr_location")
            if not isinstance(zarr_location, list):
                zarr_location = [zarr_location]
            for location in zarr_location:
                entry["properties"]["links"]["related"].append(
                    {"title": "Zarr", "href": location, "type": "application/octet-stream"}  # S3 Endpoint
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
            print(f"ERROR {ex}")
            print(f"ERROR {url_string}")
            return None
