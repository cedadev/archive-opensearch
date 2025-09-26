# encoding: utf-8
""" """
__author__ = "Richard Smith"
__date__ = "23 Mar 2020"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import urllib.parse

from django.conf import settings

from ceda_opensearch import opensearch_settings
from django_opensearch.constants import SUFFIX_MAP
from django_opensearch.opensearch.utils import thredds_path


def get_thredds_aggregation(id, format="xml"):
    return f"{settings.THREDDS_HOST}/thredds/datasets/{id}.{format}?dataset={id}"


def get_aggregation_search_link(base_url, collection_id, aggregation_id, format):
    return f"{base_url}/request?parentIdentifier={collection_id}&drsId={aggregation_id}&httpAccept={urllib.parse.quote(format)}"


def get_aggregation_capabilities(agg_dict):
    """
    Turn response from Elasticsearch into a capabilities response
    :param agg_dict:
    :return:
    """
    services = []

    for service in agg_dict["services"]:
        if service == "openeo":
            services.append(
                {
                    "title": service,
                    "href": f'{opensearch_settings.OPENEO_HOST}/collections/{agg_dict["id"].lower()}.openeo',
                }
            )

        else:
            services.append(
                {
                    "title": service,
                    "href": f'{thredds_path(service,"")}/{agg_dict["id"]}{SUFFIX_MAP.get(service,"")}',
                }
            )

    return services
