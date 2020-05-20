# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '23 Mar 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django.conf import settings
import requests
import xmltodict
from django_opensearch.constants import SUFFIX_MAP


def get_thredds_aggregation(id, format='xml'):
    return f'http://{settings.THREDDS_HOST}/thredds/datasets/{id}.{format}?dataset={id}'


def get_aggregation_capabilities(id):
    r = requests.get(get_thredds_aggregation(id))

    services = []

    data = xmltodict.parse(r.text)

    for service in data['catalog']['service']:
        services.append({
            'title': service['@name'],
            'href': f'http://{settings.THREDDS_HOST}{service["@base"]}{id}{SUFFIX_MAP.get(service["@name"],"")}'
        })

    return services