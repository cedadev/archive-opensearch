# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '18 Mar 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django_opensearch import settings as opensearch_settings

mapping = ('/neodc','')

method_map = {
    'http': '/fileServer',
    'opendap': '/dodsC',
    'wms': '/wms',
    'wcs': '/wcs'
}


def thredds_path(method, path):
    """
    Generates the correct path to work with the THREDDS instance
    :param path: File path to server
    :param method: http|opendap
    :return: path to resource
    """

    thredds_server = opensearch_settings.THREDDS_HOST
    method_path = method_map[method]
    mapped_path = path.replace(*mapping)

    if hasattr(opensearch_settings, 'BACKUP_DOWNLOADS') and method == 'http':
        if opensearch_settings.BACKUP_DOWNLOADS is not None:
            return f'{opensearch_settings.BACKUP_DOWNLOADS}{mapped_path}'

    return f'{thredds_server}/thredds{method_path}{mapped_path}'


