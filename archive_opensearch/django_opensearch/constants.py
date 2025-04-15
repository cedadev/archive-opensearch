# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '29 May 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

DEFAULT = 'default'

SUFFIX_MAP = {
    'wms': '?service=WMS&version=1.3.0&request=GetCapabilities',
    'wcs': '?service=WCS&version=1.0.0&request=GetCapabilities'
}