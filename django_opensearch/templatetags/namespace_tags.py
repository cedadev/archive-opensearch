# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '09 Oct 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django import template
from django_opensearch.opensearch.backends.base import NamespaceMap

register = template.Library()


@register.filter()
def xml_namespace(value):

    key, with_namespace = NamespaceMap.get_namespace(value)

    return with_namespace