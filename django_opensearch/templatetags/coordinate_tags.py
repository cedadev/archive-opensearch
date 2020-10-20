# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '20 Oct 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django import template

register = template.Library()


def flatten(object):
    for item in object:
        if isinstance(item, (list, tuple, set)):
            yield from flatten(item)
        else:
            yield item


@register.filter()
def expand_coordinates(value):
    """
    Expand bounding box coordinates provided as [[W,S],[E,N]]
    :param value: Bounding box
    :return: Space sep string
    """
    flattened_list = list(flatten(value))
    return ' '.join(str(v) for v in flattened_list)