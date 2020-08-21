# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '23 Mar 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django.conf import settings
from django_opensearch_osddvars.constants import SUFFIX_MAP
from django_opensearch_osddvars.opensearch.utils import thredds_path
import urllib.parse


def get_thredds_aggregation(id, format='xml'):
    return f'http://{settings.THREDDS_HOST}/thredds/datasets/{id}.{format}?dataset={id}'


def get_aggregation_search_link(base_url, collection_id, aggregation_id, format):
    return f'{base_url}/request?parentIdentifier={collection_id}&drsId={aggregation_id}&httpAccept={urllib.parse.quote(format)}'


def get_aggregation_capabilities(agg_dict):
    """
    Turn response from Elasticsearch into a capabilities response
    :param agg_dict:
    :return:
    """
    services = []

    for service in agg_dict['services']:
        services.append({
            'title': service,
            'href': f'http://{thredds_path(service,"")}/{agg_dict["id"]}{SUFFIX_MAP.get(service,"")}'
        })

    return services


def split_outside_quotes(s, delim):
    """
    Split a string s by character delim, but only when delim is not enclosed
    in double quotes.
    Return a list of the split parts (including quotes if present)
    """
    parts = []
    in_quotes = False
    temp = ""

    for char in s:
        if not in_quotes and char == delim:
            parts.append(temp)
            temp = ""
            continue

        temp += char
        if char == '"':
            in_quotes = not in_quotes

    if temp:
        parts.append(temp)
    return parts


def remove_quotes(s):
    """
    Return a string s with enclosing double quotes removed.
    """
    if not s.startswith('"') or not s.endswith('"'):
        raise ValueError("String '{}' is not wrapped in quotes".format(s))
    return s[1:-1]


def get_best_name(phenomena):
    """
    Create a best_name field which takes the best name as defined by the preference order
    :param phenomena: phenomena attributes in form {"standard_name":"time"}
    :return: best_name(string)
    """
    preference_order = ["long_name","standard_name","title","name","short_name","var_id"]

    for name in preference_order:
        best_name = phenomena.get(name)
        if best_name:
            return best_name


def parse_key(key):
    """
    Convert a bucket key from the ES aggregation response to a dictionary.
    The format is as follows:
        - All keys and values are enclosed in double quotes (")
        - key-value pairs are separated by ','
        - key and value are separated by ':'
        - if key is 'names' then value is a list separated with ';'
    """
    err_msg = "Invalid key '{}'".format(key)

    d = {}
    pairs = split_outside_quotes(key, ",")
    for pair in pairs:

        try:
            label, val_str = split_outside_quotes(pair, ":")
        except ValueError:
            raise ValueError("{}: must be exactly 1 colon in key-value pair".format(err_msg))

        try:
            label = remove_quotes(label)
            # Split val_str by ; to get list of values, remove quotes for each,
            # and filter out empty values
            val_list = [_f for _f in map(remove_quotes, split_outside_quotes(val_str, ";")) if _f]

        except ValueError as ex:
            raise ValueError("{}: {}".format(err_msg, ex))

        if label == "names":
            continue
        else:
            if len(val_list) > 1:
                raise ValueError("{}: only 'names' can contains multiple values".format(err_msg))
            if not val_list:
                continue
            d[label] = val_list[0]

    d['best_name'] = get_best_name(d)
    return d
