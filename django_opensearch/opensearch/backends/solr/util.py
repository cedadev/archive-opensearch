# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '07 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

def pairwise(l):
    i = 0
    while i < len(l):
        yield l[i], l[i + 1]
        i += 2