# encoding: utf-8
"""
Provides a base to implement and interface and makes sure all subclasses have
the required interfaces
"""
__author__ = 'Richard Smith'
__date__ = '31 Jul 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from abc import ABC, abstractmethod


class BaseLookupHandler(ABC):
    """
    Base class to define interface
    """

    @abstractmethod
    def lookup_values(self,facet, values_list):
        pass
