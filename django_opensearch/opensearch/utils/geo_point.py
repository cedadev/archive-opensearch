# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '24 Mar 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from abc import ABC, abstractmethod


class BoundingBox(ABC):

    @property
    @abstractmethod
    def north(self):
        pass

    @property
    @abstractmethod
    def east(self):
        pass

    @property
    @abstractmethod
    def south(self):
        pass

    @property
    @abstractmethod
    def west(self):
        pass

    def bbox(self):
        sw = [self.west, self.south]
        ne = [self.east, self.north]

        return [sw, ne]


class Envelope(BoundingBox):

    TYPE = 'Envelope'

    def __init__(self, envelope):
        self.envelope = envelope

    @property
    def north(self):
        return self.envelope[0][1]

    @property
    def south(self):
        return self.envelope[1][1]

    @property
    def east(self):
        return self.envelope[1][0]

    @property
    def west(self):
        return self.envelope[0][0]


class Point(BoundingBox):

    TYPE = 'Point'

    def __init__(self, point):
        self.lon, self.lat = point
        self.point = point

    @property
    def north(self):
        return self.lat

    @property
    def south(self):
        return self.lat

    @property
    def east(self):
        return self.lon

    @property
    def west(self):
        return self.lon
