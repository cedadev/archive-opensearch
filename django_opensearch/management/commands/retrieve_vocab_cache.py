# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '31 Jul 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from cci_tag_scanner.facets import Facets
import json


class Command(BaseCommand):
    help = 'Downloads vocabs from vocab server to json file'

    def handle(self, *args, **options):

        facets = Facets()
        with open(settings.VOCAB_CACHE_FILE,'w') as writer:
            json.dump(facets.to_json(), writer)