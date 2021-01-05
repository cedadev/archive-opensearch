# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '25 Jul 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from django_opensearch.opensearch.backends.base import Granule


class Granule(Granule):

    def search(self, params, request, **kwargs):
        """
        Handle searching with elasticsearch above the 10k random access
        limit.

        :param params:
        :param request:
        :param kwargs:
        :return:
        """

        search_after, reverse = self._check_scrolling(request)

        kwargs.update({
            'search_after': search_after,
            'reverse': reverse
        })

        results = super().search(params, request, **kwargs)

        self._set_search_after(request, results)

        return results

    def _check_scrolling(self, request):
        """
        Retrieve the search_after key if the request is the same with a page within 1 of the previous search
        :param request: WSGI Request
        :return:
        """

        session = request.session

        search_params = request.GET
        start_page = int(search_params.get('startPage', 1))
        last_query = session.get('last_query')

        # Only need to check if we are looking at a page higher than the first page
        if start_page > 1:

            # Check if we have a last_query in the session
            if last_query:

                last_query_page = int(last_query.get('startPage', 1))

                # If the page is not within 1 we can't use search after
                if not abs(start_page - last_query_page) <= 1:
                    return None, None

                # Check that the search parameters are the same
                ignored_params = ('startPage', 'httpAccept')
                current_search = {key for key in search_params if key not in ignored_params}
                last_search = {key for key in last_query if key not in ignored_params}

                # If searches are not the same, cannot use last query
                if not current_search == last_search:
                    return None, None

                # Check if values for search facets are the same
                for key in current_search:
                    if not search_params[key] == last_query[key]:
                        return None, None

                # Page is  +/-1, search params are the same and values for them
                # are the same. Retrieve the search_after key
                diff = start_page - last_query_page

                if diff == -1:
                    search_after = session.get('before_key')
                elif diff == 0:
                    search_after = session.get('current_key')
                else:
                    search_after = session.get('after_key')

                reverse = any((diff < 0, session.get('downpage')))

                return search_after, reverse

        return None, None

    def _set_search_after(self, request, results):

        before_key = results.search_before
        after_key = results.search_after

        session = request.session
        search_params = request.GET

        last_query = session.get('last_query')
        start_page = int(search_params.get('startPage', 1))

        # Set the last query
        session['last_query'] = search_params

        if start_page > 1:
            if last_query:
                last_query_page = int(last_query.get('startPage', 1))

                diff = start_page - last_query_page

                if diff == -1:
                    session['current_key'] = session['before_key']
                    session['downpage'] = True

                elif diff == 1:
                    session['current_key'] = session['after_key']
                    session['downpage'] = False

        session['before_key'] = before_key
        session['after_key'] = after_key