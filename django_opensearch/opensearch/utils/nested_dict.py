# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '05 Nov 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from collections import UserDict

class NestedDict(UserDict):

    def nested_get(self, key_list):
        """

        :param key_list:
        :return:
        """
        dict_nest = self.data

        for key in key_list[:-1]:
            dict_nest = dict_nest.get(key)

            if dict_nest is None:
                return

        return dict_nest.get(key_list[-1])

    def nested_put(self, key_list, value):
        """

        :param key_list:
        :param value:
        :return:
        """

        dict_nest = self.data

        for key in key_list[:-1]:
            sub_dict = dict_nest.get(key)

            if not sub_dict:
                sub_dict = {}
                dict_nest[key] = sub_dict

            dict_nest = sub_dict

        dict_nest[key_list[-1]] = value



if __name__ == '__main__':

    import unittest

    class TestNestedGet(unittest.TestCase):

        def test_more_keys_than_levels(self):
            get_test_1 = NestedDict({'a': {'int': 2}})

            test = get_test_1.nested_get(['a','b','c'])

            self.assertIsNone(test)

        def test_missing_final_key(self):

            get_test_2 = NestedDict({'a': {'b': {'int': 2}}})

            test = get_test_2.nested_get(['a', 'b', 'c'])

            self.assertIsNone(test)

        def test_existing_nested_get(self):

            get_test_3 = NestedDict({'a': {'b': {'c': 'yay'}}})

            test = get_test_3.nested_get(['a', 'b', 'c'])

            self.assertEqual(test, 'yay')


    class TestNestedPut(unittest.TestCase):

        end_goal = {'a':{'b':{'c': 'yay'}}}

        def test_no_valid_keys(self):
            put_test = NestedDict({'int': 2})

            expected = put_test.copy()
            expected.update(self.end_goal)

            put_test.nested_put(['a','b','c'], 'yay')

            self.assertEqual(put_test, expected)

        def test_does_not_change_interim(self):

            put_test = NestedDict({'a': {'int': 2}})

            put_test.nested_put(['a', 'b', 'c'], 'yay')

            self.assertEqual(put_test, {'a': {'int': 2, 'b': {'c': 'yay'}}})

        def test_overwrite_existing(self):

            put_test = NestedDict({'a': {'b': {'c': 'nope'}}})

            expected = put_test.copy()
            expected.update(self.end_goal)

            put_test.nested_put(['a', 'b', 'c'], 'yay')

            self.assertEqual(put_test, expected)

        def test_does_not_change_target(self):
            put_test = NestedDict({'a': {'b': {'int': 2}}})

            expected = put_test.copy()
            expected.update(self.end_goal)

            put_test.nested_put(['a', 'b', 'c'], 'yay')

            self.assertEqual(put_test, {'a': {'b': {'int': 2, 'c': 'yay'}}})

    unittest.main()


