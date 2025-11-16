#!/usr/bin/env python3

import unittest
from unittest.mock import patch, Mock
from parameterized import parameterized  # type: ignore
from utils import get_json
from utils import access_nested_map, memoize


class TestAccessNestedMap(unittest.TestCase):
    """TestAccessNestedMap class to test access_nested_map function
    from the utils module. It uses parameterized
    to test multiple inputs and expected outputs.

    Test Methods
    test_access_nested_map(self, nested_map, path, expected)
    This test method is decorated with @parameterized.expand and is executed
    multiple times with different input parameters.
    - Parameters:
        - nested_map: A dictionary representing the nested map to be
        accessed.
        - path: A tuple representing the keys to access the nested map.
        - expected: The expected value to be returned by access_nested_map.
    """

    @parameterized.expand(
        [
            ({"a": 1}, ("a",), 1),
            ({"a": {"b": 2}}, ("a",), {"b": 2}),
            ({"a": {"b": 2}}, ("a", "b"), 2),
        ]
    )
    def test_access_nested_map(self, nested_map, path, expected):
        self.assertEqual(access_nested_map(nested_map, path), expected)

    @parameterized.expand(
        [
            ({}, ("a",), "'a'"),
            ({"a": 1}, ("a", "b"), "'b'"),
        ]
    )
    def test_access_nested_map_exception(self, nested_map, path, expected_msg):
        with self.assertRaises(KeyError) as cm:
            access_nested_map(nested_map, path)
        self.assertEqual(str(cm.exception), expected_msg)


class TestGetJson(unittest.TestCase):
    """The TestGetJson class contains a series
        of test methods that validate the behavior of the get_json function.

    Test Methods

    test_get_json(self, url, expected_json)
    This test method is decorated with @parameterized.expand and is executed
    multiple times with different input parameters.

    - Parameters:
        - url: The URL to be tested.
        - expected_json: The expected JSON response from the get_json function.
    - Purpose:
        - To test the get_json function with various URLs and
        verify that it returns the expected JSON responses.
    - Test Logic:
        1. Call the get_json function with the provided url.
        2. Compare the returned JSON response with the expected_json.
        3. Assert that the two JSON responses are equal using self.assertEqual.

    Example Usage

    To use the TestGetJson class, you can create an instance of the class
    and run the test methods using the unittest framework.
    """

    @parameterized.expand(
        [
            ("http://example.com", {"payload": True}),
            ("http://another.com", {"data": 123}),
        ]
    )
    def test_get_json(self, url, expected):
        with patch("utils.requests.get") as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = expected
            mock_get.return_value = mock_response

            self.assertEqual(get_json(url), expected)
            mock_get.assert_called_once_with(url)


class TestMemoize(unittest.TestCase):
    """TestMemoize class to test memoize decorator from the utils module,
    ensuring the decorated method is called once and the result is cached."""

    def test_memoize(self):
        class TestClass:
            def a_method(self):
                return 42

            @memoize
            def a_property(self):
                return self.a_method()

        obj = TestClass()

        with patch.object(obj, "a_method", wraps=obj.a_method) as mock_method:
            # First access should call a_method
            self.assertEqual(obj.a_property, 42)
            mock_method.assert_called_once()

            # Second access should return cached value,not call a_method again
            self.assertEqual(obj.a_property, 42)
            mock_method.assert_called_once()
