#!/usr/bin/env python3

import unittest
from unittest.mock import patch, Mock
from parameterized import parameterized  # type: ignore
from utils import get_json
from utils import access_nested_map


class TestAccessNestedMap(unittest.TestCase):
    """TestAccessNestedMap class to test access_nested_map function"""

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
    def test_access_nested_map_exception(self, nested_map, path, expected_message):
        with self.assertRaises(KeyError) as cm:
            access_nested_map(nested_map, path)
        self.assertEqual(str(cm.exception), expected_message)


class TestGetJson(unittest.TestCase):
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
