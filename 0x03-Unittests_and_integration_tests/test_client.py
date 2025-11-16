#!/usr/bin/env python3


import unittest
from unittest.mock import patch
from parameterized import parameterized  # type: ignore
from client import GithubOrgClient


class TestGithubOrgClient(unittest.TestCase):
    """
    Test suite for the GithubOrgClient class.

    This class contains unit tests that validate the behavior of the
    GithubOrgClient methods. The tests are designed to ensure that
    external HTTP calls are not made during testing by mocking the
    get_json function. Parameterization is used to run the same test
    logic against multiple organization names.
    """

    @parameterized.expand(
        [
            ("google",),
            ("abc",),
        ]
    )
    @patch("client.get_json")
    def test_org(self, org_name, mock_get_json):
        """
        Test that GithubOrgClient.org returns the expected value.

        This test verifies that:
        - The org property correctly calls get_json with the proper URL.
        - The return value of get_json is returned unchanged by org.
        - get_json is called exactly once per organization name.
        - No external HTTP requests are made (get_json is mocked).

        Parameters
        ----------
        org_name : str
            The name of the GitHub organization to test (e.g., "google", "abc").
        mock_get_json : unittest.mock.Mock
            A mock object replacing the get_json function to prevent real HTTP calls.
        """
        expected = {"repos_url": f"https://api.github.com/orgs/{org_name}/repos"}
        mock_get_json.return_value = expected

        client = GithubOrgClient(org_name)
        self.assertEqual(client.org, expected)

        mock_get_json.assert_called_once_with(f"https://api.github.com/orgs/{org_name}")

    def test_public_repos_url(self):
        """
        Unit-test GithubOrgClient._public_repos_url.

        This test patches the `org` property to return a known payload
        containing a `repos_url`. It then verifies that the `_public_repos_url`
        property correctly extracts and returns that URL.
        """
        client = GithubOrgClient("myorg")
        payload = {"repos_url": "http://example.com/repos"}

        # Patch the org property to return the payload
        with patch.object(GithubOrgClient, "org", payload):
            self.assertEqual(client._public_repos_url, "http://example.com/repos")
