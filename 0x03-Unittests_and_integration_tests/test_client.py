#!/usr/bin/env python3


import unittest
from unittest.mock import patch
from parameterized import parameterized  # type: ignore
from client import GithubOrgClient
from parameterized import parameterized_class
from fixtures import org_payload, repos_payload, expected_repos, apache2_repos


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
        the name of the GitHub organization to test (e.g., "google", "abc")
        mock_get_json : unittest.mock.Mock
        A mock object replacing get_json prevents real HTTP calls.
        """
        name = f"https://api.github.com/orgs/{org_name}/repos"
        expected = {"repos_url": name}
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
            self.assertEqual(client._public_repos_url, "http://eg.com/repos")

    @patch("client.get_json")
    def test_public_repos(self, mock_get_json):
        """
        Unit-test GithubOrgClient.public_repos.

        This test verifies that:
        - The method returns the expected list of repository names
          based on a mocked JSON payload.
        - The `_public_repos_url` property is patched to return a
          known value.
        - The `get_json` function is called exactly once with the
          mocked URL.
        """
        # Mocked payload returned by get_json
        mock_payload = [
            {"name": "repo1"},
            {"name": "repo2"},
            {"name": "repo3"},
        ]
        mock_get_json.return_value = mock_payload

        client = GithubOrgClient("myorg")

        # Patch the _public_repos_url property to return a known URL
        with patch.object(
            GithubOrgClient, "_public_repos_url", "http://example.com/repos"
        ):
            repos = client.public_repos()

        # Expected list of repo names
        expected = ["repo1", "repo2", "repo3"]
        self.assertEqual(repos, expected)

        # Ensure the property and get_json were called once
        mock_get_json.assert_called_once_with("http://example.com/repos")

    @parameterized.expand(
        [
            ({"license": {"key": "my_license"}}, "my_license", True),
            ({"license": {"key": "other_license"}}, "my_license", False),
        ]
    )
    def test_has_license(self, repo, license_key, expected):
        """
        Unit-test GithubOrgClient.has_license.

        This test verifies that the static method correctly determines
        whether a given repository dictionary contains the specified
        license key.

        Parameters
        ----------
        repo : dict
            A repository dictionary containing a license key.
        license_key : str
            The license key to check for.
        expected : bool
            The expected boolean result of the check.
        """
        self.assertEqual(GithubOrgClient.has_license(repo, license_key), expected)


@parameterized_class(
    [
        {
            "org_payload": org_payload,
            "repos_payload": repos_payload,
            "expected_repos": expected_repos,
            "apache2_repos": apache2_repos,
        }
    ]
)
class TestIntegrationGithubOrgClient(unittest.TestCase):
    """
    Integration tests for GithubOrgClient.public_repos.

    These tests mock only the external HTTP requests (requests.get),
    while allowing the rest of the client logic to execute normally.
    The fixtures provide example payloads for organizations and repos.
    """

    @classmethod
    def setUpClass(cls):
        """
        Set up the integration test environment.

        This method patches requests.get so that no real HTTP calls
        are made. The side_effect ensures that calling .json() on the
        mocked response returns the appropriate fixture payloads
        depending on the requested URL.
        """
        cls.get_patcher = patch("client.requests.get")

        def side_effect(url):
            mock_response = Mock()
            if url == GithubOrgClient.ORG_URL.format(org="google"):
                mock_response.json.return_value = cls.org_payload
            elif url == cls.org_payload["repos_url"]:
                mock_response.json.return_value = cls.repos_payload
            else:
                mock_response.json.return_value = {}
            return mock_response

        cls.mock_get = cls.get_patcher.start()
        cls.mock_get.side_effect = side_effect

    @classmethod
    def tearDownClass(cls):
        """
        Tear down the integration test environment.

        This method stops the patcher so that requests.get is restored
        to its original behavior after the tests complete.
        """
        cls.get_patcher.stop()

    def test_public_repos(self):
        """
        Test that public_repos returns the expected list of repo names
        based on the mocked fixtures.
        """
        client = GithubOrgClient("google")
        self.assertEqual(client.public_repos(), self.expected_repos)

    def test_public_repos_with_license(self):
        """
        Test that public_repos correctly filters repos by license key.
        """
        client = GithubOrgClient("google")
        self.assertEqual(client.public_repos("apache-2.0"), self.apache2_repos)
