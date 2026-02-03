"""Tests for configuration handling."""

import os
from unittest.mock import patch

import pytest

from onedatafsspec.config import (
    get_onedata_config_from_env,
    merge_config,
    parse_onedata_url,
)


class TestParseOnedataUrl:
    """Test URL parsing functionality."""

    def test_basic_url(self):
        """Test parsing basic Onedata URL."""
        url = "onedata://token123@onezone.example.com/space1/path/to/file"
        config = parse_onedata_url(url)

        assert config["onezone_host"] == "https://onezone.example.com"
        assert config["token"] == "token123"
        assert config["path"] == "/space1/path/to/file"
        assert config["verify_ssl"] is True
        assert config["timeout"] == 30.0

    def test_url_with_query_params(self):
        """Test parsing URL with query parameters."""
        url = "onedata://token123@onezone.example.com/space1?verify_ssl=false&timeout=60&providers=provider1,provider2"
        config = parse_onedata_url(url)

        assert config["onezone_host"] == "https://onezone.example.com"
        assert config["token"] == "token123"
        assert config["path"] == "/space1"
        assert config["verify_ssl"] is False
        assert config["timeout"] == 60.0
        assert config["preferred_providers"] == ["provider1", "provider2"]

    def test_url_without_path(self):
        """Test parsing URL without path component."""
        url = "onedata://token123@onezone.example.com"
        config = parse_onedata_url(url)

        assert config["onezone_host"] == "https://onezone.example.com"
        assert config["token"] == "token123"
        assert config["path"] == "/"

    def test_invalid_protocol(self):
        """Test parsing invalid protocol."""
        url = "invalid://token123@onezone.example.com/space1"

        with pytest.raises(ValueError, match="URL must start with 'onedata://'"):
            parse_onedata_url(url)

    def test_missing_token(self):
        """Test parsing URL without token."""
        url = "onedata://onezone.example.com/space1"

        with pytest.raises(
            ValueError, match="Token must be provided as username in URL"
        ):
            parse_onedata_url(url)

    def test_missing_host(self):
        """Test parsing URL without host."""
        url = "onedata://token123@/space1"

        with pytest.raises(ValueError, match="Onezone host must be provided in URL"):
            parse_onedata_url(url)


class TestGetOnedataConfigFromEnv:
    """Test environment variable configuration."""

    def test_all_env_vars_set(self):
        """Test when all environment variables are set."""
        env_vars = {
            "ONEDATA_ONEZONE_HOST": "https://onezone.example.com",
            "ONEDATA_TOKEN": "env_token123",
            "ONEDATA_PREFERRED_PROVIDERS": "provider1,provider2,provider3",
            "ONEDATA_VERIFY_SSL": "false",
            "ONEDATA_TIMEOUT": "45",
        }

        with patch.dict(os.environ, env_vars):
            config = get_onedata_config_from_env()

            assert config["onezone_host"] == "https://onezone.example.com"
            assert config["token"] == "env_token123"
            assert config["preferred_providers"] == [
                "provider1",
                "provider2",
                "provider3",
            ]
            assert config["verify_ssl"] is False
            assert config["timeout"] == 45.0

    def test_no_env_vars_set(self):
        """Test when no environment variables are set."""
        env_vars = {
            "ONEDATA_ONEZONE_HOST": "",
            "ONEDATA_TOKEN": "",
            "ONEDATA_PREFERRED_PROVIDERS": "",
            "ONEDATA_VERIFY_SSL": "",
            "ONEDATA_TIMEOUT": "",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = get_onedata_config_from_env()

            assert config["onezone_host"] is None
            assert config["token"] is None
            assert config["preferred_providers"] is None
            assert config["verify_ssl"] is True  # default
            assert config["timeout"] == 30.0  # default

    def test_partial_env_vars_set(self):
        """Test when only some environment variables are set."""
        env_vars = {
            "ONEDATA_ONEZONE_HOST": "https://onezone.example.com",
            "ONEDATA_TOKEN": "env_token123",
        }

        with patch.dict(os.environ, env_vars, clear=True):
            config = get_onedata_config_from_env()

            assert config["onezone_host"] == "https://onezone.example.com"
            assert config["token"] == "env_token123"
            assert config["preferred_providers"] is None
            assert config["verify_ssl"] is True  # default
            assert config["timeout"] == 30.0  # default


class TestMergeConfig:
    """Test configuration merging."""

    def test_merge_priority(self):
        """Test that explicit config has highest priority."""
        url_config = {
            "onezone_host": "https://url.example.com",
            "token": "url_token",
            "verify_ssl": True,
        }

        env_config = {
            "onezone_host": "https://env.example.com",
            "token": "env_token",
            "verify_ssl": False,
            "timeout": 60,
        }

        explicit_config = {
            "onezone_host": "https://explicit.example.com",
            "preferred_providers": ["explicit_provider"],
        }

        config = merge_config(url_config, env_config, explicit_config)

        # Explicit config should override
        assert config["onezone_host"] == "https://explicit.example.com"
        assert config["preferred_providers"] == ["explicit_provider"]

        # URL config should override env config
        assert config["token"] == "url_token"
        assert config["verify_ssl"] is True

        # Env config should be used when others don't provide value
        assert config["timeout"] == 60

    def test_merge_with_none_values(self):
        """Test merging with None values."""
        url_config = {"onezone_host": None, "token": "url_token"}

        env_config = {
            "onezone_host": "https://env.example.com",
            "token": None,
            "verify_ssl": True,
        }

        explicit_config = {"onezone_host": None, "timeout": 45}

        config = merge_config(url_config, env_config, explicit_config)

        assert config["onezone_host"] == "https://env.example.com"
        assert config["token"] == "url_token"
        assert config["verify_ssl"] is True
        assert config["timeout"] == 45

    def test_merge_empty_configs(self):
        """Test merging empty configurations."""
        config = merge_config({}, {}, {})
        assert config == {}


if __name__ == "__main__":
    pytest.main([__file__])
