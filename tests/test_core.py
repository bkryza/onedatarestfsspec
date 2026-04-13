"""Tests for OnedataFileSystem core functionality."""

from unittest.mock import Mock, patch

import pytest

from onedatafilerestclient.errors import OnedataRESTError
from onedatarestfsspec.core import OnedataFile, OnedataFileSystem


@pytest.fixture
def fs():
    """Create a OnedataFileSystem instance with mocked client."""
    # Create a mock client with proper spec to avoid Mock() issues
    client_mock = Mock()

    # Set up all the methods we need with proper return values
    client_mock.list_spaces.return_value = ["space1", "space2"]
    client_mock.get_attributes.return_value = {
        "name": "test.txt",
        "type": "REG",
        "size": 100,
        "mtime": 1234567890,
        "atime": 1234567890,
        "posixPermissions": "644",
    }
    client_mock.list_children.return_value = {
        "children": [
            {"name": "file1.txt", "type": "REG", "size": 50},
            {"name": "dir1", "type": "DIR", "size": 0},
        ],
        "isLast": True,
        "nextPageToken": None,
    }
    client_mock.get_file_content.return_value = b"test content"
    client_mock.get_file_id.return_value = "file_id_123"
    client_mock.create_file.return_value = "file_id_123"
    client_mock.put_file_content.return_value = None
    client_mock.remove.return_value = None

    with (
        patch("onedatarestfsspec.core.OnedataFileRESTClient", return_value=client_mock),
        patch(
            "onedatarestfsspec.core.get_onedata_config_from_env",
            return_value={
                "onezone_host": None,
                "token": None,
                "preferred_providers": None,
                "verify_ssl": True,
                "timeout": 30,
            },
        ),
    ):

        filesystem = OnedataFileSystem(
            onezone_host="https://onezone.example.com", token="test_token"
        )

        # Make sure the filesystem uses our mock client
        filesystem.client = client_mock

        yield filesystem


class TestOnedataFileSystem:
    """Test OnedataFileSystem functionality."""

    def test_init_with_valid_params(self):
        """Test initialization with valid parameters."""
        with patch("onedatarestfsspec.core.OnedataFileRESTClient"):
            fs = OnedataFileSystem(
                onezone_host="https://onezone.example.com", token="test_token"
            )
            assert fs.onezone_host == "https://onezone.example.com"
            assert fs.token == "test_token"
            assert fs.protocol == "onedata"

    def test_init_missing_params(self):
        """Test initialization with missing required parameters."""
        with patch(
            "onedatarestfsspec.core.get_onedata_config_from_env"
        ) as mock_env_config:
            mock_env_config.return_value = {
                "onezone_host": None,
                "token": None,
                "preferred_providers": None,
                "verify_ssl": True,
                "timeout": 30,
            }
            with pytest.raises(
                ValueError, match="Both onezone_host and token must be provided"
            ):
                OnedataFileSystem()

    def test_split_onedata_path(self, fs):
        """Test path splitting functionality."""
        # Test space root
        space, path = fs._split_onedata_path(  # pylint: disable=protected-access
            "space1"
        )
        assert space == "space1"
        assert path is None

        # Test file in space
        space, path = fs._split_onedata_path(  # pylint: disable=protected-access
            "space1/dir/file.txt"
        )
        assert space == "space1"
        assert path == "dir/file.txt"

        # Test root path
        space, path = fs._split_onedata_path("/")  # pylint: disable=protected-access
        assert space == ""
        assert path is None

    def test_ls_root(self, fs):
        """Test listing root directory (spaces)."""
        result = fs.ls("/")
        assert result == ["space1", "space2"]

        # Test with detail
        result = fs.ls("/", detail=True)
        assert len(result) == 2
        assert result[0]["name"] == "space1"
        assert result[0]["type"] == "directory"

    def test_ls_space_directory(self, fs):
        """Test listing space directory."""
        result = fs.ls("space1/")
        assert "space1/file1.txt" in result
        assert "space1/dir1" in result

        # Test with detail
        result = fs.ls("space1/", detail=True)
        assert len(result) == 2
        file_info = next(item for item in result if item["name"] == "space1/file1.txt")
        assert file_info["type"] == "file"
        assert file_info["size"] == 50

    def test_info(self, fs):
        """Test getting file info."""
        info = fs.info("space1/test.txt")
        assert info["name"] == "space1/test.txt"
        assert info["type"] == "file"
        assert info["size"] == 100
        assert info["mode"] == "644"

    def test_info_not_found(self, fs):
        """Test getting info for non-existent file."""
        fs.client.get_attributes.side_effect = OnedataRESTError("enoent", 404)

        with pytest.raises(FileNotFoundError):
            fs.info("space1/nonexistent.txt")

    def test_cat_file(self, fs):
        """Test reading file content."""
        content = fs.cat_file("space1/test.txt")
        assert content == b"test content"

        fs.client.get_file_content.assert_called_once_with(
            "space1", file_path="test.txt"
        )

    def test_cat_file_with_range(self, fs):
        """Test reading file content with byte range."""
        with patch.object(fs, "_get_file_size", return_value=100):
            # Reset the mock to count calls from this test only
            fs.client.get_file_content.reset_mock()

            content = fs.cat_file("space1/test.txt", start=10, end=20)

            assert content == b"test content"
            fs.client.get_file_content.assert_called_with(
                "space1", file_path="test.txt", offset=10, size=10
            )

    def test_exists(self, fs):
        """Test checking if file exists."""
        assert fs.exists("space1/test.txt") is True

        fs.client.get_attributes.side_effect = OnedataRESTError("enoent", 404)
        assert fs.exists("space1/nonexistent.txt") is False

    def test_isdir(self, fs):
        """Test checking if path is directory."""
        fs.client.get_attributes.return_value = {"type": "DIR", "name": "dir1"}
        assert fs.isdir("space1/dir1") is True

        fs.client.get_attributes.return_value = {"type": "REG", "name": "file1.txt"}
        assert fs.isdir("space1/file1.txt") is False

    def test_isfile(self, fs):
        """Test checking if path is file."""
        fs.client.get_attributes.return_value = {"type": "REG", "name": "file1.txt"}
        assert fs.isfile("space1/file1.txt") is True

        fs.client.get_attributes.return_value = {"type": "DIR", "name": "dir1"}
        assert fs.isfile("space1/dir1") is False

    def test_size(self, fs):
        """Test getting file size."""
        size = fs.size("space1/test.txt")
        assert size == 100

    def test_makedirs(self, fs):
        """Test creating directories."""
        fs.makedirs("space1/new/dir")

        fs.client.create_file.assert_called_once_with(
            "space1", file_path="new/dir", file_type="DIR", create_parents=True
        )

    def test_rm_file(self, fs):
        """Test removing file."""
        fs.rm_file("space1/test.txt")

        fs.client.remove.assert_called_once_with("space1", file_path="test.txt")

    def test_rm_file_not_found(self, fs):
        """Test removing non-existent file."""
        fs.client.remove.side_effect = OnedataRESTError("enoent", 404)

        with pytest.raises(FileNotFoundError):
            fs.rm_file("space1/nonexistent.txt")

    def test_cp_file(self, fs):
        """Test copying file within Onedata."""
        with patch.object(fs, "_create_file", return_value="file_id_456"):
            # Reset mocks to count calls from this test only
            fs.client.get_file_content.reset_mock()
            fs.client.put_file_content.reset_mock()

            fs.cp_file("space1/source.txt", "space1/dest.txt")

            # Verify the file content was read and written
            fs.client.get_file_content.assert_called_with(
                "space1", file_path="source.txt"
            )
            fs.client.put_file_content.assert_called_with(
                "space1", data=b"test content", file_id="file_id_456"
            )


class TestOnedataFile:
    """Test OnedataFile functionality."""

    def test_init_read_mode(self, fs):
        """Test OnedataFile initialization in read mode."""
        # Mock the info method that gets called during file init
        with (
            patch.object(
                fs,
                "info",
                return_value={"name": "space1/test.txt", "size": 100, "type": "file"},
            ),
            patch.object(fs, "_get_file_size", return_value=100),
            patch.object(fs, "_get_file_id", return_value="file_id_123"),
        ):

            file_obj = OnedataFile(fs, "space1/test.txt", "rb")

            assert file_obj.space_name == "space1"
            assert file_obj.file_path == "test.txt"
            # The size comes from fs.info() during AbstractBufferedFile.__init__

    def test_fetch_range(self, fs):
        """Test fetching byte range from file."""
        with (
            patch.object(
                fs,
                "info",
                return_value={"name": "space1/test.txt", "size": 100, "type": "file"},
            ),
            patch.object(fs, "_get_file_size", return_value=100),
            patch.object(fs, "_get_file_id", return_value="file_id_123"),
        ):

            file_obj = OnedataFile(fs, "space1/test.txt", "rb")
            file_obj.file_id = "file_id_123"

            content = file_obj._fetch_range(10, 20)  # pylint: disable=protected-access

            assert content == b"test content"  # Should return mock content

    def test_upload_chunk(self, fs):
        """Test uploading chunk of data."""
        with patch.object(fs, "_create_file", return_value="file_id_789"):

            # Mock buffer with proper return values
            mock_buffer = Mock()
            mock_buffer.tell.return_value = 10
            mock_buffer.getvalue.return_value = b"chunk data"
            mock_buffer.seek.return_value = 0  # seek() should return position
            mock_buffer.truncate.return_value = None

            file_obj = OnedataFile(fs, "space1/test.txt", "wb")
            file_obj.buffer = mock_buffer
            file_obj.offset = 0

            result = file_obj._upload_chunk()  # pylint: disable=protected-access

            assert result is True
            assert file_obj.offset == 10


if __name__ == "__main__":
    pytest.main([__file__])
