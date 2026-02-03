"""Tests for utility functions."""

import pytest
from onedatafsspec.utils import (
    normalize_onedata_path,
    split_onedata_path,
    join_onedata_path,
    validate_onedata_path,
    get_parent_path,
    get_basename
)


class TestNormalizeOnedataPath:
    """Test path normalization."""
    
    def test_basic_path(self):
        """Test normalizing basic path."""
        assert normalize_onedata_path("space1/dir/file.txt") == "/space1/dir/file.txt"
    
    def test_path_with_leading_slash(self):
        """Test normalizing path with leading slash."""
        assert normalize_onedata_path("/space1/dir/file.txt") == "/space1/dir/file.txt"
    
    def test_path_with_protocol(self):
        """Test normalizing path with protocol."""
        path = "onedata://token@host/space1/dir/file.txt"
        assert normalize_onedata_path(path) == "/space1/dir/file.txt"
    
    def test_path_with_dots(self):
        """Test normalizing path with . and .. components."""
        assert normalize_onedata_path("/space1/./dir/../file.txt") == "/space1/file.txt"
    
    def test_empty_path(self):
        """Test normalizing empty path."""
        assert normalize_onedata_path("") == "/"
    
    def test_root_path(self):
        """Test normalizing root path."""
        assert normalize_onedata_path("/") == "/"
    
    def test_url_encoded_path(self):
        """Test normalizing URL-encoded path."""
        assert normalize_onedata_path("space%201/file%20name.txt") == "/space 1/file name.txt"


class TestSplitOnedataPath:
    """Test path splitting."""
    
    def test_space_only(self):
        """Test splitting space-only path."""
        space, path = split_onedata_path("/space1")
        assert space == "space1"
        assert path is None
    
    def test_space_with_file(self):
        """Test splitting space with file path."""
        space, path = split_onedata_path("/space1/dir/file.txt")
        assert space == "space1"
        assert path == "dir/file.txt"
    
    def test_root_path(self):
        """Test splitting root path."""
        space, path = split_onedata_path("/")
        assert space == ""
        assert path is None
    
    def test_empty_path(self):
        """Test splitting empty path."""
        space, path = split_onedata_path("")
        assert space == ""
        assert path is None
    
    def test_no_leading_slash(self):
        """Test splitting path without leading slash."""
        space, path = split_onedata_path("space1/dir/file.txt")
        assert space == "space1"
        assert path == "dir/file.txt"


class TestJoinOnedataPath:
    """Test path joining."""
    
    def test_space_only(self):
        """Test joining space only."""
        assert join_onedata_path("space1") == "/space1"
    
    def test_space_with_file(self):
        """Test joining space with file path."""
        assert join_onedata_path("space1", "dir/file.txt") == "/space1/dir/file.txt"
    
    def test_empty_space(self):
        """Test joining with empty space name."""
        assert join_onedata_path("", "file.txt") == "/file.txt"
    
    def test_none_file_path(self):
        """Test joining with None file path."""
        assert join_onedata_path("space1", None) == "/space1"


class TestValidateOnedataPath:
    """Test path validation."""
    
    def test_valid_paths(self):
        """Test valid paths."""
        valid_paths = [
            "/space1",
            "/space1/dir/file.txt",
            "space1/file.txt",
            "/space-name/sub_dir/file.name.ext",
            "/"
        ]
        
        for path in valid_paths:
            assert validate_onedata_path(path) is True
    
    def test_invalid_paths(self):
        """Test invalid paths."""
        invalid_paths = [
            "/space\\with\\backslash",  # space name contains backslash  
            "/space\x00with\x00null"  # space name contains null character
        ]
        
        for path in invalid_paths:
            assert validate_onedata_path(path) is False
            
        # Test that "/space/with/slash" is actually valid (space="space", file_path="with/slash")
        assert validate_onedata_path("/space/with/slash") is True


class TestGetParentPath:
    """Test getting parent path."""
    
    def test_file_path(self):
        """Test getting parent of file path."""
        assert get_parent_path("/space1/dir/file.txt") == "/space1/dir"
    
    def test_directory_path(self):
        """Test getting parent of directory path."""
        assert get_parent_path("/space1/dir/") == "/space1"
    
    def test_space_path(self):
        """Test getting parent of space path."""
        assert get_parent_path("/space1") == "/"
    
    def test_root_path(self):
        """Test getting parent of root path."""
        assert get_parent_path("/") == "/"
    
    def test_nested_path(self):
        """Test getting parent of nested path."""
        assert get_parent_path("/space1/a/b/c/d") == "/space1/a/b/c"


class TestGetBasename:
    """Test getting basename."""
    
    def test_file_basename(self):
        """Test getting basename of file."""
        assert get_basename("/space1/dir/file.txt") == "file.txt"
    
    def test_directory_basename(self):
        """Test getting basename of directory."""
        assert get_basename("/space1/dir/") == "dir"
    
    def test_space_basename(self):
        """Test getting basename of space."""
        assert get_basename("/space1") == "space1"
    
    def test_root_basename(self):
        """Test getting basename of root."""
        assert get_basename("/") == ""
    
    def test_no_slash_basename(self):
        """Test getting basename without slash."""
        assert get_basename("file.txt") == "file.txt"


if __name__ == "__main__":
    pytest.main([__file__])