"""Integration tests for OnedataRESTFSSpec using actual Onedata deployment."""

import random
import time

import pytest

from onedatarestfsspec.core import OnedataFileSystem

from .conftest import _generate_test_file_info


@pytest.mark.integration
def test_file_operations_integration(onezone_ip, onezone_admin_token):
    """Test basic file operations on actual Onedata deployment.

    This test creates a file in the test_onedatarestfsspec space, writes data to it,
    reads it back, and then removes it.
    """
    # Create filesystem instance using the test environment
    fs = OnedataFileSystem(
        onezone_host=f"https://{onezone_ip}",
        token=onezone_admin_token,
        verify_ssl=False,  # Test environment typically uses self-signed certs
    )

    # Generate unique test file name to avoid conflicts
    test_filename, test_file_path, test_content = _generate_test_file_info()

    try:
        # Verify we can list the test space
        assert "test_onedatarestfsspec" in fs.ls(
            "/"
        ), "test_onedatarestfsspec space not found"
        print(f"Initial files in test space: {fs.ls('/test_onedatarestfsspec/')}")

        # Test file creation and writing
        print(f"Creating test file: {test_file_path}")
        with fs.open(test_file_path, "wb") as f:
            f.write(test_content)

        # Verify file exists and check info
        assert fs.exists(
            test_file_path
        ), f"File {test_file_path} should exist after creation"
        file_info = fs.info(test_file_path)
        assert file_info["type"] == "file"
        assert file_info["size"] == len(test_content)
        print(f"Created file info: {file_info}")

        # Test file reading
        print(f"Reading test file: {test_file_path}")
        with fs.open(test_file_path, "rb") as f:
            assert f.read() == test_content, "Read content should match written content"

        # Test cat_file method
        assert (
            fs.cat_file(test_file_path) == test_content
        ), "cat_file content should match written content"

        # Test partial read
        assert (
            fs.cat_file(test_file_path, start=7, end=20) == test_content[7:20]
        ), "Partial read should match expected slice"

        # Verify file appears in directory listing
        assert any(
            test_filename in f for f in fs.ls("/test_onedatarestfsspec/")
        ), "Test file should appear in directory listing"

        print("All file operations completed successfully!")

    finally:
        # Clean up: remove test file
        try:
            if fs.exists(test_file_path):
                print(f"Cleaning up test file: {test_file_path}")
                fs.rm_file(test_file_path)

                # Verify file was removed
                assert not fs.exists(
                    test_file_path
                ), f"File {test_file_path} should be removed"
                print("Test file cleaned up successfully")

        except (FileNotFoundError, OSError, ValueError) as cleanup_error:
            print(
                f"Warning: Failed to cleanup test file {test_file_path}: {cleanup_error}"
            )


@pytest.mark.integration
def test_directory_operations_integration(onezone_ip, onezone_admin_token):
    """Test directory operations on actual Onedata deployment."""
    # Create filesystem instance using the test environment
    fs = OnedataFileSystem(
        onezone_host=f"https://{onezone_ip}",
        token=onezone_admin_token,
        verify_ssl=False,
    )

    # Generate unique test directory name
    timestamp = int(time.time())
    random_id = random.randint(1000, 9999)
    test_dirname = f"fsspec_test_dir_{timestamp}_{random_id}"
    test_dir_path = f"/test_onedatarestfsspec/{test_dirname}"
    test_file_in_dir = f"{test_dir_path}/nested_test_file.txt"

    test_content = b"Content in nested directory"

    try:
        # Create directory
        print(f"Creating test directory: {test_dir_path}")
        fs.makedirs(test_dir_path)

        # Verify directory exists and is a directory
        assert fs.exists(test_dir_path), f"Directory {test_dir_path} should exist"
        assert fs.isdir(test_dir_path), f"Path {test_dir_path} should be a directory"

        # Create file in directory
        print(f"Creating file in directory: {test_file_in_dir}")
        with fs.open(test_file_in_dir, "wb") as f:
            f.write(test_content)

        # Verify file in directory
        assert fs.exists(test_file_in_dir), f"File {test_file_in_dir} should exist"
        assert fs.isfile(test_file_in_dir), f"Path {test_file_in_dir} should be a file"

        # List directory contents
        dir_contents = fs.ls(test_dir_path)
        assert len(dir_contents) > 0, "Directory should contain files"
        assert any(
            "nested_test_file.txt" in f for f in dir_contents
        ), f"Directory should contain test file: {dir_contents}"

        # Read file content
        read_content = fs.cat_file(test_file_in_dir)
        assert read_content == test_content, "Content should match"

        print("Directory operations completed successfully!")

    finally:
        # Clean up: remove test file and directory
        try:
            if fs.exists(test_file_in_dir):
                print(f"Cleaning up test file: {test_file_in_dir}")
                fs.rm_file(test_file_in_dir)

            if fs.exists(test_dir_path):
                print(f"Cleaning up test directory: {test_dir_path}")
                fs.rmdir(test_dir_path)

            print("Directory test cleanup completed successfully")

        except (FileNotFoundError, OSError, ValueError) as cleanup_error:
            print(
                f"Warning: Failed to cleanup test directory {test_dir_path}: {cleanup_error}"
            )


@pytest.mark.integration
def test_filesystem_info_integration(onezone_ip, onezone_admin_token):
    """Test filesystem info and listing operations."""
    # Create filesystem instance
    fs = OnedataFileSystem(
        onezone_host=f"https://{onezone_ip}",
        token=onezone_admin_token,
        verify_ssl=False,
    )

    # Test space listing
    spaces = fs.ls("/")
    print(f"Available spaces: {spaces}")
    assert isinstance(spaces, list), "Spaces should be returned as a list"
    assert len(spaces) > 0, "Should have at least one space"
    assert (
        "test_onedatarestfsspec" in spaces
    ), "test_onedatarestfsspec space should be available"

    # Test detailed space listing
    spaces_detail = fs.ls("/", detail=True)
    print(f"Detailed spaces: {spaces_detail}")
    assert isinstance(spaces_detail, list), "Detailed spaces should be a list"
    assert all(
        "name" in space and "type" in space for space in spaces_detail
    ), "Each space should have name and type"

    # Test space content listing
    test_space_files = fs.ls("/test_onedatarestfsspec/", detail=True)
    print(f"Files in test space: {test_space_files}")

    # Verify filesystem protocol
    assert fs.protocol == "onedata", "Protocol should be 'onedata'"

    print("Filesystem info operations completed successfully!")
