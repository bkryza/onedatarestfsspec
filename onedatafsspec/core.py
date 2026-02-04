"""Core implementation of OnedataFileSystem for fsspec."""

import io
import logging
import posixpath
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from fsspec import AbstractFileSystem
from fsspec.spec import AbstractBufferedFile
from fsspec.utils import infer_storage_options
from onedatafilerestclient import OnedataFileRESTClient
from onedatafilerestclient.errors import (
    NoAvailableProviderForSpaceError,
    OnedataError,
    OnedataRESTError,
)
from onedatafilerestclient.file_attributes import FileAttrsJson
from onedatafilerestclient.types import FileId, FilePath, SpaceSpecifier

from .config import get_onedata_config_from_env, merge_config, parse_onedata_url
from .utils import normalize_onedata_path, split_onedata_path, validate_onedata_path

logger = logging.getLogger(__name__)


class OnedataFile(AbstractBufferedFile):
    """File-like object for Onedata files."""

    def __init__(
        self, fs, path, mode="rb", block_size=None, cache_type="readahead", **kwargs
    ):
        super().__init__(fs, path, mode, block_size, cache_type=cache_type, **kwargs)
        self.space_name, self.file_path = self._split_onedata_path(path)
        self.file_id = None

        if self.mode == "rb":
            try:
                self.size = fs._get_file_size(self.space_name, self.file_path)
                self.file_id = fs._get_file_id(self.space_name, self.file_path)
            except OnedataError:
                if "r" in self.mode:
                    raise
                self.size = 0

    def _split_onedata_path(self, path: str) -> Tuple[str, str]:
        """Split Onedata path into space name and file path."""
        space_name, file_path = split_onedata_path(path)
        return space_name, file_path or ""

    def _fetch_range(self, start: int, end: int) -> bytes:
        """Fetch a range of bytes from the file."""
        if self.file_id is None:
            self.file_id = self.fs._get_file_id(self.space_name, self.file_path)

        size = end - start
        return self.fs.client.get_file_content(
            self.space_name, file_id=self.file_id, offset=start, size=size
        )

    def _upload_chunk(self, final: bool = False) -> bool:
        """Upload buffered data to Onedata."""
        if self.buffer is None or not self.buffer.tell():
            return False

        if self.file_id is None:
            # Create the file if it doesn't exist
            self.file_id = self.fs._create_file(self.space_name, self.file_path)

        data = self.buffer.getvalue()
        self.fs.client.put_file_content(
            self.space_name, data=data, file_id=self.file_id, offset=self.offset
        )

        if self.fs.auto_mkdir and self.path.count("/") > 1:
            self.fs.makedirs(posixpath.dirname(self.path), exist_ok=True)

        self.offset += len(data)
        self.buffer.seek(0)
        self.buffer.truncate()

        return True

    def commit(self) -> None:
        """Commit any remaining data."""
        if self.mode not in {"rb", "ab"}:
            self._upload_chunk(final=True)
        self.discard()

    def discard(self) -> None:
        """Discard the file."""
        pass


class OnedataFileSystem(AbstractFileSystem):
    """fsspec filesystem implementation for Onedata."""

    protocol = "onedata"
    root_marker = "/"

    def __init__(
        self,
        onezone_host: str = None,
        token: str = None,
        preferred_providers: Optional[List[str]] = None,
        verify_ssl: bool = True,
        timeout: Optional[Union[float, Tuple[float, float]]] = 30,
        auto_mkdir: bool = True,
        **kwargs,
    ):
        """Initialize OnedataFileSystem.

        Parameters
        ----------
        onezone_host : str
            Onedata Onezone host URL
        token : str
            Onedata access token
        preferred_providers : list of str, optional
            List of preferred Oneprovider domains
        verify_ssl : bool, default True
            Whether to verify SSL certificates
        timeout : float or tuple, default 30
            Connection timeout
        auto_mkdir : bool, default True
            Whether to automatically create parent directories
        """
        super().__init__(**kwargs)

        # Get configuration from environment
        env_config = get_onedata_config_from_env()

        # Merge with explicitly provided config
        explicit_config = {
            "onezone_host": onezone_host,
            "token": token,
            "preferred_providers": preferred_providers,
            "verify_ssl": verify_ssl,
            "timeout": timeout,
        }

        config = merge_config({}, env_config, explicit_config)

        self.onezone_host = config.get("onezone_host")
        self.token = config.get("token")
        self.preferred_providers = config.get("preferred_providers", [])
        self.verify_ssl = config.get("verify_ssl", True)
        self.timeout = config.get("timeout", 30)
        self.auto_mkdir = auto_mkdir

        if not self.onezone_host or not self.token:
            raise ValueError(
                "Both onezone_host and token must be provided either as parameters or environment variables"
            )

        # Extract hostname/IP from onezone_host if it includes protocol
        onezone_host_for_client = self.onezone_host
        if self.onezone_host.startswith(('http://', 'https://')):
            parsed = urlparse(self.onezone_host)
            onezone_host_for_client = parsed.hostname or parsed.netloc

        self.client = OnedataFileRESTClient(
            onezone_host=onezone_host_for_client,
            token=self.token,
            preferred_providers=self.preferred_providers,
            verify_ssl=self.verify_ssl,
            timeout=self.timeout,
        )

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        """Remove the protocol from a path."""
        if path.startswith("onedata://"):
            return infer_storage_options(path)["path"]
        return path

    def _split_onedata_path(self, path: str) -> Tuple[str, Optional[str]]:
        """Split a path into space name and file path."""
        return split_onedata_path(path)

    def _get_file_id(self, space_name: str, file_path: Optional[str] = None) -> FileId:
        """Get file ID for a given space and path."""
        return self.client.get_file_id(space_name, file_path=file_path)

    def _get_file_size(self, space_name: str, file_path: Optional[str] = None) -> int:
        """Get file size for a given space and path."""
        attrs = self.client.get_attributes(
            space_name, file_path=file_path, attributes=["size"]
        )
        return attrs["size"]

    def _create_file(self, space_name: str, file_path: str) -> FileId:
        """Create a new file and return its ID."""
        return self.client.create_file(
            space_name,
            file_path=file_path,
            file_type="REG",
            create_parents=self.auto_mkdir,
        )

    def ls(
        self, path: str, detail: bool = False, **kwargs
    ) -> Union[List[str], List[Dict[str, Any]]]:
        """List contents of a directory.

        Parameters
        ----------
        path : str
            Path to list
        detail : bool, default False
            Whether to return detailed information

        Returns
        -------
        list
            List of file names or detailed file information
        """
        path = self._strip_protocol(path).rstrip("/")
        space_name, file_path = self._split_onedata_path(path)

        if not space_name:
            # List all spaces
            spaces = self.client.list_spaces()
            if detail:
                return [
                    {"name": space, "type": "directory", "size": 0} for space in spaces
                ]
            else:
                return spaces

        try:
            result = self.client.list_children(
                space_name,
                file_path=file_path,
                attributes=["name", "type", "size", "mtime", "posixPermissions"],
            )

            files = []
            for child in result["children"]:
                name = child["name"]
                full_path = (
                    f"{space_name}/{file_path}/{name}"
                    if file_path
                    else f"{space_name}/{name}"
                )

                if detail:
                    files.append(
                        {
                            "name": full_path,
                            "type": "directory" if child["type"] == "DIR" else "file",
                            "size": child.get("size", 0),
                            "mtime": child.get("mtime"),
                            "mode": child.get("posixPermissions"),
                        }
                    )
                else:
                    files.append(full_path)

            return files

        except OnedataError as e:
            if "enoent" in str(e).lower():
                raise FileNotFoundError(f"Path not found: {path}")
            raise

    def info(self, path: str, **kwargs) -> Dict[str, Any]:
        """Get file/directory information.

        Parameters
        ----------
        path : str
            Path to get info for

        Returns
        -------
        dict
            File information
        """
        path = self._strip_protocol(path).rstrip("/")
        space_name, file_path = self._split_onedata_path(path)

        if not space_name:
            raise FileNotFoundError("Root path info not available")

        try:
            attrs = self.client.get_attributes(
                space_name,
                file_path=file_path,
                attributes=[
                    "name",
                    "type",
                    "size",
                    "mtime",
                    "atime",
                    "posixPermissions",
                ],
            )

            return {
                "name": path,
                "type": "directory" if attrs["type"] == "DIR" else "file",
                "size": attrs.get("size", 0),
                "mtime": attrs.get("mtime"),
                "atime": attrs.get("atime"),
                "mode": attrs.get("posixPermissions"),
            }

        except OnedataError as e:
            if "enoent" in str(e).lower():
                raise FileNotFoundError(f"Path not found: {path}")
            raise

    def cat_file(
        self,
        path: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        **kwargs,
    ) -> bytes:
        """Read file content.

        Parameters
        ----------
        path : str
            Path to read
        start : int, optional
            Start byte position
        end : int, optional
            End byte position

        Returns
        -------
        bytes
            File content
        """
        path = self._strip_protocol(path)
        space_name, file_path = self._split_onedata_path(path)

        if not space_name or not file_path:
            raise FileNotFoundError(f"Invalid path: {path}")

        try:
            if start is not None or end is not None:
                # Get file size for bounds checking
                file_size = self._get_file_size(space_name, file_path)
                start = start or 0
                end = end or file_size
                size = end - start

                return self.client.get_file_content(
                    space_name, file_path=file_path, offset=start, size=size
                )
            else:
                return self.client.get_file_content(space_name, file_path=file_path)

        except OnedataError as e:
            if "enoent" in str(e).lower():
                raise FileNotFoundError(f"File not found: {path}")
            raise

    def put_file(self, lpath: str, rpath: str, callback=None, **kwargs) -> None:
        """Upload a local file to Onedata.

        Parameters
        ----------
        lpath : str
            Local file path
        rpath : str
            Remote file path
        callback : callable, optional
            Progress callback function
        """
        rpath = self._strip_protocol(rpath)
        space_name, file_path = self._split_onedata_path(rpath)

        if not space_name or not file_path:
            raise ValueError(f"Invalid remote path: {rpath}")

        with open(lpath, "rb") as f:
            data = f.read()

        try:
            # Create the file first
            file_id = self._create_file(space_name, file_path)

            # Upload the content
            self.client.put_file_content(space_name, data=data, file_id=file_id)

            if callback:
                callback(len(data))

        except OnedataError as e:
            raise IOError(f"Failed to upload file: {e}")

    def get_file(self, rpath: str, lpath: str, callback=None, **kwargs) -> None:
        """Download a file from Onedata to local filesystem.

        Parameters
        ----------
        rpath : str
            Remote file path
        lpath : str
            Local file path
        callback : callable, optional
            Progress callback function
        """
        data = self.cat_file(rpath)

        with open(lpath, "wb") as f:
            f.write(data)

        if callback:
            callback(len(data))

    def cp_file(self, path1: str, path2: str, **kwargs) -> None:
        """Copy a file within Onedata."""
        data = self.cat_file(path1)

        path2 = self._strip_protocol(path2)
        space_name, file_path = self._split_onedata_path(path2)

        if not space_name or not file_path:
            raise ValueError(f"Invalid destination path: {path2}")

        file_id = self._create_file(space_name, file_path)
        self.client.put_file_content(space_name, data=data, file_id=file_id)

    def rm_file(self, path: str) -> None:
        """Remove a file.

        Parameters
        ----------
        path : str
            Path to remove
        """
        path = self._strip_protocol(path)
        space_name, file_path = self._split_onedata_path(path)

        if not space_name or not file_path:
            raise ValueError(f"Invalid path: {path}")

        try:
            self.client.remove(space_name, file_path=file_path)
        except OnedataError as e:
            if "enoent" in str(e).lower():
                raise FileNotFoundError(f"File not found: {path}")
            raise

    def makedirs(self, path: str, exist_ok: bool = False) -> None:
        """Create directories.

        Parameters
        ----------
        path : str
            Directory path to create
        exist_ok : bool, default False
            Don't raise error if directory exists
        """
        path = self._strip_protocol(path).rstrip("/")
        space_name, dir_path = self._split_onedata_path(path)

        if not space_name or not dir_path:
            return  # Can't create spaces

        try:
            self.client.create_file(
                space_name, file_path=dir_path, file_type="DIR", create_parents=True
            )
        except OnedataError as e:
            if not exist_ok or "eexist" not in str(e).lower():
                raise

    def rmdir(self, path: str) -> None:
        """Remove a directory.

        Parameters
        ----------
        path : str
            Directory path to remove
        """
        self.rm_file(path)

    def exists(self, path: str) -> bool:
        """Check if a path exists.

        Parameters
        ----------
        path : str
            Path to check

        Returns
        -------
        bool
            True if path exists
        """
        try:
            self.info(path)
            return True
        except FileNotFoundError:
            return False

    def isdir(self, path: str) -> bool:
        """Check if a path is a directory.

        Parameters
        ----------
        path : str
            Path to check

        Returns
        -------
        bool
            True if path is a directory
        """
        try:
            info = self.info(path)
            return info["type"] == "directory"
        except FileNotFoundError:
            return False

    def isfile(self, path: str) -> bool:
        """Check if a path is a file.

        Parameters
        ----------
        path : str
            Path to check

        Returns
        -------
        bool
            True if path is a file
        """
        try:
            info = self.info(path)
            return info["type"] == "file"
        except FileNotFoundError:
            return False

    def size(self, path: str) -> int:
        """Get file size.

        Parameters
        ----------
        path : str
            File path

        Returns
        -------
        int
            File size in bytes
        """
        info = self.info(path)
        return info["size"]

    def open(self, path: str, mode: str = "rb", **kwargs) -> OnedataFile:
        """Open a file for reading or writing.

        Parameters
        ----------
        path : str
            File path
        mode : str, default "rb"
            File open mode

        Returns
        -------
        OnedataFile
            File handle
        """
        return OnedataFile(self, path, mode, **kwargs)


# Register with fsspec
try:
    from fsspec.registry import register_implementation

    register_implementation("onedata", OnedataFileSystem)
except ImportError:
    pass  # fsspec not available
