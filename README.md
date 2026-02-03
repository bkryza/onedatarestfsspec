# OnedataFSSpec

OnedataFSSpec is an fsspec filesystem implementation for Onedata, providing a unified interface for accessing Onedata spaces and files using the familiar fsspec API.

## Features

- Full fsspec compatibility
- Support for reading and writing files
- Directory operations (list, create, remove)
- File metadata access
- Authentication via access tokens
- Configurable provider preferences
- Environment variable configuration
- URL-based configuration

## Installation

Install from the local directory:

```bash
pip install -e ./onedatafsspec
```

## Requirements

- Python >= 3.10
- fsspec >= 2021.10.0
- onedatafilerestclient >= 25.0.0
- aiohttp

## Usage

### Basic Usage

```python
import fsspec

# Using explicit parameters
fs = fsspec.filesystem(
    'onedata',
    onezone_host='https://datahub.egi.eu',
    token='your_access_token'
)

# List spaces
spaces = fs.ls('/')
print("Available spaces:", spaces)

# List files in a space
files = fs.ls('/your-space/')
print("Files in space:", files)

# Read a file
with fs.open('/your-space/path/to/file.txt', 'r') as f:
    content = f.read()

# Write a file
with fs.open('/your-space/path/to/newfile.txt', 'w') as f:
    f.write('Hello, Onedata!')

# Get file info
info = fs.info('/your-space/path/to/file.txt')
print("File size:", info['size'])
```

### Environment Variables

Set environment variables for automatic configuration:

```bash
export ONEDATA_ONEZONE_HOST="https://datahub.egi.eu"
export ONEDATA_TOKEN="your_access_token"
export ONEDATA_PREFERRED_PROVIDERS="provider1.example.com,provider2.example.com"
export ONEDATA_VERIFY_SSL="true"
export ONEDATA_TIMEOUT="30"
```

Then use without explicit parameters:

```python
import fsspec

# Configuration will be loaded from environment variables
fs = fsspec.filesystem('onedata')
```

### URL-based Configuration

```python
import fsspec

# URL format: onedata://token@onezone_host/path?query_params
url = "onedata://your_token@datahub.egi.eu/your-space/path/to/file.txt"

# Open file directly from URL
with fsspec.open(url, 'r') as f:
    content = f.read()
```

### Advanced Usage

```python
import fsspec

fs = fsspec.filesystem(
    'onedata',
    onezone_host='https://datahub.egi.eu',
    token='your_access_token',
    preferred_providers=['provider1.example.com'],
    verify_ssl=True,
    timeout=60,
    auto_mkdir=True  # Automatically create parent directories
)

# Copy files
fs.cp('/your-space/source.txt', '/your-space/destination.txt')

# Upload local file
fs.put('/local/path/file.txt', '/your-space/remote/file.txt')

# Download file
fs.get('/your-space/remote/file.txt', '/local/path/downloaded.txt')

# Create directory
fs.makedirs('/your-space/new/directory')

# Remove file
fs.rm('/your-space/file/to/remove.txt')

# Check if file exists
if fs.exists('/your-space/some/file.txt'):
    print("File exists!")

# Get detailed listing
files = fs.ls('/your-space/', detail=True)
for file_info in files:
    print(f"{file_info['name']}: {file_info['size']} bytes")
```

## Path Format

Paths in OnedataFSSpec follow the format: `/space-name/path/within/space`

- Root path `/` lists all available spaces
- Space path `/space-name` refers to the root of a specific space
- File path `/space-name/dir/file.txt` refers to a file within a space

## Configuration Priority

Configuration is resolved in the following order (highest to lowest priority):

1. Explicit parameters passed to `fsspec.filesystem()`
2. URL parameters (when using URL-based access)
3. Environment variables

## Error Handling

OnedataFSSpec translates Onedata REST API errors to standard Python exceptions:

- `FileNotFoundError` for missing files/directories
- `IOError` for general I/O errors
- `ValueError` for invalid parameters
- `PermissionError` for access denied errors

## Limitations

- Cross-space file operations are not supported
- Symbolic links are treated as regular files
- Some advanced file attributes may not be available
- Concurrent writes to the same file are not supported

## Development

To set up for development:

```bash
# Install dependencies
pip install -e ./onedatafilerestclient
pip install -e ./onedatafsspec[test]

# Run tests
pytest onedatafsspec/tests/

# Run with coverage
pytest --cov=onedatafsspec onedatafsspec/tests/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.