# OnedataRESTFSSpec

OnedataRESTFSSpec is an fsspec filesystem implementation for Onedata, providing a unified interface for accessing Onedata spaces and files using the familiar fsspec API.

## Features

- Full fsspec compatibility
- Support for reading and writing files
- Directory operations (list, create, remove)
- File metadata access
- Authentication via access tokens
- Configurable provider preferences
- Environment variable configuration
- URL-based configuration
- OpenTelemetry metrics export via OTLP

## Installation

Install from the local directory:

```bash
pip install -e ./onedatarestfsspec
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
with fs.open('/your-space/path/to/file.txt', 'rb') as f:
    content = f.read()

# Write a file
with fs.open('/your-space/path/to/newfile.txt', 'wb') as f:
    f.write(b'Hello, Onedata!')

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

## OpenTelemetry Metrics

OnedataRESTFSSpec can export data-access metrics to any OpenTelemetry-compatible
collector (Prometheus, Grafana Tempo, Jaeger, etc.) using the OTLP protocol.

### Installation

Metrics support is an optional extra.  Install the HTTP exporter (recommended)
or the gRPC exporter, or both:

```bash
# HTTP/protobuf transport (default)
pip install 'onedatarestfsspec[monitoring]'

# gRPC transport only
pip install opentelemetry-exporter-otlp-proto-grpc
```

### Enabling metrics

Metrics are **disabled by default**.  Enable them either via a constructor
keyword argument or an environment variable:

```python
import fsspec

fs = fsspec.filesystem(
    'onedata',
    onezone_host='https://datahub.egi.eu',
    token='your_access_token',
    metrics_enabled=True,
)
```

```bash
export ONEDATA_METRICS_ENABLED=true
```

### Configuring the OTLP exporter

The exporter is configured through standard OpenTelemetry environment variables
or the corresponding constructor keyword arguments.  Keyword arguments take
precedence over environment variables.

| Constructor kwarg | Environment variable | Default | Description |
|---|---|---|---|
| `metrics_enabled` | `ONEDATA_METRICS_ENABLED` | `false` | Enable metrics export |
| `otlp_endpoint` | `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` | collector default | Full URL of the OTLP collector endpoint |
| `otlp_protocol` | `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` | Transport protocol: `http/protobuf` or `grpc` |
| `otlp_export_interval_ms` | — | `60000` | How often metrics are flushed (milliseconds) |

**Example — HTTP/protobuf transport:**

```python
import fsspec

fs = fsspec.filesystem(
    'onedata',
    onezone_host='https://dev-onezone.default.svc.cluster.local',
    token='your_access_token',
    metrics_enabled=True,
    verify_ssl=False,
    otlp_endpoint='http://localhost:9090/api/v1/otlp/v1/metrics',
    otlp_protocol='http/protobuf',
    otlp_export_interval_ms=30_000,
)
```

**Example — gRPC transport via environment variables:**

```bash
export ONEDATA_METRICS_ENABLED=true
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://otel-collector.example.com:4317
```

```python
fs = fsspec.filesystem('onedata',
                       onezone_host='https://datahub.egi.eu',
                       token='your_access_token')
```

### Available metrics

All metrics carry the following attributes, allowing time-series to be filtered
and grouped by space, file, or operation type:

| Attribute | Description |
|---|---|
| `space_id` | Onedata space identifier |
| `file_id` | Onedata internal file identifier |
| `provider_id` | Domain of the Oneprovider that served the request |
| `operation` | `"read"` or `"write"` |

| Metric name | Type | Unit | Description |
|---|---|---|---|
| `onedata_file_access_total` | Counter | ops | Total number of read and write operations. Use the `operation` attribute to distinguish reads from writes. |
| `onedata_read_bytes` | Counter | bytes | Cumulative bytes read from Onedata. |
| `onedata_written_bytes` | Counter | bytes | Cumulative bytes written to Onedata. |
| `onedata_read_duration` | Histogram | seconds | Per-operation read latency. |
| `onedata_write_duration` | Histogram | seconds | Per-operation write latency. |
| `onedata_file_throughput_bytes_per_second` | Histogram | bytes/s | Observed transfer throughput (`bytes / duration`) for each operation. |

### Example: querying metrics in Prometheus

After scraping via an OpenTelemetry Collector with a Prometheus exporter, the
metrics are available under their original names (dots replaced by underscores
where required by Prometheus).

```promql
# Read throughput (bytes/s) averaged over 5 minutes, by space
rate(onedata_read_bytes_total[5m]) by (space_id)

# 99th-percentile read latency per file
histogram_quantile(0.99, rate(onedata_read_duration_bucket[5m])) by (file_id)

# Write operation rate
rate(onedata_file_access_total[1m]) by (space_id, operation)
```

## Path Format

Paths in OnedataRESTFSSpec follow the format: `/space-name/path/within/space`

- Root path `/` lists all available spaces
- Space path `/space-name` refers to the root of a specific space
- File path `/space-name/dir/file.txt` refers to a file within a space

## Configuration Priority

Configuration is resolved in the following order (highest to lowest priority):

1. Explicit parameters passed to `fsspec.filesystem()`
2. URL parameters (when using URL-based access)
3. Environment variables

## Error Handling

OnedataRESTFSSpec translates Onedata REST API errors to standard Python exceptions:

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
pip install -e ./onedatarestfsspec[test]

# Run tests
pytest onedatarestfsspec/tests/

# Run with coverage
pytest --cov=onedatarestfsspec onedatarestfsspec/tests/
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
