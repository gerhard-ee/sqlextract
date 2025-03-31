# SQL Extract

A powerful CLI tool for extracting data from various SQL databases into CSV or Parquet formats.

## Features

- Support for multiple database types:
  - PostgreSQL
  - DuckDB
  - BigQuery
- Scalable data extraction with batch processing
- Resumable extraction support
- Output formats:
  - CSV
  - Parquet
- Configurable batch size and parallel processing
- State management for tracking extraction progress

## Installation

```bash
go install github.com/gerhardlazu/sqlextract/cmd/sqlextract@latest
```

## Usage

### PostgreSQL Example

```bash
sqlextract \
  --type postgres \
  --host localhost \
  --port 5432 \
  --username myuser \
  --password mypass \
  --database mydb \
  --schema public \
  --table mytable \
  --output data.parquet \
  --format parquet \
  --batch-size 10000
```

### DuckDB Example

```bash
sqlextract \
  --type duckdb \
  --database mydb.db \
  --table mytable \
  --output data.csv \
  --format csv \
  --batch-size 10000
```

### BigQuery Example

```bash
sqlextract \
  --type bigquery \
  --project-id my-project \
  --credentials-file credentials.json \
  --database mydataset \
  --table mytable \
  --output data.parquet \
  --format parquet \
  --batch-size 10000
```

## Configuration

The tool supports various configuration options through command-line flags:

- `--type`: Database type (postgres, duckdb, bigquery)
- `--host`: Database host (for PostgreSQL)
- `--port`: Database port (for PostgreSQL)
- `--username`: Database username (for PostgreSQL)
- `--password`: Database password (for PostgreSQL)
- `--database`: Database name
- `--schema`: Schema name (for PostgreSQL)
- `--table`: Table name to extract
- `--output`: Output file path
- `--format`: Output format (csv, parquet)
- `--batch-size`: Number of rows to extract per batch
- `--project-id`: GCP project ID (for BigQuery)
- `--credentials-file`: Path to GCP credentials file (for BigQuery)

## Development

### Prerequisites

- Go 1.21 or later
- PostgreSQL (for testing)
- DuckDB
- Google Cloud SDK (for BigQuery)

### Building from Source

```bash
git clone https://github.com/gerhardlazu/sqlextract.git
cd sqlextract
go build ./cmd/sqlextract
```

### Running Tests

```bash
go test ./...
```

## License

MIT License 