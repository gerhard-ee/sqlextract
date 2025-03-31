# SQL Extract

A powerful CLI tool for extracting data from various SQL databases into CSV or Parquet formats. Built with Go, this tool provides a simple and efficient way to extract data from different database types with support for resumable operations and state management.

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
- Comprehensive test coverage
- Production-ready CI/CD pipeline

## Installation

### From Source
```bash
git clone https://github.com/gerhard-ee/sqlextract.git
cd sqlextract
make build
```

### Using Go Install
```bash
go install github.com/gerhard-ee/sqlextract/cmd/sqlextract@latest
```

### Using Docker
```bash
docker pull gerhard-ee/sqlextract:latest
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
- Make
- Docker (optional)

### Building from Source

```bash
git clone https://github.com/gerhard-ee/sqlextract.git
cd sqlextract
make deps    # Install dependencies
make test    # Run tests
make build   # Build binary
```

### Running Tests

```bash
make test
```

### Code Coverage

```bash
make coverage
```

### Linting

```bash
make lint
```

## Project Structure

```
sqlextract/
├── cmd/
│   └── sqlextract/      # Main application entry point
├── internal/
│   └── database/        # Database implementations
├── pkg/
│   ├── database/        # Database interfaces
│   └── extractor/       # Data extraction logic
├── .github/            # GitHub Actions and templates
├── testdata/           # Test data files
└── scripts/            # Build and deployment scripts
```

## Prompts Used

The project was developed using the following prompts:

1. Initial Project Setup:
```
Create a Go CLI application named "SQL Extract" for extracting data from SQL databases (MSSQL, MySQL, PostgreSQL) into CSV or Parquet formats. The application should focus on:
- Scalability
- Resumable extraction
- Effective state management
```

2. Database Interface:
```
Implement a database interface that supports:
- Connection management
- Schema inspection
- Data extraction with batching
- Row counting
```

3. PostgreSQL Implementation:
```
Create a PostgreSQL implementation of the database interface with:
- Connection handling
- Schema inspection
- Data extraction with batching
- Error handling
```

4. Testing:
```
Create comprehensive tests for the PostgreSQL implementation, including:
- Connection tests
- Schema inspection tests
- Data extraction tests
- Error handling tests
```

5. Additional Database Support:
```
Add support for additional database types:
- BigQuery
- Snowflake
- Databricks
```

6. Production Readiness:
```
Make the project production-ready by adding:
- Source control
- CI/CD pipeline
- Documentation
- Security policy
- Issue and PR templates
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## Security

Please report security issues to [your-email@example.com]. See our [Security Policy](SECURITY.md) for more details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 