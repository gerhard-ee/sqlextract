# SQL Extract

A powerful CLI tool for extracting data from SQL databases with support for resumable operations and state management.

## Features

- Support for multiple databases:
  - PostgreSQL
  - DuckDB
  - BigQuery
  - Snowflake
  - SQL Server
- Batch processing with configurable batch size
- Resumable operations
- Multiple output formats (CSV, Parquet)
- Comprehensive test coverage
- Production-ready CI/CD pipeline

## Installation

### Using Go Install

```bash
go install github.com/gerhardlazu/sqlextract@latest
```

### Building from Source

```bash
git clone https://github.com/gerhardlazu/sqlextract.git
cd sqlextract
go build -o sqlextract cmd/sqlextract/main.go
```

### Using Docker

```bash
docker pull gerhardlazu/sqlextract:latest
```

## Usage

### PostgreSQL

```bash
sqlextract \
  --type postgres \
  --host localhost \
  --port 5432 \
  --username myuser \
  --password mypassword \
  --database mydb \
  --schema public \
  --table mytable \
  --output data.csv
```

### SQL Server

```bash
sqlextract \
  --type mssql \
  --host localhost \
  --port 1433 \
  --username sa \
  --password MyPassword123 \
  --database mydb \
  --schema dbo \
  --table mytable \
  --output data.csv
```

### BigQuery

```bash
sqlextract \
  --type bigquery \
  --project-id my-project \
  --credentials-file path/to/credentials.json \
  --database mydataset \
  --table mytable \
  --output data.parquet
```

## Development

### Prerequisites

- Go 1.21 or later
- Docker (optional)

### Building from Source

```bash
git clone https://github.com/gerhardlazu/sqlextract.git
cd sqlextract
go build ./...
```

### Running Tests

```bash
go test -v ./...
```

### Code Coverage

```bash
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Linting

```bash
golangci-lint run
```

## Project Structure

```
.
├── cmd/
│   └── sqlextract/       # CLI application
├── internal/
│   └── database/         # Database implementations
├── pkg/
│   └── database/         # Public database interface
├── .github/              # GitHub Actions and templates
├── go.mod               # Go module file
├── go.sum               # Go module checksum
└── README.md            # This file
```

## Security

Please report security issues via email rather than public GitHub issues. See [SECURITY.md](SECURITY.md) for details.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](.github/CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.