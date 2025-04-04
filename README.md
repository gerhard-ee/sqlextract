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
go install github.com/gerhard-ee/sqlextract@latest
```

### Building from Source

```bash
git clone https://github.com/gerhard-ee/sqlextract.git
cd sqlextract
go build -o sqlextract cmd/sqlextract/main.go
```

### Using Docker

```bash
docker pull gerhard-ee/sqlextract:latest
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

### DuckDB Example (macOS only)

DuckDB support is available only on macOS. Here's an example of how to use it:

```bash
# Create a DuckDB database and table
duckdb mydb.db <<EOF
CREATE TABLE users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    created_at TIMESTAMP
);

INSERT INTO users VALUES
    (1, 'John Doe', 'john@example.com', CURRENT_TIMESTAMP),
    (2, 'Jane Smith', 'jane@example.com', CURRENT_TIMESTAMP);
EOF

# Extract data to CSV
sqlextract --type duckdb --database mydb.db --table users --output users.csv

# Extract data to Parquet
sqlextract --type duckdb --database mydb.db --table users --output users.parquet --format parquet
```

## Development

### Prerequisites

- Go 1.21 or later
- Docker (optional)

### Building from Source

```bash
git clone https://github.com/gerhard-ee/sqlextract.git
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

The project uses different linting approaches for local development and CI/CD:

#### Local Development
For local development with Go 1.24.1, use the following basic Go tools:
```bash
# Basic Go checks
go vet ./...

# Code formatting
gofmt -l .

# Static analysis
go install honnef.co/go/tools/cmd/staticcheck@latest
staticcheck ./...
```

#### CI/CD Environment
The CI/CD pipeline uses golangci-lint with Go 1.23 for comprehensive linting:
```bash
golangci-lint run
```

Note: The CI/CD environment uses Go 1.23 to ensure compatibility with all linting tools. This is different from the local development environment which may use a newer Go version.

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