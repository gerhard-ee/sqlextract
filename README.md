# SQL Extract

A powerful tool for extracting data from various SQL databases to CSV or Parquet files. Perfect for data migration, backup, and analysis tasks.

## Features

- Support for multiple database types:
  - PostgreSQL
  - Microsoft SQL Server
  - Google BigQuery
  - Snowflake
  - Databricks
  - DuckDB (macOS only)
- Export to CSV or Parquet format
- Parallel extraction with configurable batch size
- Progress tracking and checkpointing
- Kubernetes integration for state management
- Cross-platform support (except DuckDB which is macOS-only)

## Installation

```bash
# Install using Go
go install github.com/gerhard-ee/sqlextract@latest

# Verify installation
sqlextract --version
```

## Usage

### Command Line Interface

```bash
sqlextract [flags] [options]

Flags:
  --help, -h              Show help
  --version, -v           Show version
  --config, -c            Path to configuration file
  --type, -t              Database type (postgres, mssql, bigquery, snowflake, databricks, duckdb)
  --host, -H              Database host
  --port, -P              Database port
  --user, -u              Database user
  --password, -p          Database password
  --database, -d          Database name
  --schema, -s            Database schema
  --table, -T             Table to extract
  --output, -o            Output file path
  --format, -f            Output format (csv, parquet)
  --batch-size, -b        Batch size for extraction (default: 1000)
  --concurrency, -C       Number of concurrent extractions (default: 1)
  --where, -w             WHERE clause for filtering data
  --key-columns, -k       Key columns for ordering
  --state-manager, -S     State manager type (memory, kubernetes)
  --namespace, -n         Kubernetes namespace (required for kubernetes state manager)

Options:
  --project-id            Google Cloud project ID (BigQuery)
  --credentials-file      Path to Google Cloud credentials file (BigQuery)
  --account               Snowflake account identifier
  --warehouse            Snowflake warehouse name
  --workspace            Databricks workspace URL
  --token                Databricks access token
  --catalog              Databricks catalog name
```

### Examples

#### Basic PostgreSQL Extraction

```bash
# Extract a single table to CSV
sqlextract --type postgres \
           --host localhost \
           --port 5432 \
           --user postgres \
           --password secret \
           --database mydb \
           --table users \
           --output users.csv

# Extract with custom batch size and concurrency
sqlextract --type postgres \
           --host localhost \
           --port 5432 \
           --user postgres \
           --password secret \
           --database mydb \
           --table large_table \
           --output large_table.csv \
           --batch-size 5000 \
           --concurrency 4

# Extract with filtering
sqlextract --type postgres \
           --host localhost \
           --port 5432 \
           --user postgres \
           --password secret \
           --database mydb \
           --table orders \
           --output recent_orders.csv \
           --where "created_at > '2024-01-01'"
```

#### BigQuery Extraction

```bash
# Extract from BigQuery to Parquet
sqlextract --type bigquery \
           --project-id my-project \
           --database my_dataset \
           --table my_table \
           --output data.parquet \
           --format parquet \
           --credentials-file /path/to/credentials.json
```

#### Snowflake Extraction

```bash
# Extract from Snowflake
sqlextract --type snowflake \
           --account my-account \
           --user my-user \
           --password my-password \
           --database my_db \
           --schema public \
           --warehouse my_warehouse \
           --table customers \
           --output customers.csv
```

#### DuckDB Example (macOS only)

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
sqlextract --type duckdb \
           --database mydb.db \
           --table users \
           --output users.csv

# Extract data to Parquet
sqlextract --type duckdb \
           --database mydb.db \
           --table users \
           --output users.parquet \
           --format parquet
```

#### Using Configuration File

Create a `config.json` file:

```json
{
  "type": "postgres",
  "host": "localhost",
  "port": 5432,
  "user": "postgres",
  "password": "secret",
  "database": "mydb",
  "schema": "public",
  "batch_size": 5000,
  "concurrency": 4
}
```

Then run:

```bash
sqlextract --config config.json \
           --table users \
           --output users.csv
```

#### Kubernetes State Management

```bash
# Extract with Kubernetes state management
sqlextract --type postgres \
           --host localhost \
           --port 5432 \
           --user postgres \
           --password secret \
           --database mydb \
           --table large_table \
           --output large_table.csv \
           --state-manager kubernetes \
           --namespace my-namespace
```

### Environment Variables

You can use environment variables for sensitive information:

```bash
export SQL_EXTRACT_PASSWORD=secret
export SQL_EXTRACT_USER=postgres
export SQL_EXTRACT_DATABASE=mydb

sqlextract --type postgres \
           --host localhost \
           --port 5432 \
           --table users \
           --output users.csv
```

## Development

### Building

```bash
# Build for current platform
go build -o sqlextract ./cmd/sqlextract

# Build for specific platform
GOOS=linux GOARCH=amd64 go build -o sqlextract-linux-amd64 ./cmd/sqlextract
```

### Testing

```bash
# Run all tests
go test -v ./...

# Run tests with coverage
go test -v -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## License

MIT