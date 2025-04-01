package database

import (
	"context"
	"fmt"

	"github.com/gerhard-ee/sqlextract/internal/state"
)

// Database defines the interface for database operations
type Database interface {
	// Connect establishes a connection to the database
	Connect() error

	// Close closes the database connection
	Close() error

	// GetTableSchema returns the schema of a table
	GetTableSchema(table string) ([]Column, error)

	// GetRowCount returns the total number of rows in a table
	GetRowCount(table string) (int64, error)

	// ExtractData extracts data from a table in batches
	ExtractData(ctx context.Context, table string, columns []Column, batchSize int, offset int64) ([][]interface{}, error)
}

// NewDatabase creates a new database instance based on the configuration
func NewDatabase(config *Config, stateManager state.Manager) (Database, error) {
	switch config.Type {
	case "postgres":
		return NewPostgresDB(config, stateManager), nil
	case "duckdb":
		return NewDuckDB(config, stateManager), nil
	case "bigquery":
		return NewBigQueryDB(config, stateManager), nil
	case "snowflake":
		return NewSnowflakeDB(config, stateManager), nil
	case "mssql":
		return NewMSSQL(config, stateManager), nil
	case "databricks":
		return NewDatabricksDB(config, stateManager), nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
}
