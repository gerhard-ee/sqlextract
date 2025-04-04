package database

import (
	"context"
	"fmt"
	"runtime"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

// Database defines the interface for database operations
type Database interface {
	// Connect establishes a connection to the database
	Connect() error
	// Close closes the database connection
	Close() error
	// ExtractData extracts data from a table and writes it to a file
	ExtractData(table, outputFile, format string, batchSize int, keyColumns, whereClause string) error
	// GetTotalRows returns the total number of rows in a table
	GetTotalRows(table string) (int64, error)
	// GetColumns returns the column names for a table
	GetColumns(table string) ([]string, error)
	// ExtractBatch extracts a batch of rows from a table
	ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error)
	// Exec executes a SQL query
	Exec(ctx context.Context, query string) error
}

// NewDatabase creates a new database instance based on the type
func NewDatabase(dbType string, cfg *config.Config, stateManager state.Manager) (Database, error) {
	switch dbType {
	case "postgres":
		return NewPostgres(cfg, stateManager)
	case "mssql":
		return NewMSSQL(cfg, stateManager)
	case "bigquery":
		return NewBigQuery(cfg, stateManager)
	case "snowflake":
		return NewSnowflake(cfg, stateManager)
	case "databricks":
		return NewDatabricks(cfg, stateManager)
	case "duckdb":
		if runtime.GOOS != "darwin" {
			return nil, fmt.Errorf("DuckDB support is only available on macOS")
		}
		return NewDuckDB(cfg, stateManager)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}
