package database

import (
	"fmt"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

// Database represents a database interface
type Database interface {
	ExtractData(table, outputFile, format string, batchSize int) error
}

// NewDatabase creates a new database instance based on the configuration
func NewDatabase(cfg *config.Config, stateManager state.Manager) (Database, error) {
	switch cfg.Type {
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
	default:
		return nil, fmt.Errorf("unsupported database type: %s", cfg.Type)
	}
}
