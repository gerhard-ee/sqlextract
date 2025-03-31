package database

import (
	"fmt"
)

// Database interface defines the methods that all database implementations must provide
type Database interface {
	Connect() error
	Close() error
	GetTableSchema(tableName string) ([]Column, error)
	ExtractData(tableName string, columns []Column, batchSize int, offset int64) ([][]interface{}, error)
	GetRowCount(tableName string) (int64, error)
}

// NewDatabase creates a new database instance based on the configuration
func NewDatabase(config *Config) (Database, error) {
	switch config.Type {
	case "postgres":
		return NewPostgresDB(config), nil
	case "duckdb":
		return NewDuckDB(config), nil
	case "bigquery":
		return NewBigQueryDB(config), nil
	case "snowflake":
		return NewSnowflake(config), nil
	case "databricks":
		return NewDatabricks(config), nil
	case "mssql":
		return NewMSSQL(config), nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}
}
