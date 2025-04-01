package ingest

import (
	"fmt"
)

// Ingester defines the interface for generating ingestion scripts
type Ingester interface {
	// GenerateCSVIngestScript generates a script to ingest CSV data
	GenerateCSVIngestScript(sourcePath, targetTable string) (string, error)

	// GenerateParquetIngestScript generates a script to ingest Parquet data
	GenerateParquetIngestScript(sourcePath, targetTable string) (string, error)
}

// NewIngester creates a new ingester based on the database type
func NewIngester(dbType string) (Ingester, error) {
	switch dbType {
	case "snowflake":
		return NewSnowflakeIngester(), nil
	case "bigquery":
		return NewBigQueryIngester(), nil
	case "databricks":
		return NewDatabricksIngester(), nil
	case "postgres":
		return NewPostgresIngester(), nil
	case "mssql":
		return NewMSSQLIngester(), nil
	default:
		return nil, fmt.Errorf("unsupported database type for ingestion: %s", dbType)
	}
}
