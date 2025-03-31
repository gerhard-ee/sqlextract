package database

import (
	"testing"
)

func TestPostgresDB(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     5432,
		Username: "postgres",
		Password: "postgres",
		Database: "test",
		Schema:   "public",
	}

	db := NewPostgresDB(config)
	if db == nil {
		t.Error("Expected non-nil PostgresDB instance")
	}
}

func TestDuckDB(t *testing.T) {
	config := &Config{
		Database: "test.db",
	}

	db := NewDuckDB(config)
	if db == nil {
		t.Error("Expected non-nil DuckDB instance")
	}
}

func TestBigQueryDB(t *testing.T) {
	config := &Config{
		ProjectID:       "test-project",
		Database:        "test_dataset",
		CredentialsFile: "credentials.json",
	}

	db := NewBigQueryDB(config)
	if db == nil {
		t.Error("Expected non-nil BigQueryDB instance")
	}
}

func TestSnowflakeDB(t *testing.T) {
	config := &Config{
		Host:      "test.snowflakecomputing.com",
		Username:  "testuser",
		Password:  "testpass",
		Database:  "testdb",
		Schema:    "public",
		Warehouse: "compute_wh",
	}

	db := NewSnowflake(config)
	if db == nil {
		t.Fatal("Failed to create Snowflake database instance")
	}
}

func TestDatabricksDB(t *testing.T) {
	config := &Config{
		Host:     "test-workspace.cloud.databricks.com",
		Username: "testuser",
		Password: "testpass",
		Database: "testdb",
		Schema:   "public",
		Catalog:  "test_catalog",
	}

	db := NewDatabricks(config)
	if db == nil {
		t.Fatal("Failed to create Databricks database instance")
	}
}

func TestMSSQLDB(t *testing.T) {
	config := &Config{
		Host:     "localhost",
		Port:     1433,
		Username: "sa",
		Password: "testpass",
		Database: "testdb",
		Schema:   "dbo",
	}

	db := NewMSSQL(config)
	if db == nil {
		t.Fatal("Failed to create SQL Server database instance")
	}
}
