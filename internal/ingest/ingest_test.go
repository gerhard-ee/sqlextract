package ingest

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIngestionScripts(t *testing.T) {
	// Create test output directory
	testDir := "test_output"
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Test each database type
	databases := []struct {
		name     string
		dbType   string
		skipTest bool
	}{
		{"Snowflake", "snowflake", false},
		{"BigQuery", "bigquery", false},
		{"Databricks", "databricks", false},
		{"PostgreSQL", "postgres", false},
		{"MSSQL", "mssql", false},
	}

	for _, db := range databases {
		t.Run(db.name, func(t *testing.T) {
			if db.skipTest {
				t.Skipf("Skipping %s test", db.name)
			}

			// Create ingester
			ingester, err := NewIngester(db.dbType)
			if err != nil {
				t.Fatalf("Failed to create %s ingester: %v", db.name, err)
			}

			// Test CSV ingestion script
			testCSVIngestion(t, ingester, testDir, db.name)

			// Test Parquet ingestion script
			testParquetIngestion(t, ingester, testDir, db.name)
		})
	}
}

func testCSVIngestion(t *testing.T, ingester Ingester, testDir, dbName string) {
	// Generate CSV ingestion script
	script, err := ingester.GenerateCSVIngestScript("source_table", "target_table")
	if err != nil {
		t.Errorf("Failed to generate CSV ingestion script for %s: %v", dbName, err)
	}

	// Verify script content
	if script == "" {
		t.Errorf("Generated CSV ingestion script for %s is empty", dbName)
	}

	// Write script to file
	scriptFile := filepath.Join(testDir, dbName+"_csv_ingest.sql")
	if err := os.WriteFile(scriptFile, []byte(script), 0644); err != nil {
		t.Errorf("Failed to write CSV ingestion script for %s: %v", dbName, err)
	}

	// Verify file exists
	if _, err := os.Stat(scriptFile); os.IsNotExist(err) {
		t.Errorf("CSV ingestion script file for %s was not created", dbName)
	}
}

func testParquetIngestion(t *testing.T, ingester Ingester, testDir, dbName string) {
	// Generate Parquet ingestion script
	script, err := ingester.GenerateParquetIngestScript("source_table", "target_table")
	if err != nil {
		t.Errorf("Failed to generate Parquet ingestion script for %s: %v", dbName, err)
	}

	// Verify script content
	if script == "" {
		t.Errorf("Generated Parquet ingestion script for %s is empty", dbName)
	}

	// Write script to file
	scriptFile := filepath.Join(testDir, dbName+"_parquet_ingest.sql")
	if err := os.WriteFile(scriptFile, []byte(script), 0644); err != nil {
		t.Errorf("Failed to write Parquet ingestion script for %s: %v", dbName, err)
	}

	// Verify file exists
	if _, err := os.Stat(scriptFile); os.IsNotExist(err) {
		t.Errorf("Parquet ingestion script file for %s was not created", dbName)
	}
}

func TestErrorHandling(t *testing.T) {
	// Test invalid database type
	_, err := NewIngester("invalid_db")
	if err == nil {
		t.Error("Expected error when creating ingester with invalid database type")
	}

	// Test nil ingester
	var ingester Ingester
	_, err = ingester.GenerateCSVIngestScript("source", "target")
	if err == nil {
		t.Error("Expected error when using nil ingester")
	}

	_, err = ingester.GenerateParquetIngestScript("source", "target")
	if err == nil {
		t.Error("Expected error when using nil ingester")
	}
}

func TestScriptContent(t *testing.T) {
	// Test Snowflake script content
	snowflakeIngester, err := NewIngester("snowflake")
	if err != nil {
		t.Fatalf("Failed to create Snowflake ingester: %v", err)
	}

	// Test CSV script content
	csvScript, err := snowflakeIngester.GenerateCSVIngestScript("source", "target")
	if err != nil {
		t.Fatalf("Failed to generate CSV script: %v", err)
	}

	// Verify CSV script contains required elements
	requiredElements := []string{
		"COPY INTO",
		"target",
		"FILE_FORMAT",
		"TYPE = 'CSV'",
		"SKIP_HEADER = 1",
		"ON_ERROR = 'CONTINUE'",
	}

	for _, element := range requiredElements {
		if !strings.Contains(csvScript, element) {
			t.Errorf("CSV script missing required element: %s", element)
		}
	}

	// Test Parquet script content
	parquetScript, err := snowflakeIngester.GenerateParquetIngestScript("source", "target")
	if err != nil {
		t.Fatalf("Failed to generate Parquet script: %v", err)
	}

	// Verify Parquet script contains required elements
	requiredElements = []string{
		"COPY INTO",
		"target",
		"FILE_FORMAT",
		"TYPE = 'PARQUET'",
		"ON_ERROR = 'CONTINUE'",
	}

	for _, element := range requiredElements {
		if !strings.Contains(parquetScript, element) {
			t.Errorf("Parquet script missing required element: %s", element)
		}
	}
}
