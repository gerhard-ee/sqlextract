package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

func TestDatabaseImplementations(t *testing.T) {
	// Create test output directory
	testDir := "test_output"
	if err := os.MkdirAll(testDir, 0755); err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create test state manager
	stateManager := state.NewMemoryManager()

	// Test configuration
	testConfig := &config.Config{
		Type:     "postgres",
		Host:     "localhost",
		Port:     5432,
		Username: "testuser",
		Password: "testpass",
		Database: "testdb",
		Schema:   "public",
	}

	// Test each database type
	testCases := []struct {
		name     string
		dbType   string
		skipTest bool
		setup    func(*config.Config) // Additional setup for specific database types
	}{
		{
			name:     "PostgreSQL",
			dbType:   "postgres",
			skipTest: false,
		},
		{
			name:     "MSSQL",
			dbType:   "mssql",
			skipTest: false,
		},
		{
			name:     "Snowflake",
			dbType:   "snowflake",
			skipTest: true, // Skip if credentials not available
			setup: func(cfg *config.Config) {
				cfg.Warehouse = "COMPUTE_WH"
			},
		},
		{
			name:     "BigQuery",
			dbType:   "bigquery",
			skipTest: true, // Skip if credentials not available
			setup: func(cfg *config.Config) {
				cfg.ProjectID = os.Getenv("BIGQUERY_PROJECT_ID")
				cfg.CredentialsFile = os.Getenv("BIGQUERY_CREDENTIALS_FILE")
			},
		},
		{
			name:     "Databricks",
			dbType:   "databricks",
			skipTest: true, // Skip if credentials not available
			setup: func(cfg *config.Config) {
				cfg.Catalog = "test_catalog"
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipTest {
				t.Skipf("Skipping %s test", tc.name)
			}

			// Clone config for this test
			cfg := *testConfig
			cfg.Type = tc.dbType

			// Run any additional setup
			if tc.setup != nil {
				tc.setup(&cfg)
			}

			// Create database instance
			db, err := NewDatabase(tc.dbType, &cfg, stateManager)
			if err != nil {
				t.Fatalf("Failed to create %s database: %v", tc.name, err)
			}

			// Test basic operations
			testBasicOperations(t, db, testDir)

			// Test concurrent operations
			testConcurrentOperations(t, db, testDir)

			// Test error handling
			testErrorHandling(t, db, testDir)
		})
	}
}

func testBasicOperations(t *testing.T, db Database, testDir string) {
	ctx := context.Background()

	// Test GetTotalRows
	totalRows, err := db.GetTotalRows(ctx, "test_table")
	if err != nil {
		t.Errorf("GetTotalRows failed: %v", err)
	}
	if totalRows < 0 {
		t.Error("Expected non-negative total rows")
	}

	// Test GetColumns
	columns, err := db.GetColumns(ctx, "test_table")
	if err != nil {
		t.Errorf("GetColumns failed: %v", err)
	}
	if len(columns) == 0 {
		t.Error("GetColumns returned empty column list")
	}

	// Test ExtractBatch
	rows, err := db.ExtractBatch(ctx, "test_table", 0, 1000)
	if err != nil {
		t.Errorf("ExtractBatch failed: %v", err)
	}
	defer rows.Close()

	// Write rows to file
	outputFile := filepath.Join(testDir, "test_output.csv")
	f, err := os.Create(outputFile)
	if err != nil {
		t.Errorf("Failed to create output file: %v", err)
	}
	defer f.Close()

	// Write header
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}
	if err := writeCSVHeader(f, columnNames); err != nil {
		t.Errorf("Failed to write CSV header: %v", err)
	}

	// Write rows
	values := make([]interface{}, len(columns))
	for rows.Next() {
		if err := rows.Scan(values...); err != nil {
			t.Errorf("Failed to scan row: %v", err)
			continue
		}
		if err := writeCSVRow(f, values); err != nil {
			t.Errorf("Failed to write CSV row: %v", err)
		}
	}

	if err := rows.Err(); err != nil {
		t.Errorf("Error iterating rows: %v", err)
	}

	// Verify output file exists
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Error("Output file was not created")
	}
}

func testConcurrentOperations(t *testing.T, db Database, testDir string) {
	ctx := context.Background()

	// Test concurrent data extraction
	errChan := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func(id int) {
			rows, err := db.ExtractBatch(ctx, "test_table", int64(id*1000), 1000)
			if err != nil {
				errChan <- err
				return
			}
			defer rows.Close()

			outputFile := filepath.Join(testDir, fmt.Sprintf("test_output_%d.csv", id))
			f, err := os.Create(outputFile)
			if err != nil {
				errChan <- err
				return
			}
			defer f.Close()

			// Write rows
			columns, err := db.GetColumns(ctx, "test_table")
			if err != nil {
				errChan <- err
				return
			}

			// Write header
			columnNames := make([]string, len(columns))
			for i, col := range columns {
				columnNames[i] = col.Name
			}
			if err := writeCSVHeader(f, columnNames); err != nil {
				errChan <- err
				return
			}

			// Write rows
			values := make([]interface{}, len(columns))
			for rows.Next() {
				if err := rows.Scan(values...); err != nil {
					errChan <- err
					return
				}
				if err := writeCSVRow(f, values); err != nil {
					errChan <- err
					return
				}
			}

			if err := rows.Err(); err != nil {
				errChan <- err
				return
			}

			errChan <- nil
		}(i)
	}

	// Wait for all goroutines to complete and check errors
	for i := 0; i < 3; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent ExtractBatch failed: %v", err)
		}
	}
}

func testErrorHandling(t *testing.T, db Database, testDir string) {
	ctx := context.Background()

	// Test with invalid table name
	_, err := db.GetTotalRows(ctx, "invalid_table")
	if err == nil {
		t.Error("Expected error when getting total rows for invalid table")
	}

	// Test with invalid offset
	_, err = db.ExtractBatch(ctx, "test_table", -1, 1000)
	if err == nil {
		t.Error("Expected error when using invalid offset")
	}
}

func writeCSVHeader(f *os.File, header []string) error {
	for i, col := range header {
		if i > 0 {
			if _, err := f.WriteString(","); err != nil {
				return err
			}
		}
		if _, err := f.WriteString(col); err != nil {
			return err
		}
	}
	_, err := f.WriteString("\n")
	return err
}

func writeCSVRow(f *os.File, values []interface{}) error {
	for i, val := range values {
		if i > 0 {
			if _, err := f.WriteString(","); err != nil {
				return err
			}
		}
		if _, err := f.WriteString(fmt.Sprintf("%v", val)); err != nil {
			return err
		}
	}
	_, err := f.WriteString("\n")
	return err
}
