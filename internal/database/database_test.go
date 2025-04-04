package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

func TestDatabaseOperations(t *testing.T) {
	// Skip test if not running on macOS
	if runtime.GOOS != "darwin" {
		t.Skip("DuckDB tests are only supported on macOS")
	}

	// Create temporary directory for test files
	testDir, err := os.MkdirTemp("", "sqlextract_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create state manager
	stateManager := state.NewMemoryManager()

	// Create DuckDB database
	dbPath := filepath.Join(testDir, "test.db")
	cfg := &config.Config{
		Type:     "duckdb",
		Database: dbPath,
	}

	// Create database instance
	db, err := NewDatabase("duckdb", cfg, stateManager)
	if err != nil {
		t.Fatalf("Failed to create DuckDB database: %v", err)
	}

	// Connect to database
	if err := db.Connect(); err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Create test table
	query := `
		CREATE TABLE test_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			age INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`
	if err := db.Exec(context.Background(), query); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		query = fmt.Sprintf(`
			INSERT INTO test_table (id, name, age)
			VALUES (%d, 'Test User %d', %d)
		`, i+1, i, 20+i)
		if err := db.Exec(context.Background(), query); err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	// Test basic operations
	testBasicOperations(t, db, testDir)

	// Test concurrent operations
	testConcurrentOperations(t, db, testDir)

	// Test error handling
	testErrorHandling(t, db, testDir)

	// Clean up
	if err := db.Close(); err != nil {
		t.Errorf("Failed to close database: %v", err)
	}
}

func testBasicOperations(t *testing.T, db Database, testDir string) {
	// Test GetTotalRows
	totalRows, err := db.GetTotalRows("test_table")
	if err != nil {
		t.Errorf("GetTotalRows failed: %v", err)
		return
	}
	if totalRows < 0 {
		t.Error("Expected non-negative total rows")
		return
	}

	// Test GetColumns
	columns, err := db.GetColumns("test_table")
	if err != nil {
		t.Errorf("GetColumns failed: %v", err)
		return
	}
	if len(columns) == 0 {
		t.Error("GetColumns returned empty column list")
		return
	}

	// Test ExtractBatch
	rows, err := db.ExtractBatch("test_table", 0, 1000, "", "")
	if err != nil {
		t.Errorf("ExtractBatch failed: %v", err)
		return
	}

	// Write rows to file
	outputFile := filepath.Join(testDir, "test_output.csv")
	f, err := os.Create(outputFile)
	if err != nil {
		t.Errorf("Failed to create output file: %v", err)
		return
	}
	defer f.Close()

	// Write header
	if err := writeCSVHeader(f, columns); err != nil {
		t.Errorf("Failed to write CSV header: %v", err)
		return
	}

	// Write rows
	for _, row := range rows {
		if err := writeCSVRow(f, row); err != nil {
			t.Errorf("Failed to write CSV row: %v", err)
			return
		}
	}
}

func testConcurrentOperations(t *testing.T, db Database, testDir string) {
	// Test concurrent reads
	errChan := make(chan error, 5)
	for i := 0; i < 5; i++ {
		go func() {
			_, err := db.ExtractBatch("test_table", 0, 1000, "", "")
			errChan <- err
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 5; i++ {
		if err := <-errChan; err != nil {
			t.Errorf("Concurrent ExtractBatch failed: %v", err)
		}
	}
}

func testErrorHandling(t *testing.T, db Database, testDir string) {
	// Test non-existent table
	_, err := db.GetTotalRows("non_existent_table")
	if err == nil {
		t.Error("Expected error for non-existent table")
	}

	// Test invalid batch size
	_, err = db.ExtractBatch("test_table", 0, -1, "", "")
	if err == nil {
		t.Error("Expected error for invalid batch size")
	}
}

func writeCSVHeader(f *os.File, columns []string) error {
	_, err := fmt.Fprintf(f, "%s\n", columns)
	return err
}

func writeCSVRow(f *os.File, row map[string]interface{}) error {
	values := make([]string, len(row))
	i := 0
	for _, v := range row {
		values[i] = fmt.Sprintf("%v", v)
		i++
	}
	_, err := fmt.Fprintf(f, "%s\n", values)
	return err
}
