//go:build darwin && !release
// +build darwin,!release

package database

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

func TestDuckDB_Connect(t *testing.T) {
	// Create a temporary database file
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create a new DuckDB instance
	config := &config.Config{
		Database: dbPath,
	}
	stateManager := state.NewMemoryStateManager()
	db, err := NewDuckDB(config, stateManager)
	if err != nil {
		t.Fatalf("Failed to create DuckDB instance: %v", err)
	}

	// Test connecting to a non-existent database
	err = db.Connect()
	if err != nil {
		t.Errorf("Connect() failed: %v", err)
	}

	// Clean up
	db.Close()
}

func TestDuckDB_ExtractData(t *testing.T) {
	// Create a temporary database file and output directory
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")
	outputDir := filepath.Join(tempDir, "output")
	os.MkdirAll(outputDir, 0755)

	// Create a new DuckDB instance
	config := &config.Config{
		Database: dbPath,
	}
	stateManager := state.NewMemoryStateManager()
	db, err := NewDuckDB(config, stateManager)
	if err != nil {
		t.Fatalf("Failed to create DuckDB instance: %v", err)
	}

	// Connect to the database
	if err := db.Connect(); err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Create a test table
	createTableSQL := `
		CREATE TABLE test_table (
			id INTEGER,
			name VARCHAR,
			value DECIMAL(10,2),
			created_at TIMESTAMP
		);
	`
	if err := db.Exec(context.Background(), createTableSQL); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	insertSQL := `
		INSERT INTO test_table VALUES
		(1, 'test1', 10.50, '2024-01-01 00:00:00'),
		(2, 'test2', 20.75, '2024-01-02 00:00:00');
	`
	if err := db.Exec(context.Background(), insertSQL); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test CSV export
	csvOutput := filepath.Join(outputDir, "test.csv")
	err = db.ExtractData("test_table", csvOutput, "csv", 1000, "", "")
	if err != nil {
		t.Errorf("ExtractData(CSV) failed: %v", err)
	}

	// Verify CSV file was created
	if _, err := os.Stat(csvOutput); os.IsNotExist(err) {
		t.Errorf("CSV output file was not created")
	}

	// Test Parquet export
	parquetOutput := filepath.Join(outputDir, "test.parquet")
	err = db.ExtractData("test_table", parquetOutput, "parquet", 1000, "", "")
	if err != nil {
		t.Errorf("ExtractData(Parquet) failed: %v", err)
	}

	// Verify Parquet file was created
	if _, err := os.Stat(parquetOutput); os.IsNotExist(err) {
		t.Errorf("Parquet output file was not created")
	}

	// Clean up
	db.Close()
}

func TestDuckDB_GetColumns(t *testing.T) {
	// Create a temporary database file
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create a new DuckDB instance
	config := &config.Config{
		Database: dbPath,
	}
	stateManager := state.NewMemoryStateManager()
	db, err := NewDuckDB(config, stateManager)
	if err != nil {
		t.Fatalf("Failed to create DuckDB instance: %v", err)
	}

	// Connect to the database
	if err := db.Connect(); err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Create a test table
	createTableSQL := `
		CREATE TABLE test_table (
			id INTEGER,
			name VARCHAR,
			value DECIMAL(10,2)
		);
	`
	if err := db.Exec(context.Background(), createTableSQL); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Get columns
	columns, err := db.GetColumns("test_table")
	if err != nil {
		t.Errorf("GetColumns() failed: %v", err)
	}

	// Verify columns
	expectedColumns := []string{"id", "name", "value"}
	if len(columns) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns))
	}

	for i, col := range expectedColumns {
		if columns[i] != col {
			t.Errorf("Expected column %s at position %d, got %s", col, i, columns[i])
		}
	}

	// Clean up
	db.Close()
}

func TestDuckDB_StateManagement(t *testing.T) {
	// Create a temporary database file
	tempDir := t.TempDir()
	dbPath := filepath.Join(tempDir, "test.db")

	// Create a new DuckDB instance
	config := &config.Config{
		Database: dbPath,
	}
	stateManager := state.NewMemoryStateManager()
	db, err := NewDuckDB(config, stateManager)
	if err != nil {
		t.Fatalf("Failed to create DuckDB instance: %v", err)
	}

	// Connect to the database
	if err := db.Connect(); err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Create a test table
	createTableSQL := `
		CREATE TABLE test_table (
			id INTEGER,
			name VARCHAR
		);
	`
	if err := db.Exec(context.Background(), createTableSQL); err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	insertSQL := `
		INSERT INTO test_table VALUES
		(1, 'test1'),
		(2, 'test2');
	`
	if err := db.Exec(context.Background(), insertSQL); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Test state management during extraction
	outputFile := filepath.Join(tempDir, "test.csv")
	err = db.ExtractData("test_table", outputFile, "csv", 1000, "", "")
	if err != nil {
		t.Errorf("ExtractData() failed: %v", err)
	}

	// Verify state was updated
	state, err := stateManager.GetState("test_table")
	if err != nil {
		t.Errorf("Failed to get state: %v", err)
	}

	if state.Status != "running" {
		t.Errorf("Expected state status 'running', got '%s'", state.Status)
	}

	if time.Since(state.LastUpdated) > time.Second {
		t.Errorf("State was not updated recently")
	}

	// Clean up
	db.Close()
}
