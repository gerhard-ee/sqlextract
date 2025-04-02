package database

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
)

func setupTestDB(t *testing.T) *DuckDB {
	// Create temporary directory for test database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &Config{
		DBName: dbPath,
	}

	db := NewDuckDB(config)

	// Connect to database
	if err := db.Connect(context.Background()); err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Create test table
	_, err := db.Exec(context.Background(), `
		CREATE TABLE test_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			age INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		_, err := db.Exec(context.Background(), `
			INSERT INTO test_table (id, name, age)
			VALUES ($1, $2, $3)
		`, i+1, fmt.Sprintf("Test User %d", i), 20+i)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	return db
}

func setupTestTable(t *testing.T, db *DuckDB) {
	// Create test table
	_, err := db.Exec(context.Background(), `
		CREATE TABLE test_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			age INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		_, err := db.Exec(context.Background(), `
			INSERT INTO test_table (id, name, age)
			VALUES ($1, $2, $3)
		`, i+1, fmt.Sprintf("Test User %d", i), 20+i)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}
}

func cleanupTestTable(t *testing.T, db *DuckDB) {
	// Drop test table
	_, err := db.Exec(context.Background(), "DROP TABLE IF EXISTS test_table")
	if err != nil {
		t.Fatalf("Failed to drop test table: %v", err)
	}
}

func TestPostgresDB_Connect(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test query
	rows, err := db.Query(context.Background(), "SELECT 1")
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("Expected at least one row")
	}

	var result int
	if err := rows.Scan(&result); err != nil {
		t.Fatalf("Failed to scan result: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected result to be 1, got %d", result)
	}
}

func TestPostgresDB_GetTableSchema(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Get table schema
	schema, err := db.GetTableSchema("test_table")
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Verify schema
	expectedColumns := []string{"id", "name", "age", "created_at"}
	if len(schema) != len(expectedColumns) {
		t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(schema))
	}

	for i, col := range expectedColumns {
		if schema[i].Name != col {
			t.Errorf("Expected column %s, got %s", col, schema[i].Name)
		}
	}
}

func TestPostgresDB_GetTotalRows(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Get total rows
	count, err := db.GetTotalRows(context.Background(), "test_table")
	if err != nil {
		t.Fatalf("Failed to get total rows: %v", err)
	}

	// Verify count
	if count != 100 {
		t.Errorf("Expected 100 rows, got %d", count)
	}
}

func TestPostgresDB_ExtractBatch(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Extract batch
	rows, err := db.ExtractBatch(context.Background(), "test_table", 0, 10)
	if err != nil {
		t.Fatalf("Failed to extract batch: %v", err)
	}
	defer rows.Close()

	// Count rows
	var count int
	for rows.Next() {
		count++
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Error iterating rows: %v", err)
	}

	// Verify count
	if count != 10 {
		t.Errorf("Expected 10 rows, got %d", count)
	}
}

func TestPostgresDB_GetColumns(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Get columns
	columns, err := db.GetColumns(context.Background(), "test_table")
	if err != nil {
		t.Fatalf("Failed to get columns: %v", err)
	}

	// Verify columns
	expectedColumns := map[string]string{
		"id":         "INTEGER",
		"name":       "VARCHAR",
		"age":        "INTEGER",
		"created_at": "TIMESTAMP",
	}

	for _, col := range columns {
		if expectedType, ok := expectedColumns[col.Name]; !ok || col.Type != expectedType {
			t.Errorf("Unexpected column type for %s: got %s, want %s", col.Name, col.Type, expectedType)
		}
	}
}

func TestPostgresDB_GetPrimaryKey(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Get primary key
	pk, err := db.GetPrimaryKey(context.Background(), "test_table")
	if err != nil {
		t.Fatalf("Failed to get primary key: %v", err)
	}

	// Verify primary key
	if pk != "id" {
		t.Errorf("Expected primary key 'id', got '%s'", pk)
	}
}
