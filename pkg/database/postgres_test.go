package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
)

var (
	testDB *PostgresDB
)

func TestMain(m *testing.M) {
	// Get database configuration from environment variables
	config := &Config{
		Host:     getEnvOrDefault("TEST_DB_HOST", "localhost"),
		Port:     getEnvOrDefaultInt("TEST_DB_PORT", 5432),
		User:     getEnvOrDefault("TEST_DB_USER", "postgres"),
		Password: getEnvOrDefault("TEST_DB_PASSWORD", "postgres"),
		DBName:   getEnvOrDefault("TEST_DB_NAME", "sqlextract_test"),
		SSLMode:  "disable",
	}

	// Create database instance
	db := NewPostgresDB(config)

	// Drop the database if it exists
	if err := db.DropDatabase(); err != nil {
		fmt.Printf("Warning: Failed to drop database: %v\n", err)
	}

	// Create the database
	if err := db.CreateDatabase(); err != nil {
		fmt.Printf("Failed to create test database: %v\n", err)
		os.Exit(1)
	}

	// Connect to the database
	if err := db.Connect(context.Background()); err != nil {
		fmt.Printf("Failed to connect to test database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// Run tests
	code := m.Run()

	// Clean up
	if err := db.DropDatabase(); err != nil {
		fmt.Printf("Warning: Failed to drop database: %v\n", err)
	}

	os.Exit(code)
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		var result int
		if _, err := fmt.Sscanf(value, "%d", &result); err == nil {
			return result
		}
	}
	return defaultValue
}

func createTestDatabase(config *Config) error {
	// Connect to default postgres database
	config.DBName = "postgres"
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.SSLMode))
	if err != nil {
		return err
	}
	defer db.Close()

	// Drop database if it exists
	dropTestDatabase(config)

	// Create new database
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", config.DBName))
	return err
}

func dropTestDatabase(config *Config) error {
	// Connect to default postgres database
	config.DBName = "postgres"
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.SSLMode))
	if err != nil {
		return err
	}
	defer db.Close()

	// Terminate existing connections
	_, err = db.Exec(fmt.Sprintf(`
		SELECT pg_terminate_backend(pg_stat_activity.pid)
		FROM pg_stat_activity
		WHERE pg_stat_activity.datname = '%s'
		AND pid <> pg_backend_pid()`, config.DBName))
	if err != nil {
		return err
	}

	// Drop database
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", config.DBName))
	return err
}

func setupTestDB(t *testing.T) *PostgresDB {
	config := &Config{
		Host:     getEnvOrDefault("TEST_DB_HOST", "localhost"),
		Port:     getEnvOrDefaultInt("TEST_DB_PORT", 5432),
		User:     getEnvOrDefault("TEST_DB_USER", "postgres"),
		Password: getEnvOrDefault("TEST_DB_PASSWORD", "postgres"),
		DBName:   getEnvOrDefault("TEST_DB_NAME", "sqlextract_test"),
		SSLMode:  "disable",
	}

	db := NewPostgresDB(config)

	// Create database
	if err := db.CreateDatabase(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Connect to database
	if err := db.Connect(context.Background()); err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	// Create test table
	_, err := db.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS test_table (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			age INTEGER,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Truncate table
	_, err = db.Exec(context.Background(), "TRUNCATE TABLE test_table RESTART IDENTITY")
	if err != nil {
		t.Fatalf("Failed to truncate table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		_, err := db.Exec(context.Background(), `
			INSERT INTO test_table (name, age)
			VALUES ($1, $2)
		`, fmt.Sprintf("Test User %d", i), 20+i)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}

	return db
}

func setupTestTable(t *testing.T, db *PostgresDB) {
	// Create test table
	_, err := db.Exec(context.Background(), `
		CREATE TABLE IF NOT EXISTS test_table (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			age INTEGER,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create test table: %v", err)
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		_, err := db.Exec(context.Background(), `
			INSERT INTO test_table (name, age)
			VALUES ($1, $2)
		`, fmt.Sprintf("Test User %d", i), 20+i)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
	}
}

func cleanupTestTable(t *testing.T, db *PostgresDB) {
	// Drop test table
	_, err := db.Exec(context.Background(), "DROP TABLE IF EXISTS test_table")
	if err != nil {
		t.Fatalf("Failed to drop test table: %v", err)
	}
}

func TestPostgresDB_Connect(t *testing.T) {
	config := &Config{
		Host:     getEnvOrDefault("TEST_DB_HOST", "localhost"),
		Port:     getEnvOrDefaultInt("TEST_DB_PORT", 5432),
		User:     getEnvOrDefault("TEST_DB_USER", "postgres"),
		Password: getEnvOrDefault("TEST_DB_PASSWORD", "postgres"),
		DBName:   getEnvOrDefault("TEST_DB_NAME", "sqlextract_test"),
		SSLMode:  "disable",
	}

	db := NewPostgresDB(config)

	// Test connection
	if err := db.Connect(context.Background()); err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
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
		"id":         "integer",
		"name":       "character varying",
		"age":        "integer",
		"created_at": "timestamp with time zone",
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
