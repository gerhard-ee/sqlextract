package extractor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"sqlextract/pkg/database"
)

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

func setupTestDB(t *testing.T) database.Database {
	config := &database.Config{
		Host:     getEnvOrDefault("TEST_DB_HOST", "localhost"),
		Port:     getEnvOrDefaultInt("TEST_DB_PORT", 5432),
		User:     getEnvOrDefault("TEST_DB_USER", "postgres"),
		Password: getEnvOrDefault("TEST_DB_PASSWORD", "postgres"),
		DBName:   getEnvOrDefault("TEST_DB_NAME", "sqlextract_test"),
		SSLMode:  "disable",
	}

	db := database.NewPostgresDB(config)

	// Drop the database if it exists
	if err := db.DropDatabase(); err != nil {
		t.Logf("Warning: Failed to drop database: %v", err)
	}

	// Create the database
	if err := db.CreateDatabase(); err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Connect to database
	if err := db.Connect(context.Background()); err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
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

func cleanupTestDB(t *testing.T, db database.Database) {
	// Drop test table
	_, err := db.Exec(context.Background(), "DROP TABLE IF EXISTS test_table")
	if err != nil {
		t.Fatalf("Failed to drop test table: %v", err)
	}

	// Drop database
	if pgdb, ok := db.(*database.PostgresDB); ok {
		if err := pgdb.DropDatabase(); err != nil {
			t.Fatalf("Failed to drop database: %v", err)
		}
	}

	// Close database connection
	db.Close()
}

func TestExtractor_CSV(t *testing.T) {
	// Test with PostgreSQL
	t.Run("postgres", func(t *testing.T) {
		db := setupTestDB(t)
		defer cleanupTestDB(t, db)

		// Create temporary output file
		tmpDir := t.TempDir()
		outputFile := filepath.Join(tmpDir, "test.csv")

		// Create extractor
		ext := NewExtractor(db, "test_table", outputFile, "csv", 1000, 1)

		// Run extraction
		if err := ext.Extract(context.Background()); err != nil {
			t.Fatalf("Failed to extract data: %v", err)
		}

		// Read output file
		data, err := os.ReadFile(outputFile)
		if err != nil {
			t.Fatalf("Failed to read output file: %v", err)
		}

		// Verify output
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 101 { // 100 rows + header
			t.Errorf("Expected 101 lines (including header), got %d", len(lines))
		}

		// Verify header
		expectedHeaders := []string{"id", "name", "age", "created_at"}
		actualHeaders := strings.Split(lines[0], ",")
		if !reflect.DeepEqual(actualHeaders, expectedHeaders) {
			t.Errorf("Expected headers %v, got %v", expectedHeaders, actualHeaders)
		}
	})

	// Test with DuckDB
	t.Run("duckdb", func(t *testing.T) {
		// Create temporary directory for test database
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		config := &database.Config{
			DBName: dbPath,
		}

		db := database.NewDuckDB(config)

		// Connect to database
		if err := db.Connect(context.Background()); err != nil {
			t.Fatalf("Failed to connect to database: %v", err)
		}
		defer db.Close()

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

		// Create output file
		outputFile := filepath.Join(tmpDir, "test.csv")

		// Create extractor
		ext := NewExtractor(db, "test_table", outputFile, "csv", 1000, 1)

		// Run extraction
		if err := ext.Extract(context.Background()); err != nil {
			t.Fatalf("Failed to extract data: %v", err)
		}

		// Read output file
		data, err := os.ReadFile(outputFile)
		if err != nil {
			t.Fatalf("Failed to read output file: %v", err)
		}

		// Verify output
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 101 { // 100 rows + header
			t.Errorf("Expected 101 lines (including header), got %d", len(lines))
		}

		// Verify header
		expectedHeaders := []string{"id", "name", "age", "created_at"}
		actualHeaders := strings.Split(lines[0], ",")
		if !reflect.DeepEqual(actualHeaders, expectedHeaders) {
			t.Errorf("Expected headers %v, got %v", expectedHeaders, actualHeaders)
		}
	})
}

func TestExtractor_Parquet(t *testing.T) {
	// Test with PostgreSQL
	t.Run("postgres", func(t *testing.T) {
		db := setupTestDB(t)
		defer cleanupTestDB(t, db)

		// Create temporary output file
		tmpDir := t.TempDir()
		outputFile := filepath.Join(tmpDir, "test.parquet")

		// Create extractor
		ext := NewExtractor(db, "test_table", outputFile, "parquet", 1000, 1)

		// Run extraction
		err := ext.Extract(context.Background())
		if err == nil {
			t.Fatal("Expected error for unsupported Parquet format")
		}
		if !strings.Contains(err.Error(), "parquet format not supported yet") {
			t.Errorf("Expected error about unsupported Parquet format, got %v", err)
		}
	})

	// Test with DuckDB
	t.Run("duckdb", func(t *testing.T) {
		// Create temporary directory for test database
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		config := &database.Config{
			DBName: dbPath,
		}

		db := database.NewDuckDB(config)

		// Connect to database
		if err := db.Connect(context.Background()); err != nil {
			t.Fatalf("Failed to connect to database: %v", err)
		}
		defer db.Close()

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

		// Create output file
		outputFile := filepath.Join(tmpDir, "test.parquet")

		// Create extractor
		ext := NewExtractor(db, "test_table", outputFile, "parquet", 1000, 1)

		// Run extraction
		err = ext.Extract(context.Background())
		if err == nil {
			t.Fatal("Expected error for unsupported Parquet format")
		}
		if !strings.Contains(err.Error(), "parquet format not supported yet") {
			t.Errorf("Expected error about unsupported Parquet format, got %v", err)
		}
	})
}

type mockDB struct {
	database.Database
	delay time.Duration
}

func (m *mockDB) GetTotalRows(ctx context.Context, table string) (int64, error) {
	time.Sleep(m.delay) // Simulate slow operation
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return 100, nil
	}
}

func (m *mockDB) GetColumns(ctx context.Context, table string) ([]database.Column, error) {
	return []database.Column{
		{Name: "id", Type: "integer"},
		{Name: "name", Type: "text"},
	}, nil
}

func (m *mockDB) ExtractBatch(ctx context.Context, table string, offset, limit int64) (database.Rows, error) {
	return nil, nil
}

func TestExtractor_Checkpoint(t *testing.T) {
	// Create temp directory for test files
	tmpDir, err := os.MkdirTemp("", "sqlextract_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create output file
	outputFile := filepath.Join(tmpDir, "test.csv")

	// Create mock database with delay
	db := &mockDB{delay: 200 * time.Millisecond}

	// Create extractor
	ext := NewExtractor(db, "test_table", outputFile, "csv", 10, 1)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start extraction in a goroutine
	errChan := make(chan error)
	go func() {
		errChan <- ext.Extract(ctx)
	}()

	// Wait a short time then cancel the context
	time.Sleep(100 * time.Millisecond)
	cancel()

	// Wait for extraction to complete with timeout
	select {
	case err := <-errChan:
		if err == nil {
			t.Fatal("Expected error for canceled context")
		}
		if !strings.Contains(strings.ToLower(err.Error()), "context") {
			t.Errorf("Expected error containing 'context', got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Extraction did not complete within timeout")
	}

	// Save checkpoint
	if err := ext.SaveCheckpoint(); err != nil {
		t.Fatalf("Failed to save checkpoint: %v", err)
	}

	// Load checkpoint
	if err := ext.LoadCheckpoint(); err != nil {
		t.Fatalf("Failed to load checkpoint: %v", err)
	}

	// Verify checkpoint data
	if ext.checkpoint == nil {
		t.Error("Expected checkpoint to be set")
	}
	if ext.checkpoint.LastOffset != 0 {
		t.Error("Expected LastOffset to be 0 since extraction was canceled")
	}
}

func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
