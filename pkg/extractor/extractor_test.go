package extractor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/database"
	"github.com/gerhard-ee/sqlextract/internal/state"
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
	stateManager := state.NewMemoryManager()

	// Create temporary directory for test database
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	config := &config.Config{
		Type:     "duckdb",
		Database: dbPath,
	}

	db, err := database.NewDatabase("duckdb", config, stateManager)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Connect to database
	if err := db.Connect(); err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
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

	return db
}

func cleanupTestDB(t *testing.T, db database.Database) {
	// Drop test table
	query := "DROP TABLE IF EXISTS test_table"
	if err := db.Exec(context.Background(), query); err != nil {
		t.Fatalf("Failed to drop test table: %v", err)
	}

	// Close database connection
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close database: %v", err)
	}
}

func TestExtractor_CSV(t *testing.T) {
	// Test with DuckDB
	t.Run("duckdb", func(t *testing.T) {
		// Skip test if not running on macOS
		if runtime.GOOS != "darwin" {
			t.Skip("DuckDB tests are only supported on macOS")
		}

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
}
