package database

import (
	"context"
	"database/sql"
	"fmt"

	sf "github.com/snowflakedb/gosnowflake"
)

// Snowflake implements the Database interface for Snowflake
type Snowflake struct {
	config *Config
	db     *sql.DB
}

// NewSnowflake creates a new Snowflake instance
func NewSnowflake(config *Config) *Snowflake {
	return &Snowflake{
		config: config,
	}
}

// Connect establishes a connection to Snowflake
func (s *Snowflake) Connect(ctx context.Context) error {
	// Create DSN
	cfg := &sf.Config{
		Account:   s.config.Host,     // account identifier
		User:      s.config.User,     // username
		Password:  s.config.Password, // password
		Database:  s.config.DBName,   // database name
		Warehouse: "COMPUTE_WH",      // default warehouse
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		return fmt.Errorf("failed to create DSN: %v", err)
	}

	// Open connection
	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	s.db = db
	return nil
}

// Close closes the Snowflake connection
func (s *Snowflake) Close() {
	if s.db != nil {
		s.db.Close()
	}
}

// GetTableSchema returns the schema of a table
func (s *Snowflake) GetTableSchema(tableName string) ([]Column, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = UPPER($1)
		ORDER BY ordinal_position
	`

	rows, err := s.db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %v", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %v", err)
	}

	return columns, nil
}

// Query executes a query and returns the results
func (s *Snowflake) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}

// Exec executes a query without returning results
func (s *Snowflake) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return s.db.ExecContext(ctx, query, args...)
}

// GetTotalRows returns the total number of rows in a table
func (s *Snowflake) GetTotalRows(ctx context.Context, table string) (int64, error) {
	var count int64
	err := s.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total rows: %v", err)
	}
	return count, nil
}

// GetColumns returns the columns of a table
func (s *Snowflake) GetColumns(ctx context.Context, table string) ([]Column, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = UPPER($1)
		ORDER BY ordinal_position
	`

	rows, err := s.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %v", err)
	}

	return columns, nil
}

// ExtractBatch extracts a batch of rows from a table
func (s *Snowflake) ExtractBatch(ctx context.Context, table string, offset, limit int64) (Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s OFFSET %d LIMIT %d",
		table, s.getPrimaryKey(table), offset, limit)
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to extract batch: %v", err)
	}
	return rows, nil
}

// CreateDatabase creates the database if it doesn't exist
func (s *Snowflake) CreateDatabase() error {
	// Connect to default database
	cfg := &sf.Config{
		Account:   s.config.Host,
		User:      s.config.User,
		Password:  s.config.Password,
		Database:  "SNOWFLAKE", // default database
		Warehouse: "COMPUTE_WH",
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		return fmt.Errorf("failed to create DSN: %v", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	// Create database
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", s.config.DBName))
	return err
}

// DropDatabase drops the database if it exists
func (s *Snowflake) DropDatabase() error {
	// Connect to default database
	cfg := &sf.Config{
		Account:   s.config.Host,
		User:      s.config.User,
		Password:  s.config.Password,
		Database:  "SNOWFLAKE", // default database
		Warehouse: "COMPUTE_WH",
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		return fmt.Errorf("failed to create DSN: %v", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}
	defer db.Close()

	// Drop database
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", s.config.DBName))
	return err
}

// GetPrimaryKey returns the primary key column of a table
func (s *Snowflake) GetPrimaryKey(ctx context.Context, table string) (string, error) {
	query := `
		SELECT column_name
		FROM information_schema.key_column_usage
		WHERE table_name = UPPER($1)
		AND constraint_name LIKE '%_PK'
		LIMIT 1
	`

	var pk string
	err := s.db.QueryRowContext(ctx, query, table).Scan(&pk)
	if err != nil {
		return "", fmt.Errorf("error getting primary key: %v", err)
	}

	return pk, nil
}

// Helper function to get primary key without context
func (s *Snowflake) getPrimaryKey(table string) string {
	pk, err := s.GetPrimaryKey(context.Background(), table)
	if err != nil {
		// Default to METADATA$ROW_ID if we can't determine the primary key
		return "METADATA$ROW_ID"
	}
	return pk
}
