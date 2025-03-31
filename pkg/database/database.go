package database

import (
	"context"
)

// Database defines the interface for database operations
type Database interface {
	// Connect establishes a connection to the database
	Connect(ctx context.Context) error

	// Close closes the database connection
	Close()

	// GetTableSchema returns the schema of a table
	GetTableSchema(tableName string) ([]Column, error)

	// Query executes a query and returns the results
	Query(ctx context.Context, query string, args ...interface{}) (Rows, error)

	// Exec executes a query without returning results
	Exec(ctx context.Context, query string, args ...interface{}) (Result, error)

	// GetTotalRows returns the total number of rows in a table
	GetTotalRows(ctx context.Context, table string) (int64, error)

	// GetColumns returns the columns of a table
	GetColumns(ctx context.Context, table string) ([]Column, error)

	// ExtractBatch extracts a batch of rows from a table
	ExtractBatch(ctx context.Context, table string, offset, limit int64) (Rows, error)

	// GetPrimaryKey returns the primary key column of a table
	GetPrimaryKey(ctx context.Context, table string) (string, error)

	// CreateDatabase creates the database if it doesn't exist
	CreateDatabase() error

	// DropDatabase drops the database if it exists
	DropDatabase() error
}

// Column represents a database column
type Column struct {
	Name     string
	Type     string
	Nullable bool
}

// Rows represents a database result set
type Rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Close() error
	Err() error
}

// Result represents the result of an Exec operation
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

// Config holds database connection configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	DBName   string
	SSLMode  string
}
