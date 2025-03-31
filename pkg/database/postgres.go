package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

// PostgresDB implements the Database interface for PostgreSQL
type PostgresDB struct {
	config *Config
	db     *sql.DB
}

// DB returns the underlying database connection
func (p *PostgresDB) DB() *sql.DB {
	return p.db
}

// NewPostgresDB creates a new PostgreSQL database instance
func NewPostgresDB(config *Config) *PostgresDB {
	return &PostgresDB{
		config: config,
	}
}

// Connect establishes a connection to the PostgreSQL database
func (p *PostgresDB) Connect(ctx context.Context) error {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.config.Host,
		p.config.Port,
		p.config.User,
		p.config.Password,
		p.config.DBName,
		p.config.SSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	p.db = db
	return nil
}

// Close closes the database connection
func (p *PostgresDB) Close() {
	if p.db != nil {
		p.db.Close()
	}
}

// GetTableSchema returns the schema of a table
func (p *PostgresDB) GetTableSchema(tableName string) ([]Column, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := p.db.Query(query, tableName)
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
func (p *PostgresDB) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return p.db.QueryContext(ctx, query, args...)
}

// Exec executes a query without returning results
func (p *PostgresDB) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return p.db.ExecContext(ctx, query, args...)
}

// GetTotalRows returns the total number of rows in a table
func (p *PostgresDB) GetTotalRows(ctx context.Context, table string) (int64, error) {
	var count int64
	err := p.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", pq.QuoteIdentifier(table))).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total rows: %v", err)
	}
	return count, nil
}

// GetColumns returns the columns of a table
func (p *PostgresDB) GetColumns(ctx context.Context, table string) ([]Column, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := p.db.QueryContext(ctx, query, table)
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
func (p *PostgresDB) ExtractBatch(ctx context.Context, table string, offset, limit int64) (Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id OFFSET $1 LIMIT $2", pq.QuoteIdentifier(table))
	rows, err := p.db.QueryContext(ctx, query, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to extract batch: %v", err)
	}
	return rows, nil
}

// CreateDatabase creates the database if it doesn't exist
func (p *PostgresDB) CreateDatabase() error {
	// Connect to postgres database first
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		p.config.Host,
		p.config.Port,
		p.config.User,
		p.config.Password,
		p.config.SSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open postgres database: %v", err)
	}
	defer db.Close()

	// Check if database exists
	var exists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", p.config.DBName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %v", err)
	}

	if !exists {
		// Create database
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", pq.QuoteIdentifier(p.config.DBName)))
		if err != nil {
			return fmt.Errorf("failed to create database: %v", err)
		}
	}

	return nil
}

// DropDatabase drops the database if it exists
func (p *PostgresDB) DropDatabase() error {
	// Connect to postgres database first
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=postgres sslmode=%s",
		p.config.Host,
		p.config.Port,
		p.config.User,
		p.config.Password,
		p.config.SSLMode,
	)

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("failed to open postgres database: %v", err)
	}
	defer db.Close()

	// Terminate all connections to the database
	_, err = db.Exec(fmt.Sprintf(`
		SELECT pg_terminate_backend(pg_stat_activity.pid)
		FROM pg_stat_activity
		WHERE pg_stat_activity.datname = %s
		AND pid <> pg_backend_pid()`, pq.QuoteLiteral(p.config.DBName)))
	if err != nil {
		return fmt.Errorf("failed to terminate database connections: %v", err)
	}

	// Drop database
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", pq.QuoteIdentifier(p.config.DBName)))
	if err != nil {
		return fmt.Errorf("failed to drop database: %v", err)
	}

	return nil
}

// GetPrimaryKey returns the primary key column of a table
func (p *PostgresDB) GetPrimaryKey(ctx context.Context, table string) (string, error) {
	query := `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		AND i.indisprimary;
	`

	var pk string
	err := p.db.QueryRowContext(ctx, query, table).Scan(&pk)
	if err != nil {
		return "", fmt.Errorf("error getting primary key: %v", err)
	}

	return pk, nil
}
