package database

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

// DuckDB implements the Database interface for DuckDB
type DuckDB struct {
	config *Config
	db     *sql.DB
}

// NewDuckDB creates a new DuckDB instance
func NewDuckDB(config *Config) *DuckDB {
	return &DuckDB{
		config: config,
	}
}

// Connect establishes a connection to the DuckDB database
func (d *DuckDB) Connect(ctx context.Context) error {
	// For DuckDB, the DBName is treated as a file path
	dbPath := d.config.DBName
	if !filepath.IsAbs(dbPath) {
		dbPath = filepath.Join(".", dbPath)
	}

	dsn := fmt.Sprintf("%s", dbPath)
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	d.db = db
	return nil
}

// Close closes the database connection
func (d *DuckDB) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

// GetTableSchema returns the schema of a table
func (d *DuckDB) GetTableSchema(tableName string) ([]Column, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := d.db.Query(query, tableName)
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
func (d *DuckDB) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return d.db.QueryContext(ctx, query, args...)
}

// Exec executes a query without returning results
func (d *DuckDB) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

// GetTotalRows returns the total number of rows in a table
func (d *DuckDB) GetTotalRows(ctx context.Context, table string) (int64, error) {
	var count int64
	err := d.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total rows: %v", err)
	}
	return count, nil
}

// GetColumns returns the columns of a table
func (d *DuckDB) GetColumns(ctx context.Context, table string) ([]Column, error) {
	query := `
		SELECT column_name, data_type, is_nullable
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`

	rows, err := d.db.QueryContext(ctx, query, table)
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
func (d *DuckDB) ExtractBatch(ctx context.Context, table string, offset, limit int64) (Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY id OFFSET $1 LIMIT $2", table)
	rows, err := d.db.QueryContext(ctx, query, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to extract batch: %v", err)
	}
	return rows, nil
}

// CreateDatabase creates the database if it doesn't exist
func (d *DuckDB) CreateDatabase() error {
	// For DuckDB, the database is created automatically when connecting
	// if the file doesn't exist
	return nil
}

// DropDatabase drops the database if it exists
func (d *DuckDB) DropDatabase() error {
	// For DuckDB, we just need to close the connection
	// The file will be deleted by the caller if needed
	return nil
}

// GetPrimaryKey returns the primary key column of a table
func (d *DuckDB) GetPrimaryKey(ctx context.Context, table string) (string, error) {
	query := `
		SELECT column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.constraint_column_usage AS ccu USING (constraint_name)
		WHERE tc.constraint_type = 'PRIMARY KEY'
		AND tc.table_name = $1
		LIMIT 1
	`

	var pk string
	err := d.db.QueryRowContext(ctx, query, table).Scan(&pk)
	if err != nil {
		return "", fmt.Errorf("error getting primary key: %v", err)
	}

	return pk, nil
}
