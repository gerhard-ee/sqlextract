package database

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	_ "github.com/databricks/databricks-sql-go"
)

// Databricks implements the Database interface for Databricks
type Databricks struct {
	config *Config
	db     *sql.DB
}

// NewDatabricks creates a new Databricks instance
func NewDatabricks(config *Config) *Databricks {
	return &Databricks{
		config: config,
	}
}

// Connect establishes a connection to Databricks
func (d *Databricks) Connect(ctx context.Context) error {
	// Create DSN
	dsn := url.URL{
		Scheme: "databricks",
		Host:   d.config.Host,
		Path:   d.config.DBName,
	}

	q := dsn.Query()
	q.Set("token", d.config.Password) // Using password field for access token
	q.Set("catalog", d.config.DBName)
	dsn.RawQuery = q.Encode()

	// Open connection
	db, err := sql.Open("databricks", dsn.String())
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	d.db = db
	return nil
}

// Close closes the Databricks connection
func (d *Databricks) Close() {
	if d.db != nil {
		d.db.Close()
	}
}

// GetTableSchema returns the schema of a table
func (d *Databricks) GetTableSchema(tableName string) ([]Column, error) {
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
func (d *Databricks) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return d.db.QueryContext(ctx, query, args...)
}

// Exec executes a query without returning results
func (d *Databricks) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

// GetTotalRows returns the total number of rows in a table
func (d *Databricks) GetTotalRows(ctx context.Context, table string) (int64, error) {
	var count int64
	err := d.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total rows: %v", err)
	}
	return count, nil
}

// GetColumns returns the columns of a table
func (d *Databricks) GetColumns(ctx context.Context, table string) ([]Column, error) {
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
func (d *Databricks) ExtractBatch(ctx context.Context, table string, offset, limit int64) (Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s OFFSET %d LIMIT %d",
		table, d.getPrimaryKey(table), offset, limit)
	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to extract batch: %v", err)
	}
	return rows, nil
}

// CreateDatabase creates the database if it doesn't exist
func (d *Databricks) CreateDatabase() error {
	// For Databricks, we don't need to create a database
	// The catalog should already exist
	return nil
}

// DropDatabase drops the database if it exists
func (d *Databricks) DropDatabase() error {
	// For Databricks, we don't drop the database
	// The catalog should be managed separately
	return nil
}

// GetPrimaryKey returns the primary key column of a table
func (d *Databricks) GetPrimaryKey(ctx context.Context, table string) (string, error) {
	// Databricks doesn't have a standard way to get primary key information
	// We'll try to get it from table properties
	query := `
		DESCRIBE EXTENDED %s
	`

	rows, err := d.db.QueryContext(ctx, fmt.Sprintf(query, table))
	if err != nil {
		return "", fmt.Errorf("error getting primary key: %v", err)
	}
	defer rows.Close()

	// Look for primary key in table properties
	for rows.Next() {
		var colName, colType, comment string
		if err := rows.Scan(&colName, &colType, &comment); err != nil {
			return "", fmt.Errorf("error scanning row: %v", err)
		}

		if colName == "Primary Key" {
			return comment, nil
		}
	}

	// If no primary key is found, return the first column
	columns, err := d.GetColumns(ctx, table)
	if err != nil {
		return "", fmt.Errorf("error getting columns: %v", err)
	}

	if len(columns) > 0 {
		return columns[0].Name, nil
	}

	return "", fmt.Errorf("no suitable primary key found")
}

// Helper function to get primary key without context
func (d *Databricks) getPrimaryKey(table string) string {
	pk, err := d.GetPrimaryKey(context.Background(), table)
	if err != nil {
		// Default to first column if we can't determine the primary key
		columns, err := d.GetColumns(context.Background(), table)
		if err != nil || len(columns) == 0 {
			return "_id"
		}
		return columns[0].Name
	}
	return pk
}
