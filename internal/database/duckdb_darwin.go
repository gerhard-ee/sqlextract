//go:build darwin && !release
// +build darwin,!release

package database

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
	_ "github.com/marcboeker/go-duckdb"
)

// duckDBDarwin is the macOS-specific implementation of DuckDB
type duckDBDarwin struct {
	*duckDBBase
	db *sql.DB
}

// NewDuckDB creates a new DuckDB instance for macOS
func NewDuckDB(cfg *config.Config, stateManager state.Manager) (DuckDB, error) {
	base, err := newDuckDBBase(cfg, stateManager)
	if err != nil {
		return nil, err
	}
	return &duckDBDarwin{
		duckDBBase: base,
	}, nil
}

// Connect establishes a connection to the DuckDB database
func (d *duckDBDarwin) Connect() error {
	// For DuckDB, the Database is treated as a file path
	dbPath := d.config.Database
	if !filepath.IsAbs(dbPath) {
		dbPath = filepath.Join(".", dbPath)
	}

	dsn := fmt.Sprintf("%s", dbPath)
	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	d.db = db
	return nil
}

// Close closes the database connection
func (d *duckDBDarwin) Close() error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// GetTableSchema returns the schema of a table
func (d *duckDBDarwin) GetTableSchema(tableName string) ([]Column, error) {
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

// Exec executes a SQL query
func (d *duckDBDarwin) Exec(ctx context.Context, query string) error {
	_, err := d.db.ExecContext(ctx, query)
	return err
}

// GetTotalRows returns the total number of rows in a table
func (d *duckDBDarwin) GetTotalRows(table string) (int64, error) {
	var count int64
	err := d.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}

// GetColumns returns the column names for a table
func (d *duckDBDarwin) GetColumns(table string) ([]string, error) {
	schema, err := d.GetTableSchema(table)
	if err != nil {
		return nil, err
	}
	columns := make([]string, len(schema))
	for i, col := range schema {
		columns[i] = col.Name
	}
	return columns, nil
}

// ExtractBatch extracts a batch of rows from a table
func (d *duckDBDarwin) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table, limit, offset)
	rows, err := d.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query rows: %v", err)
	}
	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	var result []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return result, nil
}

// ExtractData extracts data from a table and writes it to a file
func (d *duckDBDarwin) ExtractData(table, outputFile, format string, batchSize int, keyColumns, whereClause string) error {
	query := fmt.Sprintf("COPY %s TO '%s' (FORMAT %s)", table, outputFile, format)
	_, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to extract data: %v", err)
	}
	return nil
}
