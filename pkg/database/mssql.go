package database

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gerhard-ee/sqlextract/internal/config"
)

// MSSQL implements the Database interface for Microsoft SQL Server
type MSSQL struct {
	config *config.Config
	db     *sql.DB
}

// NewMSSQL creates a new SQL Server instance
func NewMSSQL(config *config.Config) *MSSQL {
	return &MSSQL{
		config: config,
	}
}

// Connect establishes a connection to SQL Server
func (m *MSSQL) Connect(ctx context.Context) error {
	dsn := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=%s",
		m.config.User,
		m.config.Password,
		m.config.Host,
		m.config.Port,
		m.config.Database,
	)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	m.db = db
	return nil
}

// Close closes the SQL Server connection
func (m *MSSQL) Close() {
	if m.db != nil {
		m.db.Close()
	}
}

// GetTableSchema returns the schema of a table
func (m *MSSQL) GetTableSchema(tableName string) ([]Column, error) {
	query := `
		SELECT 
			c.name AS column_name,
			t.name AS data_type,
			c.is_nullable
		FROM sys.columns c
		INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
		WHERE object_id = OBJECT_ID(@p1)
		ORDER BY c.column_id
	`

	rows, err := m.db.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table schema: %v", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		var nullable bool
		if err := rows.Scan(&col.Name, &col.Type, &nullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		col.Nullable = nullable
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %v", err)
	}

	return columns, nil
}

// Query executes a query and returns the results
func (m *MSSQL) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	return m.db.QueryContext(ctx, query, args...)
}

// Exec executes a query without returning results
func (m *MSSQL) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	return m.db.ExecContext(ctx, query, args...)
}

// GetTotalRows returns the total number of rows in a table
func (m *MSSQL) GetTotalRows(table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var count int64
	err := m.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total rows: %v", err)
	}
	return count, nil
}

// GetColumns returns the columns of a table
func (m *MSSQL) GetColumns(table string) ([]string, error) {
	query := `
		SELECT c.name
		FROM sys.columns c
		WHERE object_id = OBJECT_ID(@p1)
		ORDER BY c.column_id
	`

	rows, err := m.db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %v", err)
	}

	return columns, nil
}

// ExtractBatch extracts a batch of rows from a table
func (m *MSSQL) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	// Build query
	query := fmt.Sprintf("SELECT * FROM %s", table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if keyColumns != "" {
		query += " ORDER BY " + keyColumns
	}
	query += fmt.Sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit)

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	// Prepare result slice
	var result []map[string]interface{}
	for rows.Next() {
		// Create slice of pointers to values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan row into values
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		// Convert values to map
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

// CreateDatabase creates the database if it doesn't exist
func (m *MSSQL) CreateDatabase() error {
	query := fmt.Sprintf("IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '%s') CREATE DATABASE %s", m.config.Database, m.config.Database)
	_, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create database: %v", err)
	}
	return nil
}

// DropDatabase drops the database if it exists
func (m *MSSQL) DropDatabase() error {
	query := fmt.Sprintf("IF EXISTS (SELECT * FROM sys.databases WHERE name = '%s') DROP DATABASE %s", m.config.Database, m.config.Database)
	_, err := m.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to drop database: %v", err)
	}
	return nil
}

// GetPrimaryKey returns the primary key column of a table
func (m *MSSQL) GetPrimaryKey(ctx context.Context, table string) (string, error) {
	query := `
		SELECT c.name
		FROM sys.index_columns ic
		INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		WHERE i.is_primary_key = 1
		AND OBJECT_NAME(ic.object_id) = @p1
		ORDER BY ic.key_ordinal
		LIMIT 1
	`

	var pk string
	err := m.db.QueryRowContext(ctx, query, table).Scan(&pk)
	if err != nil {
		return "", fmt.Errorf("error getting primary key: %v", err)
	}

	return pk, nil
}

// Helper function to get primary key without context
func (m *MSSQL) getPrimaryKey(table string) string {
	pk, err := m.GetPrimaryKey(context.Background(), table)
	if err != nil {
		// Default to first column if we can't determine the primary key
		return "1"
	}
	return pk
}
