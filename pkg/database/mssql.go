package database

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
)

// MSSQL implements the Database interface for Microsoft SQL Server
type MSSQL struct {
	config *Config
	db     *sql.DB
}

// NewMSSQL creates a new SQL Server instance
func NewMSSQL(config *Config) *MSSQL {
	return &MSSQL{
		config: config,
	}
}

// Connect establishes a connection to SQL Server
func (m *MSSQL) Connect(ctx context.Context) error {
	dsn := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=%s&schema=%s",
		m.config.User,
		m.config.Password,
		m.config.Host,
		m.config.Port,
		m.config.DBName,
		m.config.Schema,
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
func (m *MSSQL) GetTotalRows(ctx context.Context, table string) (int64, error) {
	var count int64
	err := m.db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total rows: %v", err)
	}
	return count, nil
}

// GetColumns returns the columns of a table
func (m *MSSQL) GetColumns(ctx context.Context, table string) ([]Column, error) {
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

	rows, err := m.db.QueryContext(ctx, query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
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

// ExtractBatch extracts a batch of rows from a table
func (m *MSSQL) ExtractBatch(ctx context.Context, table string, offset, limit int64) (Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT * FROM %s ORDER BY %s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY",
		table, m.getPrimaryKey(table), offset, limit)
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to extract batch: %v", err)
	}
	return rows, nil
}

// CreateDatabase creates the database if it doesn't exist
func (m *MSSQL) CreateDatabase() error {
	// Connect to master database first
	dsn := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=master",
		m.config.User,
		m.config.Password,
		m.config.Host,
		m.config.Port,
	)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("failed to open master database: %v", err)
	}
	defer db.Close()

	// Check if database exists
	var exists bool
	err = db.QueryRow("SELECT CASE WHEN EXISTS (SELECT * FROM sys.databases WHERE name = @p1) THEN 1 ELSE 0 END", m.config.DBName).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %v", err)
	}

	if !exists {
		// Create database
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", m.config.DBName))
		if err != nil {
			return fmt.Errorf("failed to create database: %v", err)
		}
	}

	return nil
}

// DropDatabase drops the database if it exists
func (m *MSSQL) DropDatabase() error {
	// Connect to master database first
	dsn := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=master",
		m.config.User,
		m.config.Password,
		m.config.Host,
		m.config.Port,
	)

	db, err := sql.Open("sqlserver", dsn)
	if err != nil {
		return fmt.Errorf("failed to open master database: %v", err)
	}
	defer db.Close()

	// Terminate all connections to the database
	_, err = db.Exec(fmt.Sprintf(`
		DECLARE @kill varchar(8000) = '';
		SELECT @kill = @kill + 'kill ' + CONVERT(varchar(5), spid) + ';'
		FROM master..sysprocesses
		WHERE dbid = db_id('%s')
		AND spid <> @@SPID;
		EXEC(@kill);`, m.config.DBName))
	if err != nil {
		return fmt.Errorf("failed to terminate database connections: %v", err)
	}

	// Drop database
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", m.config.DBName))
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
