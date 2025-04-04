//go:build darwin && !release
// +build darwin,!release

package database

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gerhard-ee/sqlextract/internal/state"
	_ "github.com/marcboeker/go-duckdb"
)

func (db *DuckDB) Connect() error {
	conn, err := sql.Open("duckdb", db.config.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to DuckDB: %v", err)
	}

	db.db = conn
	return nil
}

func (db *DuckDB) Close() error {
	if sqlDB, ok := db.db.(*sql.DB); ok && sqlDB != nil {
		return sqlDB.Close()
	}
	return nil
}

func (db *DuckDB) ExtractData(table, outputFile, format string, batchSize int, keyColumns, whereClause string) error {
	sqlDB, ok := db.db.(*sql.DB)
	if !ok || sqlDB == nil {
		return fmt.Errorf("database connection not initialized")
	}

	// Get current state
	currentState, err := db.stateManager.GetState(table)
	if err != nil {
		// Create new state if it doesn't exist
		currentState = &state.State{
			Table:       table,
			LastUpdated: time.Now(),
			Status:      "running",
		}
		if err := db.stateManager.CreateState(currentState); err != nil {
			return fmt.Errorf("failed to create state: %v", err)
		}
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Get total rows
	totalRows, err := db.GetTotalRows(table)
	if err != nil {
		return fmt.Errorf("failed to get total rows: %v", err)
	}

	// Get columns
	columns, err := db.GetColumns(table)
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	// Write header if CSV format
	if format == "csv" {
		if _, err := fmt.Fprintf(file, "%s\n", strings.Join(columns, ",")); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Process data in batches
	processedRows := int64(0)
	for offset := int64(0); offset < totalRows; offset += int64(batchSize) {
		rows, err := db.ExtractBatch(table, offset, int64(batchSize), keyColumns, whereClause)
		if err != nil {
			return fmt.Errorf("failed to extract batch: %v", err)
		}

		// Write rows
		for _, row := range rows {
			if format == "csv" {
				values := make([]string, len(columns))
				for i, col := range columns {
					if val := row[col]; val == nil {
						values[i] = "NULL"
					} else {
						values[i] = fmt.Sprintf("%v", val)
					}
				}
				if _, err := fmt.Fprintf(file, "%s\n", strings.Join(values, ",")); err != nil {
					return fmt.Errorf("failed to write row: %v", err)
				}
			}
			processedRows++
		}

		// Update state
		if err := db.stateManager.UpdateState(table, processedRows); err != nil {
			return fmt.Errorf("failed to update state: %v", err)
		}
	}

	return nil
}

func (db *DuckDB) GetTotalRows(table string) (int64, error) {
	sqlDB, ok := db.db.(*sql.DB)
	if !ok || sqlDB == nil {
		return 0, fmt.Errorf("database connection not initialized")
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var count int64
	err := sqlDB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}

func (db *DuckDB) GetColumns(table string) ([]string, error) {
	sqlDB, ok := db.db.(*sql.DB)
	if !ok || sqlDB == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position", table)
	rows, err := sqlDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		columns = append(columns, column)
	}
	return columns, nil
}

func (db *DuckDB) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	sqlDB, ok := db.db.(*sql.DB)
	if !ok || sqlDB == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	// Build the query with WHERE clause and ORDER BY if key columns are provided
	query := fmt.Sprintf("SELECT * FROM %s", table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if keyColumns != "" {
		query += " ORDER BY " + keyColumns
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	rows, err := sqlDB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
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

	return result, nil
}

func (db *DuckDB) GetTableSchema(tableName string) ([]Column, error) {
	sqlDB, ok := db.db.(*sql.DB)
	if !ok || sqlDB == nil {
		return nil, fmt.Errorf("database connection not initialized")
	}

	query := fmt.Sprintf(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`)

	rows, err := sqlDB.Query(query, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		err := rows.Scan(&col.Name, &col.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to scan schema row: %v", err)
		}
		columns = append(columns, col)
	}

	return columns, nil
}

func (db *DuckDB) GetRowCount(tableName string) (int64, error) {
	sqlDB, ok := db.db.(*sql.DB)
	if !ok || sqlDB == nil {
		return 0, fmt.Errorf("database connection not initialized")
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int64
	err := sqlDB.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}

func (db *DuckDB) Exec(ctx context.Context, query string) error {
	sqlDB, ok := db.db.(*sql.DB)
	if !ok || sqlDB == nil {
		return fmt.Errorf("database connection not initialized")
	}

	_, err := sqlDB.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	return nil
}
