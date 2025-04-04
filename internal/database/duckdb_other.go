//go:build !darwin
// +build !darwin

package database

import (
	"context"
	"fmt"
)

func (db *DuckDB) Connect() error {
	return fmt.Errorf("DuckDB support is only available on macOS")
}

func (db *DuckDB) Close() error {
	return nil
}

func (db *DuckDB) ExtractData(table, outputFile, format string, batchSize int, keyColumns, whereClause string) error {
	return fmt.Errorf("DuckDB support is only available on macOS")
}

func (db *DuckDB) GetTotalRows(table string) (int64, error) {
	return 0, fmt.Errorf("DuckDB support is only available on macOS")
}

func (db *DuckDB) GetColumns(table string) ([]string, error) {
	return nil, fmt.Errorf("DuckDB support is only available on macOS")
}

func (db *DuckDB) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("DuckDB support is only available on macOS")
}

func (db *DuckDB) GetTableSchema(tableName string) ([]Column, error) {
	return nil, fmt.Errorf("DuckDB support is only available on macOS")
}

func (db *DuckDB) GetRowCount(tableName string) (int64, error) {
	return 0, fmt.Errorf("DuckDB support is only available on macOS")
}

func (db *DuckDB) Exec(ctx context.Context, query string) error {
	return fmt.Errorf("DuckDB support is only available on macOS")
}
