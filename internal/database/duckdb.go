//go:build !test
// +build !test

package database

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDB struct {
	config       *config.Config
	db           *sql.DB
	stateManager state.Manager
}

func NewDuckDB(cfg *config.Config, stateManager state.Manager) (Database, error) {
	return nil, fmt.Errorf("DuckDB support is only available in test builds")
}

func (db *DuckDB) Connect() error {
	return fmt.Errorf("DuckDB support is only available in test builds")
}

func (db *DuckDB) Close() error {
	return nil
}

func (db *DuckDB) ExtractData(table, outputFile, format string, batchSize int, keyColumns, whereClause string) error {
	return fmt.Errorf("DuckDB support is only available in test builds")
}

func (db *DuckDB) GetTotalRows(table string) (int64, error) {
	return 0, fmt.Errorf("DuckDB support is only available in test builds")
}

func (db *DuckDB) GetColumns(table string) ([]string, error) {
	return nil, fmt.Errorf("DuckDB support is only available in test builds")
}

func (db *DuckDB) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("DuckDB support is only available in test builds")
}

func (db *DuckDB) GetTableSchema(tableName string) ([]Column, error) {
	return nil, fmt.Errorf("DuckDB support is only available in test builds")
}

func (db *DuckDB) GetRowCount(tableName string) (int64, error) {
	return 0, fmt.Errorf("DuckDB support is only available in test builds")
}

func (db *DuckDB) Exec(ctx context.Context, query string) error {
	return fmt.Errorf("DuckDB support is only available in test builds")
}
