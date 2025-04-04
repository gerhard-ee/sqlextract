//go:build !release
// +build !release

package database

import (
	"context"
	"fmt"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

// DuckDB is the interface for DuckDB operations
type DuckDB interface {
	Database
}

// duckDBBase is the base implementation of DuckDB
type duckDBBase struct {
	config       *config.Config
	stateManager state.Manager
}

// newDuckDBBase creates a new base DuckDB instance
func newDuckDBBase(cfg *config.Config, stateManager state.Manager) (*duckDBBase, error) {
	return &duckDBBase{
		config:       cfg,
		stateManager: stateManager,
	}, nil
}

// Connect establishes a connection to the database
func (d *duckDBBase) Connect() error {
	return fmt.Errorf("not implemented")
}

// Close closes the database connection
func (d *duckDBBase) Close() error {
	return nil
}

// ExtractData extracts data from a table and writes it to a file
func (d *duckDBBase) ExtractData(table, outputFile, format string, batchSize int, keyColumns, whereClause string) error {
	return fmt.Errorf("not implemented")
}

// GetTotalRows returns the total number of rows in a table
func (d *duckDBBase) GetTotalRows(table string) (int64, error) {
	return 0, fmt.Errorf("not implemented")
}

// GetColumns returns the column names for a table
func (d *duckDBBase) GetColumns(table string) ([]string, error) {
	return nil, fmt.Errorf("not implemented")
}

// ExtractBatch extracts a batch of rows from a table
func (d *duckDBBase) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}

// Exec executes a SQL query
func (d *duckDBBase) Exec(ctx context.Context, query string) error {
	return fmt.Errorf("not implemented")
}
