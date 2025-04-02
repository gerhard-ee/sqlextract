package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

type DatabricksDB struct {
	config       *config.Config
	db           *sql.DB
	stateManager state.Manager
}

func NewDatabricks(cfg *config.Config, stateManager state.Manager) (Database, error) {
	db := &DatabricksDB{
		config:       cfg,
		stateManager: stateManager,
	}
	if err := db.Connect(); err != nil {
		return nil, fmt.Errorf("failed to connect to Databricks: %v", err)
	}
	return db, nil
}

func (db *DatabricksDB) Connect() error {
	// Databricks connection string format:
	// "databricks://token:<access_token>@<host>:443/default?catalog=<catalog>&schema=<schema>"
	connStr := fmt.Sprintf("databricks://token:%s@%s:443/%s?catalog=%s&schema=%s",
		db.config.Password, // Using Password field for access token
		db.config.Host,
		db.config.Database,
		db.config.Catalog,
		db.config.Schema)

	var err error
	db.db, err = sql.Open("databricks", connStr)
	if err != nil {
		return fmt.Errorf("failed to open connection: %v", err)
	}

	return nil
}

func (db *DatabricksDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func (db *DatabricksDB) ExtractData(table, outputFile, format string, batchSize int) error {
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
		rows, err := db.ExtractBatch(table, offset, int64(batchSize))
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

func (db *DatabricksDB) GetTotalRows(table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var count int64
	err := db.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}

func (db *DatabricksDB) GetColumns(table string) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position", table)
	rows, err := db.db.Query(query)
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

func (db *DatabricksDB) ExtractBatch(table string, offset, limit int64) ([]map[string]interface{}, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table, limit, offset)
	rows, err := db.db.Query(query)
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
