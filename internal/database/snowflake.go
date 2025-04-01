package database

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
	_ "github.com/snowflakedb/gosnowflake"
)

type SnowflakeDB struct {
	db           *sql.DB
	config       *config.Config
	stateManager state.Manager
}

func NewSnowflake(cfg *config.Config, stateManager state.Manager) (Database, error) {
	connStr := fmt.Sprintf(
		"%s:%s@%s/%s/%s?warehouse=%s",
		cfg.Username, cfg.Password,
		cfg.Host, cfg.Database, cfg.Schema,
		cfg.Warehouse,
	)

	db, err := sql.Open("snowflake", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	return &SnowflakeDB{
		db:           db,
		config:       cfg,
		stateManager: stateManager,
	}, nil
}

func (db *SnowflakeDB) ExtractData(table, outputFile, format string, batchSize int) error {
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

	// Get primary key columns
	pkColumns, err := db.getPrimaryKeyColumns(table)
	if err != nil {
		return fmt.Errorf("failed to get primary key columns: %v", err)
	}

	// Build query
	query := fmt.Sprintf("SELECT * FROM %s", table)
	if len(pkColumns) > 0 {
		query += " ORDER BY " + strings.Join(pkColumns, ", ")
	}

	// Execute query
	rows, err := db.db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	// Write header
	if format == "csv" {
		if _, err := fmt.Fprintf(file, "%s\n", strings.Join(columns, ",")); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Scan rows
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	processedRows := int64(0)
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("failed to scan row: %v", err)
		}

		// Write row
		if format == "csv" {
			rowValues := make([]string, len(columns))
			for i, v := range values {
				if v == nil {
					rowValues[i] = "NULL"
				} else {
					rowValues[i] = fmt.Sprintf("%v", v)
				}
			}
			if _, err := fmt.Fprintf(file, "%s\n", strings.Join(rowValues, ",")); err != nil {
				return fmt.Errorf("failed to write row: %v", err)
			}
		}

		processedRows++
		if processedRows%int64(batchSize) == 0 {
			// Update state
			if err := db.stateManager.UpdateState(table, processedRows); err != nil {
				return fmt.Errorf("failed to update state: %v", err)
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %v", err)
	}

	// Update final state
	if err := db.stateManager.UpdateState(table, processedRows); err != nil {
		return fmt.Errorf("failed to update final state: %v", err)
	}

	return nil
}

func (db *SnowflakeDB) getPrimaryKeyColumns(table string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = $1
		AND is_identity = 'YES'
		ORDER BY ordinal_position;
	`

	rows, err := db.db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %v", err)
		}
		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating primary key columns: %v", err)
	}

	return columns, nil
}
