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
	_ "github.com/microsoft/go-mssqldb"
)

type MSSQLDB struct {
	db           *sql.DB
	config       *config.Config
	stateManager state.Manager
}

func NewMSSQL(cfg *config.Config, stateManager state.Manager) (Database, error) {
	connStr := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=%s",
		cfg.Username, cfg.Password,
		cfg.Host, cfg.Port, cfg.Database,
	)

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %v", err)
	}

	return &MSSQLDB{
		db:           db,
		config:       cfg,
		stateManager: stateManager,
	}, nil
}

func (db *MSSQLDB) ExtractData(table, outputFile, format string, batchSize int) error {
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

func (db *MSSQLDB) GetTotalRows(table string) (int64, error) {
	// Try to get an exact count first
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	var count int64
	err := db.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total rows: %v", err)
	}

	// If the count is too large, use an approximate count
	if count > 1000000 {
		query = fmt.Sprintf(`
			SELECT SUM(row_count)
			FROM sys.dm_db_partition_stats
			WHERE object_id = OBJECT_ID(@p1)
			AND index_id < 2
		`)
		err = db.db.QueryRow(query, sql.Named("p1", table)).Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to get approximate row count: %v", err)
		}
	}

	return count, nil
}

func (db *MSSQLDB) GetColumns(table string) ([]string, error) {
	query := `
		SELECT column_name
		FROM information_schema.columns
		WHERE table_name = @p1
		ORDER BY ordinal_position;
	`

	rows, err := db.db.Query(query, sql.Named("p1", table))
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %v", err)
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

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %v", err)
	}

	return columns, nil
}

func (db *MSSQLDB) ExtractBatch(table string, offset, limit int64) ([]map[string]interface{}, error) {
	// Get primary key columns for ordering
	pkColumns, err := db.getPrimaryKeyColumns(table)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key columns: %v", err)
	}

	// Build query
	query := fmt.Sprintf("SELECT * FROM %s", table)
	if len(pkColumns) > 0 {
		query += " ORDER BY " + strings.Join(pkColumns, ", ")
	}
	query += fmt.Sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit)

	// Execute query
	rows, err := db.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	// Scan rows
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	var result []map[string]interface{}
	for rows.Next() {
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		row := make(map[string]interface{})
		for i, v := range values {
			row[columns[i]] = v
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return result, nil
}

func (db *MSSQLDB) getPrimaryKeyColumns(table string) ([]string, error) {
	query := `
		SELECT c.name
		FROM sys.indexes i
		INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id
			AND i.index_id = ic.index_id
		INNER JOIN sys.columns c ON ic.object_id = c.object_id
			AND ic.column_id = c.column_id
		WHERE i.is_primary_key = 1
			AND i.object_id = OBJECT_ID(@p1)
		ORDER BY ic.key_ordinal;
	`

	rows, err := db.db.Query(query, sql.Named("p1", table))
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
