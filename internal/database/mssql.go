package database

import (
	"context"
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
		cfg.User, cfg.Password,
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

func (db *MSSQLDB) Connect() error {
	connStr := fmt.Sprintf(
		"sqlserver://%s:%s@%s:%d?database=%s",
		db.config.User, db.config.Password,
		db.config.Host, db.config.Port, db.config.Database,
	)

	var err error
	db.db, err = sql.Open("sqlserver", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %v", err)
	}

	if err := db.db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %v", err)
	}

	return nil
}

func (db *MSSQLDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func (db *MSSQLDB) ExtractData(table, outputFile, format string, batchSize int, keyColumns, whereClause string) error {
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

func (db *MSSQLDB) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	// Build query
	query := fmt.Sprintf("SELECT * FROM %s", table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if keyColumns != "" {
		query += " ORDER BY " + keyColumns
	}
	query += fmt.Sprintf(" OFFSET %d ROWS FETCH NEXT %d ROWS ONLY", offset, limit)

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

func (db *MSSQLDB) Exec(ctx context.Context, query string) error {
	_, err := db.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}
	return nil
}
