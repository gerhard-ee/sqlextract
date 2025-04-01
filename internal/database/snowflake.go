package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/gerhard-ee/sqlextract/internal/state"
	_ "github.com/snowflakedb/gosnowflake"
)

type SnowflakeDB struct {
	config *Config
	db     *sql.DB
	state  state.Manager
}

func NewSnowflakeDB(config *Config, stateManager state.Manager) Database {
	return &SnowflakeDB{
		config: config,
		state:  stateManager,
	}
}

func (db *SnowflakeDB) Connect() error {
	dsn := fmt.Sprintf("%s:%s@%s/%s/%s?warehouse=%s",
		db.config.Username,
		db.config.Password,
		db.config.Host,
		db.config.Database,
		db.config.Schema,
		db.config.Warehouse)

	conn, err := sql.Open("snowflake", dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to Snowflake: %v", err)
	}

	db.db = conn
	return nil
}

func (db *SnowflakeDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func (db *SnowflakeDB) GetTableSchema(tableName string) ([]Column, error) {
	query := fmt.Sprintf("DESC TABLE %s", tableName)
	rows, err := db.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var name, dataType, kind, null, default_, primary, unique, comment, expression sql.NullString
		err := rows.Scan(&name, &dataType, &kind, &null, &default_, &primary, &unique, &comment, &expression)
		if err != nil {
			return nil, fmt.Errorf("failed to scan schema row: %v", err)
		}

		columns = append(columns, Column{
			Name: name.String,
			Type: dataType.String,
		})
	}

	return columns, nil
}

func (db *SnowflakeDB) ExtractData(ctx context.Context, table string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
	// Get or create state for this extraction
	jobID := fmt.Sprintf("%s-%s-%d", db.config.Database, table, time.Now().UnixNano())
	currentState, err := db.state.GetState(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get state: %v", err)
	}

	if currentState == nil {
		// Create new state
		currentState = &state.State{
			JobID:       jobID,
			Table:       table,
			LastOffset:  offset,
			LastUpdated: time.Now(),
			Status:      "running",
		}
		if err := db.state.CreateState(ctx, currentState); err != nil {
			return nil, fmt.Errorf("failed to create state: %v", err)
		}
	}

	// Try to acquire lock
	locked, err := db.state.LockState(ctx, jobID, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %v", err)
	}
	if !locked {
		return nil, fmt.Errorf("another process is already running this extraction")
	}
	defer db.state.UnlockState(ctx, jobID)

	// Build column list for SELECT
	var columnNames []string
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
	}

	// Build WHERE clause for keyset pagination
	var whereClause string
	var args []interface{}
	if currentState.LastValues != nil {
		// Use last values from state for keyset pagination
		whereClause = "WHERE "
		for i, col := range columns {
			if i > 0 {
				whereClause += " OR ("
				for j := 0; j < i; j++ {
					whereClause += fmt.Sprintf("%s = ? AND ", columns[j].Name)
				}
				whereClause += fmt.Sprintf("%s > ?)", col.Name)
			} else {
				whereClause += fmt.Sprintf("%s > ?", col.Name)
			}
			args = append(args, currentState.LastValues[i])
		}
	}

	// Build ORDER BY clause
	orderBy := "ORDER BY " + strings.Join(columnNames, ", ")

	// Build the complete query
	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		%s
		%s
		LIMIT %d
	`, strings.Join(columnNames, ", "), table, whereClause, orderBy, batchSize)

	// Execute query
	rows, err := db.db.QueryContext(ctx, query, args...)
	if err != nil {
		currentState.Status = "failed"
		currentState.Error = err.Error()
		db.state.UpdateState(ctx, currentState)
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	var result [][]interface{}
	var lastValues []interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			currentState.Status = "failed"
			currentState.Error = err.Error()
			db.state.UpdateState(ctx, currentState)
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		result = append(result, values)
		lastValues = values
	}

	// Update state with last values
	currentState.LastValues = lastValues
	currentState.LastUpdated = time.Now()
	currentState.ProcessedRows += int64(len(result))
	if err := db.state.UpdateState(ctx, currentState); err != nil {
		return nil, fmt.Errorf("failed to update state: %v", err)
	}

	return result, nil
}

func (db *SnowflakeDB) GetRowCount(tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int64
	err := db.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}
