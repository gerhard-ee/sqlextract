package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/gerhardlazu/sqlextract/internal/state"
)

type MSSQL struct {
	config *Config
	db     *sql.DB
	state  state.Manager
}

func NewMSSQL(config *Config, stateManager state.Manager) Database {
	return &MSSQL{
		config: config,
		state:  stateManager,
	}
}

func (m *MSSQL) Connect() error {
	connStr := fmt.Sprintf("server=%s;port=%d;user id=%s;password=%s;database=%s",
		m.config.Host,
		m.config.Port,
		m.config.Username,
		m.config.Password,
		m.config.Database)

	db, err := sql.Open("mssql", connStr)
	if err != nil {
		return fmt.Errorf("failed to open connection: %v", err)
	}

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %v", err)
	}

	m.db = db
	return nil
}

func (m *MSSQL) Close() error {
	if m.db != nil {
		return m.db.Close()
	}
	return nil
}

func (m *MSSQL) GetTableSchema(table string) ([]Column, error) {
	query := `
		SELECT 
			COLUMN_NAME,
			DATA_TYPE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = @p1
		ORDER BY ORDINAL_POSITION
	`
	if m.config.Schema != "" {
		query = `
			SELECT 
				COLUMN_NAME,
				DATA_TYPE
			FROM INFORMATION_SCHEMA.COLUMNS
			WHERE TABLE_SCHEMA = @p1 AND TABLE_NAME = @p2
			ORDER BY ORDINAL_POSITION
		`
	}

	var rows *sql.Rows
	var err error
	if m.config.Schema != "" {
		rows, err = m.db.Query(query, m.config.Schema, table)
	} else {
		rows, err = m.db.Query(query, table)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var col Column
		if err := rows.Scan(&col.Name, &col.Type); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return columns, nil
}

func (m *MSSQL) GetRowCount(table string) (int64, error) {
	var query string
	if m.config.Schema != "" {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", m.config.Schema, table)
	} else {
		query = fmt.Sprintf("SELECT COUNT(*) FROM %s", table)
	}

	var count int64
	if err := m.db.QueryRow(query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}

	return count, nil
}

func (m *MSSQL) ExtractData(ctx context.Context, table string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
	// Get or create state for this extraction
	jobID := fmt.Sprintf("%s-%s-%d", m.config.Database, table, time.Now().UnixNano())
	state, err := m.state.GetState(ctx, jobID)
	if err != nil {
		return nil, fmt.Errorf("failed to get state: %v", err)
	}

	if state == nil {
		// Create new state
		state = &state.State{
			JobID:       jobID,
			Table:       table,
			LastOffset:  offset,
			LastUpdated: time.Now(),
			Status:      "running",
		}
		if err := m.state.CreateState(ctx, state); err != nil {
			return nil, fmt.Errorf("failed to create state: %v", err)
		}
	}

	// Try to acquire lock
	locked, err := m.state.LockState(ctx, jobID, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %v", err)
	}
	if !locked {
		return nil, fmt.Errorf("another process is already running this extraction")
	}
	defer m.state.UnlockState(ctx, jobID)

	// Get primary key columns for ordering
	pkColumns, err := m.getPrimaryKeyColumns(table)
	if err != nil {
		state.Status = "failed"
		state.Error = err.Error()
		m.state.UpdateState(ctx, state)
		return nil, fmt.Errorf("failed to get primary key columns: %v", err)
	}

	// Build column list for SELECT
	var columnNames []string
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
	}

	// Build WHERE clause for keyset pagination
	var whereClause string
	var args []interface{}
	if state.LastValues != nil {
		// Use last values from state for keyset pagination
		whereClause = "WHERE "
		for i, col := range pkColumns {
			if i > 0 {
				whereClause += " OR ("
				for j := 0; j < i; j++ {
					whereClause += fmt.Sprintf("%s = @p%d AND ", pkColumns[j], j+1)
				}
				whereClause += fmt.Sprintf("%s > @p%d)", col, i+1)
			} else {
				whereClause += fmt.Sprintf("%s > @p%d", col, i+1)
			}
			args = append(args, state.LastValues[i])
		}
	}

	// Build ORDER BY clause
	orderBy := "ORDER BY " + strings.Join(pkColumns, ", ")

	// Build the complete query
	var query string
	if m.config.Schema != "" {
		query = fmt.Sprintf(`
			SELECT %s
			FROM %s.%s
			%s
			%s
			FETCH NEXT %d ROWS ONLY
		`, strings.Join(columnNames, ", "), m.config.Schema, table, whereClause, orderBy, batchSize)
	} else {
		query = fmt.Sprintf(`
			SELECT %s
			FROM %s
			%s
			%s
			FETCH NEXT %d ROWS ONLY
		`, strings.Join(columnNames, ", "), table, whereClause, orderBy, batchSize)
	}

	// Execute query
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		state.Status = "failed"
		state.Error = err.Error()
		m.state.UpdateState(ctx, state)
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
			state.Status = "failed"
			state.Error = err.Error()
			m.state.UpdateState(ctx, state)
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		result = append(result, values)
		lastValues = values
	}

	if err := rows.Err(); err != nil {
		state.Status = "failed"
		state.Error = err.Error()
		m.state.UpdateState(ctx, state)
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	// Update state with last values
	state.LastValues = lastValues
	state.LastUpdated = time.Now()
	state.ProcessedRows += int64(len(result))
	if err := m.state.UpdateState(ctx, state); err != nil {
		return nil, fmt.Errorf("failed to update state: %v", err)
	}

	return result, nil
}

// getPrimaryKeyColumns returns the primary key columns of a table
func (m *MSSQL) getPrimaryKeyColumns(table string) ([]string, error) {
	query := `
		SELECT c.name
		FROM sys.index_columns ic
		INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
		INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		WHERE i.is_primary_key = 1
		AND OBJECT_NAME(ic.object_id) = @p1
		ORDER BY ic.key_ordinal
	`

	rows, err := m.db.Query(query, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key columns: %v", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to scan column: %v", err)
		}
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %v", err)
	}

	if len(columns) == 0 {
		// If no primary key found, use the first column as a fallback
		query = `
			SELECT name
			FROM sys.columns
			WHERE object_id = OBJECT_ID(@p1)
			ORDER BY column_id
			LIMIT 1
		`
		var col string
		if err := m.db.QueryRow(query, table).Scan(&col); err != nil {
			return nil, fmt.Errorf("failed to get fallback column: %v", err)
		}
		columns = []string{col}
	}

	return columns, nil
}

// getLastRowValues returns the values of the primary key columns for the last row in the previous batch
func (m *MSSQL) getLastRowValues(table string, pkColumns []string, offset int64) ([]interface{}, error) {
	query := fmt.Sprintf(`
		SELECT %s
		FROM %s
		ORDER BY %s
		OFFSET %d ROWS
		FETCH NEXT 1 ROWS ONLY
	`, strings.Join(pkColumns, ", "), table, strings.Join(pkColumns, ", "), offset-1)

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get last row values: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, fmt.Errorf("no rows found at offset %d", offset-1)
	}

	values := make([]interface{}, len(pkColumns))
	scanArgs := make([]interface{}, len(pkColumns))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	if err := rows.Scan(scanArgs...); err != nil {
		return nil, fmt.Errorf("failed to scan last row values: %v", err)
	}

	return values, nil
}
