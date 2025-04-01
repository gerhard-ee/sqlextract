package database

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/gerhardlazu/sqlextract/internal/state"
	_ "github.com/lib/pq"
)

type PostgresDB struct {
	config *Config
	db     *sql.DB
	state  state.Manager
}

func NewPostgresDB(config *Config, stateManager state.Manager) Database {
	return &PostgresDB{
		config: config,
		state:  stateManager,
	}
}

func (db *PostgresDB) Connect() error {
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		db.config.Host,
		db.config.Port,
		db.config.Username,
		db.config.Password,
		db.config.Database)

	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	db.db = conn
	return nil
}

func (db *PostgresDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func (db *PostgresDB) GetTableSchema(tableName string) ([]Column, error) {
	query := fmt.Sprintf(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position
	`)

	rows, err := db.db.Query(query, db.config.Schema, tableName)
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

func (p *PostgresDB) ExtractData(ctx context.Context, table string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
	// Get or create state for this extraction
	jobID := fmt.Sprintf("%s-%s-%d", p.config.Database, table, time.Now().UnixNano())
	state, err := p.state.GetState(ctx, jobID)
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
		if err := p.state.CreateState(ctx, state); err != nil {
			return nil, fmt.Errorf("failed to create state: %v", err)
		}
	}

	// Try to acquire lock
	locked, err := p.state.LockState(ctx, jobID, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %v", err)
	}
	if !locked {
		return nil, fmt.Errorf("another process is already running this extraction")
	}
	defer p.state.UnlockState(ctx, jobID)

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
		for i, col := range columns {
			if i > 0 {
				whereClause += " OR ("
				for j := 0; j < i; j++ {
					whereClause += fmt.Sprintf("%s = $%d AND ", columns[j].Name, j+1)
				}
				whereClause += fmt.Sprintf("%s > $%d)", col.Name, i+1)
			} else {
				whereClause += fmt.Sprintf("%s > $%d", col.Name, i+1)
			}
			args = append(args, state.LastValues[i])
		}
	}

	// Build ORDER BY clause
	orderBy := "ORDER BY " + strings.Join(columnNames, ", ")

	// Build the complete query
	var query string
	if p.config.Schema != "" {
		query = fmt.Sprintf(`
			SELECT %s
			FROM %s.%s
			%s
			%s
			LIMIT %d
		`, strings.Join(columnNames, ", "), p.config.Schema, table, whereClause, orderBy, batchSize)
	} else {
		query = fmt.Sprintf(`
			SELECT %s
			FROM %s
			%s
			%s
			LIMIT %d
		`, strings.Join(columnNames, ", "), table, whereClause, orderBy, batchSize)
	}

	// Execute query
	rows, err := p.db.QueryContext(ctx, query, args...)
	if err != nil {
		state.Status = "failed"
		state.Error = err.Error()
		p.state.UpdateState(ctx, state)
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
			p.state.UpdateState(ctx, state)
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		result = append(result, values)
		lastValues = values
	}

	if err := rows.Err(); err != nil {
		state.Status = "failed"
		state.Error = err.Error()
		p.state.UpdateState(ctx, state)
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	// Update state with last values
	state.LastValues = lastValues
	state.LastUpdated = time.Now()
	state.ProcessedRows += int64(len(result))
	if err := p.state.UpdateState(ctx, state); err != nil {
		return nil, fmt.Errorf("failed to update state: %v", err)
	}

	return result, nil
}

func (db *PostgresDB) GetRowCount(tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", db.config.Schema, tableName)
	var count int64
	err := db.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}
