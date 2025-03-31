package database

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/denisenkom/go-mssqldb"
)

type MSSQL struct {
	config *Config
	db     *sql.DB
}

func NewMSSQL(config *Config) Database {
	return &MSSQL{
		config: config,
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

func (m *MSSQL) ExtractData(table string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
	var columnNames []string
	for _, col := range columns {
		columnNames = append(columnNames, col.Name)
	}

	var query string
	if m.config.Schema != "" {
		query = fmt.Sprintf(`
			SELECT %s
			FROM %s.%s
			ORDER BY (SELECT NULL)
			OFFSET %d ROWS
			FETCH NEXT %d ROWS ONLY
		`, strings.Join(columnNames, ", "), m.config.Schema, table, offset, batchSize)
	} else {
		query = fmt.Sprintf(`
			SELECT %s
			FROM %s
			ORDER BY (SELECT NULL)
			OFFSET %d ROWS
			FETCH NEXT %d ROWS ONLY
		`, strings.Join(columnNames, ", "), table, offset, batchSize)
	}

	rows, err := m.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	defer rows.Close()

	var result [][]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		result = append(result, values)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return result, nil
}
