package database

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

type DuckDB struct {
	config *Config
	db     *sql.DB
}

func NewDuckDB(config *Config) *DuckDB {
	return &DuckDB{
		config: config,
	}
}

func (db *DuckDB) Connect() error {
	conn, err := sql.Open("duckdb", db.config.Database)
	if err != nil {
		return fmt.Errorf("failed to connect to DuckDB: %v", err)
	}

	db.db = conn
	return nil
}

func (db *DuckDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func (db *DuckDB) GetTableSchema(tableName string) ([]Column, error) {
	query := fmt.Sprintf(`
		SELECT column_name, data_type
		FROM information_schema.columns
		WHERE table_name = $1
		ORDER BY ordinal_position
	`)

	rows, err := db.db.Query(query, tableName)
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

func (db *DuckDB) ExtractData(tableName string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d",
		strings.Join(columnNames, ", "),
		tableName,
		batchSize,
		offset)

	rows, err := db.db.Query(query)
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

		err := rows.Scan(scanArgs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}

		result = append(result, values)
	}

	return result, nil
}

func (db *DuckDB) GetRowCount(tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int64
	err := db.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}
