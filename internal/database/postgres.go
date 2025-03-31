package database

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
)

type PostgresDB struct {
	config *Config
	db     *sql.DB
}

func NewPostgresDB(config *Config) *PostgresDB {
	return &PostgresDB{
		config: config,
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

func (db *PostgresDB) ExtractData(tableName string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	query := fmt.Sprintf("SELECT %s FROM %s.%s LIMIT %d OFFSET %d",
		strings.Join(columnNames, ", "),
		db.config.Schema,
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

func (db *PostgresDB) GetRowCount(tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", db.config.Schema, tableName)
	var count int64
	err := db.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}
