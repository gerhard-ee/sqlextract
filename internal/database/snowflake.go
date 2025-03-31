package database

import (
	"database/sql"
	"fmt"
	"strings"

	sf "github.com/snowflakedb/gosnowflake"
)

type SnowflakeDB struct {
	config *Config
	db     *sql.DB
}

func NewSnowflake(config *Config) *SnowflakeDB {
	return &SnowflakeDB{
		config: config,
	}
}

func (db *SnowflakeDB) Connect() error {
	dsn, err := sf.DSN(&sf.Config{
		Account:   db.config.Host,
		User:      db.config.Username,
		Password:  db.config.Password,
		Database:  db.config.Database,
		Warehouse: db.config.Warehouse,
		Schema:    db.config.Schema,
	})
	if err != nil {
		return fmt.Errorf("failed to create Snowflake DSN: %v", err)
	}

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

func (db *SnowflakeDB) ExtractData(tableName string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
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

func (db *SnowflakeDB) GetRowCount(tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int64
	err := db.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}
