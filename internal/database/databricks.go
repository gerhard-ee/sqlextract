package database

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/databricks/databricks-sql-go"
)

type DatabricksDB struct {
	config *Config
	db     *sql.DB
}

func NewDatabricks(config *Config) *DatabricksDB {
	return &DatabricksDB{
		config: config,
	}
}

func (db *DatabricksDB) Connect() error {
	// Databricks connection string format:
	// "databricks://token:<access_token>@<host>:443/default?catalog=<catalog>&schema=<schema>"
	connStr := fmt.Sprintf("databricks://token:%s@%s:443/%s?catalog=%s&schema=%s",
		db.config.Password, // Using Password field for access token
		db.config.Host,
		db.config.Database,
		db.config.Catalog,
		db.config.Schema)

	conn, err := sql.Open("databricks", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to Databricks: %v", err)
	}

	db.db = conn
	return nil
}

func (db *DatabricksDB) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func (db *DatabricksDB) GetTableSchema(tableName string) ([]Column, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s", tableName)
	rows, err := db.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schema: %v", err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var colName, colType, comment sql.NullString
		err := rows.Scan(&colName, &colType, &comment)
		if err != nil {
			return nil, fmt.Errorf("failed to scan schema row: %v", err)
		}

		columns = append(columns, Column{
			Name: colName.String,
			Type: colType.String,
		})
	}

	return columns, nil
}

func (db *DatabricksDB) ExtractData(tableName string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
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

func (db *DatabricksDB) GetRowCount(tableName string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	var count int64
	err := db.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}
	return count, nil
}
