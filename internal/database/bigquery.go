package database

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

type BigQueryDB struct {
	config *Config
	client *bigquery.Client
}

func NewBigQueryDB(config *Config) *BigQueryDB {
	return &BigQueryDB{
		config: config,
	}
}

func (db *BigQueryDB) Connect() error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, db.config.ProjectID, option.WithCredentialsFile(db.config.CredentialsFile))
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	db.client = client
	return nil
}

func (db *BigQueryDB) Close() error {
	if db.client != nil {
		return db.client.Close()
	}
	return nil
}

func (db *BigQueryDB) GetTableSchema(tableName string) ([]Column, error) {
	ctx := context.Background()
	tableRef := db.client.Dataset(db.config.Database).Table(tableName)
	table, err := tableRef.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	var columns []Column
	for _, field := range table.Schema {
		columns = append(columns, Column{
			Name: field.Name,
			Type: fieldTypeToString(field.Type),
		})
	}

	return columns, nil
}

func (db *BigQueryDB) ExtractData(tableName string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
	ctx := context.Background()
	tableRef := db.client.Dataset(db.config.Database).Table(tableName)

	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	query := fmt.Sprintf("SELECT %s FROM %s LIMIT %d OFFSET %d",
		strings.Join(columnNames, ", "),
		tableRef.FullyQualifiedName(),
		batchSize,
		offset)

	it, err := db.client.Query(query).Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	var result [][]interface{}
	for {
		var row []interface{}
		err := it.Next(&row)
		if err != nil {
			break
		}
		result = append(result, row)
	}

	return result, nil
}

func (db *BigQueryDB) GetRowCount(tableName string) (int64, error) {
	ctx := context.Background()
	tableRef := db.client.Dataset(db.config.Database).Table(tableName)

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableRef.FullyQualifiedName())
	it, err := db.client.Query(query).Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to execute count query: %v", err)
	}

	var count int64
	err = it.Next(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get row count: %v", err)
	}

	return count, nil
}

// Helper function to convert BigQuery field type to string
func fieldTypeToString(fieldType bigquery.FieldType) string {
	switch fieldType {
	case bigquery.StringFieldType:
		return "STRING"
	case bigquery.IntegerFieldType:
		return "INTEGER"
	case bigquery.FloatFieldType:
		return "FLOAT"
	case bigquery.BooleanFieldType:
		return "BOOLEAN"
	case bigquery.TimestampFieldType:
		return "TIMESTAMP"
	case bigquery.RecordFieldType:
		return "RECORD"
	case bigquery.DateFieldType:
		return "DATE"
	case bigquery.TimeFieldType:
		return "TIME"
	case bigquery.DateTimeFieldType:
		return "DATETIME"
	case bigquery.NumericFieldType:
		return "NUMERIC"
	case bigquery.BytesFieldType:
		return "BYTES"
	case bigquery.GeographyFieldType:
		return "GEOGRAPHY"
	default:
		return "UNKNOWN"
	}
}
