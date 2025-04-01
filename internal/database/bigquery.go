package database

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/gerhardlazu/sqlextract/internal/state"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type BigQueryDB struct {
	config *Config
	client *bigquery.Client
	state  state.Manager
}

func NewBigQueryDB(config *Config, stateManager state.Manager) Database {
	return &BigQueryDB{
		config: config,
		state:  stateManager,
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

func (b *BigQueryDB) ExtractData(ctx context.Context, table string, columns []Column, batchSize int, offset int64) ([][]interface{}, error) {
	// Get or create state for this extraction
	jobID := fmt.Sprintf("%s-%s-%d", b.config.Database, table, time.Now().UnixNano())
	currentState, err := b.state.GetState(ctx, jobID)
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
		if err := b.state.CreateState(ctx, currentState); err != nil {
			return nil, fmt.Errorf("failed to create state: %v", err)
		}
	}

	// Try to acquire lock
	locked, err := b.state.LockState(ctx, jobID, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %v", err)
	}
	if !locked {
		return nil, fmt.Errorf("another process is already running this extraction")
	}
	defer b.state.UnlockState(ctx, jobID)

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
					whereClause += fmt.Sprintf("%s = @p%d AND ", columns[j].Name, j+1)
				}
				whereClause += fmt.Sprintf("%s > @p%d)", col.Name, i+1)
			} else {
				whereClause += fmt.Sprintf("%s > @p%d", col.Name, i+1)
			}
			args = append(args, currentState.LastValues[i])
		}
	}

	// Build ORDER BY clause
	orderBy := "ORDER BY " + strings.Join(columnNames, ", ")

	// Build the complete query
	var query string
	if b.config.Schema != "" {
		query = fmt.Sprintf(`
			SELECT %s
			FROM %s.%s
			%s
			%s
			LIMIT %d
		`, strings.Join(columnNames, ", "), b.config.Schema, table, whereClause, orderBy, batchSize)
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
	q := b.client.Query(query)
	q.Parameters = make([]bigquery.QueryParameter, len(args))
	for i, arg := range args {
		q.Parameters[i] = bigquery.QueryParameter{
			Name:  fmt.Sprintf("p%d", i+1),
			Value: arg,
		}
	}

	it, err := q.Read(ctx)
	if err != nil {
		currentState.Status = "failed"
		currentState.Error = err.Error()
		b.state.UpdateState(ctx, currentState)
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	var result [][]interface{}
	var lastValues []interface{}
	for {
		var values []interface{}
		err := it.Next(&values)
		if err == iterator.Done {
			break
		}
		if err != nil {
			currentState.Status = "failed"
			currentState.Error = err.Error()
			b.state.UpdateState(ctx, currentState)
			return nil, fmt.Errorf("failed to read row: %v", err)
		}

		result = append(result, values)
		lastValues = values
	}

	// Update state with last values
	currentState.LastValues = lastValues
	currentState.LastUpdated = time.Now()
	currentState.ProcessedRows += int64(len(result))
	if err := b.state.UpdateState(ctx, currentState); err != nil {
		return nil, fmt.Errorf("failed to update state: %v", err)
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
