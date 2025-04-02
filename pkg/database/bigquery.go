package database

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/gerhard-ee/sqlextract/internal/config"
	"google.golang.org/api/iterator"
)

// BigQuery implements the Database interface for Google BigQuery
type BigQuery struct {
	config *config.Config
	client *bigquery.Client
}

// NewBigQuery creates a new BigQuery instance
func NewBigQuery(config *config.Config) (*BigQuery, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, config.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return &BigQuery{
		config: config,
		client: client,
	}, nil
}

// Connect establishes a connection to BigQuery
func (b *BigQuery) Connect(ctx context.Context) error {
	// Connection is established in NewBigQuery
	return nil
}

// Close closes the BigQuery connection
func (b *BigQuery) Close() {
	if b.client != nil {
		b.client.Close()
	}
}

// GetTableSchema returns the schema of a table
func (b *BigQuery) GetTableSchema(tableName string) ([]Column, error) {
	// Split dataset and table name
	parts := strings.Split(tableName, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid table name format, expected 'dataset.table', got %s", tableName)
	}
	dataset, tableName := parts[0], parts[1]

	// Get table metadata
	md, err := b.client.Dataset(dataset).Table(tableName).Metadata(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	// Convert schema to Column slice
	columns := make([]Column, len(md.Schema))
	for i, field := range md.Schema {
		columns[i] = Column{
			Name: field.Name,
			Type: string(field.Type),
		}
	}

	return columns, nil
}

// Query executes a query and returns the results
func (b *BigQuery) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	// Create query
	q := b.client.Query(query)

	// Run query
	job, err := q.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %v", err)
	}

	// Wait for query to complete
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for query: %v", err)
	}

	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}

	// Get iterator
	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read results: %v", err)
	}

	return &bigQueryRows{it: it}, nil
}

// Exec executes a query without returning results
func (b *BigQuery) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	// Create query
	q := b.client.Query(query)

	// Run query
	job, err := q.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %v", err)
	}

	// Wait for query to complete
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for query: %v", err)
	}

	if err := status.Err(); err != nil {
		return nil, fmt.Errorf("query failed: %v", err)
	}

	return &bigQueryResult{}, nil
}

// GetTotalRows returns the total number of rows in a table
func (b *BigQuery) GetTotalRows(table string) (int64, error) {
	// Split dataset and table name
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid table name format, expected 'dataset.table', got %s", table)
	}
	dataset, tableName := parts[0], parts[1]

	// Get table metadata
	md, err := b.client.Dataset(dataset).Table(tableName).Metadata(context.Background())
	if err != nil {
		return 0, fmt.Errorf("failed to get table metadata: %v", err)
	}

	return int64(md.NumRows), nil
}

// GetColumns returns the columns of a table
func (b *BigQuery) GetColumns(table string) ([]string, error) {
	// Split dataset and table name
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid table name format, expected 'dataset.table', got %s", table)
	}
	dataset, tableName := parts[0], parts[1]

	// Get table metadata
	md, err := b.client.Dataset(dataset).Table(tableName).Metadata(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	// Extract column names
	columns := make([]string, len(md.Schema))
	for i, field := range md.Schema {
		columns[i] = field.Name
	}

	return columns, nil
}

// ExtractBatch extracts a batch of rows from a table
func (b *BigQuery) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	// Split dataset and table name
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid table name format, expected 'dataset.table', got %s", table)
	}
	dataset, tableName := parts[0], parts[1]

	// Build query
	query := fmt.Sprintf("SELECT * FROM %s.%s", dataset, tableName)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if keyColumns != "" {
		query += " ORDER BY " + keyColumns
	}
	query += " LIMIT " + strconv.FormatInt(limit, 10) + " OFFSET " + strconv.FormatInt(offset, 10)

	// Execute query
	it, err := b.Query(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	// Convert rows to map slice
	var result []map[string]interface{}
	for it.Next() {
		var row map[string]interface{}
		if err := it.Scan(&row); err != nil {
			return nil, fmt.Errorf("failed to scan row: %v", err)
		}
		result = append(result, row)
	}

	if err := it.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return result, nil
}

// CreateDatabase creates the database if it doesn't exist
func (b *BigQuery) CreateDatabase() error {
	// For BigQuery, we don't need to create a database
	// The project and dataset should already exist
	return nil
}

// DropDatabase drops the database if it exists
func (b *BigQuery) DropDatabase() error {
	// For BigQuery, we don't drop the database
	// The project and dataset should be managed separately
	return nil
}

// GetPrimaryKey returns the primary key column of a table
func (b *BigQuery) GetPrimaryKey(ctx context.Context, table string) (string, error) {
	// Split dataset and table name
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid table name format, expected 'dataset.table', got %s", table)
	}
	dataset, tableName := parts[0], parts[1]

	// Get table metadata
	md, err := b.client.Dataset(dataset).Table(tableName).Metadata(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to get table metadata: %v", err)
	}

	// Look for a clustering field or the first field
	if len(md.Clustering.Fields) > 0 {
		return md.Clustering.Fields[0], nil
	}

	if len(md.Schema) > 0 {
		return md.Schema[0].Name, nil
	}

	return "", fmt.Errorf("no suitable primary key found")
}

// Helper function to get primary key without context
func (b *BigQuery) getPrimaryKey(table string) string {
	pk, err := b.GetPrimaryKey(context.Background(), table)
	if err != nil {
		// Default to first column if we can't determine the primary key
		return "_ROWID_"
	}
	return pk
}

// bigQueryResult implements the Result interface for BigQuery
type bigQueryResult struct{}

func (r *bigQueryResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("LastInsertId is not supported by BigQuery")
}

func (r *bigQueryResult) RowsAffected() (int64, error) {
	return 0, fmt.Errorf("RowsAffected is not supported by BigQuery")
}

// bigQueryRows implements the Rows interface for BigQuery
type bigQueryRows struct {
	it         *bigquery.RowIterator
	currentRow map[string]interface{}
	err        error
}

func (r *bigQueryRows) Next() bool {
	r.currentRow = make(map[string]interface{})
	r.err = r.it.Next(&r.currentRow)
	return r.err == nil
}

func (r *bigQueryRows) Scan(dest ...interface{}) error {
	if len(dest) == 1 {
		switch d := dest[0].(type) {
		case *map[string]interface{}:
			*d = r.currentRow
			return nil
		}
	}
	return fmt.Errorf("unsupported scan type")
}

func (r *bigQueryRows) Close() error {
	return nil // BigQuery iterator doesn't need explicit closing
}

func (r *bigQueryRows) Err() error {
	if r.err == nil {
		return nil
	}
	if r.err == iterator.Done {
		return nil
	}
	return r.err
}
