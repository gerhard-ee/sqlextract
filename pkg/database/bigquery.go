package database

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/option"
)

// BigQuery implements the Database interface for Google BigQuery
type BigQuery struct {
	config *Config
	client *bigquery.Client
}

// NewBigQuery creates a new BigQuery instance
func NewBigQuery(config *Config) *BigQuery {
	return &BigQuery{
		config: config,
	}
}

// Connect establishes a connection to BigQuery
func (b *BigQuery) Connect(ctx context.Context) error {
	// For BigQuery, DBName is the project ID
	client, err := bigquery.NewClient(ctx, b.config.DBName, option.WithCredentialsFile(b.config.Password))
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	b.client = client
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
	dataset, table := parts[0], parts[1]

	// Get table metadata
	md, err := b.client.Dataset(dataset).Table(table).Metadata(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	var columns []Column
	for _, field := range md.Schema {
		columns = append(columns, Column{
			Name:     field.Name,
			Type:     field.Type.String(),
			Nullable: !field.Required,
		})
	}

	return columns, nil
}

// Query executes a query and returns the results
func (b *BigQuery) Query(ctx context.Context, query string, args ...interface{}) (Rows, error) {
	// Create query
	q := b.client.Query(fmt.Sprintf(query, args...))

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

	return job.Read(ctx)
}

// Exec executes a query without returning results
func (b *BigQuery) Exec(ctx context.Context, query string, args ...interface{}) (Result, error) {
	// Create query
	q := b.client.Query(fmt.Sprintf(query, args...))

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
func (b *BigQuery) GetTotalRows(ctx context.Context, table string) (int64, error) {
	// Split dataset and table name
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid table name format, expected 'dataset.table', got %s", table)
	}
	dataset, tableName := parts[0], parts[1]

	// Get table metadata
	md, err := b.client.Dataset(dataset).Table(tableName).Metadata(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get table metadata: %v", err)
	}

	return md.NumRows, nil
}

// GetColumns returns the columns of a table
func (b *BigQuery) GetColumns(ctx context.Context, table string) ([]Column, error) {
	// Split dataset and table name
	parts := strings.Split(table, ".")
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid table name format, expected 'dataset.table', got %s", table)
	}
	dataset, tableName := parts[0], parts[1]

	// Get table metadata
	md, err := b.client.Dataset(dataset).Table(tableName).Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %v", err)
	}

	var columns []Column
	for _, field := range md.Schema {
		columns = append(columns, Column{
			Name:     field.Name,
			Type:     field.Type.String(),
			Nullable: !field.Required,
		})
	}

	return columns, nil
}

// ExtractBatch extracts a batch of rows from a table
func (b *BigQuery) ExtractBatch(ctx context.Context, table string, offset, limit int64) (Rows, error) {
	query := fmt.Sprintf("SELECT * FROM `%s` ORDER BY %s LIMIT %d OFFSET %d",
		table, b.getPrimaryKey(table), limit, offset)

	return b.Query(ctx, query)
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
