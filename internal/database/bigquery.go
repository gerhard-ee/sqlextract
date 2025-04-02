package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

type BigQueryDB struct {
	client       *bigquery.Client
	config       *config.Config
	stateManager state.Manager
}

func NewBigQuery(cfg *config.Config, stateManager state.Manager) (Database, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("failed to create BigQuery client: %v", err)
	}

	return &BigQueryDB{
		client:       client,
		config:       cfg,
		stateManager: stateManager,
	}, nil
}

func (db *BigQueryDB) ExtractData(table, outputFile, format string, batchSize int, keyColumns, whereClause string) error {
	// Get current state
	currentState, err := db.stateManager.GetState(table)
	if err != nil {
		// Create new state if it doesn't exist
		currentState = &state.State{
			Table:       table,
			LastUpdated: time.Now(),
			Status:      "running",
		}
		if err := db.stateManager.CreateState(currentState); err != nil {
			return fmt.Errorf("failed to create state: %v", err)
		}
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(filepath.Dir(outputFile), 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Get total rows
	totalRows, err := db.GetTotalRows(table)
	if err != nil {
		return fmt.Errorf("failed to get total rows: %v", err)
	}

	// Get columns
	columns, err := db.GetColumns(table)
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	// Write header if CSV format
	if format == "csv" {
		if _, err := fmt.Fprintf(file, "%s\n", strings.Join(columns, ",")); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}
	}

	// Process data in batches
	processedRows := int64(0)
	for offset := int64(0); offset < totalRows; offset += int64(batchSize) {
		rows, err := db.ExtractBatch(table, offset, int64(batchSize), keyColumns, whereClause)
		if err != nil {
			return fmt.Errorf("failed to extract batch: %v", err)
		}

		// Write rows
		for _, row := range rows {
			if format == "csv" {
				values := make([]string, len(columns))
				for i, col := range columns {
					if val := row[col]; val == nil {
						values[i] = "NULL"
					} else {
						values[i] = fmt.Sprintf("%v", val)
					}
				}
				if _, err := fmt.Fprintf(file, "%s\n", strings.Join(values, ",")); err != nil {
					return fmt.Errorf("failed to write row: %v", err)
				}
			}
			processedRows++
		}

		// Update state
		if err := db.stateManager.UpdateState(table, processedRows); err != nil {
			return fmt.Errorf("failed to update state: %v", err)
		}
	}

	return nil
}

func (db *BigQueryDB) ExtractBatch(table string, offset, limit int64, keyColumns, whereClause string) ([]map[string]interface{}, error) {
	// Build query
	query := fmt.Sprintf("SELECT * FROM `%s.%s.%s`", db.config.ProjectID, db.config.Database, table)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}
	if keyColumns != "" {
		query += " ORDER BY " + keyColumns
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	// Execute query
	ctx := context.Background()
	q := db.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	// Get column names
	columns, err := db.GetColumns(table)
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	var result []map[string]interface{}
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err != nil {
			break
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		result = append(result, row)
	}

	return result, nil
}

func (db *BigQueryDB) GetTotalRows(table string) (int64, error) {
	// Try to get an exact count first
	query := fmt.Sprintf("SELECT COUNT(*) as count FROM `%s.%s.%s`", db.config.ProjectID, db.config.Database, table)
	ctx := context.Background()
	q := db.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %v", err)
	}

	var count int64
	err = it.Next(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get total rows: %v", err)
	}

	// If the count is too large, use an approximate count
	if count > 1000000 {
		query = fmt.Sprintf("SELECT row_count FROM `%s.%s.__TABLES__` WHERE table_id = '%s'",
			db.config.ProjectID, db.config.Database, table)
		q = db.client.Query(query)
		it, err = q.Read(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to execute approximate count query: %v", err)
		}
		err = it.Next(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to get approximate row count: %v", err)
		}
	}

	return count, nil
}

func (db *BigQueryDB) GetColumns(table string) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM `%s.%s.INFORMATION_SCHEMA.COLUMNS` WHERE table_name = '%s' ORDER BY ordinal_position", db.config.ProjectID, db.config.Database, table)
	ctx := context.Background()
	q := db.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	var columns []string
	for {
		var column string
		err := it.Next(&column)
		if err != nil {
			break
		}
		columns = append(columns, column)
	}

	return columns, nil
}

func (db *BigQueryDB) Close() error {
	if db.client != nil {
		return db.client.Close()
	}
	return nil
}

func (db *BigQueryDB) Connect() error {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, db.config.ProjectID)
	if err != nil {
		return fmt.Errorf("failed to create BigQuery client: %v", err)
	}
	db.client = client
	return nil
}
