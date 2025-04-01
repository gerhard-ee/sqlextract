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

func (db *BigQueryDB) ExtractData(table, outputFile, format string, batchSize int) error {
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

	// Build query
	query := fmt.Sprintf("SELECT * FROM `%s.%s.%s`", db.config.ProjectID, db.config.Database, table)

	// Execute query
	ctx := context.Background()
	q := db.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	// Get column names from the first row
	var firstRow []bigquery.Value
	if err := it.Next(&firstRow); err != nil {
		return fmt.Errorf("failed to read first row: %v", err)
	}

	columns := make([]string, len(firstRow))
	for i := range firstRow {
		columns[i] = fmt.Sprintf("column_%d", i+1)
	}

	// Create output file
	file, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	// Write header
	if format == "csv" {
		if _, err := fmt.Fprintf(file, "%s\n", strings.Join(columns, ",")); err != nil {
			return fmt.Errorf("failed to write header: %v", err)
		}

		// Write first row
		rowValues := make([]string, len(firstRow))
		for i, v := range firstRow {
			if v == nil {
				rowValues[i] = "NULL"
			} else {
				rowValues[i] = fmt.Sprintf("%v", v)
			}
		}
		if _, err := fmt.Fprintf(file, "%s\n", strings.Join(rowValues, ",")); err != nil {
			return fmt.Errorf("failed to write first row: %v", err)
		}
	}

	// Process remaining rows
	processedRows := int64(1)
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err != nil {
			break
		}

		// Write row
		if format == "csv" {
			rowValues := make([]string, len(values))
			for i, v := range values {
				if v == nil {
					rowValues[i] = "NULL"
				} else {
					rowValues[i] = fmt.Sprintf("%v", v)
				}
			}
			if _, err := fmt.Fprintf(file, "%s\n", strings.Join(rowValues, ",")); err != nil {
				return fmt.Errorf("failed to write row: %v", err)
			}
		}

		processedRows++
		if processedRows%int64(batchSize) == 0 {
			// Update state
			if err := db.stateManager.UpdateState(table, processedRows); err != nil {
				return fmt.Errorf("failed to update state: %v", err)
			}
		}
	}

	// Update final state
	if err := db.stateManager.UpdateState(table, processedRows); err != nil {
		return fmt.Errorf("failed to update final state: %v", err)
	}

	return nil
}

func (db *BigQueryDB) GetTotalRows(table string) (int64, error) {
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

func (db *BigQueryDB) ExtractBatch(table string, offset, limit int64) ([]map[string]interface{}, error) {
	// Build query
	query := fmt.Sprintf("SELECT * FROM `%s.%s.%s` LIMIT %d OFFSET %d", db.config.ProjectID, db.config.Database, table, limit, offset)

	// Execute query
	ctx := context.Background()
	q := db.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}

	// Get column names from the first row
	var firstRow []bigquery.Value
	if err := it.Next(&firstRow); err != nil {
		return nil, fmt.Errorf("failed to read first row: %v", err)
	}

	columns := make([]string, len(firstRow))
	for i := range firstRow {
		columns[i] = fmt.Sprintf("column_%d", i+1)
	}

	// Create result with first row
	result := []map[string]interface{}{
		make(map[string]interface{}),
	}
	for i, v := range firstRow {
		result[0][columns[i]] = v
	}

	// Scan remaining rows
	for {
		var values []bigquery.Value
		err := it.Next(&values)
		if err != nil {
			break
		}

		row := make(map[string]interface{})
		for i, v := range values {
			row[columns[i]] = v
		}
		result = append(result, row)
	}

	return result, nil
}
