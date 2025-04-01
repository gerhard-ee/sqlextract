package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"

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

	client, err := bigquery.NewClient(ctx, cfg.ProjectID, option.WithCredentialsFile(cfg.CredentialsFile))
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
	query := fmt.Sprintf("SELECT * FROM %s", table)
	if db.config.Schema != "" {
		query = fmt.Sprintf("SELECT * FROM %s.%s", db.config.Schema, table)
	}

	// Execute query
	ctx := context.Background()
	q := db.client.Query(query)
	it, err := q.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute query: %v", err)
	}

	// Get schema from query
	job, err := q.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run query: %v", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for query: %v", err)
	}

	if err := status.Err(); err != nil {
		return fmt.Errorf("query failed: %v", err)
	}

	// Get column names from the first row
	var firstRow []bigquery.Value
	if err := it.Next(&firstRow); err != nil && err != iterator.Done {
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
		var row []bigquery.Value
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read row: %v", err)
		}

		// Write row
		if format == "csv" {
			rowValues := make([]string, len(row))
			for i, v := range row {
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
