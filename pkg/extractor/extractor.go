package extractor

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"sqlextract/pkg/database"
)

type ParquetWriter interface {
	Write(interface{}) error
	WriteStop() error
}

// Extractor handles data extraction from a database table
type Extractor struct {
	db           database.Database
	table        string
	outputFile   string
	format       string
	batchSize    int64
	concurrency  int
	checkpoint   *Checkpoint
	checkpointMu sync.Mutex
	lastID       int64
}

// Checkpoint represents the extraction progress
type Checkpoint struct {
	Table      string `json:"table"`
	LastOffset int64  `json:"last_offset"`
}

// NewExtractor creates a new Extractor instance
func NewExtractor(db database.Database, table, outputFile, format string, batchSize int64, concurrency int) *Extractor {
	return &Extractor{
		db:          db,
		table:       table,
		outputFile:  outputFile,
		format:      format,
		batchSize:   batchSize,
		concurrency: concurrency,
	}
}

// Extract extracts data from the database table
func (e *Extractor) Extract(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Get total rows
	totalRows, err := e.db.GetTotalRows(ctx, e.table)
	if err != nil {
		return fmt.Errorf("failed to get total rows: %v", err)
	}

	// Get table columns
	columns, err := e.db.GetColumns(ctx, e.table)
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	// Create output file
	file, err := os.Create(e.outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer file.Close()

	// Create writer based on format
	switch e.format {
	case "csv":
		if err := e.extractToCSV(ctx, file, columns, totalRows); err != nil {
			return fmt.Errorf("failed to extract to CSV: %v", err)
		}
	case "parquet":
		if err := e.extractToParquet(ctx, file, columns, totalRows); err != nil {
			return fmt.Errorf("failed to extract to Parquet: %v", err)
		}
	default:
		return fmt.Errorf("unsupported format: %s", e.format)
	}

	return nil
}

// SaveCheckpoint saves the current extraction progress
func (e *Extractor) SaveCheckpoint() error {
	if e.checkpoint == nil {
		e.checkpoint = &Checkpoint{
			Table:      e.table,
			LastOffset: e.lastID,
		}
	}

	// Create checkpoint file
	checkpointFile := e.getCheckpointFile()
	file, err := os.Create(checkpointFile)
	if err != nil {
		return fmt.Errorf("failed to create checkpoint file: %v", err)
	}
	defer file.Close()

	// Write checkpoint data
	if err := json.NewEncoder(file).Encode(e.checkpoint); err != nil {
		return fmt.Errorf("failed to write checkpoint: %v", err)
	}

	return nil
}

// LoadCheckpoint loads the last saved extraction progress
func (e *Extractor) LoadCheckpoint() error {
	checkpointFile := e.getCheckpointFile()

	// Check if checkpoint file exists
	if _, err := os.Stat(checkpointFile); os.IsNotExist(err) {
		return nil
	}

	// Open checkpoint file
	file, err := os.Open(checkpointFile)
	if err != nil {
		return fmt.Errorf("failed to open checkpoint file: %v", err)
	}
	defer file.Close()

	// Read checkpoint data
	var checkpoint Checkpoint
	if err := json.NewDecoder(file).Decode(&checkpoint); err != nil {
		return fmt.Errorf("failed to read checkpoint: %v", err)
	}

	// Verify table name
	if checkpoint.Table != e.table {
		return fmt.Errorf("checkpoint table mismatch: got %s, want %s", checkpoint.Table, e.table)
	}

	e.checkpoint = &checkpoint
	e.lastID = checkpoint.LastOffset
	return nil
}

func (e *Extractor) getCheckpointFile() string {
	dir := filepath.Dir(e.outputFile)
	base := filepath.Base(e.outputFile)
	ext := filepath.Ext(base)
	name := base[:len(base)-len(ext)]
	return filepath.Join(dir, fmt.Sprintf("%s.checkpoint", name))
}

func (e *Extractor) extractToCSV(ctx context.Context, file *os.File, columns []database.Column, totalRows int64) error {
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	var header []string
	for _, col := range columns {
		header = append(header, col.Name)
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %v", err)
	}

	// Extract data in batches
	for offset := e.lastID; offset < totalRows; offset += e.batchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			rows, err := e.db.ExtractBatch(ctx, e.table, offset, e.batchSize)
			if err != nil {
				return fmt.Errorf("failed to extract batch: %v", err)
			}
			defer rows.Close()

			// Write rows
			for rows.Next() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					values := make([]interface{}, len(columns))
					valuePtrs := make([]interface{}, len(columns))
					for i := range values {
						valuePtrs[i] = &values[i]
					}

					if err := rows.Scan(valuePtrs...); err != nil {
						return fmt.Errorf("failed to scan row: %v", err)
					}

					record := make([]string, len(columns))
					for i, val := range values {
						if val == nil {
							record[i] = ""
						} else {
							record[i] = fmt.Sprintf("%v", val)
						}
					}

					if err := writer.Write(record); err != nil {
						return fmt.Errorf("failed to write record: %v", err)
					}
				}
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("error iterating rows: %v", err)
			}

			e.lastID = offset + e.batchSize
		}
	}

	return nil
}

func (e *Extractor) extractToParquet(ctx context.Context, file *os.File, columns []database.Column, totalRows int64) error {
	// TODO: Implement Parquet extraction
	return fmt.Errorf("parquet format not supported yet")
}
