package extractor

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gerhard-ee/sqlextract/internal/database"
)

// ParquetWriter defines the interface for writing Parquet files
type ParquetWriter interface {
	Write(values []interface{}) error
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

// NewParquetWriter creates a new ParquetWriter instance
func NewParquetWriter(file *os.File, columns []database.Column) ParquetWriter {
	// TODO: Implement actual Parquet writer using a Parquet library
	// For now, return a mock implementation that writes CSV format
	return &mockParquetWriter{
		file:    file,
		columns: columns,
	}
}

type mockParquetWriter struct {
	file    *os.File
	columns []database.Column
}

func (w *mockParquetWriter) Write(values []interface{}) error {
	// Convert values to strings and write as CSV for now
	rowValues := make([]string, len(values))
	for i, v := range values {
		if v == nil {
			rowValues[i] = "NULL"
		} else {
			rowValues[i] = fmt.Sprintf("%v", v)
		}
	}
	_, err := fmt.Fprintf(w.file, "%s\n", strings.Join(rowValues, ","))
	return err
}

func (w *mockParquetWriter) WriteStop() error {
	return nil
}

// Extract extracts data from the database table
func (e *Extractor) Extract(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	// Get total rows
	totalRows, err := e.db.GetTotalRows(e.table)
	if err != nil {
		return fmt.Errorf("failed to get total rows: %v", err)
	}

	// Get table columns
	columnNames, err := e.db.GetColumns(e.table)
	if err != nil {
		return fmt.Errorf("failed to get columns: %v", err)
	}

	// Convert column names to Column structs
	columns := make([]database.Column, len(columnNames))
	for i, name := range columnNames {
		columns[i] = database.Column{
			Name: name,
			Type: "string", // Default type, should be determined by the database
		}
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
			rows, err := e.db.ExtractBatch(e.table, offset, e.batchSize, "", "")
			if err != nil {
				return fmt.Errorf("failed to extract batch: %v", err)
			}

			// Write rows
			for _, row := range rows {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					values := make([]string, len(columns))
					for i, col := range columns {
						if val := row[col.Name]; val == nil {
							values[i] = ""
						} else {
							values[i] = fmt.Sprintf("%v", val)
						}
					}

					if err := writer.Write(values); err != nil {
						return fmt.Errorf("failed to write record: %v", err)
					}
				}
			}

			e.lastID = offset + e.batchSize
		}
	}

	return nil
}

func (e *Extractor) extractToParquet(ctx context.Context, file *os.File, columns []database.Column, totalRows int64) error {
	// Create Parquet writer
	writer := NewParquetWriter(file, columns)
	defer writer.WriteStop()

	// Extract data in batches
	for offset := e.lastID; offset < totalRows; offset += e.batchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			rows, err := e.db.ExtractBatch(e.table, offset, e.batchSize, "", "")
			if err != nil {
				return fmt.Errorf("failed to extract batch: %v", err)
			}

			// Write rows
			for _, row := range rows {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					values := make([]interface{}, len(columns))
					for i, col := range columns {
						values[i] = row[col.Name]
					}

					if err := writer.Write(values); err != nil {
						return fmt.Errorf("failed to write row: %v", err)
					}
				}
			}

			e.lastID = offset + e.batchSize
		}
	}

	return nil
}

// convertToParquetType converts a database value to the appropriate Parquet type
func convertToParquetType(val interface{}, colType string) interface{} {
	if val == nil {
		return nil
	}

	switch colType {
	case "integer", "int", "int4", "int8", "bigint":
		switch v := val.(type) {
		case int64:
			return v
		case int32:
			return int64(v)
		case int:
			return int64(v)
		default:
			return nil
		}
	case "float", "float4", "float8", "double", "real":
		switch v := val.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		default:
			return nil
		}
	case "text", "varchar", "char", "string":
		switch v := val.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		default:
			return fmt.Sprintf("%v", v)
		}
	case "boolean", "bool":
		switch v := val.(type) {
		case bool:
			return v
		default:
			return nil
		}
	case "timestamp", "datetime", "date":
		switch v := val.(type) {
		case time.Time:
			return v
		default:
			return nil
		}
	default:
		return fmt.Sprintf("%v", val)
	}
}
