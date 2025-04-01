package ingest

import (
	"fmt"
)

type DatabricksIngester struct{}

func NewDatabricksIngester() Ingester {
	return &DatabricksIngester{}
}

func (i *DatabricksIngester) GenerateCSVIngestScript(sourcePath, targetTable string) (string, error) {
	script := fmt.Sprintf(`-- Databricks CSV Ingestion Script
-- Generated by SQLExtract
-- Target Table: %s
-- Source File: %s

-- Create a temporary view from the CSV file
CREATE OR REPLACE TEMPORARY VIEW temp_csv_view
USING csv
OPTIONS (
  path = '%s',
  header = 'true',
  inferSchema = 'true',
  delimiter = ',',
  nullValue = 'NULL'
);

-- Insert data from the temporary view to the target table
INSERT OVERWRITE TABLE %s
SELECT * FROM temp_csv_view;

-- Clean up
DROP VIEW IF EXISTS temp_csv_view;`, targetTable, sourcePath, sourcePath, targetTable)

	return script, nil
}

func (i *DatabricksIngester) GenerateParquetIngestScript(sourcePath, targetTable string) (string, error) {
	script := fmt.Sprintf(`-- Databricks Parquet Ingestion Script
-- Generated by SQLExtract
-- Target Table: %s
-- Source File: %s

-- Create a temporary view from the Parquet file
CREATE OR REPLACE TEMPORARY VIEW temp_parquet_view
USING parquet
OPTIONS (
  path = '%s',
  mergeSchema = 'true'
);

-- Insert data from the temporary view to the target table
INSERT OVERWRITE TABLE %s
SELECT * FROM temp_parquet_view;

-- Clean up
DROP VIEW IF EXISTS temp_parquet_view;`, targetTable, sourcePath, sourcePath, targetTable)

	return script, nil
}
