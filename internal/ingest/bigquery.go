package ingest

import (
	"fmt"
)

type BigQueryIngester struct{}

func NewBigQueryIngester() Ingester {
	return &BigQueryIngester{}
}

func (i *BigQueryIngester) GenerateCSVIngestScript(sourcePath, targetTable string) (string, error) {
	script := fmt.Sprintf(`-- BigQuery CSV Ingestion Script
-- Generated by SQLExtract
-- Target Table: %s
-- Source File: %s

-- Load data from CSV file
LOAD DATA OVERWRITE INTO %s
FROM FILES (
  format = 'CSV',
  field_delimiter = ',',
  skip_leading_rows = 1,
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
)
OPTIONS (
  null_marker = 'NULL',
  ignore_unknown_values = TRUE
)
FROM FILES (
  uris = ['%s']
);`, targetTable, sourcePath, targetTable, sourcePath)

	return script, nil
}

func (i *BigQueryIngester) GenerateParquetIngestScript(sourcePath, targetTable string) (string, error) {
	script := fmt.Sprintf(`-- BigQuery Parquet Ingestion Script
-- Generated by SQLExtract
-- Target Table: %s
-- Source File: %s

-- Load data from Parquet file
LOAD DATA OVERWRITE INTO %s
FROM FILES (
  format = 'PARQUET'
)
OPTIONS (
  ignore_unknown_values = TRUE
)
FROM FILES (
  uris = ['%s']
);`, targetTable, sourcePath, targetTable, sourcePath)

	return script, nil
}
