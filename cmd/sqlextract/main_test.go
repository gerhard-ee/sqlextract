package main

import (
	"flag"
	"os"
	"testing"
)

func TestFlagValidation(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantExit bool
	}{
		{
			name:     "missing required flags",
			args:     []string{"cmd"},
			wantExit: true,
		},
		{
			name: "valid flags postgres",
			args: []string{
				"cmd",
				"-database=postgres",
				"-host=localhost",
				"-port=5432",
				"-user=postgres",
				"-password=postgres",
				"-dbname=testdb",
				"-table=test_table",
				"-output=output.csv",
				"-format=csv",
			},
			wantExit: false,
		},
		{
			name: "valid flags duckdb",
			args: []string{
				"cmd",
				"-database=duckdb",
				"-dbname=test.db",
				"-table=test_table",
				"-output=output.csv",
				"-format=csv",
			},
			wantExit: false,
		},
		{
			name: "invalid format",
			args: []string{
				"cmd",
				"-database=postgres",
				"-host=localhost",
				"-port=5432",
				"-user=postgres",
				"-password=postgres",
				"-dbname=testdb",
				"-table=test_table",
				"-output=output.csv",
				"-format=invalid",
			},
			wantExit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Save original args and restore them after test
			oldArgs := os.Args
			defer func() { os.Args = oldArgs }()

			// Set test args
			os.Args = tt.args

			// Create a new flag set for testing
			fs := flag.NewFlagSet("test", flag.ExitOnError)

			// Initialize flags
			dbType := fs.String("database", "", "Database type (mssql, mysql, postgres)")
			host := fs.String("host", "", "Database host")
			port := fs.Int("port", 0, "Database port")
			user := fs.String("user", "", "Database user")
			password := fs.String("password", "", "Database password")
			dbName := fs.String("dbname", "", "Database name")
			table := fs.String("table", "", "Table to extract")
			outputFile := fs.String("output", "", "Output file path")
			format := fs.String("format", "csv", "Output format (csv or parquet)")

			// Parse flags
			fs.Parse(tt.args[1:])

			// Validate required flags
			if *dbType == "" || *table == "" || *outputFile == "" {
				if !tt.wantExit {
					t.Error("Expected flags to be valid")
				}
				return
			}

			// Validate database-specific flags
			switch *dbType {
			case "postgres":
				if *host == "" || *port == 0 || *user == "" || *password == "" || *dbName == "" {
					if !tt.wantExit {
						t.Error("Expected PostgreSQL flags to be valid")
					}
					return
				}
			case "duckdb":
				if *dbName == "" {
					if !tt.wantExit {
						t.Error("Expected DuckDB flags to be valid")
					}
					return
				}
			}

			// Validate format
			if *format != "csv" && *format != "parquet" {
				if !tt.wantExit {
					t.Error("Expected format to be valid")
				}
				return
			}

			if tt.wantExit {
				t.Error("Expected flags to be invalid")
			}
		})
	}
}
