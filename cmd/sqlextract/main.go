package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/database"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

var (
	// Database connection flags
	dbType     = flag.String("type", "", "Database type (postgres, mssql, bigquery, snowflake, databricks, duckdb)")
	dbHost     = flag.String("host", "", "Database host")
	dbPort     = flag.Int("port", 0, "Database port")
	dbUser     = flag.String("user", "", "Database user")
	dbPassword = flag.String("password", "", "Database password")
	dbName     = flag.String("database", "", "Database name")
	dbSchema   = flag.String("schema", "", "Database schema (optional)")

	// BigQuery specific flags
	bqProjectID = flag.String("project", "", "Google Cloud project ID (required for BigQuery)")
	bqLocation  = flag.String("location", "", "BigQuery dataset location (optional)")

	// Snowflake specific flags
	sfAccount   = flag.String("account", "", "Snowflake account identifier")
	sfWarehouse = flag.String("warehouse", "", "Snowflake warehouse name")
	sfRole      = flag.String("role", "", "Snowflake role name")

	// Databricks specific flags
	dbWorkspace = flag.String("workspace", "", "Databricks workspace URL")
	dbToken     = flag.String("token", "", "Databricks access token")
	dbCatalog   = flag.String("catalog", "", "Databricks catalog name")

	// Extraction flags
	table        = flag.String("table", "", "Table name to extract")
	output       = flag.String("output", "", "Output file path (CSV or Parquet)")
	outputFormat = flag.String("format", "csv", "Output format (csv or parquet)")
	batchSize    = flag.Int("batch-size", 1000, "Number of rows to extract in each batch")
	keyColumns   = flag.String("keys", "", "Comma-separated list of key columns for pagination")
	whereClause  = flag.String("where", "", "SQL WHERE clause for filtering data")

	// State management flags
	namespace = flag.String("namespace", "default", "Kubernetes namespace for state management")
	stateType = flag.String("state-type", "memory", "State management type (memory or kubernetes)")

	// Help flag
	showHelp = flag.Bool("help", false, "Show detailed help information")
)

func initFlags() {
	flag.Parse()

	if *showHelp {
		printHelp()
		os.Exit(0)
	}

	// Validate required flags
	if *dbType == "" {
		log.Fatal("Database type is required. Use -help for more information.")
	}
	if *table == "" {
		log.Fatal("Table name is required. Use -help for more information.")
	}
	if *output == "" {
		log.Fatal("Output file path is required. Use -help for more information.")
	}

	// Validate database-specific required flags
	switch *dbType {
	case "postgres", "mssql":
		if *dbHost == "" || *dbPort == 0 || *dbUser == "" || *dbPassword == "" || *dbName == "" {
			log.Fatal("Host, port, user, password, and database name are required for Postgres/MSSQL. Use -help for more information.")
		}
	case "bigquery":
		if *bqProjectID == "" {
			log.Fatal("Project ID is required for BigQuery. Use -help for more information.")
		}
	case "snowflake":
		if *sfAccount == "" || *sfWarehouse == "" || *sfRole == "" || *dbUser == "" || *dbPassword == "" {
			log.Fatal("Account, warehouse, role, user, and password are required for Snowflake. Use -help for more information.")
		}
	case "databricks":
		if *dbWorkspace == "" || *dbToken == "" || *dbCatalog == "" {
			log.Fatal("Workspace URL, access token, and catalog are required for Databricks. Use -help for more information.")
		}
	case "duckdb":
		if *dbName == "" {
			log.Fatal("Database file path is required for DuckDB. Use -help for more information.")
		}
	default:
		log.Fatalf("Unsupported database type: %s. Use -help for more information.", *dbType)
	}
}

func printHelp() {
	// ANSI color codes
	const (
		headerColor   = "\033[1;36m" // Cyan
		sectionColor  = "\033[1;33m" // Yellow
		flagColor     = "\033[1;32m" // Green
		exampleColor  = "\033[1;35m" // Magenta
		requiredColor = "\033[1;31m" // Red
		optionalColor = "\033[1;34m" // Blue
		resetColor    = "\033[0m"    // Reset
	)

	// Read help text from file
	helpText, err := os.ReadFile("cmd/sqlextract/help.txt")
	if err != nil {
		// Fallback to basic help if file is not found
		fmt.Printf("%sSQL Extract - A powerful tool for extracting data from SQL databases%s\n", headerColor, resetColor)
		fmt.Printf("\n%sUSAGE:%s\n", sectionColor, resetColor)
		fmt.Printf("  sqlextract [OPTIONS] --type <TYPE> --table <TABLE> --output <FILE>\n\n")
		fmt.Printf("%sREQUIRED OPTIONS:%s\n", sectionColor, resetColor)
		fmt.Printf("  %s--type, -t%s <TYPE>        Database type (postgres, mssql, bigquery, snowflake, databricks, duckdb)\n", flagColor, resetColor)
		fmt.Printf("  %s--table, -T%s <TABLE>      Table name to extract\n", flagColor, resetColor)
		fmt.Printf("  %s--output, -o%s <FILE>      Output file path (CSV or Parquet)\n", flagColor, resetColor)
		fmt.Printf("\nFor more information, visit: https://github.com/gerhard-ee/sqlextract\n")
		return
	}

	// Print help text with colors
	fmt.Printf("%s%s%s\n", headerColor, string(helpText), resetColor)
}

func main() {
	initFlags()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Initialize state manager
	var stateManager state.Manager
	var err error
	if *stateType == "kubernetes" {
		stateManager, err = state.NewKubernetesManager(*namespace)
		if err != nil {
			log.Fatalf("Failed to create Kubernetes state manager: %v", err)
		}
	} else {
		stateManager = state.NewMemoryManager()
	}

	// Create database configuration
	dbConfig := &config.Config{
		Type:     *dbType,
		Host:     *dbHost,
		Port:     *dbPort,
		User:     *dbUser,
		Password: *dbPassword,
		Database: *dbName,
		Schema:   *dbSchema,
		// BigQuery specific
		ProjectID: *bqProjectID,
		Location:  *bqLocation,
		// Snowflake specific
		Account:   *sfAccount,
		Warehouse: *sfWarehouse,
		Role:      *sfRole,
		// Databricks specific
		Workspace: *dbWorkspace,
		Token:     *dbToken,
		Catalog:   *dbCatalog,
	}

	// Create database instance
	db, err := database.NewDatabase(*dbType, dbConfig, stateManager)
	if err != nil {
		log.Fatalf("Failed to create database instance: %v", err)
	}

	// Connect to database
	if err := db.Connect(); err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	// Extract data
	if err := db.ExtractData(*table, *output, *outputFormat, *batchSize, *keyColumns, *whereClause); err != nil {
		log.Fatalf("Failed to extract data: %v", err)
	}

	log.Println("Extraction completed successfully")
}
