package database

// Config holds the configuration for database connections
type Config struct {
	Type            string
	Host            string
	Port            int
	Username        string
	Password        string
	Database        string
	Schema          string
	ProjectID       string
	CredentialsFile string

	// Cloud-specific fields
	Warehouse string // For Snowflake
	Catalog   string // For Databricks
}

// Column represents a database column
type Column struct {
	Name string
	Type string
}
