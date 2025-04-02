package config

// Config represents the database configuration
type Config struct {
	// Common fields
	Type     string
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Schema   string

	// BigQuery specific
	ProjectID string
	Location  string

	// Snowflake specific
	Account   string
	Warehouse string
	Role      string

	// Databricks specific
	Workspace string
	Token     string
	Catalog   string
}
