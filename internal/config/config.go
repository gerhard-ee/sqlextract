package config

// Config represents the database configuration
type Config struct {
	// Common fields
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	Schema   string `json:"schema"`

	// BigQuery specific
	ProjectID       string `json:"project_id"`
	Location        string `json:"location"`
	CredentialsFile string `json:"credentials_file"`

	// Snowflake specific
	Account   string `json:"account"`
	Warehouse string `json:"warehouse"`
	Role      string `json:"role"`

	// Databricks specific
	Workspace string `json:"workspace"`
	Token     string `json:"token"`
	Catalog   string `json:"catalog"`
}
