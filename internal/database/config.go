package database

// Config represents database configuration
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
	Warehouse       string
	Catalog         string
}

// Column represents a database column
type Column struct {
	Name string
	Type string
}
