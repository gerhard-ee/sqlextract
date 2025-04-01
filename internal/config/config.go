package config

// Config represents the database configuration
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
