//go:build !release
// +build !release

package database

import (
	"github.com/gerhard-ee/sqlextract/internal/config"
	"github.com/gerhard-ee/sqlextract/internal/state"
)

// DuckDB implements the Database interface
type DuckDB struct {
	config       *config.Config
	db           interface{} // sql.DB for darwin, nil for others
	stateManager state.Manager
}

// NewDuckDB creates a new DuckDB instance
func NewDuckDB(cfg *config.Config, stateManager state.Manager) (Database, error) {
	db := &DuckDB{
		config:       cfg,
		stateManager: stateManager,
	}
	if err := db.Connect(); err != nil {
		return nil, err
	}
	return db, nil
}
