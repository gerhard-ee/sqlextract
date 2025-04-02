package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
)

func main() {
	db, err := sql.Open("duckdb", "test.db")
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create test table
	_, err = db.Exec(`
		CREATE TABLE test_table AS 
		SELECT * FROM (VALUES 
			(1, 'test1'), 
			(2, 'test2')
		) AS t(id, name);
	`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}

	fmt.Println("Test table created successfully")
}
