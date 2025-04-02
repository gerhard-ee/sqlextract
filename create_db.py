import duckdb

def create_database_and_table():
    # Connect to DuckDB and create the database file if it doesn't exist
    conn = duckdb.connect('test_db.duckdb')
    
    try:
        # Create the sales_transactions table
        conn.execute("""
        CREATE TABLE IF NOT EXISTS sales_transactions(
            transaction_id INTEGER PRIMARY KEY,
            user_id INTEGER,
            product_name VARCHAR,
            quantity INTEGER,
            unit_price DECIMAL(10,2),
            transaction_timestamp TIMESTAMP
        )
        """)
        
        print("Database 'test_db.duckdb' created successfully!")
        print("Table 'sales_transactions' created successfully!")
        
    except Exception as e:
        print(f"Error creating database or table: {e}")
    
    finally:
        # Close the connection
        conn.close()

if __name__ == "__main__":
    create_database_and_table()

