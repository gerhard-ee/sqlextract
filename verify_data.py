#!/usr/bin/env python3
"""
verify_data.py - Script to verify data integrity and distribution in sales_transactions table

This script runs a series of SQL queries against the test_db.duckdb database to check:
1. Duplicate transaction_ids (should be 0 due to PRIMARY KEY constraint)
2. Value ranges and basic statistics for all columns
3. Product distribution statistics

Usage:
    python verify_data.py
"""

import duckdb
import sys
from tabulate import tabulate


def connect_to_db(db_path="test_db.duckdb"):
    """Establish connection to DuckDB database"""
    try:
        return duckdb.connect(db_path, read_only=True)
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def check_duplicates(conn):
    """Check for duplicate transaction_ids"""
    print("\n=== Checking for Duplicate Transaction IDs ===")
    query = """
    SELECT COUNT(*) - COUNT(DISTINCT transaction_id) as duplicate_count 
    FROM sales_transactions;
    """
    
    result = conn.execute(query).fetchall()
    
    print(tabulate(result, headers=["duplicate_count"], tablefmt="psql"))
    
    if result[0][0] == 0:
        print("✅ No duplicates found. PRIMARY KEY constraint is working properly.")
    else:
        print(f"⚠️ Found {result[0][0]} duplicate transaction_ids! This violates the PRIMARY KEY constraint.")


def check_value_ranges(conn):
    """Check value ranges and basic statistics for all columns"""
    print("\n=== Value Ranges and Basic Statistics ===")
    query = """
    SELECT 
        MIN(transaction_id) as min_trans_id,
        MAX(transaction_id) as max_trans_id,
        MIN(user_id) as min_user_id,
        MAX(user_id) as max_user_id,
        MIN(quantity) as min_quantity,
        MAX(quantity) as max_quantity,
        MIN(unit_price) as min_price,
        MAX(unit_price) as max_price,
        MIN(transaction_timestamp) as earliest_date,
        MAX(transaction_timestamp) as latest_date
    FROM sales_transactions;
    """
    
    result = conn.execute(query).fetchall()
    
    # Transpose the results for better readability
    headers = [
        "min_trans_id", "max_trans_id", 
        "min_user_id", "max_user_id", 
        "min_quantity", "max_quantity", 
        "min_price", "max_price", 
        "earliest_date", "latest_date"
    ]
    
    # Create a list of [header, value] pairs for better display
    transposed = [[headers[i], result[0][i]] for i in range(len(headers))]
    
    print(tabulate(transposed, headers=["Metric", "Value"], tablefmt="psql"))


def check_product_distribution(conn):
    """Check product distribution statistics"""
    print("\n=== Product Distribution ===")
    query = """
    SELECT 
        product_name,
        COUNT(*) as total_sales,
        AVG(quantity) as avg_quantity,
        AVG(unit_price) as avg_price
    FROM sales_transactions
    GROUP BY product_name
    ORDER BY total_sales DESC;
    """
    
    result = conn.execute(query).fetchall()
    
    print(tabulate(
        result, 
        headers=["product_name", "total_sales", "avg_quantity", "avg_price"], 
        tablefmt="psql",
        floatfmt=".2f"
    ))
    
    # Calculate percentage distribution
    total_records = sum(row[1] for row in result)
    print("\n=== Sales Distribution by Product ===")
    distribution = [(row[0], row[1], (row[1]/total_records)*100) for row in result]
    
    print(tabulate(
        distribution, 
        headers=["product_name", "total_sales", "percentage (%)"], 
        tablefmt="psql",
        floatfmt=[None, None, ".2f"]
    ))


def main():
    """Main function to execute all verification checks"""
    try:
        # Check if tabulate is installed, otherwise inform user
        try:
            import tabulate
        except ImportError:
            print("The 'tabulate' package is not installed. Installing it will improve output formatting.")
            print("You can install it with: pip install tabulate")
        
        print("🔍 Starting data verification process...")
        conn = connect_to_db()
        
        # Run all verification checks
        check_duplicates(conn)
        check_value_ranges(conn)
        check_product_distribution(conn)
        
        print("\n✅ Data verification completed successfully!")
        conn.close()
        
    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

