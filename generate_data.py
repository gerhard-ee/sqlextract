#!/usr/bin/env python3
"""
Generate 200,000 rows of realistic test data and insert into DuckDB database.
Uses batch inserts (1000 rows per batch) for better performance.
"""

import duckdb
import random
from datetime import datetime, timedelta
import time
from tqdm import tqdm
import sys
import logging
from typing import List, Tuple, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_generation.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("data_generator")

# Constants
DATABASE_FILE = "test_db.duckdb"
TABLE_NAME = "sales_transactions"
TOTAL_ROWS = 200_000
BATCH_SIZE = 1_000
MIN_USER_ID = 1000
MAX_USER_ID = 9999
MIN_QUANTITY = 1
MAX_QUANTITY = 20
MIN_PRICE = 0.99
MAX_PRICE = 999.99

# Realistic product names
PRODUCT_NAMES = [
    "Premium Wireless Headphones",
    "Ergonomic Office Chair",
    "Ultra HD Smart TV 55\"",
    "Ceramic Coffee Mug Set",
    "Professional Chef Knife",
    "Organic Cotton T-Shirt",
    "Fitness Tracking Watch",
    "Bluetooth Portable Speaker",
    "Memory Foam Mattress",
    "Stainless Steel Water Bottle",
    "Robot Vacuum Cleaner",
    "Solar Power Bank",
    "Leather Wallet Slim",
    "Noise Cancelling Earbuds",
    "Professional DSLR Camera",
    "Smart Home Security System",
    "Adjustable Dumbbell Set",
    "Bamboo Cutting Board",
    "Cast Iron Skillet Set",
    "Air Purifier HEPA Filter"
]

def connect_to_database() -> duckdb.DuckDBPyConnection:
    """Connect to the DuckDB database."""
    try:
        conn = duckdb.connect(DATABASE_FILE)
        logger.info(f"Successfully connected to database: {DATABASE_FILE}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        raise

def generate_timestamps(start_years_ago: int = 3) -> datetime:
    """Generate a random timestamp within the last 3 years."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * start_years_ago)
    
    # Calculate time difference in seconds
    time_diff = (end_date - start_date).total_seconds()
    
    # Generate a random timestamp
    random_seconds = random.randint(0, int(time_diff))
    return start_date + timedelta(seconds=random_seconds)

def generate_batch_data(start_id: int) -> List[Tuple[Any, ...]]:
    """Generate a batch of data rows."""
    batch_data = []
    
    for i in range(BATCH_SIZE):
        transaction_id = start_id + i
        user_id = random.randint(MIN_USER_ID, MAX_USER_ID)
        product_name = random.choice(PRODUCT_NAMES)
        quantity = random.randint(MIN_QUANTITY, MAX_QUANTITY)
        unit_price = round(random.uniform(MIN_PRICE, MAX_PRICE), 2)
        transaction_timestamp = generate_timestamps()
        
        batch_data.append((
            transaction_id,
            user_id,
            product_name,
            quantity,
            unit_price,
            transaction_timestamp
        ))
    
    return batch_data

def insert_batch(conn: duckdb.DuckDBPyConnection, batch_data: List[Tuple[Any, ...]]) -> None:
    """Insert a batch of data using parameterized query."""
    try:
        # Prepare SQL statement with parameterized query
        query = f"""
        INSERT INTO {TABLE_NAME} (
            transaction_id, user_id, product_name, quantity, unit_price, transaction_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?)
        """
        
        # Execute batch insert
        conn.executemany(query, batch_data)
        
    except Exception as e:
        logger.error(f"Error inserting batch data: {e}")
        raise

def verify_data(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify the inserted data by running a few queries."""
    try:
        # Count rows
        row_count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        logger.info(f"Total rows in {TABLE_NAME}: {row_count}")
        
        # Sample data
        logger.info("Sample of inserted data:")
        sample = conn.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5").fetchall()
        for row in sample:
            logger.info(row)
            
        # Aggregate statistics
        avg_price = conn.execute(f"SELECT AVG(unit_price) FROM {TABLE_NAME}").fetchone()[0]
        max_quantity = conn.execute(f"SELECT MAX(quantity) FROM {TABLE_NAME}").fetchone()[0]
        unique_products = conn.execute(f"SELECT COUNT(DISTINCT product_name) FROM {TABLE_NAME}").fetchone()[0]
        
        logger.info(f"Average price: ${avg_price:.2f}")
        logger.info(f"Maximum quantity: {max_quantity}")
        logger.info(f"Number of unique products: {unique_products}")
        
    except Exception as e:
        logger.error(f"Error verifying data: {e}")

def main() -> None:
    """Main function to generate and insert data."""
    start_time = time.time()
    logger.info(f"Starting data generation: {TOTAL_ROWS} rows with batch size {BATCH_SIZE}")
    
    try:
        # Connect to the database
        conn = connect_to_database()
        
        # Create a transaction for the entire operation
        with conn.cursor() as cursor:
            # Generate and insert data in batches
            num_batches = TOTAL_ROWS // BATCH_SIZE
            
            # Use tqdm for progress indication
            for batch_idx in tqdm(range(num_batches), desc="Inserting batches"):
                start_id = batch_idx * BATCH_SIZE + 1
                batch_data = generate_batch_data(start_id)
                insert_batch(conn, batch_data)
                
                # Commit every 10 batches to avoid huge transactions
                if batch_idx % 10 == 0:
                    conn.commit()
            
            # Final commit
            conn.commit()
        
        # Verify the data
        verify_data(conn)
        
        # Close connection
        conn.close()
        
        elapsed_time = time.time() - start_time
        logger.info(f"Data generation completed successfully in {elapsed_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Data generation failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

