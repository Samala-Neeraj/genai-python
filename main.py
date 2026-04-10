"""
Main entry point for the e-commerce data pipeline project.

This script:
1. Imports SyntheticDataGenerator from src.data_generator
2. Imports config from src.config
3. Generates customers, products, and orders
4. Saves each as Parquet files in data/raw/ folder
5. Prints generation time and file sizes
6. Uses proper error handling with try/except
"""

import logging
import sys
import time
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from data_generator import SyntheticDataGenerator
from config import DATA_RAW_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def format_file_size(size_bytes: int) -> str:
    """
    Convert bytes to human-readable file size format.

    Args:
        size_bytes (int): Size in bytes.

    Returns:
        str: Formatted size string (e.g., "1.5 MB").
    """
    for unit in ["B", "KB", "MB", "GB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"


def get_file_size(filepath: Path) -> int:
    """
    Get the size of a file in bytes.

    Args:
        filepath (Path): Path to the file.

    Returns:
        int: File size in bytes.
    """
    if filepath.exists():
        return filepath.stat().st_size
    return 0


def main() -> None:
    """
    Main function to generate synthetic e-commerce data.

    This function:
    - Creates a SyntheticDataGenerator instance
    - Generates customers, products, and orders
    - Saves each as Parquet files
    - Prints generation statistics
    - Handles errors gracefully
    """
    logger.info("=" * 80)
    logger.info("E-commerce Synthetic Data Generation Pipeline")
    logger.info("=" * 80)

    try:
        # Step 1: Create data generator
        logger.info("\n[Step 1/5] Initializing SyntheticDataGenerator...")
        generator = SyntheticDataGenerator(
            num_customers=100000,
            num_products=10000,
            num_orders=1000000,
            seed=42,  # Set seed for reproducibility
        )
        logger.info("✓ Generator initialized successfully")

        # Step 2: Generate all data
        logger.info("\n[Step 2/5] Generating synthetic data...")
        start_time = time.time()
        
        customers_df, products_df, orders_df = generator.generate_all()
        
        generation_time = time.time() - start_time
        logger.info(f"✓ Data generation completed in {generation_time:.2f} seconds")

        # Step 3: Display data statistics
        logger.info("\n[Step 3/5] Data Statistics:")
        logger.info(f"  - Customers: {len(customers_df):,}")
        logger.info(f"    • Age range: {customers_df['age'].min()}-{customers_df['age'].max()} years")
        logger.info(f"    • Mean age: {customers_df['age'].mean():.1f} years")
        logger.info(f"  - Products: {len(products_df):,}")
        logger.info(f"    • Price range: ${products_df['price'].min():.2f}-${products_df['price'].max():.2f}")
        logger.info(f"    • Mean price: ${products_df['price'].mean():.2f}")
        logger.info(f"    • Average stock: {products_df['stock'].mean():.1f} units")
        logger.info(f"  - Orders: {len(orders_df):,}")
        logger.info(f"    • Average quantity per order: {orders_df['quantity'].mean():.2f}")
        logger.info(f"    • Order statuses: {dict(orders_df['order_status'].value_counts())}")

        # Step 4: Save to Parquet files
        logger.info("\n[Step 4/5] Saving data to Parquet files...")
        save_start_time = time.time()
        
        customers_path = DATA_RAW_DIR / "customers.parquet"
        products_path = DATA_RAW_DIR / "products.parquet"
        orders_path = DATA_RAW_DIR / "orders.parquet"
        
        generator.save_to_parquet(customers_df, customers_path)
        generator.save_to_parquet(products_df, products_path)
        generator.save_to_parquet(orders_df, orders_path)
        
        save_time = time.time() - save_start_time
        logger.info(f"✓ Data saved successfully in {save_time:.2f} seconds")

        # Step 5: Display file sizes
        logger.info("\n[Step 5/5] File Sizes:")
        
        customers_size = get_file_size(customers_path)
        products_size = get_file_size(products_path)
        orders_size = get_file_size(orders_path)
        total_size = customers_size + products_size + orders_size
        
        logger.info(f"  - customers.parquet: {format_file_size(customers_size)}")
        logger.info(f"  - products.parquet: {format_file_size(products_size)}")
        logger.info(f"  - orders.parquet: {format_file_size(orders_size)}")
        logger.info(f"  - Total size: {format_file_size(total_size)}")

        # Summary
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Total Generation Time: {generation_time:.2f} seconds")
        logger.info(f"Total Save Time: {save_time:.2f} seconds")
        logger.info(f"Total Execution Time: {generation_time + save_time:.2f} seconds")
        logger.info(f"Output Directory: {DATA_RAW_DIR}")
        logger.info("=" * 80)
        logger.info("✓ Data generation pipeline completed successfully!")
        logger.info("=" * 80)

    except FileNotFoundError as e:
        logger.error(f"File not found error: {str(e)}")
        logger.error("Make sure the data/raw/ directory exists.")
        sys.exit(1)

    except MemoryError:
        logger.error("Insufficient memory to generate all data.")
        logger.error("Try reducing num_customers, num_products, or num_orders.")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error during data generation: {str(e)}")
        logger.exception("Full traceback:")
        sys.exit(1)


if __name__ == "__main__":
    main()
