"""
Run analytics on e-commerce data using PySpark.

This script:
1. Imports SalesAnalytics from src.spark_analytics
2. Creates Spark session
3. Loads customers.parquet, products.parquet, orders.parquet from data/raw/
4. Runs all three analytics methods (top_customers, sales_by_category, monthly_trends)
5. Displays results using .show()
6. Prints execution time for each operation
7. Stops Spark session at the end
"""

import logging
import sys
import time
from pathlib import Path

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from spark_analytics import SalesAnalytics
from config import DATA_RAW_DIR

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def print_section(title: str) -> None:
    """
    Print a formatted section header.

    Args:
        title (str): Section title to display.
    """
    print("\n" + "=" * 100)
    print(f" {title}")
    print("=" * 100)


def print_timing(operation: str, elapsed_time: float) -> None:
    """
    Print formatted timing information.

    Args:
        operation (str): Name of the operation.
        elapsed_time (float): Time elapsed in seconds.
    """
    print(f"✓ {operation} completed in {elapsed_time:.2f} seconds")


def main() -> None:
    """
    Main function to run analytics pipeline.
    """
    print_section("E-COMMERCE ANALYTICS ENGINE - PySpark")

    # Track total execution time
    total_start_time = time.time()

    # Initialize analytics engine
    logger.info("Initializing SalesAnalytics engine...")
    analytics = SalesAnalytics(app_name="EcommerceAnalyticsEngine", memory="4g")

    try:
        # Step 1: Create Spark session
        print_section("STEP 1: CREATING SPARK SESSION")
        session_start = time.time()
        spark = analytics.create_spark_session()
        session_time = time.time() - session_start
        print_timing("Spark session creation", session_time)

        # Step 2: Load data from Parquet files
        print_section("STEP 2: LOADING DATA")
        
        # Load customers
        logger.info(f"Loading customers from {DATA_RAW_DIR / 'customers.parquet'}...")
        load_customers_start = time.time()
        customers_df = analytics.load_parquet(DATA_RAW_DIR / "customers.parquet")
        load_customers_time = time.time() - load_customers_start
        print_timing("Customers loaded", load_customers_time)

        # Load products
        logger.info(f"Loading products from {DATA_RAW_DIR / 'products.parquet'}...")
        load_products_start = time.time()
        products_df = analytics.load_parquet(DATA_RAW_DIR / "products.parquet")
        load_products_time = time.time() - load_products_start
        print_timing("Products loaded", load_products_time)

        # Load orders
        logger.info(f"Loading orders from {DATA_RAW_DIR / 'orders.parquet'}...")
        load_orders_start = time.time()
        orders_df = analytics.load_parquet(DATA_RAW_DIR / "orders.parquet")
        load_orders_time = time.time() - load_orders_start
        print_timing("Orders loaded", load_orders_time)

        # Display data summary
        print_section("DATA SUMMARY")
        print(f"Customers:  {customers_df.count():>12,} rows  |  {len(customers_df.columns):>3} columns")
        print(f"Products:   {products_df.count():>12,} rows  |  {len(products_df.columns):>3} columns")
        print(f"Orders:     {orders_df.count():>12,} rows  |  {len(orders_df.columns):>3} columns")

        # Step 3: Run Analytics - Top Customers by Revenue
        print_section("ANALYSIS 1: TOP 10 CUSTOMERS BY REVENUE")
        analysis1_start = time.time()
        top_customers_df = analytics.top_customers_by_revenue(orders_df, products_df, n=10)
        analysis1_time = time.time() - analysis1_start
        
        print(f"\nTop 10 Customers by Total Revenue:")
        print("-" * 100)
        top_customers_df.show(10, truncate=False)
        print_timing("Top customers analysis", analysis1_time)

        # Step 4: Run Analytics - Sales by Category
        print_section("ANALYSIS 2: SALES BY PRODUCT CATEGORY")
        analysis2_start = time.time()
        category_sales_df = analytics.sales_by_category(orders_df, products_df)
        analysis2_time = time.time() - analysis2_start
        
        print(f"\nSales Metrics by Product Category:")
        print("-" * 100)
        category_sales_df.show(truncate=False)
        print_timing("Category sales analysis", analysis2_time)

        # Step 5: Run Analytics - Monthly Trends
        print_section("ANALYSIS 3: MONTHLY SALES TRENDS WITH MOM GROWTH")
        analysis3_start = time.time()
        monthly_trends_df = analytics.monthly_trends(orders_df, products_df)
        analysis3_time = time.time() - analysis3_start
        
        print(f"\nMonthly Sales Trends with Month-over-Month Growth:")
        print("-" * 100)
        monthly_trends_df.show(20, truncate=False)
        print_timing("Monthly trends analysis", analysis3_time)

        # Step 6: Summary Statistics
        print_section("ANALYSIS SUMMARY")
        
        # Top customer metrics
        top_customer_revenue = top_customers_df.select("total_revenue").rdd.flatMap(lambda x: x).collect()
        if top_customer_revenue:
            print(f"\nTop Customer by Revenue:")
            print(f"  • Revenue Range: ${min(top_customer_revenue):.2f} - ${max(top_customer_revenue):.2f}")
            print(f"  • Average of Top 10: ${sum(top_customer_revenue) / len(top_customer_revenue):.2f}")

        # Category metrics
        category_totals = category_sales_df.select("total_revenue", "total_units_sold").rdd.flatMap(lambda x: x).collect()
        if len(category_totals) >= 2:
            revenues = category_totals[::2]  # Every other element is revenue
            units = category_totals[1::2]     # Every other element is units
            print(f"\nCategory Sales Overview:")
            print(f"  • Total Categories: {category_sales_df.count()}")
            print(f"  • Total Revenue: ${sum(revenues):.2f}")
            print(f"  • Total Units Sold: {int(sum(units)):,}")
            print(f"  • Average Revenue per Category: ${sum(revenues) / len(revenues):.2f}")

        # Monthly metrics
        monthly_total_revenue = monthly_trends_df.select("monthly_revenue").rdd.flatMap(lambda x: x).collect()
        if monthly_total_revenue:
            print(f"\nMonthly Metrics:")
            print(f"  • Total Months: {monthly_trends_df.count()}")
            print(f"  • Total Revenue (All Months): ${sum(monthly_total_revenue):.2f}")
            print(f"  • Average Monthly Revenue: ${sum(monthly_total_revenue) / len(monthly_total_revenue):.2f}")

        # Step 7: Execution Time Summary
        total_elapsed_time = time.time() - total_start_time
        
        print_section("EXECUTION TIME SUMMARY")
        print(f"\nOperation Timings:")
        print(f"  • Spark Session Creation:     {session_time:>8.2f}s")
        print(f"  • Load Customers Data:        {load_customers_time:>8.2f}s")
        print(f"  • Load Products Data:         {load_products_time:>8.2f}s")
        print(f"  • Load Orders Data:           {load_orders_time:>8.2f}s")
        print(f"  • Top Customers Analysis:     {analysis1_time:>8.2f}s")
        print(f"  • Category Sales Analysis:    {analysis2_time:>8.2f}s")
        print(f"  • Monthly Trends Analysis:    {analysis3_time:>8.2f}s")
        print(f"  {'─' * 40}")
        print(f"  • TOTAL EXECUTION TIME:       {total_elapsed_time:>8.2f}s")

        logger.info("Analytics pipeline completed successfully!")

    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        logger.error("Make sure the Parquet files exist in data/raw/")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Error during analytics: {str(e)}")
        logger.exception("Full traceback:")
        sys.exit(1)

    finally:
        # Step 8: Stop Spark session
        logger.info("Stopping Spark session...")
        analytics.stop_spark_session()
        logger.info("Analytics pipeline finished.")


if __name__ == "__main__":
    main()
