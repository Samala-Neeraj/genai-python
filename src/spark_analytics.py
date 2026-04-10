"""
PySpark analytics module for analyzing e-commerce data.

This module provides the SalesAnalytics class for performing distributed
analytics on large-scale e-commerce datasets using Apache Spark.
"""

import logging
from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    avg,
    desc,
    asc,
    year,
    month,
    lag,
    round as spark_round,
    lit,
    coalesce,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class SalesAnalytics:
    """
    Analytics engine for e-commerce sales data using PySpark.

    This class provides distributed analytics capabilities including:
    - Loading Parquet files efficiently
    - Analyzing top customers by revenue
    - Category-level sales metrics
    - Time-series trend analysis with month-over-month growth
    - Window functions for advanced aggregations
    """

    def __init__(self, app_name: str = "SalesAnalytics", memory: str = "4g") -> None:
        """
        Initialize the SalesAnalytics engine.

        Args:
            app_name (str): Name of the Spark application. Default: "SalesAnalytics".
            memory (str): Memory allocation per executor. Default: "4g".
        """
        self.app_name = app_name
        self.memory = memory
        self.spark: Optional[SparkSession] = None
        logger.info(f"Initialized SalesAnalytics with app_name='{app_name}', memory='{memory}'")

    def create_spark_session(self) -> SparkSession:
        """
        Create and configure an optimized Spark session for 16GB laptop with 8 cores.

        Configuration optimized for processing 1 million e-commerce orders:
        - 6GB driver memory (leaves 10GB for OS on 16GB laptop)
        - 1600 shuffle partitions (200 × 8 cores for optimal parallelism)
        - Adaptive Query Execution (AQE) enabled for runtime optimization
        - Kryo serialization for 3x faster data transfer
        - Coalesce shuffle partitions to reduce overhead
        - Skew join detection for unbalanced data
        - Optimized timeouts and locality settings for laptop environment

        Returns:
            SparkSession: Optimized Spark session for analytics.
        """
        logger.info(f"Creating optimized Spark session: {self.app_name}")

        self.spark = (
            SparkSession.builder
            .appName(self.app_name)
            .master("local[*]")
            # Memory configuration: 6GB driver, 6GB executor (safe for 16GB laptop)
            .config("spark.driver.memory", "6g")
            .config("spark.executor.memory", "6g")
            .config("spark.driver.maxResultSize", "2g")
            # Adaptive Query Execution (AQE) - Optimizes queries at runtime
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.dynamicPartitionPruning.enabled", "true")
            # Shuffle partitions: 200 × 8 cores = 1600 (optimal for 1M orders)
            .config("spark.sql.shuffle.partitions", "1600")
            # Kryo serialization for better performance (3x faster than Java)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "256m")
            # Query optimization
            .config("spark.sql.statistics.histogram.enabled", "true")
            .config("spark.sql.broadcastTimeout", "300")
            # Broadcast optimization
            .config("spark.sql.autoBroadcastJoinThreshold", "100mb")
            # Resilience and timeouts for laptop environment
            .config("spark.task.maxFailures", "4")
            .config("spark.network.timeout", "800")
            .config("spark.locality.wait", "3000")
            .getOrCreate()
        )

        logger.info("✓ Spark session created successfully (optimized for 16GB laptop)")
        logger.info(f"  - Driver Memory: 6GB | Executor Memory: 6GB")
        logger.info(f"  - Shuffle Partitions: 1600 (200 × 8 cores)")
        logger.info(f"  - Adaptive Query Execution: Enabled")
        logger.info(f"  - Kryo Serialization: Enabled (3x faster)")
        logger.info(f"  - Skew Join Detection: Enabled")

        return self.spark

    def load_parquet(self, path: Path) -> DataFrame:
        """
        Load a Parquet file into a Spark DataFrame.

        Parquet is a columnar format that provides:
        - Efficient compression
        - Fast schema inference
        - Predicate pushdown optimization

        Args:
            path (Path): Path to the Parquet file or directory.

        Returns:
            DataFrame: Spark DataFrame loaded from Parquet.

        Raises:
            FileNotFoundError: If the Parquet file doesn't exist.
            Exception: If Spark session is not initialized.
        """
        if self.spark is None:
            raise RuntimeError("Spark session not initialized. Call create_spark_session() first.")

        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {path}")

        logger.info(f"Loading Parquet file: {path}")
        df = self.spark.read.parquet(str(path))
        logger.info(f"✓ Loaded {df.count():,} rows with {len(df.columns)} columns")

        return df

    def top_customers_by_revenue(
        self,
        orders_df: DataFrame,
        products_df: DataFrame,
        n: int = 10,
    ) -> DataFrame:
        """
        Find top N customers by total revenue spent.

        This method:
        - Joins orders with products to get pricing information
        - Calculates total revenue per order (quantity * price)
        - Aggregates revenue by customer
        - Ranks customers by total spending
        - Returns top N results

        Args:
            orders_df (DataFrame): Orders table with columns: order_id, customer_id, product_id, quantity
            products_df (DataFrame): Products table with columns: product_id, price
            n (int): Number of top customers to return. Default: 10.

        Returns:
            DataFrame: Top customers with columns:
                - customer_id: Unique customer identifier
                - total_revenue: Total amount spent by customer
                - order_count: Number of orders placed
                - avg_order_value: Average revenue per order
        """
        logger.info(f"Calculating top {n} customers by revenue...")

        # Join orders with products to get prices
        orders_with_prices = orders_df.join(
            products_df.select("product_id", "price"),
            on="product_id",
            how="inner"
        )

        # Calculate order revenue (quantity * price)
        orders_with_revenue = orders_with_prices.withColumn(
            "order_revenue",
            spark_round(col("quantity") * col("price"), 2)
        )

        # Aggregate by customer
        customer_stats = orders_with_revenue.groupBy("customer_id").agg(
            spark_sum("order_revenue").alias("total_revenue"),
            count("order_id").alias("order_count"),
            spark_round(spark_sum("order_revenue") / count("order_id"), 2).alias("avg_order_value"),
        ).orderBy(desc("total_revenue"))

        # Get top N
        top_customers = customer_stats.limit(n)

        logger.info(f"✓ Found top {n} customers by revenue")
        return top_customers

    def sales_by_category(
        self,
        orders_df: DataFrame,
        products_df: DataFrame,
    ) -> DataFrame:
        """
        Analyze sales metrics grouped by product category.

        This method:
        - Joins orders with products to get category information
        - Calculates revenue per order (quantity * price)
        - Groups by category and aggregates metrics
        - Returns category-level insights

        Args:
            orders_df (DataFrame): Orders table with columns: order_id, customer_id, product_id, quantity
            products_df (DataFrame): Products table with columns: product_id, category, price

        Returns:
            DataFrame: Sales by category with columns:
                - category: Product category name
                - total_revenue: Total revenue from category
                - total_units_sold: Total units sold in category
                - order_count: Number of orders in category
                - avg_unit_price: Average price per unit in category
                - avg_order_value: Average revenue per order in category
        """
        logger.info("Calculating sales metrics by product category...")

        # Join orders with products to get categories and prices
        orders_with_categories = orders_df.join(
            products_df.select("product_id", "category", "price"),
            on="product_id",
            how="inner"
        )

        # Calculate revenue per order
        orders_with_revenue = orders_with_categories.withColumn(
            "order_revenue",
            spark_round(col("quantity") * col("price"), 2)
        )

        # Group by category and aggregate
        category_sales = orders_with_revenue.groupBy("category").agg(
            spark_sum("order_revenue").alias("total_revenue"),
            spark_sum("quantity").alias("total_units_sold"),
            count("order_id").alias("order_count"),
            spark_round(
                spark_sum("order_revenue") / spark_sum("quantity"),
                2
            ).alias("avg_unit_price"),
            spark_round(
                spark_sum("order_revenue") / count("order_id"),
                2
            ).alias("avg_order_value"),
        ).orderBy(desc("total_revenue"))

        logger.info("✓ Calculated sales metrics by category")
        return category_sales

    def monthly_trends(
        self,
        orders_df: DataFrame,
        products_df: DataFrame,
    ) -> DataFrame:
        """
        Analyze monthly sales trends with month-over-month growth.

        This method:
        - Joins orders with products to get pricing
        - Extracts year and month from order_date
        - Calculates monthly revenue
        - Uses Window functions to calculate month-over-month growth percentage
        - Returns chronologically sorted results

        Formula for MoM Growth:
        ((Current Month Revenue - Previous Month Revenue) / Previous Month Revenue) * 100

        Args:
            orders_df (DataFrame): Orders table with columns: order_date, product_id, quantity
            products_df (DataFrame): Products table with columns: product_id, price

        Returns:
            DataFrame: Monthly trends with columns:
                - year: Year of the sales
                - month: Month of the sales (1-12)
                - monthly_revenue: Total revenue for the month
                - order_count: Number of orders in the month
                - mom_growth_pct: Month-over-month growth percentage
                  (None for first month)
        """
        logger.info("Calculating monthly sales trends with MoM growth...")

        # Join orders with products to get prices
        orders_with_prices = orders_df.join(
            products_df.select("product_id", "price"),
            on="product_id",
            how="inner"
        )

        # Calculate revenue per order
        orders_with_revenue = orders_with_prices.withColumn(
            "order_revenue",
            spark_round(col("quantity") * col("price"), 2)
        )

        # Extract year and month
        orders_with_date = orders_with_revenue.withColumn(
            "year",
            year(col("order_date"))
        ).withColumn(
            "month",
            month(col("order_date"))
        )

        # Group by year and month
        monthly_sales = orders_with_date.groupBy("year", "month").agg(
            spark_sum("order_revenue").alias("monthly_revenue"),
            count("order_id").alias("order_count"),
        )

        # Define window for calculating previous month revenue
        window_spec = Window.orderBy("year", "month")

        # Calculate month-over-month growth using lag function
        monthly_with_growth = monthly_sales.withColumn(
            "prev_month_revenue",
            lag("monthly_revenue").over(window_spec)
        ).withColumn(
            "mom_growth_pct",
            spark_round(
                ((col("monthly_revenue") - col("prev_month_revenue"))
                 / col("prev_month_revenue") * lit(100)),
                2
            )
        ).drop("prev_month_revenue")

        # Sort by year and month
        result = monthly_with_growth.orderBy("year", "month")

        logger.info("✓ Calculated monthly trends with MoM growth")
        return result

    def stop_spark_session(self) -> None:
        """
        Stop the Spark session and release resources.

        This should be called at the end of analytics to clean up.
        """
        if self.spark is not None:
            logger.info("Stopping Spark session...")
            self.spark.stop()
            self.spark = None
            logger.info("✓ Spark session stopped")


def main() -> None:
    """
    Main entry point for PySpark analytics demonstration.
    """
    from config import DATA_RAW_DIR

    logger.info("=" * 80)
    logger.info("E-commerce Sales Analytics Pipeline")
    logger.info("=" * 80)

    # Initialize analytics engine
    analytics = SalesAnalytics(app_name="EcommerceSalesAnalytics", memory="4g")

    try:
        # Create Spark session
        spark = analytics.create_spark_session()

        # Load data from Parquet files
        logger.info("\nLoading data from Parquet files...")
        customers_df = analytics.load_parquet(DATA_RAW_DIR / "customers.parquet")
        products_df = analytics.load_parquet(DATA_RAW_DIR / "products.parquet")
        orders_df = analytics.load_parquet(DATA_RAW_DIR / "orders.parquet")

        # Display loaded data info
        logger.info(f"\nData Summary:")
        logger.info(f"  - Customers: {customers_df.count():,} rows")
        logger.info(f"  - Products: {products_df.count():,} rows")
        logger.info(f"  - Orders: {orders_df.count():,} rows")

        # Analysis 1: Top Customers by Revenue
        logger.info("\n" + "=" * 80)
        logger.info("Analysis 1: Top 10 Customers by Revenue")
        logger.info("=" * 80)
        top_customers = analytics.top_customers_by_revenue(orders_df, products_df, n=10)
        top_customers.show(10, truncate=False)

        # Analysis 2: Sales by Category
        logger.info("\n" + "=" * 80)
        logger.info("Analysis 2: Sales by Product Category")
        logger.info("=" * 80)
        category_sales = analytics.sales_by_category(orders_df, products_df)
        category_sales.show(truncate=False)

        # Analysis 3: Monthly Trends
        logger.info("\n" + "=" * 80)
        logger.info("Analysis 3: Monthly Sales Trends with MoM Growth")
        logger.info("=" * 80)
        monthly_trends = analytics.monthly_trends(orders_df, products_df)
        monthly_trends.show(20, truncate=False)

        logger.info("\n" + "=" * 80)
        logger.info("✓ Analytics pipeline completed successfully!")
        logger.info("=" * 80)

    except Exception as e:
        logger.error(f"Error during analytics: {str(e)}")
        logger.exception("Full traceback:")
        raise
    finally:
        analytics.stop_spark_session()


if __name__ == "__main__":
    main()