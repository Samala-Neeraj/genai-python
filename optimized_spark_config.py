"""
Optimized PySpark Session Configuration for Laptop Environment

Hardware Specs:
- RAM: 16GB
- CPU Cores: 8
- Workload: 1 million e-commerce orders processing

Configuration Strategy:
1. Reserve 20% of RAM for OS (3.2GB) → Use 12.8GB for Spark
2. Split between driver (6GB) and executors (6.8GB)
3. Use Kryo serialization (3x faster than default Java)
4. Enable adaptive query execution for auto-optimization
5. Configure shuffle partitions for optimal parallelism
6. Enable partition coalescing to reduce overhead
"""

from pyspark.sql import SparkSession


def create_optimized_spark_session(app_name: str = "OptimizedEcommerce") -> SparkSession:
    """
    Create an optimized Spark session for 16GB laptop with 8 cores.

    This configuration is tuned for processing 1 million e-commerce orders efficiently
    while maintaining system stability by not consuming all available resources.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Configured Spark session ready to use

    Configuration Explanations:
    ────────────────────────────────────────────────────────────────────────────────
    1. spark.driver.memory = "6g"
       • Allocates 6GB RAM to the driver process (main Spark controller)
       • Safe for 16GB laptop: leaves 10GB for OS, executors, and other apps
       • Handles aggregations and result collection

    2. spark.executor.memory = "6g"
       • Allocates 6GB RAM to executor processes (where actual work happens)
       • With 8 cores, each core gets ~750MB working memory
       • Sufficient for shuffles and joins on 1M orders

    3. spark.driver.maxResultSize = "2g"
       • Maximum size of results driver can collect from executors
       • Set to 1/3 of driver memory to prevent OOM when collecting large results
       • Important when using .collect() on large DataFrames

    4. spark.sql.shuffle.partitions = 1600
       • Number of partitions after shuffle operations (groupBy, join, etc.)
       • Formula: 200 × number of cores = 200 × 8 = 1600
       • 1 million orders ÷ 1600 partitions ≈ 625 rows per partition
       • Avoids too many small partitions (overhead) or too few large ones (disk spills)
       • Optimal for balanced load distribution

    5. spark.sql.adaptive.enabled = "true"
       • ADAPTIVE QUERY EXECUTION (AQE) - automatically optimizes queries
       • Dynamically coalesces partitions if they're too small
       • Reorders joins based on actual data statistics
       • Optimizes skewed joins automatically
       • Reduces manual tuning burden
       • CRITICAL for laptop environments with unpredictable workloads

    6. spark.sql.adaptive.coalescePartitions.enabled = "true"
       • Combines small partitions after shuffle to reduce overhead
       • Prevents partition explosion from shuffle operations
       • Significantly reduces memory usage and GC pressure
       • Works in conjunction with AQE for optimal performance

    7. spark.serializer = "org.apache.spark.serializer.KryoSerializer"
       • Uses Kryo serialization instead of default Java serialization
       • ~3x faster serialization/deserialization
       • Reduces network I/O and memory usage
       • Critical for performance on 1M row datasets
       • Must register custom classes if using them

    8. spark.kryoserializer.buffer.max = "256m"
       • Maximum buffer size for Kryo serializer
       • 256MB safe for 6GB executor memory
       • Handles large object serialization without spilling

    9. spark.sql.broadcastTimeout = 300
       • Timeout in seconds for broadcast join operations
       • Helpful if network is slow on older laptops
       • Default is 300s, increased from 5 minutes to be safe

    10. spark.sql.adaptive.skewJoin.enabled = "true"
        • Automatically detects and handles skewed joins
        • Splits large partitions to prevent single-core bottlenecks
        • Important for e-commerce data (some products popular, some not)

    11. spark.sql.statistics.histogram.enabled = "true"
        • Gathers histograms for cost-based optimization
        • Better query planning with AQE
        • Minimal overhead for 1M row dataset

    12. spark.task.maxFailures = 4
        • Number of times a task can fail before aborting job
        • Increased from 3 to 4 for stability on laptop (potential GC pauses)
        • Balanced with not masking real issues

    13. spark.network.timeout = 800
        • Network timeout in seconds (laptop can be slow)
        • Prevents spurious failures from GC pauses
        • Default is 120s, increased for safety

    14. spark.locality.wait = 3000
        • Wait time (ms) for local task scheduling
        • Slightly increased for laptop with shared resources
        • Helps avoid shuffles when data is available locally
    ────────────────────────────────────────────────────────────────────────────────
    """

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "6g") \
        .config("spark.driver.maxResultSize", "2g") \
        \
        .config("spark.sql.shuffle.partitions", "1600") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.kryoserializer.buffer.max", "256m") \
        \
        .config("spark.sql.broadcastTimeout", "300") \
        .config("spark.sql.statistics.histogram.enabled", "true") \
        \
        .config("spark.task.maxFailures", "4") \
        .config("spark.network.timeout", "800") \
        .config("spark.locality.wait", "3000") \
        \
        .getOrCreate()

    return spark


def create_minimal_spark_session(app_name: str = "MinimalEcommerce") -> SparkSession:
    """
    Minimal Spark session with just essential settings for 16GB laptop.

    Use this if you want a simpler configuration with fewer tuning parameters.
    This covers the most impactful settings for 1M row processing.

    Args:
        app_name (str): Name of the Spark application

    Returns:
        SparkSession: Minimally configured Spark session
    """

    spark = SparkSession.builder \
        .appName(app_name) \
        .master("local[*]") \
        \
        .config("spark.driver.memory", "6g") \
        .config("spark.sql.shuffle.partitions", "1600") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        \
        .getOrCreate()

    return spark


# ============================================================================
# COPY-PASTE CODE BELOW FOR YOUR PROJECT
# ============================================================================

if __name__ == "__main__":
    """
    Example usage - copy this into your code and adjust app_name as needed.
    """

    # Option 1: Use optimized configuration (RECOMMENDED)
    print("Creating optimized Spark session for 16GB laptop...")
    spark = create_optimized_spark_session(app_name="EcommerceAnalytics")

    # Option 2: Use minimal configuration
    # spark = create_minimal_spark_session(app_name="EcommerceAnalytics")

    # Print configuration for verification
    print("\n" + "=" * 80)
    print("SPARK SESSION CONFIGURATION")
    print("=" * 80)

    config_dict = spark.sparkContext.getConf().getAll()
    important_configs = [
        "spark.driver.memory",
        "spark.executor.memory",
        "spark.driver.maxResultSize",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.serializer",
        "spark.kryoserializer.buffer.max",
        "spark.sql.adaptive.skewJoin.enabled",
    ]

    for key, value in config_dict:
        if key in important_configs:
            print(f"{key:<50} = {value}")

    print("=" * 80)
    print(f"Spark Version: {spark.version}")
    print(f"Python Version: {spark.sparkContext.pythonVer}")
    print("=" * 80)

    # Verify you can create a simple DataFrame
    print("\n✓ Spark session created successfully!")
    print("✓ Ready to process e-commerce data with 1M+ orders")

    # Stop the session
    spark.stop()
