"""
Apache Spark Basics with PySpark
From Krish Naik's Big Data Course

Introduction to Apache Spark for big data processing.
"""

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
    from pyspark.sql.functions import col, sum as spark_sum, avg, count, max as spark_max
    SPARK_AVAILABLE = True
except ImportError:
    print("PySpark not installed. Install with: pip install pyspark")
    SPARK_AVAILABLE = False

def create_spark_session():
    """
    Create and configure Spark session
    """
    if not SPARK_AVAILABLE:
        return None
        
    spark = SparkSession.builder \
        .appName("BigDataCourse") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print(f"Spark Version: {spark.version}")
    print(f"Spark UI available at: {spark.sparkContext.uiWebUrl}")
    
    return spark

def spark_basics(spark):
    """
    Demonstrate basic Spark operations
    """
    if not spark:
        return
        
    print("\n=== Spark Basics ===")
    
    # Create sample data
    data = [
        ("Alice", 25, "Engineer", 75000),
        ("Bob", 30, "Manager", 85000),
        ("Charlie", 35, "Engineer", 95000),
        ("Diana", 28, "Analyst", 72000),
        ("Eve", 32, "Manager", 88000)
    ]
    
    columns = ["Name", "Age", "Role", "Salary"]
    
    # Create DataFrame
    df = spark.createDataFrame(data, columns)
    
    print("Sample DataFrame:")
    df.show()
    
    print("Schema:")
    df.printSchema()
    
    print("DataFrame info:")
    print(f"Rows: {df.count()}")
    print(f"Columns: {len(df.columns)}")
    
    return df

def dataframe_operations(spark, df):
    """
    Demonstrate DataFrame operations
    """
    if not spark or not df:
        return
        
    print("\n=== DataFrame Operations ===")
    
    # Select specific columns
    print("Select Name and Salary:")
    df.select("Name", "Salary").show()
    
    # Filter data
    print("Engineers only:")
    df.filter(col("Role") == "Engineer").show()
    
    print("High earners (Salary > 80000):")
    df.filter(col("Salary") > 80000).show()
    
    # Add new column
    print("Add bonus column (10% of salary):")
    df_with_bonus = df.withColumn("Bonus", col("Salary") * 0.1)
    df_with_bonus.show()
    
    # Groupby operations
    print("Average salary by role:")
    df.groupBy("Role").agg(
        avg("Salary").alias("avg_salary"),
        count("*").alias("count")
    ).show()
    
    # Sort data
    print("Sort by salary (descending):")
    df.orderBy(col("Salary").desc()).show()

def spark_sql_operations(spark, df):
    """
    Demonstrate Spark SQL operations
    """
    if not spark or not df:
        return
        
    print("\n=== Spark SQL Operations ===")
    
    # Register DataFrame as temporary view
    df.createOrReplaceTempView("employees")
    
    # SQL queries
    print("SQL: Select all employees:")
    spark.sql("SELECT * FROM employees").show()
    
    print("SQL: Average salary by role:")
    spark.sql("""
        SELECT Role, 
               AVG(Salary) as avg_salary,
               COUNT(*) as count
        FROM employees 
        GROUP BY Role
        ORDER BY avg_salary DESC
    """).show()
    
    print("SQL: Young high earners:")
    spark.sql("""
        SELECT Name, Age, Salary 
        FROM employees 
        WHERE Age < 30 AND Salary > 70000
    """).show()

def file_operations(spark):
    """
    Demonstrate file I/O operations with Spark
    """
    if not spark:
        return
        
    print("\n=== File Operations ===")
    
    # Create sample CSV data
    csv_data = [
        ("Product", "Category", "Price", "Quantity"),
        ("Laptop", "Electronics", 1000.0, 50),
        ("Mouse", "Electronics", 25.0, 200),
        ("Desk", "Furniture", 300.0, 30),
        ("Chair", "Furniture", 150.0, 75),
        ("Phone", "Electronics", 800.0, 100)
    ]
    
    # Create DataFrame
    columns = csv_data[0]
    data = csv_data[1:]
    
    df_products = spark.createDataFrame(data, columns)
    
    print("Products DataFrame:")
    df_products.show()
    
    # Save as Parquet (more efficient for Spark)
    parquet_path = "/tmp/products.parquet"
    df_products.write.mode("overwrite").parquet(parquet_path)
    print(f"Saved to Parquet: {parquet_path}")
    
    # Read from Parquet
    df_from_parquet = spark.read.parquet(parquet_path)
    print("Loaded from Parquet:")
    df_from_parquet.show()
    
    # Save as CSV
    csv_path = "/tmp/products_spark.csv"
    df_products.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)
    print(f"Saved to CSV: {csv_path}")

def performance_optimization(spark):
    """
    Demonstrate performance optimization techniques
    """
    if not spark:
        return
        
    print("\n=== Performance Optimization ===")
    
    # Create larger dataset for demonstration
    import random
    
    large_data = []
    categories = ["Electronics", "Furniture", "Clothing", "Books", "Sports"]
    
    for i in range(10000):
        large_data.append((
            f"Product_{i}",
            random.choice(categories),
            round(random.uniform(10, 1000), 2),
            random.randint(1, 100)
        ))
    
    df_large = spark.createDataFrame(large_data, ["Product", "Category", "Price", "Quantity"])
    
    print(f"Large dataset size: {df_large.count()} rows")
    
    # Cache frequently used DataFrame
    df_large.cache()
    print("DataFrame cached in memory")
    
    # Partitioning
    print(f"Default partitions: {df_large.rdd.getNumPartitions()}")
    
    # Repartition for better performance
    df_repartitioned = df_large.repartition(4, "Category")
    print(f"After repartitioning: {df_repartitioned.rdd.getNumPartitions()}")
    
    # Aggregation with optimization
    print("Category-wise statistics:")
    df_repartitioned.groupBy("Category").agg(
        count("*").alias("product_count"),
        avg("Price").alias("avg_price"),
        spark_sum(col("Price") * col("Quantity")).alias("total_value")
    ).show()

def spark_configuration_tips():
    """
    Display Spark configuration tips for big data
    """
    print("\n=== Spark Configuration Tips ===")
    
    tips = [
        "1. Memory Configuration:",
        "   - Set spark.executor.memory based on available RAM",
        "   - Use spark.executor.memoryFraction (default 0.6)",
        "",
        "2. Parallelism:",
        "   - Set spark.sql.shuffle.partitions (default 200)",
        "   - Aim for 2-3 tasks per CPU core",
        "",
        "3. Serialization:",
        "   - Use Kryo serializer: spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "",
        "4. Storage:",
        "   - Use Parquet format for better performance",
        "   - Consider data compression (snappy, gzip)",
        "",
        "5. Caching:",
        "   - Cache frequently accessed DataFrames",
        "   - Use appropriate storage levels (MEMORY_ONLY, MEMORY_AND_DISK)",
        "",
        "Example Spark Submit:",
        "spark-submit --master local[*] \\",
        "  --conf spark.executor.memory=4g \\",
        "  --conf spark.driver.memory=2g \\",
        "  --conf spark.sql.shuffle.partitions=100 \\",
        "  your_script.py"
    ]
    
    for tip in tips:
        print(tip)

if __name__ == "__main__":
    print("=== Apache Spark with PySpark ===")
    
    if not SPARK_AVAILABLE:
        print("Please install PySpark to run this module:")
        print("pip install pyspark")
        exit(1)
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Basic operations
        df = spark_basics(spark)
        
        # DataFrame operations
        dataframe_operations(spark, df)
        
        # SQL operations
        spark_sql_operations(spark, df)
        
        # File operations
        file_operations(spark)
        
        # Performance optimization
        performance_optimization(spark)
        
        # Configuration tips
        spark_configuration_tips()
        
    finally:
        # Stop Spark session
        if spark:
            spark.stop()
            print("\nSpark session stopped.")