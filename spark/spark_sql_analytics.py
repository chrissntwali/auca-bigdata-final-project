"""
spark_sql_analytics.py
-----------------------
Spark SQL queries for PART 2 of the project.

Demonstrates using Spark SQL to perform complex analytical queries on
DataFrames derived from the raw JSON files -- satisfying the requirement
to "perform complex queries on dataframes derived from your JSON files."

Run from the spark/ directory:
    python3 spark_sql_analytics.py
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = os.environ.get("DATA_DIR", "../data")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "../spark/output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_spark():
    return (
        SparkSession.builder
        .appName("EcommerceSparkSQL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print("Loading DataFrames...")
    users = spark.read.json(os.path.join(DATA_DIR, "users.json"))
    products = spark.read.json(os.path.join(DATA_DIR, "products.json"))
    transactions = spark.read.json(os.path.join(DATA_DIR, "transactions.json"))
    sessions = spark.read.json(os.path.join(DATA_DIR, "sessions_*.json"))

    # Flatten transaction items for SQL queries
    txn_items = transactions.select(
        "transaction_id", "user_id", "timestamp", "total", "payment_method", "status",
        F.explode("items").alias("item")
    ).select(
        "transaction_id", "user_id", "timestamp", "total", "payment_method", "status",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.unit_price").alias("unit_price"),
        F.col("item.subtotal").alias("subtotal"),
    )

    # Register SQL views
    users.createOrReplaceTempView("users")
    products.createOrReplaceTempView("products")
    transactions.createOrReplaceTempView("transactions")
    sessions.createOrReplaceTempView("sessions")
    txn_items.createOrReplaceTempView("txn_items")

    # ------------------------------------------------------------------
    # SQL Query 1: Revenue by category with product count and avg price
    # Simulates what would be a cross-system query in production:
    # transaction line items (notionally from MongoDB) joined with
    # product catalog (notionally from MongoDB products collection)
    # ------------------------------------------------------------------
    print("\n=== SQL Query 1: Revenue by Category ===")
    q1 = spark.sql("""
        SELECT
            p.category_id,
            COUNT(DISTINCT p.product_id)        AS product_count,
            SUM(t.quantity)                      AS total_units_sold,
            ROUND(SUM(t.subtotal), 2)            AS total_revenue,
            ROUND(AVG(p.base_price), 2)          AS avg_product_price,
            ROUND(SUM(t.subtotal) /
                  COUNT(DISTINCT t.transaction_id), 2) AS avg_order_value
        FROM txn_items t
        JOIN products p ON t.product_id = p.product_id
        GROUP BY p.category_id
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    q1.show(truncate=False)
    q1.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(OUTPUT_DIR, "sql_revenue_by_category")
    )

    # ------------------------------------------------------------------
    # SQL Query 2: Top 10 users by total spending with order frequency
    # Simulates joining user profiles (MongoDB users collection) with
    # transaction history (MongoDB transactions collection) in Spark
    # ------------------------------------------------------------------
    print("\n=== SQL Query 2: Top 10 Users by Spending ===")
    q2 = spark.sql("""
        SELECT
            u.user_id,
            u.geo_data.country                   AS country,
            u.geo_data.state                     AS state,
            COUNT(DISTINCT t.transaction_id)     AS order_count,
            ROUND(SUM(t.total), 2)               AS total_spent,
            ROUND(AVG(t.total), 2)               AS avg_order_value,
            MIN(t.timestamp)                     AS first_purchase,
            MAX(t.timestamp)                     AS last_purchase
        FROM transactions t
        JOIN users u ON t.user_id = u.user_id
        WHERE t.status = 'completed'
        GROUP BY u.user_id, u.geo_data.country, u.geo_data.state
        ORDER BY total_spent DESC
        LIMIT 10
    """)
    q2.show(truncate=False)
    q2.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(OUTPUT_DIR, "sql_top_users")
    )

    # ------------------------------------------------------------------
    # SQL Query 3: Device/referrer conversion funnel
    # Simulates querying session behavioral data (notionally from HBase)
    # loaded into Spark DataFrames -- cross-system integration demo
    # ------------------------------------------------------------------
    print("\n=== SQL Query 3: Conversion Rate by Device and Referrer ===")
    q3 = spark.sql("""
        SELECT
            device_profile.type                  AS device_type,
            referrer,
            COUNT(*)                             AS total_sessions,
            SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END)
                                                 AS converted_sessions,
            ROUND(
                SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END)
                * 100.0 / COUNT(*), 2)           AS conversion_rate_pct,
            ROUND(AVG(duration_seconds), 0)      AS avg_session_duration_sec
        FROM sessions
        GROUP BY device_profile.type, referrer
        ORDER BY conversion_rate_pct DESC
    """)
    q3.show(truncate=False)
    q3.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(OUTPUT_DIR, "sql_conversion_funnel")
    )

    # ------------------------------------------------------------------
    # SQL Query 4: Monthly revenue trend with month-over-month growth
    # ------------------------------------------------------------------
    print("\n=== SQL Query 4: Monthly Revenue Trend ===")
    q4 = spark.sql("""
        SELECT
            DATE_FORMAT(CAST(timestamp AS TIMESTAMP), 'yyyy-MM') AS month,
            COUNT(DISTINCT transaction_id)       AS order_count,
            COUNT(DISTINCT user_id)              AS unique_buyers,
            ROUND(SUM(total), 2)                 AS revenue,
            ROUND(AVG(total), 2)                 AS avg_order_value
        FROM transactions
        WHERE status = 'completed'
        GROUP BY DATE_FORMAT(CAST(timestamp AS TIMESTAMP), 'yyyy-MM')
        ORDER BY month
    """)
    q4.show(truncate=False)
    q4.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(OUTPUT_DIR, "sql_monthly_revenue")
    )

    print(f"\nSpark SQL complete. Outputs written to {OUTPUT_DIR}/")
    spark.stop()


if __name__ == "__main__":
    main()
