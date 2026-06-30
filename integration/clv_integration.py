"""
clv_integration.py
-------------------
PART 3: Integrated Analytical Query — Customer Lifetime Value (CLV) Estimation

Business Question:
    Which customers are our most valuable, and how does their browsing
    engagement (session behavior) relate to their purchasing value?

Data Sources Combined:
    - MongoDB (transactions collection): financial data — total spend,
      order count, average order value per user
    - Sessions JSON (simulating HBase user_sessions table): behavioral
      data — session frequency, average session duration, conversion rate,
      preferred device, referrer source per user

Processing Steps:
    1. Pull transaction aggregates per user from MongoDB
    2. Load session behavioral aggregates per user from JSON files via Spark
       (in production this would be a Spark-HBase connector scan)
    3. Join both datasets in Spark on user_id
    4. Compute CLV score and segment customers into 4 tiers
    5. Output results for visualization

Why these technologies:
    - MongoDB: natural fit for per-user financial aggregates (embedded
      purchase_summary already computed at load time)
    - Spark: handles the large-scale session join (100k sessions across
      2k users) efficiently with distributed processing; also provides
      the SQL interface for the final segmentation query
    - HBase (simulated here via JSON): would provide efficient range-scan
      retrieval of all sessions for a given user via the user_id prefix
      row key design

Run from the integration/ directory:
    python3 clv_integration.py
"""
import os
import json
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
DB_NAME = "ecommerce_analytics"
DATA_DIR = os.environ.get("DATA_DIR", "../data")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "../integration/output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_spark():
    return (
        SparkSession.builder
        .appName("CLV_Integration")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


def fetch_mongo_user_financials():
    """
    Step 1: Pull per-user financial aggregates from MongoDB.
    Uses the pre-computed purchase_summary embedded on each user document
    (the 'computed pattern' from our schema design) for O(1) reads.
    """
    print("Fetching user financial data from MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    cursor = db.users.find(
        {"purchase_summary.total_orders": {"$gt": 0}},
        {
            "_id": 0,
            "user_id": 1,
            "geo_data.country": 1,
            "geo_data.state": 1,
            "registration_date": 1,
            "purchase_summary.total_orders": 1,
            "purchase_summary.total_spent": 1,
            "purchase_summary.avg_order_value": 1,
            "purchase_summary.favorite_category": 1,
        }
    )

    records = []
    for doc in cursor:
        ps = doc.get("purchase_summary", {})
        geo = doc.get("geo_data", {})
        records.append({
            "user_id": doc["user_id"],
            "country": geo.get("country"),
            "state": geo.get("state"),
            "total_orders": ps.get("total_orders", 0),
            "total_spent": ps.get("total_spent", 0.0),
            "avg_order_value": ps.get("avg_order_value", 0.0),
            "favorite_category": ps.get("favorite_category"),
        })

    client.close()
    print(f"  -> Fetched {len(records):,} users with purchase history from MongoDB")
    return records


def compute_session_behavioral_metrics(spark):
    """
    Step 2: Compute per-user behavioral metrics from session data.
    In production: Spark-HBase connector scans user_sessions table
    using user_id prefix row key. Here: reads from JSON files directly.
    """
    print("Computing session behavioral metrics from session data (HBase simulation)...")

    sessions = spark.read.json(os.path.join(DATA_DIR, "sessions_*.json"))

    behavioral = sessions.groupBy("user_id").agg(
        F.count("*").alias("total_sessions"),
        F.round(F.avg("duration_seconds"), 0).alias("avg_session_duration_sec"),
        F.round(
            F.sum(F.when(F.col("conversion_status") == "converted", 1).otherwise(0))
            * 100.0 / F.count("*"), 2
        ).alias("personal_conversion_rate_pct"),
        F.first("device_profile.type").alias("primary_device"),
        F.first("referrer").alias("primary_referrer"),
        F.sum(F.when(F.col("conversion_status") == "converted", 1).otherwise(0))
            .alias("converted_sessions"),
    )

    print(f"  -> Computed behavioral metrics for {behavioral.count():,} users")
    return behavioral


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Step 1: MongoDB financial data
    mongo_records = fetch_mongo_user_financials()
    financials_df = spark.createDataFrame(mongo_records)
    print(f"  Financials DataFrame: {financials_df.count():,} rows")

    # Step 2: Session behavioral metrics (HBase simulation via Spark)
    behavioral_df = compute_session_behavioral_metrics(spark)

    # Step 3: Join in Spark on user_id
    print("\nJoining financial and behavioral data in Spark...")
    combined = financials_df.join(behavioral_df, on="user_id", how="left")
    combined = combined.fillna({
        "total_sessions": 0,
        "avg_session_duration_sec": 0,
        "personal_conversion_rate_pct": 0.0,
        "converted_sessions": 0,
    })

    # Step 4: Compute CLV score
    # CLV formula: weighted combination of:
    #   - total_spent (60% weight) — primary financial signal
    #   - order_frequency (20% weight) — loyalty signal
    #   - engagement_score (20% weight) — behavioral signal from HBase/sessions
    print("Computing CLV scores...")
    combined = combined.withColumn(
        "engagement_score",
        F.round(
            (F.col("total_sessions") * 0.4) +
            (F.col("avg_session_duration_sec") / 60 * 0.3) +
            (F.col("personal_conversion_rate_pct") * 0.3),
            2
        )
    )

    # Normalize each component to 0-100 scale for fair weighting
    max_spent = combined.agg(F.max("total_spent")).collect()[0][0]
    max_orders = combined.agg(F.max("total_orders")).collect()[0][0]
    max_engagement = combined.agg(F.max("engagement_score")).collect()[0][0]

    combined = combined.withColumn(
        "clv_score",
        F.round(
            (F.col("total_spent") / max_spent * 100 * 0.60) +
            (F.col("total_orders") / max_orders * 100 * 0.20) +
            (F.col("engagement_score") / max_engagement * 100 * 0.20),
            2
        )
    )

    # Step 5: Segment into 4 tiers using percentiles
    percentiles = combined.approxQuantile("clv_score", [0.25, 0.50, 0.75], 0.01)
    p25, p50, p75 = percentiles

    combined = combined.withColumn(
        "clv_tier",
        F.when(F.col("clv_score") >= p75, "Premium")
         .when(F.col("clv_score") >= p50, "High Value")
         .when(F.col("clv_score") >= p25, "Medium Value")
         .otherwise("Low Value")
    )

    combined.createOrReplaceTempView("clv_results")

    # Summary by tier
    print("\n=== CLV Tier Summary ===")
    tier_summary = spark.sql("""
        SELECT
            clv_tier,
            COUNT(*)                            AS user_count,
            ROUND(AVG(clv_score), 2)            AS avg_clv_score,
            ROUND(AVG(total_spent), 2)          AS avg_total_spent,
            ROUND(AVG(total_orders), 1)         AS avg_orders,
            ROUND(AVG(total_sessions), 1)       AS avg_sessions,
            ROUND(AVG(personal_conversion_rate_pct), 2) AS avg_conversion_rate
        FROM clv_results
        GROUP BY clv_tier
        ORDER BY avg_clv_score DESC
    """)
    tier_summary.show(truncate=False)

    # Top 15 highest-value customers
    print("\n=== Top 15 Highest CLV Customers ===")
    top_customers = spark.sql("""
        SELECT
            user_id, country, clv_tier, clv_score,
            total_spent, total_orders, total_sessions,
            personal_conversion_rate_pct, primary_device
        FROM clv_results
        ORDER BY clv_score DESC
        LIMIT 15
    """)
    top_customers.show(truncate=False)

    # Geographic CLV distribution
    print("\n=== CLV by Country (Top 10) ===")
    geo_clv = spark.sql("""
        SELECT
            country,
            COUNT(*)                    AS user_count,
            ROUND(AVG(clv_score), 2)    AS avg_clv_score,
            ROUND(SUM(total_spent), 2)  AS total_revenue
        FROM clv_results
        GROUP BY country
        ORDER BY total_revenue DESC
        LIMIT 10
    """)
    geo_clv.show(truncate=False)

    # Save outputs
    combined.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(OUTPUT_DIR, "clv_full_results")
    )
    tier_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(OUTPUT_DIR, "clv_tier_summary")
    )

    print(f"\nIntegration complete. CLV scores computed for "
          f"{combined.count():,} users.")
    print(f"Outputs written to {OUTPUT_DIR}/")
    print(f"\nCLV score thresholds: p25={p25:.1f}, p50={p50:.1f}, p75={p75:.1f}")

    spark.stop()


if __name__ == "__main__":
    main()
