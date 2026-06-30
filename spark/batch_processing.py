"""
batch_processing.py
--------------------
PySpark batch jobs for PART 2 of the project:

  1. Data cleaning & normalization of the raw JSON files into clean DataFrames
  2. "Frequently bought together" product affinity (market-basket style,
     based on transaction co-occurrence)
  3. Cohort analysis: group users by registration month, track spending in
     subsequent months

Run:
    pip install pyspark
    python batch_processing.py
(Requires Java 11+ on PATH. No Hadoop/cluster needed -- runs in local mode.)
"""
import os
import itertools
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

DATA_DIR = os.environ.get("DATA_DIR", "../data")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "../spark/output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_spark():
    return (
        SparkSession.builder
        .appName("EcommerceBatchAnalytics")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )


# ---------------------------------------------------------------------
# 1. Clean & normalize raw data
# ---------------------------------------------------------------------
def load_and_clean(spark):
    print("Loading raw JSON into Spark DataFrames...")

    users = spark.read.json(os.path.join(DATA_DIR, "users.json"))
    products = spark.read.json(os.path.join(DATA_DIR, "products.json"))
    categories = spark.read.json(os.path.join(DATA_DIR, "categories.json"))
    transactions = spark.read.json(os.path.join(DATA_DIR, "transactions.json"))
    sessions = spark.read.json(os.path.join(DATA_DIR, "sessions_*.json"))

    # --- Cleaning: standardize timestamp columns to TimestampType ---
    users_clean = (
        users
        .withColumn("registration_date", F.to_timestamp("registration_date"))
        .withColumn("last_active", F.to_timestamp("last_active"))
        .dropDuplicates(["user_id"])
        .na.drop(subset=["user_id"])  # user_id is the required key; drop malformed rows
    )

    products_clean = (
        products
        .withColumn("creation_date", F.to_timestamp("creation_date"))
        .withColumn("current_stock", F.coalesce(F.col("current_stock"), F.lit(0)))
        .withColumn("is_active", F.coalesce(F.col("is_active"), F.lit(False)))
        .dropDuplicates(["product_id"])
        .na.drop(subset=["product_id"])
    )

    transactions_clean = (
        transactions
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)))
        .filter(F.col("total").isNotNull() & (F.col("total") >= 0))  # drop invalid/negative totals
        .dropDuplicates(["transaction_id"])
    )

    sessions_clean = (
        sessions
        .withColumn("start_time", F.to_timestamp("start_time"))
        .withColumn("end_time", F.to_timestamp("end_time"))
        .filter(F.col("duration_seconds") > 0)  # drop zero/negative-duration sessions as malformed
        .dropDuplicates(["session_id"])
    )

    print(f"  users:        {users_clean.count():,} clean rows")
    print(f"  products:     {products_clean.count():,} clean rows")
    print(f"  categories:   {categories.count():,} rows")
    print(f"  transactions: {transactions_clean.count():,} clean rows")
    print(f"  sessions:     {sessions_clean.count():,} clean rows")

    return {
        "users": users_clean,
        "products": products_clean,
        "categories": categories,
        "transactions": transactions_clean,
        "sessions": sessions_clean,
    }


# ---------------------------------------------------------------------
# 2. Product affinity: "users who bought X also bought Y"
# ---------------------------------------------------------------------
def product_affinity(spark, transactions_df, top_n=20):
    """
    Market-basket-style co-occurrence: for each transaction, explode its
    items, then self-join on transaction_id to find product pairs bought
    together. This demonstrates Spark's distributed join + aggregation
    over the full transaction set.
    """
    print("\nComputing product affinity (frequently bought together)...")

    items = transactions_df.select(
        "transaction_id",
        F.explode("items").alias("item")
    ).select(
        "transaction_id",
        F.col("item.product_id").alias("product_id")
    ).dropDuplicates(["transaction_id", "product_id"])

    # Self-join to get all product pairs within the same transaction
    a = items.alias("a")
    b = items.alias("b")
    pairs = (
        a.join(b, on="transaction_id")
        .filter(F.col("a.product_id") < F.col("b.product_id"))  # avoid duplicate/reverse pairs
        .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
        .agg(F.count("*").alias("co_purchase_count"))
        .orderBy(F.desc("co_purchase_count"))
    )

    top_pairs = pairs.limit(top_n)
    top_pairs.show(top_n, truncate=False)
    top_pairs.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(OUTPUT_DIR, "product_affinity")
    )
    return top_pairs


# ---------------------------------------------------------------------
# 3. Cohort analysis: registration month vs. spending in subsequent months
# ---------------------------------------------------------------------
def cohort_analysis(spark, users_df, transactions_df):
    """
    Groups users by registration month (their "cohort"), then measures
    total spend per cohort in each subsequent calendar month -- a standard
    retention/value cohort analysis.
    """
    print("\nRunning cohort analysis (registration month x spending month)...")

    cohorts = users_df.select(
        "user_id",
        F.date_format("registration_date", "yyyy-MM").alias("cohort_month")
    )

    txn_months = transactions_df.select(
        "user_id",
        "total",
        F.date_format("timestamp", "yyyy-MM").alias("txn_month")
    )

    joined = txn_months.join(cohorts, on="user_id", how="inner")

    cohort_table = (
        joined.groupBy("cohort_month", "txn_month")
        .agg(
            F.sum("total").alias("total_revenue"),
            F.countDistinct("user_id").alias("active_users"),
            F.count("*").alias("order_count"),
        )
        .orderBy("cohort_month", "txn_month")
    )

    cohort_table.show(30, truncate=False)
    cohort_table.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(OUTPUT_DIR, "cohort_analysis")
    )
    return cohort_table


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    dfs = load_and_clean(spark)

    for name, df in dfs.items():
        df.createOrReplaceTempView(name)

    product_affinity(spark, dfs["transactions"])
    cohort_analysis(spark, dfs["users"], dfs["transactions"])

    print(f"\nBatch processing complete. Outputs written to {OUTPUT_DIR}/")
    spark.stop()


if __name__ == "__main__":
    main()
