"""
Silver Layer Transformation
============================
Reads raw Bronze Parquet → cleans, deduplicates, enriches → writes Silver Parquet.

What happens here:
  1. Cast data types correctly
  2. Handle nulls
  3. Deduplicate on txn_id
  4. Validate (drop bad rows)
  5. Enrich with derived columns
  6. Write to data/silver partitioned by txn_date
"""

import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
    BooleanType, TimestampType
)


# ── Load config ───────────────────────────────────────────────────────────────

def load_config(config_path: str = "configs/config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark_session(app_name: str, log_level: str = "WARN") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")   # keep low for local runs
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark


# ── Schema enforcement ────────────────────────────────────────────────────────

def enforce_schema(df):
    """
    Cast all columns to their correct types.
    Bronze data is read from Parquet so types are mostly preserved,
    but we enforce explicitly to be safe.
    """
    df = (
        df
        .withColumn("txn_id",            F.col("txn_id").cast(StringType()))
        .withColumn("txn_timestamp",      F.to_timestamp("txn_timestamp", "yyyy-MM-dd HH:mm:ss"))
        .withColumn("txn_date",           F.to_date("txn_date", "yyyy-MM-dd"))
        .withColumn("txn_hour",           F.col("txn_hour").cast(IntegerType()))
        .withColumn("txn_type",           F.col("txn_type").cast(StringType()))
        .withColumn("sender_vpa",         F.col("sender_vpa").cast(StringType()))
        .withColumn("receiver_vpa",       F.col("receiver_vpa").cast(StringType()))
        .withColumn("sender_bank",        F.col("sender_bank").cast(StringType()))
        .withColumn("receiver_bank",      F.col("receiver_bank").cast(StringType()))
        .withColumn("amount",             F.col("amount").cast(DoubleType()))
        .withColumn("currency",           F.col("currency").cast(StringType()))
        .withColumn("status",             F.col("status").cast(StringType()))
        .withColumn("failure_reason",     F.col("failure_reason").cast(StringType()))
        .withColumn("merchant_category",  F.col("merchant_category").cast(StringType()))
        .withColumn("merchant_name",      F.col("merchant_name").cast(StringType()))
        .withColumn("utr_number",         F.col("utr_number").cast(StringType()))
        .withColumn("device_type",        F.col("device_type").cast(StringType()))
        .withColumn("app_name",           F.col("app_name").cast(StringType()))
        .withColumn("is_anomaly",         F.col("is_anomaly").cast(BooleanType()))
    )
    return df


# ── Null handling ─────────────────────────────────────────────────────────────

def handle_nulls(df):
    """
    - Critical columns (txn_id, amount, status): drop rows where null
    - Optional columns (merchant_category, failure_reason): fill with 'UNKNOWN'
    """
    # Drop rows where critical fields are null
    df = df.dropna(subset=["txn_id", "txn_timestamp", "amount", "status"])

    # Fill optional nulls
    df = df.fillna({
        "failure_reason":    "N/A",
        "merchant_category": "UNKNOWN",
        "merchant_name":     "UNKNOWN",
    })

    return df


# ── Deduplication ─────────────────────────────────────────────────────────────

def deduplicate(df):
    """
    Remove duplicate transactions based on txn_id.
    Keep the first occurrence ordered by txn_timestamp.
    
    Interview talking point:
      In real UPI systems, duplicate txn_ids can occur due to retry logic
      or network failures. Deduplication is critical before any aggregation.
    """
    from pyspark.sql.window import Window

    window = Window.partitionBy("txn_id").orderBy("txn_timestamp")
    df = (
        df
        .withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )
    return df


# ── Validation ────────────────────────────────────────────────────────────────

def validate(df):
    """
    Drop rows that fail basic business validation rules.
    Log counts before and after for auditability.
    """
    before = df.count()

    df = df.filter(
        # Amount must be positive
        (F.col("amount") > 0) &
        # Amount must not exceed UPI limit (₹10 lakh per transaction)
        (F.col("amount") <= 1000000) &
        # Status must be one of known values
        (F.col("status").isin(["SUCCESS", "FAILED", "PENDING"])) &
        # txn_type must be valid
        (F.col("txn_type").isin(["P2P", "P2M"])) &
        # VPA must contain '@'
        (F.col("sender_vpa").contains("@")) &
        (F.col("receiver_vpa").contains("@"))
    )

    after = df.count()
    print(f"  Validation: dropped {before - after} invalid rows ({before} → {after})")
    return df


# ── Enrichment ────────────────────────────────────────────────────────────────

def enrich(df):
    """
    Add derived columns that make downstream Gold aggregations easier
    and give analysts richer data to work with.
    
    These columns are the ones Power BI and Snowflake SQL will use directly.
    """
    df = (
        df
        # Flag high value transactions (above ₹50,000)
        .withColumn("is_high_value",
            F.when(F.col("amount") >= 50000, True).otherwise(False)
        )

        # Flag odd hour transactions (between 1 AM and 5 AM)
        .withColumn("is_odd_hour",
            F.when(F.col("txn_hour").between(1, 5), True).otherwise(False)
        )

        # Amount bucket for spend analysis
        .withColumn("amount_bucket",
            F.when(F.col("amount") < 500,    "micro")
             .when(F.col("amount") < 5000,   "small")
             .when(F.col("amount") < 50000,  "medium")
             .otherwise("large")
        )

        # Whether sender and receiver are on same bank (internal transfer)
        .withColumn("is_same_bank",
            F.when(F.col("sender_bank") == F.col("receiver_bank"), True).otherwise(False)
        )

        # Day of week (useful for weekly spend patterns)
        .withColumn("day_of_week",
            F.date_format("txn_date", "EEEE")
        )

        # Week number (useful for weekly aggregations)
        .withColumn("week_of_year",
            F.weekofyear("txn_date")
        )

        # Processing timestamp (when Silver layer ran)
        .withColumn("processed_at",
            F.current_timestamp()
        )
    )
    return df


# ── Writer ────────────────────────────────────────────────────────────────────

def write_silver(df, output_dir: str):
    """
    Write Silver Parquet partitioned by txn_date.
    Overwrite the partition if it already exists (idempotent).
    """
    (
        df.write
        .mode("overwrite")
        .partitionBy("txn_date")
        .parquet(output_dir)
    )
    print(f"  Silver layer written to: {output_dir}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_silver(config_path: str = "configs/config.yaml"):
    config = load_config(config_path)

    bronze_path = config["paths"]["bronze"]
    silver_path = config["paths"]["silver"]
    app_name    = config["spark"]["app_name"]
    log_level   = config["spark"]["log_level"]

    print("\n── Silver layer starting ──")
    print(f"  Reading from : {bronze_path}")
    print(f"  Writing to   : {silver_path}")

    spark = create_spark_session(app_name, log_level)

    # Read all Bronze Parquet
    df = spark.read.parquet(bronze_path)
    print(f"  Bronze rows loaded: {df.count()}")

    # Run transformations in sequence
    df = enforce_schema(df)
    df = handle_nulls(df)
    df = deduplicate(df)
    df = validate(df)
    df = enrich(df)

    print(f"  Final Silver rows: {df.count()}")
    print(f"  Silver columns: {len(df.columns)}")

    write_silver(df, silver_path)

    spark.stop()
    print("── Silver layer complete ──\n")


if __name__ == "__main__":
    run_silver()




