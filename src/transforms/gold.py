"""
Gold Layer Transformation
==========================
Reads cleaned Silver Parquet → creates aggregated analytical tables → writes Gold Parquet.

4 tables produced:
  1. hourly_txn_summary      — volume, amount, success/failure rate by hour
  2. bank_failure_analysis   — failure rates per bank
  3. merchant_spend_analysis — spend breakdown by merchant category
  4. anomaly_summary         — flagged suspicious transactions by bank and date
"""

import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F



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
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        .config("spark.sql.warehouse.dir", "spark-warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(log_level)
    return spark


# ── Gold table 1: Hourly transaction summary ──────────────────────────────────

def hourly_txn_summary(df):
    """
    Aggregates transaction volume and amount by date and hour.
    Used for: time-series charts, peak hour analysis, SLA monitoring.

    Interview talking point:
      This is the primary table Power BI uses for the hourly volume chart.
      success_rate and failure_rate are pre-computed here so Power BI
      doesn't need to calculate them at query time.
    """
    return (
        df.groupBy("txn_date", "txn_hour")
        .agg(
            F.count("txn_id").alias("total_txns"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_txns"),
            F.sum(F.when(F.col("status") == "FAILED",  1).otherwise(0)).alias("failed_txns"),
            F.sum(F.when(F.col("status") == "PENDING", 1).otherwise(0)).alias("pending_txns"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.round(F.max("amount"), 2).alias("max_amount"),
        )
        .withColumn("success_rate", F.round(F.col("successful_txns") / F.col("total_txns") * 100, 2))
        .withColumn("failure_rate", F.round(F.col("failed_txns")     / F.col("total_txns") * 100, 2))
        .withColumn("processed_at", F.current_timestamp())
        .orderBy("txn_date", "txn_hour")
    )


# ── Gold table 2: Bank failure analysis ───────────────────────────────────────

def bank_failure_analysis(df):
    """
    Failure rate per sending bank.
    Used for: identifying problematic banks, SLA breach alerts.

    Interview talking point:
      In real UPI systems, certain banks have higher timeout rates
      during peak hours. This table feeds the failure rate alert in Power BI.
    """
    return (
        df.groupBy("sender_bank")
        .agg(
            F.count("txn_id").alias("total_txns"),
            F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_txns"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .withColumn("failure_rate_pct", F.round(F.col("failed_txns") / F.col("total_txns") * 100, 2))
        .withColumn("processed_at", F.current_timestamp())
        .orderBy(F.desc("failure_rate_pct"))
    )


# ── Gold table 3: Merchant category spend analysis ────────────────────────────

def merchant_spend_analysis(df):
    """
    Spend breakdown by merchant category (P2M transactions only).
    Used for: consumer spend trends, category-wise revenue analysis.

    Interview talking point:
      Filtered to P2M only because P2P transactions don't have
      a merchant_category — including them would skew averages.
    """
    return (
        df.filter(F.col("txn_type") == "P2M")
        .groupBy("merchant_category")
        .agg(
            F.count("txn_id").alias("total_txns"),
            F.round(F.sum("amount"),  2).alias("total_spend"),
            F.round(F.avg("amount"),  2).alias("avg_spend"),
            F.round(F.max("amount"),  2).alias("max_spend"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_txns"),
        )
        .withColumn("success_rate", F.round(F.col("successful_txns") / F.col("total_txns") * 100, 2))
        .withColumn("processed_at", F.current_timestamp())
        .orderBy(F.desc("total_spend"))
    )


# ── Gold table 4: Anomaly summary ─────────────────────────────────────────────

def anomaly_summary(df):
    """
    Summary of flagged anomalous transactions by date and bank.
    Used for: fraud monitoring dashboard, alert triggers.

    Interview talking point:
      is_anomaly was set in the simulator as ground truth.
      In production, the Silver layer would flag anomalies using
      z-score or IQR logic before they reach the Gold layer.
    """
    return (
        df.filter(F.col("is_anomaly"))
        .groupBy("txn_date", "sender_bank")
        .agg(
            F.count("txn_id").alias("anomaly_count"),
            F.round(F.sum("amount"), 2).alias("total_anomaly_amount"),
            F.round(F.avg("amount"), 2).alias("avg_anomaly_amount"),
            F.round(F.max("amount"), 2).alias("max_anomaly_amount"),
        )
        .withColumn("processed_at", F.current_timestamp())
        .orderBy(F.desc("anomaly_count"))
    )


# ── Writer ────────────────────────────────────────────────────────────────────

def write_gold_table(df, output_dir: str, table_name: str):
    path = os.path.join(output_dir, table_name)
    df.write.mode("overwrite").parquet(path)
    print(f"  Written: {path} ({df.count()} rows)")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_gold(config_path: str = "configs/config.yaml"):
    config = load_config(config_path)

    silver_path = config["paths"]["silver"]
    gold_path   = config["paths"]["gold"]
    app_name    = config["spark"]["app_name"]
    log_level   = config["spark"]["log_level"]

    print("\n── Gold layer starting ──")
    print(f"  Reading from : {silver_path}")
    print(f"  Writing to   : {gold_path}")

    spark = create_spark_session(app_name, log_level)

    df = spark.read.parquet(silver_path)
    print(f"  Silver rows loaded: {df.count()}")

    os.makedirs(gold_path, exist_ok=True)

    write_gold_table(hourly_txn_summary(df),    gold_path, "hourly_txn_summary")
    write_gold_table(bank_failure_analysis(df), gold_path, "bank_failure_analysis")
    write_gold_table(merchant_spend_analysis(df), gold_path, "merchant_spend_analysis")
    write_gold_table(anomaly_summary(df),       gold_path, "anomaly_summary")

    spark.stop()
    print("── Gold layer complete ──\n")


if __name__ == "__main__":
    run_gold()