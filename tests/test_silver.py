"""
Unit tests for Silver layer transformations.
Run with: pytest tests/test_silver.py -v
"""

import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, BooleanType


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .appName("UPI_Test_Silver") \
        .master("local[1]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def sample_df(spark):
    data = [
        ("txn_001", "2026-03-21 10:00:00", "2026-03-21", 10, "P2P", "user1@sbi", "user2@hdfc", "SBI", "HDFC", 1500.0, "INR", "SUCCESS", None, None, None, "UTR123", "Android", "GPay", False),
        ("txn_002", "2026-03-21 11:00:00", "2026-03-21", 11, "P2M", "user3@icici", "merchant@axisbank", "ICICI", "Axis", 500.0, "INR", "FAILED", "Insufficient funds", "Grocery", "DMart", "UTR124", "iOS", "PhonePe", False),
        ("txn_001", "2026-03-21 10:00:00", "2026-03-21", 10, "P2P", "user1@sbi", "user2@hdfc", "SBI", "HDFC", 1500.0, "INR", "SUCCESS", None, None, None, "UTR123", "Android", "GPay", False),  # duplicate
        ("txn_003", "2026-03-21 03:00:00", "2026-03-21", 3,  "P2P", "user4@kotak", "user5@pnb", "Kotak", "PNB", 75000.0, "INR", "SUCCESS", None, None, None, "UTR125", "Web", "BHIM", True),
    ]
    columns = ["txn_id", "txn_timestamp", "txn_date", "txn_hour", "txn_type", "sender_vpa", "receiver_vpa", "sender_bank", "receiver_bank", "amount", "currency", "status", "failure_reason", "merchant_category", "merchant_name", "utr_number", "device_type", "app_name", "is_anomaly"]
    return spark.createDataFrame(data, columns)


def test_deduplication(spark, sample_df):
    """txn_001 appears twice — after dedup should have 3 unique rows."""
    from pyspark.sql.window import Window
    window = Window.partitionBy("txn_id").orderBy("txn_timestamp")
    df = sample_df.withColumn("row_num", F.row_number().over(window)) \
                  .filter(F.col("row_num") == 1).drop("row_num")
    assert df.count() == 3


def test_null_handling(spark, sample_df):
    """failure_reason nulls should be filled with N/A."""
    df = sample_df.fillna({"failure_reason": "N/A", "merchant_category": "UNKNOWN"})
    null_count = df.filter(F.col("failure_reason").isNull()).count()
    assert null_count == 0


def test_validation_amount(spark, sample_df):
    """All amounts should be positive and under 10 lakh."""
    df = sample_df.filter((F.col("amount") > 0) & (F.col("amount") <= 1000000))
    assert df.count() == sample_df.count()


def test_enrichment_is_high_value(spark, sample_df):
    """txn_003 has amount 75000 — should be flagged as high value."""
    df = sample_df.withColumn("is_high_value", F.when(F.col("amount") >= 50000, True).otherwise(False))
    high_value = df.filter(F.col("is_high_value") == True).count()
    assert high_value == 1


def test_enrichment_is_odd_hour(spark, sample_df):
    """txn_003 is at hour 3 — should be flagged as odd hour."""
    df = sample_df.withColumn("is_odd_hour", F.when(F.col("txn_hour").between(1, 5), True).otherwise(False))
    odd_hour = df.filter(F.col("is_odd_hour") == True).count()
    assert odd_hour == 1


def test_enrichment_amount_bucket(spark, sample_df):
    """500 should be small, 1500 should be small, 75000 should be large."""
    df = sample_df.withColumn("amount_bucket",
        F.when(F.col("amount") < 500,   "micro")
         .when(F.col("amount") < 5000,  "small")
         .when(F.col("amount") < 50000, "medium")
         .otherwise("large")
    )
    buckets = {row["txn_id"]: row["amount_bucket"] for row in df.collect()}
    assert buckets["txn_001"] == "small"
    assert buckets["txn_003"] == "large"