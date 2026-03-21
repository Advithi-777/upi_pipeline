"""
Snowflake Loader
=================
Loads Gold layer Parquet tables into Snowflake.

Tables loaded:
  - HOURLY_TXN_SUMMARY
  - BANK_FAILURE_ANALYSIS
  - MERCHANT_SPEND_ANALYSIS
  - ANOMALY_SUMMARY
"""

import os
import yaml
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv("configs/.env")


# ── Load config ───────────────────────────────────────────────────────────────

def load_config(config_path: str = "configs/config.yaml") -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


# ── Snowflake connection ──────────────────────────────────────────────────────

def get_snowflake_conn():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "UPI_WH"),
        database  = os.getenv("SNOWFLAKE_DATABASE",  "UPI_DB"),
        schema    = "GOLD",
    )


# ── Read Gold Parquet ─────────────────────────────────────────────────────────

def read_gold_tables(gold_path: str) -> dict:
    spark = (
        SparkSession.builder
        .appName("UPI_Snowflake_Loader")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    tables = {
        "HOURLY_TXN_SUMMARY":      spark.read.parquet(f"{gold_path}/hourly_txn_summary").toPandas(),
        "BANK_FAILURE_ANALYSIS":   spark.read.parquet(f"{gold_path}/bank_failure_analysis").toPandas(),
        "MERCHANT_SPEND_ANALYSIS": spark.read.parquet(f"{gold_path}/merchant_spend_analysis").toPandas(),
        "ANOMALY_SUMMARY":         spark.read.parquet(f"{gold_path}/anomaly_summary").toPandas(),
    }
    spark.stop()
    return tables


# ── Prepare dataframe ─────────────────────────────────────────────────────────

def prepare_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.upper() for c in df.columns]
    if "TXN_DATE" in df.columns:
        df["TXN_DATE"] = df["TXN_DATE"].astype(str)
    if "PROCESSED_AT" in df.columns:
        df["PROCESSED_AT"] = df["PROCESSED_AT"].astype(str)
    return df


# ── Load to Snowflake ─────────────────────────────────────────────────────────

def load_to_snowflake(tables: dict, conn):
    cursor = conn.cursor()
    for table_name, df in tables.items():
        df = prepare_df(df)
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        success, nchunks, nrows, _ = write_pandas(
            conn              = conn,
            df                = df,
            table_name        = table_name,
            auto_create_table = True,
            overwrite         = True,
        )
        print(f"  {table_name}: {nrows} rows loaded ✅")
    cursor.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def run_snowflake_load(config_path: str = "configs/config.yaml"):
    config    = load_config(config_path)
    gold_path = config["paths"]["gold"]

    print("\n── Snowflake load starting ──")
    print(f"  Reading from: {gold_path}")

    tables = read_gold_tables(gold_path)
    conn   = get_snowflake_conn()

    load_to_snowflake(tables, conn)

    conn.close()
    print("── Snowflake load complete ──\n")


if __name__ == "__main__":
    run_snowflake_load()