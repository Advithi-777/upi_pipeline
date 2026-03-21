"""
UPI Pipeline DAG
=================
Orchestrates the full UPI transaction insights pipeline.

Schedule: Daily at 6:00 AM IST
Tasks:
  1. generate_bronze  — runs Python simulator, generates Bronze Parquet
  2. run_silver       — PySpark Silver transformation
  3. run_gold         — PySpark Gold aggregations

Flow:
  generate_bronze >> run_silver >> run_gold
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# ── Default args ──────────────────────────────────────────────────────────────

default_args = {
    "owner":            "upi_pipeline",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}


# ── Task functions ────────────────────────────────────────────────────────────

def generate_bronze(**context):
    """
    Task 1 — Run the UPI simulator to generate Bronze Parquet.
    In production this would be replaced by ADF pipeline trigger.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")   # adjust to your project root

    from src.simulator.upi_simulator import generate_batch
    filepath = generate_batch(n_records=1000, output_dir="data/bronze")
    print(f"Bronze generated: {filepath}")

    # Push filepath to XCom so downstream tasks can reference it
    context["ti"].xcom_push(key="bronze_path", value=filepath)


def run_silver(**context):
    """
    Task 2 — Run PySpark Silver transformation.
    Reads Bronze Parquet, cleans and enriches, writes Silver Parquet.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.transforms.silver import run_silver as silver_transform
    silver_transform(config_path="configs/config.yaml")
    print("Silver transformation complete")


def run_gold(**context):
    """
    Task 3 — Run PySpark Gold aggregations.
    Reads Silver Parquet, produces 4 analytical Gold tables.
    """
    import sys
    sys.path.insert(0, "/opt/airflow")

    from src.transforms.gold import run_gold as gold_transform
    gold_transform(config_path="configs/config.yaml")
    print("Gold aggregations complete")


# ── DAG definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="upi_transaction_pipeline",
    description="UPI Transaction Insights — Bronze → Silver → Gold",
    default_args=default_args,
    schedule_interval="0 6 * * *",    # daily at 6 AM
    start_date=days_ago(1),
    catchup=False,
    tags=["upi", "fintech", "etl"],
) as dag:

    t1_generate_bronze = PythonOperator(
        task_id="generate_bronze",
        python_callable=generate_bronze,
        provide_context=True,
    )

    t2_run_silver = PythonOperator(
        task_id="run_silver",
        python_callable=run_silver,
        provide_context=True,
    )

    t3_run_gold = PythonOperator(
        task_id="run_gold",
        python_callable=run_gold,
        provide_context=True,
    )

    # ── Task dependencies (execution order) ───────────────────────────────────
    t1_generate_bronze >> t2_run_silver >> t3_run_gold