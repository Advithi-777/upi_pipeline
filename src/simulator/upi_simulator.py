"""
UPI Transaction Data Simulator
================================
Generates realistic UPI transaction events and saves them as
date-partitioned Parquet files — ready for ADF pickup / ADLS Bronze layer.

Usage:
    python upi_simulator.py                  # generates 1 batch (1000 txns)
    python upi_simulator.py --records 5000   # custom record count
    python upi_simulator.py --schedule       # runs every 60s (streaming simulation)
"""

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker("en_IN")

# ── Reference data ────────────────────────────────────────────────────────────

BANKS = [
    "SBI", "HDFC", "ICICI", "Axis", "Kotak",
    "PNB", "BOB", "Canara", "Union", "IDFC",
]

MERCHANT_CATEGORIES = [
    "Food & Beverage", "Grocery", "Fuel", "E-commerce",
    "Utilities", "Education", "Healthcare", "Travel",
    "Entertainment", "Clothing", "Electronics", "Insurance",
]

TRANSACTION_TYPES = ["P2P", "P2M"]          # peer-to-peer, peer-to-merchant
STATUSES = ["SUCCESS", "FAILED", "PENDING"]
STATUS_WEIGHTS = [0.88, 0.09, 0.03]         # realistic UPI success rate ~88%

FAILURE_REASONS = [
    "Insufficient funds",
    "Bank server timeout",
    "Invalid VPA",
    "Daily limit exceeded",
    "UPI PIN incorrect",
    None,   # SUCCESS rows get None
]

# ── VPA generator ─────────────────────────────────────────────────────────────

def generate_vpa(bank: str) -> str:
    """Generate a realistic UPI Virtual Payment Address."""
    handle_map = {
        "SBI": "sbi", "HDFC": "hdfcbank", "ICICI": "icici",
        "Axis": "axisbank", "Kotak": "kotak", "PNB": "pnb",
        "BOB": "barodampay", "Canara": "cnrb", "Union": "unionbank",
        "IDFC": "idfcbank",
    }
    user_part = random.choice([
        fake.user_name(),
        fake.phone_number().replace(" ", "").replace("-", "")[:10],
        f"{fake.first_name().lower()}{random.randint(1, 999)}",
    ])
    return f"{user_part}@{handle_map.get(bank, 'upi')}"


# ── Single transaction factory ─────────────────────────────────────────────────

def generate_transaction(txn_time: datetime | None = None) -> dict:
    if txn_time is None:
        txn_time = datetime.now() - timedelta(seconds=random.randint(0, 3600))

    txn_type = random.choices(TRANSACTION_TYPES, weights=[0.45, 0.55])[0]
    sender_bank = random.choice(BANKS)
    receiver_bank = random.choice(BANKS)
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS)[0]

    # Amount distribution: most txns are small, occasional large ones
    amount_tier = random.choices(
        ["micro", "small", "medium", "large"],
        weights=[0.40, 0.35, 0.18, 0.07]
    )[0]
    amount_ranges = {
        "micro":  (1,    500),
        "small":  (500,  5000),
        "medium": (5000, 50000),
        "large":  (50000, 200000),
    }
    lo, hi = amount_ranges[amount_tier]
    amount = round(random.uniform(lo, hi), 2)

    failure_reason = None
    if status == "FAILED":
        failure_reason = random.choice(FAILURE_REASONS[:-1])  # exclude None

    merchant_category = None
    merchant_name = None
    if txn_type == "P2M":
        merchant_category = random.choice(MERCHANT_CATEGORIES)
        merchant_name = fake.company()

    return {
        "txn_id":            str(uuid.uuid4()),
        "txn_timestamp":     txn_time.strftime("%Y-%m-%d %H:%M:%S"),
        "txn_date":          txn_time.strftime("%Y-%m-%d"),
        "txn_hour":          txn_time.hour,
        "txn_type":          txn_type,
        "sender_vpa":        generate_vpa(sender_bank),
        "receiver_vpa":      generate_vpa(receiver_bank),
        "sender_bank":       sender_bank,
        "receiver_bank":     receiver_bank,
        "amount":            amount,
        "currency":          "INR",
        "status":            status,
        "failure_reason":    failure_reason,
        "merchant_category": merchant_category,
        "merchant_name":     merchant_name,
        "utr_number":        f"UTR{random.randint(100000000000, 999999999999)}",
        "device_type":       random.choice(["Android", "iOS", "Web"]),
        "app_name":          random.choice(["GPay", "PhonePe", "Paytm", "BHIM", "AmazonPay"]),
        "is_anomaly":        False,   # anomaly flagging happens in PySpark Silver layer
    }


# ── Anomaly injection (5% of records) ─────────────────────────────────────────

def inject_anomalies(records: list[dict]) -> list[dict]:
    """
    Inject realistic anomaly patterns into ~5% of transactions.
    These are the ground truth labels the Silver layer will try to catch.
    """
    anomaly_count = max(1, int(len(records) * 0.05))
    anomaly_indices = random.sample(range(len(records)), anomaly_count)

    for i in anomaly_indices:
        pattern = random.choice(["spike", "odd_hour", "round_amount", "high_frequency"])

        if pattern == "spike":
            # Unusually large amount
            records[i]["amount"] = round(random.uniform(150000, 500000), 2)

        elif pattern == "odd_hour":
            # Transaction at 2–4 AM
            odd_ts = datetime.now().replace(
                hour=random.randint(2, 4),
                minute=random.randint(0, 59)
            )
            records[i]["txn_timestamp"] = odd_ts.strftime("%Y-%m-%d %H:%M:%S")
            records[i]["txn_hour"] = odd_ts.hour

        elif pattern == "round_amount":
            # Suspiciously round large amount
            records[i]["amount"] = float(random.choice([
                100000, 200000, 250000, 500000
            ]))

        elif pattern == "high_frequency":
            # Same sender VPA repeated (simulated — mark the record)
            records[i]["sender_vpa"] = "suspicious_user@sbi"

        records[i]["is_anomaly"] = True

    return records


# ── Batch writer ──────────────────────────────────────────────────────────────

def generate_batch(n_records: int = 1000, output_dir: str = "data/bronze") -> str:
    """Generate a batch of UPI transactions and save as Parquet."""
    now = datetime.now()
    records = [generate_transaction() for _ in range(n_records)]
    records = inject_anomalies(records)

    df = pd.DataFrame(records)

    # Partition path: data/bronze/txn_date=2024-01-15/
    partition_path = os.path.join(
        output_dir,
        f"txn_date={now.strftime('%Y-%m-%d')}"
    )
    os.makedirs(partition_path, exist_ok=True)

    filename = f"upi_txns_{now.strftime('%Y%m%d_%H%M%S')}.parquet"
    filepath = os.path.join(partition_path, filename)
    df.to_parquet(filepath, index=False, engine="pyarrow")

    print(f"[{now.strftime('%H:%M:%S')}] Generated {n_records} records → {filepath}")
    print(f"  Status breakdown: {df['status'].value_counts().to_dict()}")
    print(f"  Anomalies injected: {df['is_anomaly'].sum()}")
    return filepath


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="UPI Transaction Simulator")
    parser.add_argument("--records",  type=int, default=1000,  help="Records per batch")
    parser.add_argument("--output",   type=str, default="data/bronze", help="Output directory")
    parser.add_argument("--schedule", action="store_true", help="Run every 60s continuously")
    parser.add_argument("--interval", type=int, default=60, help="Seconds between batches")
    args = parser.parse_args()

    if args.schedule:
        print(f"Running in scheduled mode — generating batch every {args.interval}s. Ctrl+C to stop.\n")
        while True:
            generate_batch(n_records=args.records, output_dir=args.output)
            time.sleep(args.interval)
    else:
        generate_batch(n_records=args.records, output_dir=args.output)


if __name__ == "__main__":
    main()

