# UPI Transaction Insights Pipeline

A production-grade ETL data engineering project that simulates, processes, and visualizes UPI (Unified Payments Interface) transaction data using a modern Azure-based tech stack.

---

## Problem Statement

UPI processes 15+ billion transactions per month in India. Banks and fintechs need real-time visibility into:
- Transaction failure rates by bank
- Merchant category spend patterns
- Suspicious transaction detection
- Hourly volume trends

This pipeline solves exactly that — from raw transaction events to an analytics dashboard.

---

## Architecture

```
Python Simulator
      ↓
Bronze Layer (ADLS Gen2 / Local Parquet)
      ↓
PySpark Silver Layer (Cleanse → Deduplicate → Enrich)
      ↓
PySpark Gold Layer (Aggregate → Anomaly Flag)
      ↓
Snowflake (Analytics Warehouse)
      ↓
Streamlit Dashboard
```

Orchestrated by **Apache Airflow** | Quality checked by **GitHub Actions CI/CD**

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Simulation | Python, Faker |
| Ingestion | Azure Data Factory |
| Storage | ADLS Gen2, Parquet |
| Transformation | PySpark (Medallion Architecture) |
| Orchestration | Apache Airflow |
| Warehouse | Snowflake |
| Dashboard | Streamlit + Plotly |
| CI/CD | GitHub Actions + Ruff |

---

## Medallion Architecture

### Bronze Layer
- Raw Parquet files partitioned by `txn_date`
- 1000 simulated UPI transactions per batch
- 19 columns including VPA, bank, amount, status, merchant category

### Silver Layer
- Type casting and schema enforcement
- Null handling (critical fields dropped, optional filled)
- Deduplication on `txn_id` using window functions
- Validation (amount limits, VPA format, status values)
- Enrichment: `is_high_value`, `is_odd_hour`, `amount_bucket`, `is_same_bank`, `day_of_week`

### Gold Layer (4 analytical tables)
- `hourly_txn_summary` — volume, amount, success/failure rate by hour
- `bank_failure_analysis` — failure rates per sending bank
- `merchant_spend_analysis` — spend breakdown by merchant category
- `anomaly_summary` — flagged suspicious transactions by date and bank

---

## Dashboard KPIs

- Total transaction volume and amount
- Average transaction value
- Hourly transaction volume (line chart)
- Bank failure rate ranking (bar chart)
- Merchant category spend (bar chart)
- Anomaly summary table

---

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/Advithi-777/upi_pipeline.git
cd upi_pipeline
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 3. Configure environment
```bash
cp configs/.env.example configs/.env
# Fill in your Snowflake credentials in configs/.env
```

### 4. Run the simulator
```bash
python src/simulator/upi_simulator.py
```

### 5. Run the dashboard
```bash
streamlit run dashboard.py
```

---

## CI/CD

Every push to main triggers GitHub Actions which:
1. Sets up Python 3.11 and Java 11
2. Installs all dependencies
3. Runs ruff linting on all source files
4. Runs pytest unit tests

---
