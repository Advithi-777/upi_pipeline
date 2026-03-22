"""
UPI Transaction Insights Dashboard
====================================
Streamlit dashboard connecting to Snowflake Gold layer.

Run with: streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv("configs/.env")

# ── Page config ───────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="UPI Transaction Insights",
    page_icon="💳",
    layout="wide"
)

# ── Snowflake connection ──────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_data():
    conn = snowflake.connector.connect(
    account   = "mshkneu-vq44359",
    user      = "ADVITHI",
    password  = "Anuradharavi@2003",
    warehouse = "UPI_WH",
    database  = "UPI_DB",
    schema    = "GOLD",
)

    hourly   = pd.read_sql("SELECT * FROM HOURLY_TXN_SUMMARY ORDER BY TXN_HOUR",   conn)
    bank     = pd.read_sql("SELECT * FROM BANK_FAILURE_ANALYSIS ORDER BY FAILURE_RATE_PCT DESC", conn)
    merchant = pd.read_sql("SELECT * FROM MERCHANT_SPEND_ANALYSIS ORDER BY TOTAL_SPEND DESC",    conn)
    anomaly  = pd.read_sql("SELECT * FROM ANOMALY_SUMMARY ORDER BY ANOMALY_COUNT DESC",          conn)

    conn.close()
    return hourly, bank, merchant, anomaly


# ── Header ────────────────────────────────────────────────────────────────────

st.title("💳 UPI Transaction Insights")
st.markdown("Real-time analytics pipeline — Simulator → PySpark → Snowflake → Streamlit")
st.markdown("---")

# ── Load data ─────────────────────────────────────────────────────────────────

with st.spinner("Loading data from Snowflake..."):
    hourly, bank, merchant, anomaly = load_data()

# ── KPI cards ─────────────────────────────────────────────────────────────────

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        label="Total Transactions",
        value=f"{hourly['TOTAL_TXNS'].sum():,}",
    )

with col2:
    st.metric(
        label="Total Volume (₹)",
        value=f"₹{hourly['TOTAL_AMOUNT'].sum():,.0f}",
    )

with col3:
    avg_amount = hourly['AVG_AMOUNT'].mean()
    st.metric(
        label="Avg Transaction (₹)",
        value=f"₹{avg_amount:,.0f}",
    )

with col4:
    st.metric(
        label="Total Anomalies",
        value=f"{anomaly['ANOMALY_COUNT'].sum():,}",
        delta="Flagged transactions",
        delta_color="inverse"
    )

st.markdown("---")

# ── Row 1: Hourly volume + Bank failure ───────────────────────────────────────

col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Hourly Transaction Volume")
    fig1 = px.line(
        hourly,
        x="TXN_HOUR",
        y="TOTAL_TXNS",
        markers=True,
        labels={"TXN_HOUR": "Hour of Day", "TOTAL_TXNS": "Total Transactions"},
        color_discrete_sequence=["#1f77b4"]
    )
    fig1.update_layout(
        xaxis=dict(tickmode="linear", tick0=0, dtick=1),
        height=350
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.subheader("🏦 Bank Failure Rate (%)")
    fig2 = px.bar(
        bank,
        x="FAILURE_RATE_PCT",
        y="SENDER_BANK",
        orientation="h",
        labels={"FAILURE_RATE_PCT": "Failure Rate (%)", "SENDER_BANK": "Bank"},
        color="FAILURE_RATE_PCT",
        color_continuous_scale="Reds"
    )
    fig2.update_layout(height=350)
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 2: Merchant spend + Anomaly ──────────────────────────────────────────

col3, col4 = st.columns(2)

with col3:
    st.subheader("🛒 Merchant Category Spend (₹)")
    fig3 = px.bar(
        merchant,
        x="TOTAL_SPEND",
        y="MERCHANT_CATEGORY",
        orientation="h",
        labels={"TOTAL_SPEND": "Total Spend (₹)", "MERCHANT_CATEGORY": "Category"},
        color="TOTAL_SPEND",
        color_continuous_scale="Blues"
    )
    fig3.update_layout(height=400)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.subheader("🚨 Anomaly Summary")
    st.dataframe(
        anomaly[["TXN_DATE", "SENDER_BANK", "ANOMALY_COUNT", "TOTAL_ANOMALY_AMOUNT"]],
        use_container_width=True,
        height=400
    )

# ── Raw data expander ─────────────────────────────────────────────────────────

st.markdown("---")
with st.expander("📊 View Raw Gold Layer Data"):
    tab1, tab2, tab3, tab4 = st.tabs([
        "Hourly Summary", "Bank Failure", "Merchant Spend", "Anomalies"
    ])
    with tab1:
        st.dataframe(hourly, use_container_width=True)
    with tab2:
        st.dataframe(bank, use_container_width=True)
    with tab3:
        st.dataframe(merchant, use_container_width=True)
    with tab4:
        st.dataframe(anomaly, use_container_width=True)

st.markdown("---")
st.caption("Built with PySpark · Snowflake · Apache Airflow · Streamlit | UPI Pipeline Project")