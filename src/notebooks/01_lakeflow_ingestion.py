# Databricks notebook source
# MAGIC %md
# MAGIC # ZebPay - Lakeflow Ingestion Pipeline
# MAGIC
# MAGIC This notebook simulates a Lakeflow Connect ingestion pipeline from ZebPay's PostgreSQL source systems into the Bronze layer.
# MAGIC
# MAGIC **Sources:**
# MAGIC - `zebpay_postgres.public.users` → `zebpay.bronze.raw_users`
# MAGIC - `zebpay_postgres.public.trades` → `zebpay.bronze.raw_trades`
# MAGIC - `zebpay_postgres.public.wallets` → `zebpay.bronze.raw_wallets`
# MAGIC - `zebpay_postgres.public.transactions` → `zebpay.bronze.raw_transactions`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Configuration

# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion: Users

# COMMAND ----------

@dlt.table(
    name="raw_users",
    comment="Raw user data ingested from ZebPay PostgreSQL via Lakeflow Connect",
    table_properties={
        "quality": "bronze",
        "source": "postgres_cdc",
        "pipelines.autoOptimize.zOrderCols": "user_id"
    }
)
def ingest_raw_users():
    return spark.table("zebpay.bronze.raw_users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion: Trades

# COMMAND ----------

@dlt.table(
    name="raw_trades",
    comment="Raw trade data ingested from ZebPay trade engine via Lakeflow Connect",
    table_properties={
        "quality": "bronze",
        "source": "trade_engine",
        "pipelines.autoOptimize.zOrderCols": "trade_id"
    }
)
def ingest_raw_trades():
    return spark.table("zebpay.bronze.raw_trades")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion: Wallets

# COMMAND ----------

@dlt.table(
    name="raw_wallets",
    comment="Raw wallet data ingested from ZebPay wallet service via Lakeflow Connect",
    table_properties={
        "quality": "bronze",
        "source": "wallet_service"
    }
)
def ingest_raw_wallets():
    return spark.table("zebpay.bronze.raw_wallets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion: Transactions

# COMMAND ----------

@dlt.table(
    name="raw_transactions",
    comment="Raw transaction data ingested from ZebPay transaction service via Lakeflow Connect",
    table_properties={
        "quality": "bronze",
        "source": "txn_service",
        "pipelines.autoOptimize.zOrderCols": "txn_id"
    }
)
def ingest_raw_transactions():
    return spark.table("zebpay.bronze.raw_transactions")
