# Databricks notebook source
# MAGIC %md
# MAGIC # ZebPay - Lakeflow Connect Ingestion
# MAGIC
# MAGIC This notebook demonstrates a **Lakeflow Connect** CDC ingestion pipeline from ZebPay's PostgreSQL database into the Bronze layer.
# MAGIC
# MAGIC **Architecture:**
# MAGIC ```
# MAGIC PostgreSQL (Source)  →  Lakeflow Connect (CDC)  →  Bronze Ingestion (Delta)
# MAGIC ┌──────────────┐      ┌─────────────────────┐      ┌─────────────────────────┐
# MAGIC │ public.users │ ───► │ Change Data Capture  │ ───► │ bronze_ingestion.users   │
# MAGIC │ public.trades│ ───► │ - Initial snapshot   │ ───► │ bronze_ingestion.trades  │
# MAGIC │ public.wallets│───► │ - Incremental CDC    │ ───► │ bronze_ingestion.wallets │
# MAGIC │ public.txns  │ ───► │ - Schema evolution   │ ───► │ bronze_ingestion.txns    │
# MAGIC └──────────────┘      └─────────────────────┘      └─────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > **Note:** In production, Lakeflow Connect would be configured via the UI or API with
# MAGIC > a real PostgreSQL connection. This notebook simulates the CDC pattern for demo purposes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lakeflow Connect Configuration
# MAGIC
# MAGIC In production, configure via **Data Ingestion > Lakeflow Connect**:
# MAGIC
# MAGIC | Setting | Value |
# MAGIC |---------|-------|
# MAGIC | **Source Type** | PostgreSQL |
# MAGIC | **Host** | zebpay-prod-db.ap-south-1.rds.amazonaws.com |
# MAGIC | **Port** | 5432 |
# MAGIC | **Database** | zebpay_production |
# MAGIC | **Schema** | public |
# MAGIC | **Tables** | users, trades, wallets, transactions |
# MAGIC | **Ingestion Mode** | CDC (Change Data Capture) |
# MAGIC | **Target Catalog** | zebpay |
# MAGIC | **Target Schema** | bronze_ingestion |
# MAGIC | **Schedule** | Continuous |
# MAGIC
# MAGIC **Production Setup Steps:**
# MAGIC 1. Create Secret Scope: `databricks secrets create-scope zebpay-secrets`
# MAGIC 2. Create UC Connection:
# MAGIC    ```sql
# MAGIC    CREATE CONNECTION zebpay_postgres TYPE postgresql
# MAGIC    OPTIONS (host '...', port '5432', user secret(...), password secret(...));
# MAGIC    ```
# MAGIC 3. Configure Lakeflow Connect via UI → Select tables → Enable CDC → Start

# COMMAND ----------

import dlt
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Users Ingestion (CDC from PostgreSQL)

# COMMAND ----------

@dlt.table(
    name="ingested_users",
    comment="CDC ingestion from zebpay_postgres.public.users via Lakeflow Connect",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "user_id",
        "source.system": "postgresql",
        "source.table": "public.users",
        "ingestion.mode": "cdc",
        "ingestion.connector": "lakeflow_connect"
    }
)
def ingest_users():
    return (
        spark.table("zebpay.bronze.raw_users")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("lakeflow_connect_postgres"))
        .withColumn("_cdc_operation", lit("INSERT"))
        .withColumn("_cdc_sequence", monotonically_increasing_id())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trades Ingestion (CDC from PostgreSQL)

# COMMAND ----------

@dlt.table(
    name="ingested_trades",
    comment="CDC ingestion from zebpay_postgres.public.trades via Lakeflow Connect",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "trade_id",
        "source.system": "postgresql",
        "source.table": "public.trades",
        "ingestion.mode": "cdc",
        "ingestion.connector": "lakeflow_connect"
    }
)
def ingest_trades():
    return (
        spark.table("zebpay.bronze.raw_trades")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("lakeflow_connect_postgres"))
        .withColumn("_cdc_operation", lit("INSERT"))
        .withColumn("_cdc_sequence", monotonically_increasing_id())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wallets Ingestion (CDC from PostgreSQL)

# COMMAND ----------

@dlt.table(
    name="ingested_wallets",
    comment="CDC ingestion from zebpay_postgres.public.wallets via Lakeflow Connect",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "wallet_id",
        "source.system": "postgresql",
        "source.table": "public.wallets",
        "ingestion.mode": "cdc",
        "ingestion.connector": "lakeflow_connect"
    }
)
def ingest_wallets():
    return (
        spark.table("zebpay.bronze.raw_wallets")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("lakeflow_connect_postgres"))
        .withColumn("_cdc_operation", lit("INSERT"))
        .withColumn("_cdc_sequence", monotonically_increasing_id())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transactions Ingestion (CDC from PostgreSQL)

# COMMAND ----------

@dlt.table(
    name="ingested_transactions",
    comment="CDC ingestion from zebpay_postgres.public.transactions via Lakeflow Connect",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.zOrderCols": "txn_id",
        "source.system": "postgresql",
        "source.table": "public.transactions",
        "ingestion.mode": "cdc",
        "ingestion.connector": "lakeflow_connect"
    }
)
def ingest_transactions():
    return (
        spark.table("zebpay.bronze.raw_transactions")
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source", lit("lakeflow_connect_postgres"))
        .withColumn("_cdc_operation", lit("INSERT"))
        .withColumn("_cdc_sequence", monotonically_increasing_id())
    )
