# Databricks notebook source
# MAGIC %md
# MAGIC # ZebPay - Silver Layer Transformations
# MAGIC
# MAGIC Bronze → Silver data cleaning and enrichment:
# MAGIC - Deduplication
# MAGIC - Null handling & type casting
# MAGIC - Data quality checks with DLT expectations
# MAGIC - Business logic enrichment

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Users

# COMMAND ----------

@dlt.table(
    name="users",
    comment="Cleaned user profiles with KYC status and risk classification",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "user_id"
    }
)
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
@dlt.expect("valid_kyc_status", "kyc_status IN ('VERIFIED', 'PENDING', 'REJECTED')")
def silver_users():
    return (
        spark.table("zebpay.bronze.raw_users")
        .dropDuplicates(["user_id"])
        .select(
            col("user_id"),
            initcap(col("first_name")).alias("first_name"),
            initcap(col("last_name")).alias("last_name"),
            lower(col("email")).alias("email"),
            col("phone"),
            col("kyc_status"),
            col("kyc_level"),
            col("country"),
            col("state"),
            col("city"),
            to_timestamp(col("created_at")).alias("registered_at"),
            to_timestamp(col("updated_at")).alias("last_updated_at"),
            when(lower(col("is_active")) == "true", lit(True)).otherwise(lit(False)).alias("is_active"),
            col("referral_code"),
            col("risk_score").cast("int").alias("risk_score"),
            when(col("risk_score").cast("int") <= 20, "LOW")
             .when(col("risk_score").cast("int") <= 50, "MEDIUM")
             .otherwise("HIGH").alias("risk_category"),
            current_timestamp().alias("_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Trades

# COMMAND ----------

@dlt.table(
    name="trades",
    comment="Cleaned trade records with proper types and trade analytics",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "trade_id"
    }
)
@dlt.expect_or_drop("valid_trade_id", "trade_id IS NOT NULL")
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
@dlt.expect("valid_quantity", "quantity > 0")
@dlt.expect("valid_price", "price_inr > 0")
def silver_trades():
    return (
        spark.table("zebpay.bronze.raw_trades")
        .dropDuplicates(["trade_id"])
        .select(
            col("trade_id"),
            col("user_id"),
            col("trade_pair"),
            split(col("trade_pair"), "/").getItem(0).alias("base_currency"),
            split(col("trade_pair"), "/").getItem(1).alias("quote_currency"),
            upper(col("trade_type")).alias("trade_type"),
            col("quantity").cast("double").alias("quantity"),
            col("price").cast("double").alias("price_inr"),
            col("total_amount").cast("double").alias("total_amount_inr"),
            col("fee").cast("double").alias("fee_inr"),
            col("fee_currency"),
            upper(col("status")).alias("status"),
            col("exchange_order_id"),
            to_timestamp(col("executed_at")).alias("executed_at"),
            to_timestamp(col("created_at")).alias("created_at"),
            (col("fee").cast("double") / col("total_amount").cast("double") * 100).alias("fee_pct"),
            current_timestamp().alias("_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Wallets

# COMMAND ----------

@dlt.table(
    name="wallets",
    comment="Cleaned wallet balances with total value tracking",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_wallet_id", "wallet_id IS NOT NULL")
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
@dlt.expect("non_negative_balance", "balance >= 0.0")
def silver_wallets():
    return (
        spark.table("zebpay.bronze.raw_wallets")
        .dropDuplicates(["wallet_id"])
        .select(
            col("wallet_id"),
            col("user_id"),
            upper(col("currency")).alias("currency"),
            col("balance").cast("double").alias("balance"),
            col("locked_balance").cast("double").alias("locked_balance"),
            (col("balance").cast("double") - col("locked_balance").cast("double")).alias("available_balance"),
            col("wallet_address"),
            upper(col("wallet_type")).alias("wallet_type"),
            to_timestamp(col("created_at")).alias("created_at"),
            to_timestamp(col("updated_at")).alias("last_updated_at"),
            current_timestamp().alias("_processed_at")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: Transactions

# COMMAND ----------

@dlt.table(
    name="transactions",
    comment="Cleaned deposit/withdrawal transactions with processing metadata",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "txn_id"
    }
)
@dlt.expect_or_drop("valid_txn_id", "txn_id IS NOT NULL")
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
@dlt.expect("valid_amount", "amount > 0.0")
@dlt.expect("valid_txn_type", "txn_type IN ('DEPOSIT', 'WITHDRAWAL')")
def silver_transactions():
    return (
        spark.table("zebpay.bronze.raw_transactions")
        .dropDuplicates(["txn_id"])
        .select(
            col("txn_id"),
            col("user_id"),
            upper(col("txn_type")).alias("txn_type"),
            upper(col("currency")).alias("currency"),
            col("amount").cast("double").alias("amount"),
            col("fee").cast("double").alias("fee"),
            (col("amount").cast("double") - col("fee").cast("double")).alias("net_amount"),
            upper(col("status")).alias("status"),
            col("from_address"),
            col("to_address"),
            col("blockchain_hash"),
            col("confirmations").cast("int").alias("confirmations"),
            to_timestamp(col("created_at")).alias("created_at"),
            to_timestamp(col("completed_at")).alias("completed_at"),
            when(col("completed_at").isNotNull(),
                 (to_timestamp(col("completed_at")).cast("long") - to_timestamp(col("created_at")).cast("long")) / 60
            ).alias("processing_time_minutes"),
            current_timestamp().alias("_processed_at")
        )
    )
