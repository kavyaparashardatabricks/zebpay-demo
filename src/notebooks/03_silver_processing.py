# Databricks notebook source
# MAGIC %md
# MAGIC # ZebPay - Silver Layer Processing
# MAGIC
# MAGIC This notebook performs bronze → silver ETL processing:
# MAGIC 1. **Deduplication** - Remove duplicate CDC records
# MAGIC 2. **Data Cleaning** - Null handling, type casting, standardization
# MAGIC 3. **Enrichment** - Derived columns, risk scoring, business logic
# MAGIC 4. **Quality Checks** - Validate data before writing to silver
# MAGIC
# MAGIC **Schedule**: Runs every 30 minutes via Databricks Workflow

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

CATALOG = "zebpay"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver_secured"

spark.sql(f"USE CATALOG {CATALOG}")

run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
print(f"Processing run: {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Process Users (Bronze → Silver)

# COMMAND ----------

print("Processing raw_users → users")

raw_users = spark.table(f"{BRONZE_SCHEMA}.raw_users")
print(f"  Bronze row count: {raw_users.count()}")

# Dedup: keep latest record per user_id
deduped_users = raw_users.withColumn(
    "rn", row_number().over(
        Window.partitionBy("user_id").orderBy(col("updated_at").desc())
    )
).filter("rn = 1").drop("rn")

# Clean and transform
silver_users = deduped_users.select(
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

# Quality checks
null_emails = silver_users.filter("email IS NULL").count()
null_user_ids = silver_users.filter("user_id IS NULL").count()
print(f"  Quality: {null_emails} null emails, {null_user_ids} null user_ids")

if null_user_ids > 0:
    print("  WARNING: Found null user_ids - filtering them out")
    silver_users = silver_users.filter("user_id IS NOT NULL")

# Write to silver (merge for idempotency)
silver_users.createOrReplaceTempView("staged_users")

spark.sql(f"""
    MERGE INTO {SILVER_SCHEMA}.users AS target
    USING staged_users AS source
    ON target.user_id = source.user_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

final_count = spark.table(f"{SILVER_SCHEMA}.users").count()
print(f"  Silver users count: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Process Trades (Bronze → Silver)

# COMMAND ----------

from pyspark.sql.window import Window

print("Processing raw_trades → trades")

raw_trades = spark.table(f"{BRONZE_SCHEMA}.raw_trades")
print(f"  Bronze row count: {raw_trades.count()}")

# Dedup
deduped_trades = raw_trades.withColumn(
    "rn", row_number().over(
        Window.partitionBy("trade_id").orderBy(col("created_at").desc())
    )
).filter("rn = 1").drop("rn")

# Clean and enrich
silver_trades = deduped_trades.select(
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
    round(col("fee").cast("double") / col("total_amount").cast("double") * 100, 4).alias("fee_pct"),
    current_timestamp().alias("_processed_at")
).filter("trade_id IS NOT NULL AND user_id IS NOT NULL")

# Quality: check for negative quantities or prices
bad_trades = silver_trades.filter("quantity <= 0 OR price_inr <= 0").count()
print(f"  Quality: {bad_trades} trades with invalid quantity/price")

# Write
silver_trades.createOrReplaceTempView("staged_trades")
spark.sql(f"""
    MERGE INTO {SILVER_SCHEMA}.trades AS target
    USING staged_trades AS source
    ON target.trade_id = source.trade_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

final_count = spark.table(f"{SILVER_SCHEMA}.trades").count()
print(f"  Silver trades count: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Process Wallets (Bronze → Silver)

# COMMAND ----------

print("Processing raw_wallets → wallets")

raw_wallets = spark.table(f"{BRONZE_SCHEMA}.raw_wallets")
print(f"  Bronze row count: {raw_wallets.count()}")

deduped_wallets = raw_wallets.withColumn(
    "rn", row_number().over(
        Window.partitionBy("wallet_id").orderBy(col("updated_at").desc())
    )
).filter("rn = 1").drop("rn")

silver_wallets = deduped_wallets.select(
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
).filter("wallet_id IS NOT NULL AND user_id IS NOT NULL")

# Quality: check for negative balances
negative_balances = silver_wallets.filter("balance < 0").count()
print(f"  Quality: {negative_balances} wallets with negative balance")

silver_wallets.createOrReplaceTempView("staged_wallets")
spark.sql(f"""
    MERGE INTO {SILVER_SCHEMA}.wallets AS target
    USING staged_wallets AS source
    ON target.wallet_id = source.wallet_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

final_count = spark.table(f"{SILVER_SCHEMA}.wallets").count()
print(f"  Silver wallets count: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Process Transactions (Bronze → Silver)

# COMMAND ----------

print("Processing raw_transactions → transactions")

raw_txns = spark.table(f"{BRONZE_SCHEMA}.raw_transactions")
print(f"  Bronze row count: {raw_txns.count()}")

deduped_txns = raw_txns.withColumn(
    "rn", row_number().over(
        Window.partitionBy("txn_id").orderBy(col("created_at").desc())
    )
).filter("rn = 1").drop("rn")

silver_txns = deduped_txns.select(
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
         round((unix_timestamp(to_timestamp(col("completed_at"))) - unix_timestamp(to_timestamp(col("created_at")))) / 60.0, 2)
    ).alias("processing_time_minutes"),
    current_timestamp().alias("_processed_at")
).filter("txn_id IS NOT NULL AND user_id IS NOT NULL")

silver_txns.createOrReplaceTempView("staged_txns")
spark.sql(f"""
    MERGE INTO {SILVER_SCHEMA}.transactions AS target
    USING staged_txns AS source
    ON target.txn_id = source.txn_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

final_count = spark.table(f"{SILVER_SCHEMA}.transactions").count()
print(f"  Silver transactions count: {final_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Processing Summary

# COMMAND ----------

print(f"\n{'='*60}")
print(f"Processing Run Complete: {run_id}")
print(f"{'='*60}")

summary = []
for table in ["users", "trades", "wallets", "transactions"]:
    count = spark.table(f"{SILVER_SCHEMA}.{table}").count()
    summary.append((table, count))
    print(f"  {SILVER_SCHEMA}.{table}: {count} rows")

print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Audit: Data Quality Metrics

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quick data quality dashboard
# MAGIC SELECT 'users' AS table_name,
# MAGIC        COUNT(*) AS total_rows,
# MAGIC        SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS null_emails,
# MAGIC        SUM(CASE WHEN is_active = false THEN 1 ELSE 0 END) AS inactive_users,
# MAGIC        SUM(CASE WHEN risk_category = 'HIGH' THEN 1 ELSE 0 END) AS high_risk_users
# MAGIC FROM zebpay.silver_secured.users
# MAGIC UNION ALL
# MAGIC SELECT 'trades',
# MAGIC        COUNT(*),
# MAGIC        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN total_amount_inr > 500000 THEN 1 ELSE 0 END)
# MAGIC FROM zebpay.silver_secured.trades
# MAGIC UNION ALL
# MAGIC SELECT 'transactions',
# MAGIC        COUNT(*),
# MAGIC        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END),
# MAGIC        SUM(CASE WHEN amount > 100000 THEN 1 ELSE 0 END)
# MAGIC FROM zebpay.silver_secured.transactions
