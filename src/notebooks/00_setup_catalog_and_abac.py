# Databricks notebook source
# MAGIC %md
# MAGIC # ZebPay - Catalog & ABAC Setup
# MAGIC
# MAGIC This notebook is run as part of the CI/CD deployment to set up:
# MAGIC 1. Unity Catalog schemas (if not exists)
# MAGIC 2. ABAC row filter and column mask functions
# MAGIC 3. Apply ABAC policies to silver_secured tables
# MAGIC
# MAGIC **Note:** This notebook is idempotent - safe to run multiple times.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parameters

# COMMAND ----------

dbutils.widgets.text("catalog", "zebpay", "Catalog Name")
dbutils.widgets.text("environment", "dev", "Environment")

catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")

print(f"Setting up catalog: {catalog} in environment: {environment}")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create ABAC Functions

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE FUNCTION silver.region_filter(user_region STRING)
RETURN IF(
  IS_ACCOUNT_GROUP_MEMBER('compliance_team'),
  true,
  user_region = CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('region_west') THEN 'Maharashtra'
    WHEN IS_ACCOUNT_GROUP_MEMBER('region_south') THEN 'Karnataka'
    WHEN IS_ACCOUNT_GROUP_MEMBER('region_north') THEN 'Delhi'
    ELSE user_region
  END
)
""")
print("Created: region_filter")

spark.sql("""
CREATE OR REPLACE FUNCTION silver.trade_visibility_filter(trade_amount DOUBLE)
RETURN IF(
  IS_ACCOUNT_GROUP_MEMBER('compliance_team') OR IS_ACCOUNT_GROUP_MEMBER('trading_desk'),
  true,
  trade_amount <= 500000
)
""")
print("Created: trade_visibility_filter")

spark.sql("""
CREATE OR REPLACE FUNCTION silver.txn_status_filter(txn_status STRING)
RETURN IF(
  IS_ACCOUNT_GROUP_MEMBER('ops_team') OR IS_ACCOUNT_GROUP_MEMBER('compliance_team'),
  true,
  txn_status = 'COMPLETED'
)
""")
print("Created: txn_status_filter")

spark.sql("""
CREATE OR REPLACE FUNCTION silver.mask_email(email STRING)
RETURN IF(
  IS_ACCOUNT_GROUP_MEMBER('compliance_team'),
  email,
  CONCAT(LEFT(email, 2), '***@', SPLIT(email, '@')[1])
)
""")
print("Created: mask_email")

spark.sql("""
CREATE OR REPLACE FUNCTION silver.mask_phone(phone STRING)
RETURN IF(
  IS_ACCOUNT_GROUP_MEMBER('compliance_team'),
  phone,
  CONCAT(LEFT(phone, 4), '****', RIGHT(phone, 2))
)
""")
print("Created: mask_phone")

spark.sql("""
CREATE OR REPLACE FUNCTION silver.mask_wallet_address(addr STRING)
RETURN IF(
  IS_ACCOUNT_GROUP_MEMBER('compliance_team') OR IS_ACCOUNT_GROUP_MEMBER('ops_team'),
  addr,
  CASE WHEN addr IS NULL THEN NULL
       ELSE CONCAT(LEFT(addr, 6), '...', RIGHT(addr, 4))
  END
)
""")
print("Created: mask_wallet_address")

spark.sql("""
CREATE OR REPLACE FUNCTION silver.mask_blockchain_hash(hash_val STRING)
RETURN IF(
  IS_ACCOUNT_GROUP_MEMBER('compliance_team'),
  hash_val,
  CASE WHEN hash_val IS NULL THEN NULL
       ELSE CONCAT(LEFT(hash_val, 8), '...')
  END
)
""")
print("Created: mask_blockchain_hash")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply ABAC Policies to silver_secured Tables
# MAGIC
# MAGIC These are applied after the tables exist (post-processing job run).
# MAGIC Uses `IF EXISTS` pattern via try/except for idempotency.

# COMMAND ----------

abac_statements = [
    # Row filters
    "ALTER TABLE silver_secured.users SET ROW FILTER silver.region_filter ON (state)",
    "ALTER TABLE silver_secured.trades SET ROW FILTER silver.trade_visibility_filter ON (total_amount_inr)",
    "ALTER TABLE silver_secured.transactions SET ROW FILTER silver.txn_status_filter ON (status)",
    # Column masks
    "ALTER TABLE silver_secured.users ALTER COLUMN email SET MASK silver.mask_email",
    "ALTER TABLE silver_secured.users ALTER COLUMN phone SET MASK silver.mask_phone",
    "ALTER TABLE silver_secured.wallets ALTER COLUMN wallet_address SET MASK silver.mask_wallet_address",
    "ALTER TABLE silver_secured.transactions ALTER COLUMN blockchain_hash SET MASK silver.mask_blockchain_hash",
    "ALTER TABLE silver_secured.transactions ALTER COLUMN from_address SET MASK silver.mask_wallet_address",
    "ALTER TABLE silver_secured.transactions ALTER COLUMN to_address SET MASK silver.mask_wallet_address",
]

for stmt in abac_statements:
    try:
        spark.sql(stmt)
        print(f"  OK: {stmt[:80]}...")
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            print(f"  SKIP (table not yet created): {stmt[:60]}...")
        else:
            print(f"  WARN: {str(e)[:100]}")

print("\nABAC setup complete!")
