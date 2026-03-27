# ZebPay Demo

Databricks demo environment for ZebPay crypto exchange data platform, managed with Databricks Asset Bundles (DAB) and CI/CD.

## Architecture

```
PostgreSQL (Source)
       │
       ▼
┌─────────────────────────┐
│  Lakeflow Connect CDC   │  04_lakeflow_connect_ingestion
│  → bronze_ingestion.*   │
└────────┬────────────────┘
         ▼
┌─────────────────────────┐
│  Bronze Layer           │  zebpay.bronze.raw_*
│  (raw ingested data)    │
└────────┬────────────────┘
         ▼
┌─────────────────────────┐
│  DLT Silver Pipeline    │  02_silver_transformations
│  → silver.* (views)     │
└────────┬────────────────┘
         ▼
┌─────────────────────────┐
│  Silver Processing      │  03_silver_processing
│  → silver_secured.*     │  MERGE + data quality
│  + ABAC policies        │  row filters + column masks
└────────┬────────────────┘
         ▼
┌─────────────────────────┐
│  Genie Space            │  Natural language queries
│  (silver_secured.*)     │
└─────────────────────────┘
```

## Project Structure

```
zebpay-demo/
├── databricks.yml                       # Bundle config with dev/prod targets
├── resources/
│   ├── lakeflow_connect_pipeline.yml    # Lakeflow Connect DLT pipeline
│   ├── silver_pipeline.yml              # Bronze-to-Silver DLT pipeline
│   ├── processing_job.yml               # Multi-task processing workflow
│   ├── setup_job.yml                    # Catalog & ABAC setup job
│   └── schemas.yml                      # UC schema definitions
├── src/notebooks/
│   ├── 00_setup_catalog_and_abac.py     # ABAC functions & policies
│   ├── 01_lakeflow_ingestion.py         # DLT ingestion definitions
│   ├── 02_silver_transformations.py     # DLT silver with expectations
│   ├── 03_silver_processing.py          # MERGE-based ETL + quality
│   └── 04_lakeflow_connect_ingestion.py # Lakeflow Connect CDC pipeline
└── .github/workflows/
    ├── pr-validation.yml                # Validate on PR
    ├── deploy-dev.yml                   # Deploy on push to dev
    └── deploy-prod.yml                  # Deploy on push to main
```

## Git Workflow

```
feature/new  →  dev  →  main
    │              │        │
    │         Deploy to   Deploy to
    │          DEV ws     PROD ws
    │
  Development
  & testing
```

1. Create feature branches from `dev`
2. PR to `dev` → validates bundle → auto-deploys to dev workspace
3. PR from `dev` to `main` → validates both targets → auto-deploys to prod workspace

## GitHub Secrets Required

| Secret | Description |
|--------|-------------|
| `DEV_WORKSPACE_HOST` | `https://dbc-1138952c-c48b.cloud.databricks.com` |
| `DEV_WORKSPACE_TOKEN` | PAT or SP token for dev workspace |
| `PROD_WORKSPACE_HOST` | `https://dbc-a8f8bec6-2c5f.cloud.databricks.com` |
| `PROD_WORKSPACE_TOKEN` | PAT or SP token for prod workspace |

## Local Development

```bash
# Validate
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Run a specific job
databricks bundle run silver_layer_processing --target dev

# Deploy to prod
databricks bundle deploy --target prod
```

## ABAC Policies

| Table | Row Filter | Column Masks |
|-------|-----------|--------------|
| users | Region-based access | email, phone |
| trades | High-value visibility (>5L) | - |
| wallets | - | wallet_address |
| transactions | Status-based (non-completed hidden) | blockchain_hash, from/to addresses |

Groups: `compliance_team` (full), `trading_desk`, `ops_team`, `region_west/south/north`
