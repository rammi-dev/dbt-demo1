# dbt Dremio Presentation - Demo Execution Flow

This document outlines the step-by-step process to demonstrate the dbt project with Dremio and Nessie.

## Prerequisites
- Dremio instance running with Nessie catalog configured as `nessie`.
- dbt configured with `dlh_demo` profile.
- Activate dbt virtual environment: `source dbt-test/bin/activate`

## Environment Targets

This project supports multiple environments via dbt targets:

- **`dev`** (default): Local development environment
- **`integration`**: Integration/staging environment
- **`prod`**: Production environment

To run against a specific target:
```bash
# Development (default)
dbt run

# Integration
dbt run --target integration

# Production
dbt run --target prod
```

All commands below use the default `dev` target. To use a different target, add `--target <name>` to any dbt command.

## Step 1: Setup Source Tables (DDL)
Run the DDL script to create the Iceberg tables in the Nessie catalog.

```bash
# Run in Dremio SQL Runner or via API
setup/01_create_source_tables.sql
```

## Step 2: Initial Data Load (DML)
Insert the initial dataset (Partitions 1-5, dates 2024-01-01 to 2024-01-05).

```bash
# Run in Dremio SQL Runner or via API
setup/02_insert_initial_data.sql
```

## Step 3: Run Snapshots
Create the snapshot tables before running models that depend on them.

```bash
# First run - full snapshot (override default to check all data)
dbt snapshot --vars '{"snapshot_lookback_days": null}'

# Subsequent runs - incremental snapshot (uses default: last 30 days)
dbt snapshot
```

*Expected Output:*
- `customers_snapshot`: Created in `nessie.dlh_demo_snapshots`
- `orders_snapshot`: Created in `nessie.dlh_demo_snapshots`

## Step 4: Initial dbt Run
Run the dbt project to materialize all models.

```bash
dbt run
```

*Expected Output:*
- `base_data`: Ephemeral (not materialized)
- `customer_orders`: Table in `nessie.dlh_demo_normalized`
- `stage_1_view`: Table in `nessie.dlh_demo_presentation`
- `stage_2_table`: Table in `nessie.dlh_demo_presentation`
- `stage_3_incremental`: Incremental Table in `nessie.dlh_demo_presentation` (processed 5 partitions)
- `stage_4_reflection`: View in `nessie.dlh_demo_presentation` (with Reflection)
- `stage_5_scd_analysis`: Table in `nessie.dlh_demo_presentation`

## Step 5: Verify Initial State
Run tests to ensure data integrity.

```bash
dbt test
```

## Step 6: Incremental Data Load (DML)
Insert new data for the next day (Partition 6, date 2024-01-06).

```bash
# Run in Dremio SQL Runner or via API
setup/03_insert_incremental_data.sql
```

## Step 7: Incremental dbt Run
Run dbt again, specifically targeting the incremental model to demonstrate efficient processing.

```bash
# Set environment variable for partition pruning (REQUIRED)
# This variable is used by the get_partition_filter macro to filter data
export DBT_PARTITION_DATE='2024-01-06'

# Run incremental model
dbt run --select stage_3_incremental
```

*Expected Output:*
- `stage_3_incremental`: Only processes new data (Partition 6)

## Step 8: Full Refresh Options

### Option A: Full Refresh Specific Model
Rebuild the incremental model from scratch (useful after schema/logic changes):

```bash
dbt run --select stage_3_incremental --full-refresh
```

### Option B: Full Refresh All Models
Rebuild all models from scratch:

```bash
dbt run --full-refresh
```

### Option C: Full Refresh Downstream Models
Refresh models that depend on the incremental model:

```bash
dbt run --select stage_3_incremental+ --full-refresh
```

## Step 9: Update Downstream Models (Optional)
## Step 9: Update Downstream Models (Optional)
Demonstrate full refresh of downstream models to reflect new data.

```bash
dbt run --select stage_4_reflection stage_5_scd_analysis
```

## Step 10: Final Verification
Run tests again to confirm new data is correctly integrated.

```bash
dbt test
```

## Additional Commands

### Run Specific Model with Dependencies
```bash
# Run a model and all its upstream dependencies
dbt run --select +stage_4_reflection

# Run a model and all its downstream dependencies
dbt run --select stage_2_table+
```

### Run by Tag
```bash
# Run all incremental models
dbt run --select tag:incremental

# Run all presentation layer models
dbt run --select tag:presentation
```

### Run Modified Models Only
```bash
# Run models that have changed since last run
dbt run --select state:modified --state ./target
```
