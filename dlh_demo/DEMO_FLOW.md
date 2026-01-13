# dbt Dremio Presentation - Demo Execution Flow

This document outlines the step-by-step process to demonstrate the dbt project with Dremio and Nessie.

## Prerequisites
- Dremio instance running with Nessie catalog configured as `nessie`.
- dbt configured with `dlh_demo` profile.
- Python 3.8+ and [uv](https://github.com/astral-sh/uv) installed.
- Create and activate the virtual environment with uv:

```bash
# From the repo root
cd /home/ca743/Work/repos/dbt-demo1

# Create venv and install dbt-dremio
uv venv
source .venv/bin/activate
uv pip install dbt-dremio

# Point dbt to the repo-level profiles.yml
export DBT_PROFILES_DIR=$(pwd)
cd dlh_demo
```

All dbt commands below assume you are in the `dlh_demo` directory, the venv is active, and `DBT_PROFILES_DIR` is set.

## Step 0: Verify dbt Configuration

Run `dbt debug` to confirm profile, adapter, and Dremio connectivity are working:

```bash
dbt debug
```

*Expected Output:*
- All checks pass (profile found, adapter installed, connection successful)

If `dbt debug` fails, check:
- `DBT_PROFILES_DIR` is set to the repo root (where `profiles.yml` lives)
- `dbt-dremio` adapter is installed in your venv
- Dremio is running and reachable at `127.0.0.1:9047`
- User/password in `profiles.yml` match your Dremio credentials

## Step 0.5: Install dbt Package Dependencies

Install packages defined in `packages.yml` (e.g., `dbt_expectations`):

```bash
dbt deps
```

*Expected Output:*
- Packages installed to `dbt_packages/`

If you skip this step, you'll see:
> Compilation Error: dbt found 1 package(s) specified in packages.yml, but only 0 package(s) installed in dbt_packages.

## Configure Nessie Catalog in Dremio (Storage Tab)
If you haven’t wired Dremio to your Nessie + MinIO/S3 storage yet, add a Nessie source and complete the Storage Tab settings:

1. In Dremio UI, go to Sources → Add Source → select "Nessie" (or "Arctic/Nessie").
2. Basic settings:
	- Source Name: `nessie` (matches the catalog name used by dbt)
	- Endpoint URL: `http://nessie:19120`
	- Default Branch: `main`
	- Authentication: None (for the demo setup)
3. Storage Tab (links Nessie to your data lake storage):
	- Storage Type: S3-compatible (MinIO or AWS S3)
	- Access Key / Secret Key: provide your storage credentials
	- Root Path: base path for Iceberg tables (use bucket name only, e.g., `warehouse` — no leading slash)
	- Region: `us-east-1`
	- Path-style access: ON
	- Compatibility mode: ON
	- Endpoint (for MinIO): `http://minio:9000`
	- API verification (host-side): `http://localhost:19120/api/v1/trees` and `http://localhost:19120/api/v2/trees` should return 200. Inside containers, use `http://nessie:19120`.
4. Connection Properties (click "Add Property") — essential for S3-compatible storage:
	- Name: `fs.s3a.path.style.access` | Value: `true`
	- Name: `fs.s3a.endpoint` | Value: `minio:9000` (or your S3-compatible host:port)
	- Name: `fs.s3a.connection.ssl.enabled` | Value: `false` (use HTTP for MinIO)

### Troubleshooting table creation (Iceberg writes)

If you see errors like `Failed to write manifest list file` when creating tables:
- Ensure the bucket exists (e.g., `warehouse`) and Root Path is exactly that bucket name (no leading slash)
- Confirm Path-style access and Compatibility mode are ON
- Verify connection properties above (especially `fs.s3a.connection.ssl.enabled=false` for MinIO)
- Create the `raw` namespace/folder under `nessie` if it doesn’t exist
- Try a minimal CTAS to validate write path:

```sql
CREATE TABLE nessie.raw.ctas_test AS SELECT 1 AS id;
```

If CTAS fails, check the Dremio job error details (AccessDenied / NoSuchBucket / UnknownHost) and adjust credentials, bucket, or endpoint accordingly.
5. Click "Test", then "Save". You should see a top-level catalog entry named `nessie` in Dremio.

Tip: Create the bucket (e.g., `warehouse`) in MinIO Console at `http://localhost:9001` (user: `admin`, password: `password`) before saving the source.

### Resetting Nessie (Clean Slate)

The demo Nessie server uses **in-memory storage**, so all data (tables, namespaces) is lost on restart. To reset Nessie and start fresh:

```bash
# From repo root
docker compose restart nessie
```

⚠️ **After restarting Nessie:**
1. Refresh the Nessie source in Dremio (or re-add it if needed)
2. Re-run the setup SQL scripts (`01_create_source_tables.sql`, `02_insert_initial_data.sql`)
3. Re-run `dbt snapshot` and `dbt run`

This is useful when you want to:
- Delete unwanted namespaces/schemas
- Start the demo from scratch
- Clear corrupted or test data

## Environment target

This project currently defines only one dbt target: `dev`.

- The active profile in `profiles.yml` is:
	- `dlh_demo.target: dev`
	- Only `dlh_demo.outputs.dev` is present

If you need `integration` or `prod`, add corresponding outputs to `profiles.yml` and switch with `--target <name>`. Example:

```yaml
dlh_demo:
	target: dev
	outputs:
		dev:
			type: dremio
			software_host: 127.0.0.1
			port: 9047
			user: rami
			password: rami123!
			enterprise_catalog_namespace: nessie
			use_ssl: false
			threads: 2
		integration:
			type: dremio
			software_host: integration-host
			port: 9047
			user: <user>
			password: <password>
			enterprise_catalog_namespace: nessie
			use_ssl: false
			threads: 2
		prod:
			type: dremio
			software_host: prod-host
			port: 9047
			user: <user>
			password: <password>
			enterprise_catalog_namespace: nessie
			use_ssl: false
			threads: 2
```

Once added, you can run:

```bash
dbt run                      # uses dev
dbt run --target integration # after you define it
dbt run --target prod        # after you define it
```

Below, commands assume the default `dev` target.

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

### Serve dbt Documentation
Generate and serve interactive documentation for your dbt project:

```bash
# Generate documentation (catalog + manifest)
dbt docs generate

# Serve documentation locally (opens browser at http://localhost:8080)
dbt docs serve

# Serve on a specific port
dbt docs serve --port 8081
```

*Documentation includes:*
- Model lineage graph (DAG)
- Model descriptions and column details
- Test coverage
- Source definitions
- Snapshot configurations

---

## Complete Incremental Flow (End-to-End)

This section demonstrates a complete incremental data pipeline from initial load through multiple incremental updates.

### Prerequisites
- Nessie restarted (clean slate): `docker compose restart nessie`
- Nessie catalog configured in Dremio (see above)
- Virtual environment active with dbt-dremio installed

### Phase 1: Initial Setup and Full Load

```bash
# 1. Navigate to project directory
cd /home/ca743/Work/repos/dbt-demo1
source .venv/bin/activate
export DBT_PROFILES_DIR=$(pwd)
cd dlh_demo

# 2. Verify dbt configuration
dbt debug

# 3. Install package dependencies
dbt deps
```

**In Dremio SQL Runner:**
```sql
-- 4. Create source tables (DDL)
-- Run: setup/01_create_source_tables.sql

-- 5. Insert initial data (Partitions 1-5: 2024-01-01 to 2024-01-05)
-- Run: setup/02_insert_initial_data.sql
```

```bash
# 6. Run initial snapshots
dbt snapshot

# 7. Run all models (full load)
dbt run

# 8. Verify with tests
dbt test
```

*Expected State After Phase 1:*
- Source tables: `orders_iceberg` (20 rows), `customers_iceberg` (10 rows)
- Snapshots created in `dlh_demo_snapshots`
- All presentation models materialized
- `stage_3_incremental`: 20 rows (all initial data)

### Phase 2: First Incremental Update

**In Dremio SQL Runner:**
```sql
-- 1. Insert new orders for 2024-01-06 (Partition 6)
-- Run: setup/03_insert_incremental_data.sql
```

```bash
# 2. Refresh upstream models FIRST (stage_1_view and stage_2_table)
# These must be refreshed before incremental, as stage_3 depends on them
dbt run --select stage_1_view stage_2_table

# 3. Set partition date for incremental processing
export DBT_PARTITION_DATE='2024-01-06'

# 4. Run incremental model (appends only new partition data)
dbt run --select stage_3_incremental

# OR: Run all in correct dependency order with one command
dbt run --select +stage_3_incremental

# 5. Update snapshots to capture any changes
dbt snapshot

# 6. Refresh downstream models
dbt run --select stage_4_reflection stage_5_scd_analysis
```

*Expected State After Phase 2:*
- `stage_1_view`: Refreshed with new orders
- `stage_2_table`: Refreshed with updated customer aggregations
- `stage_3_incremental`: 24 rows (+4 new orders from partition 6)
- Snapshots updated with new order data

**Important:** The `+` prefix in `+stage_3_incremental` runs all upstream dependencies first:
- `stage_1_view` (depends on `base_data` ephemeral)
- `stage_2_table` (depends on `stage_1_view`)
- Then `stage_3_incremental`

### Phase 3: SCD Type 2 Update (Record Changes)

**In Dremio SQL Runner:**
```sql
-- 1. Insert updated versions of existing records
-- Run: setup/04_update_data_for_scd.sql
-- This inserts new rows with same keys but newer timestamps
```

```bash
# 2. Run snapshots to capture SCD changes
dbt snapshot

# 3. Check snapshot results (should show rows with dbt_valid_to populated)
# Query in Dremio:
# SELECT customer_id, customer_segment, dbt_valid_from, dbt_valid_to
# FROM nessie.dlh_demo_snapshots.customers_snapshot
# WHERE customer_id IN (1, 2, 4, 5, 8, 10)
# ORDER BY customer_id, dbt_valid_from;

# 4. Refresh SCD analysis model
dbt run --select stage_5_scd_analysis
```

*Expected State After Phase 3:*
- `customers_snapshot`: Historical versions with `dbt_valid_to` populated
- `orders_snapshot`: Historical versions of updated orders
- `stage_5_scd_analysis`: Shows customer change history

### Phase 4: Additional Incremental Partition

**In Dremio SQL Runner:**
```sql
-- 1. Add more data for 2024-01-07 (Partition 7)
INSERT INTO nessie.raw.orders_iceberg 
(order_id, customer_id, order_date, amount, product_category, order_status)
VALUES
    (1025, 1, DATE '2024-01-07', 225.00, 'Electronics', 'Pending'),
    (1026, 3, DATE '2024-01-07', 89.99, 'Books', 'Completed'),
    (1027, 7, DATE '2024-01-07', 350.00, 'Home & Garden', 'Pending');
```

```bash
# 2. Update partition date
export DBT_PARTITION_DATE='2024-01-07'

# 3. Refresh ALL upstream + incremental in one command
dbt run --select +stage_3_incremental

# This runs in dependency order:
# 1. base_data (ephemeral - compiled inline)
# 2. stage_1_view (refreshed with new data)
# 3. stage_2_table (refreshed with new aggregations)
# 4. stage_3_incremental (appends partition 7 only)

# 4. Verify row count increased
# Query: SELECT COUNT(*) FROM nessie.dlh_demo_presentation.stage_3_incremental
```

### Troubleshooting Incremental Models

**Error: "table already exists"**

This is a known issue with dbt-dremio adapter. The adapter doesn't properly detect existing tables for incremental processing.

*Solution 1: Drop and recreate*
```sql
-- In Dremio SQL Runner
DROP TABLE nessie.dlh_demo_presentation.stage_3_incremental;
```
```bash
dbt run --select stage_3_incremental --full-refresh
```

*Solution 2: Drop leftover temp table*
```sql
-- In Dremio SQL Runner
DROP TABLE IF EXISTS nessie.dlh_demo_presentation."stage_3_incremental__dbt_tmp";
```

**Understanding Incremental Strategy**

dbt-dremio 1.10.0 only supports `incremental_strategy='append'`:
- ✅ `append`: Inserts new rows (no updates to existing)
- ❌ `merge`: Not supported by dbt-dremio adapter
- ❌ `delete+insert`: Not supported

With `append` strategy:
1. Use `is_incremental()` to filter only NEW data
2. Set `DBT_PARTITION_DATE` environment variable
3. The macro `get_partition_filter` builds the WHERE clause

**Macro: get_partition_filter**
```sql
-- Example generated SQL when DBT_PARTITION_DATE='2024-01-06':
WHERE order_date = DATE '2024-01-06'
```

### Verification Queries

Run these in Dremio SQL Runner to verify incremental processing:

```sql
-- Check row counts by partition
SELECT order_date, COUNT(*) as row_count
FROM nessie.dlh_demo_presentation.stage_3_incremental
GROUP BY order_date
ORDER BY order_date;

-- Check snapshot history
SELECT customer_id, customer_segment, dbt_valid_from, dbt_valid_to,
       CASE WHEN dbt_valid_to IS NULL THEN 'CURRENT' ELSE 'HISTORICAL' END as status
FROM nessie.dlh_demo_snapshots.customers_snapshot
ORDER BY customer_id, dbt_valid_from;

-- Check SCD analysis
SELECT customer_id, total_versions, historical_versions
FROM nessie.dlh_demo_presentation.stage_5_scd_analysis
WHERE is_current_version = true
ORDER BY total_versions DESC;
```

