# Cancer Analytics Platform

Oncology pathway analytics project using PySpark, Airflow, Snowflake internal stage storage, Snowpark/SQL processing, and a Streamlit reporting layer.

## Architecture

- `airflow/`: orchestration DAGs for medallion processing and Snowflake warehouse promotion
- `pyspark_jobs/`: ingestion, cleansing, dimensional modeling, preview utilities
- `snowflake/`: warehouse setup SQL, RAW -> DW load SQL, semantic model
- `utils/`: local CSV to Snowflake loaders
- `dashboards/`: Streamlit dashboard reading Snowflake semantic views
- `data/raw/`: seed source extracts

## Local pipeline flow

1. Store source CSV extracts in `data/raw`.
2. Run local PySpark ETL to create `data/bronze`, `data/silver`, and data quality outputs.
3. Run [project_2.sql](/d:/Projects/cancer-analytics-project/snowflake/project_2.sql) to create Snowflake `RAW`, `DW`, `SEMANTIC`, and `OPS` schemas plus an internal stage.
4. Upload the source extracts into the Snowflake internal stage and load `RAW` with [load_stage_to_snowflake.py](/d:/Projects/cancer-analytics-project/utils/load_stage_to_snowflake.py).
5. Run [snowpark_transform.py](/d:/Projects/cancer-analytics-project/snowflake/snowpark_transform.py) or [load_dw_from_raw.sql](/d:/Projects/cancer-analytics-project/snowflake/load_dw_from_raw.sql) to build SCD2 dimensions, facts, provider bridge records, semantic views, and audit records.
6. Export curated Snowflake outputs into a local `data/gold` folder with [export_gold_from_snowflake.py](/d:/Projects/cancer-analytics-project/utils/export_gold_from_snowflake.py).
7. Run Streamlit against Snowflake semantic views.

## PySpark

Default local mode:

```powershell
$env:CAP_STORAGE_MODE="local"
$env:CAP_DATA_ROOT="D:\Projects\cancer-analytics-project\data"
python pyspark_jobs/raw_to_bronze.py
python pyspark_jobs/bronze_to_silver.py
python pyspark_jobs/silver_to_gold.py
```

## Snowflake

Setup the warehouse objects:

```sql
-- run snowflake/project_2.sql
```

Upload staged source files and load RAW:

```powershell
python utils/load_stage_to_snowflake.py
```

Promote RAW into DW/SEMANTIC/OPS with Snowpark:

```powershell
python snowflake/snowpark_transform.py
```

Or with SQL:

```sql
-- run snowflake/load_dw_from_raw.sql
```

Export a local gold layer from Snowflake:

```powershell
python utils/export_gold_from_snowflake.py
```

The Cortex Analyst semantic definition is in [cortex_analyst_semantic_model.yaml](/d:/Projects/cancer-analytics-project/snowflake/cortex_analyst_semantic_model.yaml).
Use the verified prompts in that file to test Cortex Analyst against:
- `SEMANTIC.V_ENCOUNTER_OVERVIEW`
- `SEMANTIC.V_PROVIDER_PERFORMANCE`

## Dashboard

Set Snowflake environment variables and run:

```powershell
streamlit run dashboards/app.py
```

The dashboard uses:
- `SEMANTIC.V_ENCOUNTER_OVERVIEW`
- `SEMANTIC.V_FINANCIAL_ANALYTICS`
- `SEMANTIC.V_PROVIDER_PERFORMANCE`
- `OPS.PIPELINE_AUDIT`

If Snowflake credentials are missing, it falls back to local CSV files for preview.

## Screenshots

### Project Architecture

![Project Architecture](docs/images/Project-Architecture.png)

### Dashboard Overview

![Dashboard Overview](docs/images/dashboard-overview.png)

### Clinical Overview

![Clinical Overview](docs/images/clinical-overview.png)

### Provider Analytics

![Provider Analytics](docs/images/dashboard-provider.png)

### Airflow DAG Success

![Airflow DAG Success](docs/images/airflow-dag-success.png)

### Cortex Analyst Query

![Cortex Analyst Query](docs/images/cortex-analyst-query.png)

## Notes

- This project does not require AWS.
- The Snowflake internal stage is the cloud storage landing layer for source CSV extracts.
- Provider performance is modeled through deterministic encounter-to-provider assignment in the warehouse because the source encounter feed does not include `provider_id`.
- Snowflake handles warehouse-side modeling, semantic views, masking policies, access control, and Cortex Analyst semantic metadata.
