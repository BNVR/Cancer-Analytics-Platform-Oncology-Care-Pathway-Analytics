from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import snowflake.connector


PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_DATA_DIR = PROJECT_ROOT / "data" / "gold"

EXPORT_OBJECTS = {
    "dim_patients_current": "SELECT * FROM DW.DIM_PATIENTS_CURRENT",
    "dim_cancer_catalog_current": "SELECT * FROM DW.DIM_CANCER_CATALOG_CURRENT",
    "dim_providers": "SELECT * FROM DW.DIM_PROVIDERS",
    "dim_date": "SELECT * FROM DW.DIM_DATE",
    "fact_encounters": "SELECT * FROM DW.FACT_ENCOUNTERS",
    "encounter_overview": "SELECT * FROM SEMANTIC.V_ENCOUNTER_OVERVIEW",
    "financial_analytics": "SELECT * FROM SEMANTIC.V_FINANCIAL_ANALYTICS",
    "provider_performance": "SELECT * FROM SEMANTIC.V_PROVIDER_PERFORMANCE",
    "pipeline_audit": "SELECT * FROM OPS.PIPELINE_AUDIT",
}


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "CANCER_ANALYTICS_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "SEMANTIC"),
        role=os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    )


def export_query(conn, name: str, query: str) -> None:
    print(f"Exporting {name}")
    df = pd.read_sql(query, conn)
    output_path = GOLD_DATA_DIR / f"{name}.csv"
    df.to_csv(output_path, index=False)
    print(f"Wrote {len(df)} rows to {output_path}")


def main() -> None:
    GOLD_DATA_DIR.mkdir(parents=True, exist_ok=True)

    with connect() as conn:
        for name, query in EXPORT_OBJECTS.items():
            export_query(conn, name, query)


if __name__ == "__main__":
    main()
