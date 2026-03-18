from datetime import datetime, UTC
import os
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"

TABLE_FILE_MAP = {
    "PATIENTS": "patients.csv",
    "PROVIDERS": "providers.csv",
    "ENCOUNTERS": "encounters.csv",
    "CANCER_CATALOG": "cancer_catalog.csv",
    "BILLING": "billing.csv",
}


def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "CANCER_ANALYTICS_DB"),
        schema="RAW",
        role=os.environ.get("SNOWFLAKE_ROLE", "CANCER_ANALYTICS_ADMIN"),
    )


def prepare_dataframe(table_name: str, csv_path: Path, pipeline_run_id: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = [column.strip().upper().replace(" ", "_").replace("-", "_") for column in df.columns]
    df["PIPELINE_RUN_ID"] = pipeline_run_id
    df["INGESTED_AT"] = datetime.now(UTC).replace(tzinfo=None)
    df["SOURCE_FILE_NAME"] = csv_path.name

    if table_name in {"PATIENTS", "ENCOUNTERS"} and "DOB" in df.columns:
        df["DOB"] = pd.to_datetime(df["DOB"], errors="coerce").dt.date
    if table_name == "ENCOUNTERS" and "DATE" in df.columns:
        df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce").dt.date

    return df


def main():
    pipeline_run_id = datetime.now(UTC).strftime("%Y%m%d%H%M%S")

    with connect() as conn:
        with conn.cursor() as cur:
            cur.execute("USE DATABASE CANCER_ANALYTICS_DB")
            cur.execute("USE SCHEMA RAW")

        for table_name, file_name in TABLE_FILE_MAP.items():
            csv_path = RAW_DATA_DIR / file_name
            df = prepare_dataframe(table_name, csv_path, pipeline_run_id)

            print(f"Loading {csv_path.name} into RAW.{table_name}")
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE IF EXISTS RAW.{table_name}")

            success, _, rows, _ = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                schema="RAW",
                auto_create_table=False,
                overwrite=False,
            )
            if not success:
                raise RuntimeError(f"Failed to load RAW.{table_name}")
            print(f"Loaded {rows} rows into RAW.{table_name}")


if __name__ == "__main__":
    main()
