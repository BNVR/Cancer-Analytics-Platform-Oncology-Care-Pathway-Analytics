import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

try:
    import snowflake.connector
except ImportError:  # pragma: no cover
    snowflake = None


@dataclass
class DashboardBundle:
    encounter_overview: pd.DataFrame
    financial_analytics: pd.DataFrame
    provider_performance: pd.DataFrame
    audit_log: pd.DataFrame


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def _snowflake_configured() -> bool:
    required = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ]
    return all(os.getenv(key) for key in required)


def _connect_to_snowflake():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "SEMANTIC"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


def _load_from_snowflake() -> DashboardBundle:
    if snowflake is None or not _snowflake_configured():
        raise RuntimeError("Snowflake connection details are incomplete.")

    query_map = {
        "encounter_overview": "SELECT * FROM SEMANTIC.V_ENCOUNTER_OVERVIEW",
        "financial_analytics": "SELECT * FROM SEMANTIC.V_FINANCIAL_ANALYTICS",
        "provider_performance": "SELECT * FROM SEMANTIC.V_PROVIDER_PERFORMANCE",
        "audit_log": "SELECT * FROM OPS.PIPELINE_AUDIT ORDER BY RECORDED_AT DESC",
    }

    with _connect_to_snowflake() as conn:
        loaded = {
            name: pd.read_sql(query, conn)
            for name, query in query_map.items()
        }

    return DashboardBundle(**loaded)


def _load_from_local_files() -> DashboardBundle:
    patients = pd.read_csv(RAW_DATA_DIR / "patients.csv")
    encounters = pd.read_csv(RAW_DATA_DIR / "encounters.csv")
    billing = pd.read_csv(RAW_DATA_DIR / "billing.csv")
    cancer = pd.read_csv(RAW_DATA_DIR / "cancer_catalog.csv")
    providers = pd.read_csv(RAW_DATA_DIR / "providers.csv")

    provider_dim = providers.copy().reset_index(drop=True)
    encounter_with_provider = encounters.copy().reset_index(drop=True)
    encounter_with_provider["_provider_index"] = encounter_with_provider.index % max(len(provider_dim), 1)
    encounter_with_provider = encounter_with_provider.merge(
        provider_dim.reset_index().rename(columns={"index": "_provider_index"}),
        on="_provider_index",
        how="left",
    )

    encounter_overview = (
        encounter_with_provider.merge(billing, on=["Encounter ID", "Patient ID"], how="left")
        .merge(cancer[["Patient ID", "Cancer Type", "Stage"]], on="Patient ID", how="left")
        .merge(patients[["Patient ID", "Name", "Gender", "Age", "Smoking_Status"]], on="Patient ID", how="left")
    )

    financial_analytics = (
        encounter_overview.assign(ENCOUNTER_DATE=pd.to_datetime(encounter_overview["Date"]))
        .groupby(["ENCOUNTER_DATE", "Treatment", "Insurance", "Payment_Status"], as_index=False)
        .agg(
            ENCOUNTER_COUNT=("Encounter ID", "nunique"),
            TOTAL_BILLING_AMOUNT=("Amount", "sum"),
            AVG_BILLING_AMOUNT=("Amount", "mean"),
        )
    )

    provider_performance = (
        encounter_overview.groupby(["Provider ID", "Name_x", "Specialty", "Location"], as_index=False)
        .agg(
            ENCOUNTER_COUNT=("Encounter ID", "nunique"),
            PATIENT_COUNT=("Patient ID", "nunique"),
            AVG_TREATMENT_RESPONSE_SCORE=("Treatment_Response_Score", "mean"),
            TOTAL_BILLING_AMOUNT=("Amount", "sum"),
            AVG_BILLING_AMOUNT=("Amount", "mean"),
        )
        .rename(columns={"Name_x": "PROVIDER_NAME"})
    )

    audit_log = pd.DataFrame(
        [{"PIPELINE_STEP": "local_preview", "TABLE_NAME": "local_files", "STATUS": "FALLBACK", "RECORDED_AT": pd.Timestamp.utcnow()}]
    )

    return DashboardBundle(
        encounter_overview=encounter_overview,
        financial_analytics=financial_analytics,
        provider_performance=provider_performance,
        audit_log=audit_log,
    )


def load_dashboard_bundle() -> tuple[DashboardBundle, str]:
    try:
        return _load_from_snowflake(), "snowflake"
    except Exception:
        return _load_from_local_files(), "local"
