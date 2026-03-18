import os
import subprocess
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "ramana",
    "start_date": datetime(2026, 3, 14),
    "retries": 1,
}


def resolve_jobs_root() -> Path:
    container_root = Path("/opt/airflow/pyspark_jobs")
    if container_root.exists():
        return container_root
    return Path(__file__).resolve().parents[2] / "pyspark_jobs"


def resolve_project_root() -> Path:
    container_root = Path("/opt/project")
    if container_root.exists():
        return container_root
    return Path(__file__).resolve().parents[2]


def run_spark_job(script_name: str):
    jobs_root = Path(os.getenv("PYSPARK_JOBS_ROOT", str(resolve_jobs_root())))
    spark_submit_bin = os.getenv("SPARK_SUBMIT_BIN", "spark-submit")
    storage_mode = os.getenv("CAP_STORAGE_MODE", "local").lower()
    extra_packages = os.getenv(
        "SPARK_EXTRA_PACKAGES",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    )

    command = [spark_submit_bin]
    if storage_mode == "s3" and extra_packages:
        command.extend(["--packages", extra_packages])
    command.append(str(jobs_root / script_name))

    result = subprocess.run(
        command,
        check=False,
        env=os.environ.copy(),
        text=True,
        capture_output=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    result.check_returncode()


def run_python_script(script_path: str):
    project_root = Path(os.getenv("PIPELINE_PROJECT_ROOT", str(resolve_project_root())))
    python_bin = os.getenv("PIPELINE_PYTHON_BIN", os.sys.executable)
    result = subprocess.run(
        [python_bin, str(project_root / script_path)],
        check=False,
        env=os.environ.copy(),
        text=True,
        capture_output=True,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    result.check_returncode()


with DAG(
    dag_id="cancer_pipeline_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Snowflake-centered oncology analytics pipeline",
) as dag:
    upload_stage_and_load_raw = PythonOperator(
        task_id="upload_stage_and_load_raw",
        python_callable=run_python_script,
        op_kwargs={"script_path": "utils/load_stage_to_snowflake.py"},
    )

    snowpark_transform = PythonOperator(
        task_id="snowpark_transform",
        python_callable=run_python_script,
        op_kwargs={"script_path": "snowflake/snowpark_transform.py"},
    )

    export_gold = PythonOperator(
        task_id="export_gold",
        python_callable=run_python_script,
        op_kwargs={"script_path": "utils/export_gold_from_snowflake.py"},
    )

    upload_stage_and_load_raw >> snowpark_transform >> export_gold
