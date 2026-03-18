import os
import re
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim


def _as_uri(path_value: Path) -> str:
    return path_value.resolve().as_uri()


def build_spark(app_name: str) -> SparkSession:
    python_executable = sys.executable
    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[1]")
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.python.worker.reuse", "false")
        .config("spark.python.use.daemon", "false")
        .config("spark.python.worker.faulthandler.enabled", "true")
        .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
    )

    if get_storage_mode() == "s3":
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )

    return builder.getOrCreate()


def get_storage_mode() -> str:
    return os.getenv("CAP_STORAGE_MODE", "local").strip().lower()


def get_paths() -> dict[str, str]:
    storage_mode = get_storage_mode()

    if storage_mode == "s3":
        bucket = os.getenv("CAP_S3_BUCKET", "cancer-analytics-project-nvr").strip()
        base_path = f"s3a://{bucket}"
        return {
            "raw": f"{base_path}/raw",
            "bronze": f"{base_path}/bronze",
            "silver": f"{base_path}/silver",
            "gold": f"{base_path}/gold",
            "ops": f"{base_path}/ops",
        }

    project_root = Path(__file__).resolve().parents[1]
    data_root = Path(os.getenv("CAP_DATA_ROOT", str(project_root / "data")))

    return {
        "raw": _as_uri(data_root / "raw"),
        "bronze": _as_uri(data_root / "bronze"),
        "silver": _as_uri(data_root / "silver"),
        "gold": _as_uri(data_root / "gold"),
        "ops": _as_uri(data_root / "ops"),
    }


def normalise_column_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def normalise_columns(df):
    renamed_df = df
    for source_name in df.columns:
        renamed_df = renamed_df.withColumnRenamed(source_name, normalise_column_name(source_name))
    return renamed_df


def trim_string_columns(df):
    trimmed_df = df
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            trimmed_df = trimmed_df.withColumn(field.name, trim(col(field.name)))
    return trimmed_df


def spark_path_exists(spark: SparkSession, path: str) -> bool:
    try:
        jvm = spark._jvm
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(path), hadoop_conf)
        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))
    except Exception:
        return False
