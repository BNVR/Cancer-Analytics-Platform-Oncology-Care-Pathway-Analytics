from pipeline_utils import build_spark, get_paths


def main():
    spark = build_spark("Inspect Silver Data")
    paths = get_paths()

    tables = ["billing", "patients", "providers", "encounters", "cancer_catalog"]

    for table in tables:
        print("\n=============================")
        print(f"TABLE: {table}")
        print("=============================")

        df = spark.read.parquet(f"{paths['silver']}/{table}")
        print("Columns:")
        print(df.columns)
        print("\nSchema:")
        df.printSchema()
        print("\nSample rows:")
        df.show(5, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
