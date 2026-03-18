from pipeline_utils import build_spark, get_paths


def main():
    spark = build_spark("Preview Gold Fact")
    paths = get_paths()

    df = spark.read.parquet(f"{paths['gold']}/fact_encounters")
    df.printSchema()
    df.show(10, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
