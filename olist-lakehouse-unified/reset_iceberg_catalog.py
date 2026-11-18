
from common.spark_utils import spark_session

def main():
    spark = spark_session()

    namespaces = ["bronze", "silver", "gold"]

    for namespace in namespaces:
        namespace_name = f"iceberg.{namespace}"

        print(f"\n========== RESET {namespace_name} ==========")

        tables = spark.sql(
            f"SHOW TABLES IN {namespace_name}"
        ).collect()

        for row in tables:
            table_name = row["tableName"]
            full_name = f"{namespace_name}.`{table_name}`"

            print(f"[DROP] {full_name}")

            spark.sql(f"DROP TABLE IF EXISTS {full_name}")

        print(f"[DONE] {namespace_name}")

    spark.catalog.clearCache()
    spark.stop()

    print("\n[FINISHED] Iceberg catalog reset completed.")


if __name__ == "__main__":
    main()
