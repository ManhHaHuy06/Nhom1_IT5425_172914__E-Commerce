
from common.spark_utils import spark_session

def main():
    spark = spark_session()

    for namespace in ["bronze", "silver", "gold"]:
        print(f"\n========== iceberg.{namespace} ==========")

        try:
            tables = spark.sql(
                f"SHOW TABLES IN iceberg.{namespace}"
            ).collect()

            if not tables:
                print("[EMPTY] No registered tables")
            else:
                for row in tables:
                    print("[FOUND]", row["tableName"])

        except Exception as exc:
            print("[ERROR]", namespace, str(exc))

    spark.stop()


if __name__ == "__main__":
    main()
