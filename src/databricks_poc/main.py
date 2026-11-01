import argparse

from databricks.sdk.runtime import spark

from databricks_poc.ingestion.customer_ingestion import ingest_customers


def main():
    parser = argparse.ArgumentParser(description="Databricks customer ingestion POC")

    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)

    args = parser.parse_args()

    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql(f"USE SCHEMA `{args.schema}`")

    ingest_customers(spark)


if __name__ == "__main__":
    main()
