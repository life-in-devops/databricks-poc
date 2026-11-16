import argparse

from pyspark.sql import SparkSession

from databricks_poc.ingestion.customer_ingestion import ingest_customers


def main():
    parser = argparse.ArgumentParser(description="Customer ingestion into Databricks Bronze and Silver tables")

    parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog catalog name",
    )

    parser.add_argument(
        "--schema",
        required=True,
        help="Unity Catalog schema name",
    )

    parser.add_argument(
        "--source-file",
        default="customers.csv",
        help="Customer CSV filename in the Unity Catalog volume",
    )

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    ingest_customers(
        spark=spark,
        catalog=args.catalog,
        schema=args.schema,
        source_file=args.source_file,
    )


if __name__ == "__main__":
    main()
