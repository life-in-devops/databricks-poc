
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

SOURCE_PATH = "/Volumes/poc/dev/customer_data/customers.csv"

BRONZE_TABLE = "poc.dev.bronze_customers"
SILVER_TABLE = "poc.dev.silver_customers"

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("monthly_charges", DoubleType(), True),
    StructField("total_charges", DoubleType(), True),
    StructField("churn", StringType(), True),
])


def ingest_customers(spark: SparkSession) -> None:
    # 1. Read source CSV
    customers_df = (
        spark.read
        .option("header", "true")
        .schema(CUSTOMER_SCHEMA)
        .csv(SOURCE_PATH)
    )

    # 2. Add ingestion metadata
    bronze_df = customers_df.withColumn(
        "ingestion_timestamp", current_timestamp()
    )

    # 3. Write raw data to Bronze
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(BRONZE_TABLE)
    )

    # 4. Read Bronze data
    bronze_data = spark.table(BRONZE_TABLE)

    # 5. Basic data-quality filtering and cleaning
    silver_df = (
        bronze_data
        .filter(col("customer_id").isNotNull())
        .filter(col("age").isNotNull())
        .filter(col("tenure_months").isNotNull())
        .filter(col("monthly_charges").isNotNull())
        .withColumn("gender", trim(col("gender")))
        .withColumn("churn", trim(col("churn")))
        .filter(col("churn").isin("Yes", "No"))
    )

    # 6. Write cleaned data to Silver
    (
        silver_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SILVER_TABLE)
    )

    print("Customer ingestion completed successfully.")
    print(f"Bronze records: {spark.table(BRONZE_TABLE).count()}")
    print(f"Silver records: {spark.table(SILVER_TABLE).count()}")