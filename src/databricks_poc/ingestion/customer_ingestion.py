from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, trim
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("gender", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("tenure_months", IntegerType(), True),
        StructField("monthly_charges", DoubleType(), True),
        StructField("total_charges", DoubleType(), True),
        StructField("churn", StringType(), True),
    ]
)


def ingest_customers(spark: SparkSession, catalog: str, schema: str) -> None:
    # Environment-specific paths
    source_path = f"/Volumes/{catalog}/{schema}/customer_data/customers.csv"
    bronze_table = f"{catalog}.{schema}.bronze_customers"
    silver_table = f"{catalog}.{schema}.silver_customers"

    # 1. Read source CSV
    customers_df = spark.read.option("header", "true").schema(CUSTOMER_SCHEMA).csv(source_path)

    # 2. Add ingestion metadata
    bronze_df = customers_df.withColumn("ingestion_timestamp", current_timestamp())

    # 3. Write raw data to Bronze
    (bronze_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table))

    # 4. Read Bronze data
    bronze_data = spark.table(bronze_table)

    # 5. Basic data-quality filtering and cleaning
    silver_df = (
        bronze_data.filter(col("customer_id").isNotNull())
        .filter(col("age").isNotNull())
        .filter(col("tenure_months").isNotNull())
        .filter(col("monthly_charges").isNotNull())
        .withColumn("gender", trim(col("gender")))
        .withColumn("churn", trim(col("churn")))
        .filter(col("churn").isin("Yes", "No"))
    )

    # 6. Write cleaned data to Silver
    (silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table))

    print("Customer ingestion completed successfully.")
    print(f"Bronze records: {spark.table(bronze_table).count()}")
    print(f"Silver records: {spark.table(silver_table).count()}")
