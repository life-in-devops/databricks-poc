import argparse

import mlflow
import mlflow.sklearn
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    model_name = f"{args.catalog}.{args.schema}.customer_churn_model"
    source_table = f"{args.catalog}.{args.schema}.silver_customers"
    output_table = f"{args.catalog}.{args.schema}.customer_churn_predictions"

    mlflow.set_registry_uri("databricks-uc")

    model = mlflow.sklearn.load_model(model_uri=f"models:/{model_name}@Champion")

    customers = (
        spark.table(source_table)
        .select(
            "customer_id",
            "gender",
            "age",
            "tenure_months",
            "monthly_charges",
            "total_charges",
        )
        .dropna()
    )

    data = customers.toPandas()

    features = [
        "gender",
        "age",
        "tenure_months",
        "monthly_charges",
        "total_charges",
    ]

    predictions = model.predict(data[features])

    data["predicted_churn"] = ["Yes" if prediction == 1 else "No" for prediction in predictions]

    result = spark.createDataFrame(data[["customer_id", "predicted_churn"]]).withColumn(
        "prediction_timestamp", current_timestamp()
    )

    (result.write.format("delta").mode("overwrite").saveAsTable(output_table))

    print(f"Model: {model_name}@Champion")
    print(f"Predictions written to: {output_table}")
    print(f"Total predictions: {result.count()}")


if __name__ == "__main__":
    main()
