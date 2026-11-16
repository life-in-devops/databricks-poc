import argparse

import mlflow
import mlflow.sklearn
from pyspark.sql import SparkSession
from sklearn.compose import ColumnTransformer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--experiment", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    table_name = f"{args.catalog}.{args.schema}.silver_customers"

    df = (
        spark.table(table_name)
        .select(
            "gender",
            "age",
            "tenure_months",
            "monthly_charges",
            "total_charges",
            "churn",
        )
        .dropna()
    )

    data = df.toPandas()
    data = data[data["churn"].isin(["Yes", "No"])]

    if len(data) < 20 or data["churn"].nunique() != 2:
        raise ValueError("Insufficient training data or missing churn class")

    features = [
        "gender",
        "age",
        "tenure_months",
        "monthly_charges",
        "total_charges",
    ]

    X = data[features]
    y = (data["churn"] == "Yes").astype(int)

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y,
    )

    preprocessing = ColumnTransformer(
        transformers=[
            ("gender", OneHotEncoder(handle_unknown="ignore"), ["gender"]),
            (
                "numeric",
                StandardScaler(),
                ["age", "tenure_months", "monthly_charges", "total_charges"],
            ),
        ]
    )

    model = Pipeline(
        steps=[
            ("preprocessing", preprocessing),
            ("classifier", LogisticRegression(max_iter=1000, random_state=42)),
        ]
    )

    mlflow.set_experiment(args.experiment)

    with mlflow.start_run(run_name="customer_churn_logistic_regression"):
        mlflow.log_params(
            {
                "algorithm": "LogisticRegression",
                "training_table": table_name,
                "training_records": len(X_train),
                "test_records": len(X_test),
                "test_size": 0.2,
                "random_state": 42,
            }
        )

        model.fit(X_train, y_train)
        predictions = model.predict(X_test)

        metrics = {
            "accuracy": accuracy_score(y_test, predictions),
            "precision": precision_score(y_test, predictions, zero_division=0),
            "recall": recall_score(y_test, predictions, zero_division=0),
            "f1_score": f1_score(y_test, predictions, zero_division=0),
        }
        mlflow.log_metrics(metrics)

        input_example = X_train.head(3)
        signature = mlflow.models.infer_signature(input_example, model.predict(input_example))

        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            name="customer_churn_classifier",
            signature=signature,
            input_example=input_example,
        )

        print("Training completed")
        print(f"Training table: {table_name}")
        print(f"Metrics: {metrics}")
        print(f"Model URI: {model_info.model_uri}")


if __name__ == "__main__":
    main()
