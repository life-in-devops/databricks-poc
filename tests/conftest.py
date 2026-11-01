
# """Pytest fixtures for local and Databricks integration tests."""

# import csv
# import json
# import os
# import pathlib

# import pytest
# from pyspark.sql import SparkSession


# @pytest.fixture(scope="session")
# def spark() -> SparkSession:
#     """Create a Spark session for integration tests.

#     Databricks Connect is used when DATABRICKS_INTEGRATION_TESTS=true.
#     Otherwise, use local Spark.
#     """
#     if os.getenv("DATABRICKS_INTEGRATION_TESTS", "false").lower() == "true":
#         from databricks.connect import DatabricksSession

#         return DatabricksSession.builder.getOrCreate()

#     return SparkSession.builder.master("local[2]").appName("databricks-poc-tests").getOrCreate()


# @pytest.fixture()
# def load_fixture(spark: SparkSession):
#     """Load JSON or CSV test data from the fixtures directory."""

#     def _loader(filename: str):
#         path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
#         suffix = path.suffix.lower()

#         if suffix == ".json":
#             rows = json.loads(path.read_text())
#             return spark.createDataFrame(rows)

#         if suffix == ".csv":
#             with path.open(newline="") as file:
#                 rows = list(csv.DictReader(file))
#             return spark.createDataFrame(rows)

#         raise ValueError(f"Unsupported fixture type for: {filename}")

#     return _loader

"""Pytest configuration for offline unit tests."""