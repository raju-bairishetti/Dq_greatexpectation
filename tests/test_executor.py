import pytest
from pyspark.sql import SparkSession
from dq_framework.executor import RuleExecutor
from pyspark.sql import Row

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("test_executor").getOrCreate()
    yield spark
    spark.stop()

def test_execute_pass_and_fail(spark):
    data = [Row(id=1, val=10), Row(id=2, val=20), Row(id=3, val=30)]
    df = spark.createDataFrame(data)
    rules = [
        {
            "expectation_name": "expect_column_values_to_not_be_null",
            "classification": "technical",
            "weight": 1.0,
            "criticality": "critical",
            "description": "No nulls allowed",
            "column": "val"
        },
        {
            "expectation_name": "expect_column_values_to_be_between",
            "classification": "business",
            "weight": 2.0,
            "criticality": "high",
            "description": "Value between 0 and 25",
            "column": "val",
            "params": {"min_value": 0, "max_value": 25}
        }
    ]
    executor = RuleExecutor(spark)
    results = executor.run(df, rules)
    assert any(res['success'] for res in results)
    assert any(not res['success'] for res in results)
