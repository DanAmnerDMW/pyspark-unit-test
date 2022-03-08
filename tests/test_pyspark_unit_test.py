from pyspark.sql import SparkSession, DataFrame

import pytest
from pyspark_unit_test import job


# Get one spark session for the whole test session
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.getOrCreate()


def test_uppercase(spark_session) -> None:

    # Set up data
    columns = ["Subject 1", "Subject 2", "Subject 3"]
    data = [["java", "dbms", "python"], ["OOPS", "SQL", "Machine Learning"]]
    expected_data = [["JAVA", "DBMS", "PYTHON"], ["OOPS", "SQL", "MACHINE LEARNING"]]

    # Create dataframes
    df: DataFrame = spark_session.createDataFrame(data, columns)
    expected_df: DataFrame = spark_session.createDataFrame(expected_data, columns)

    # Apply Transformations
    upper_df = job.to_uppercase(df)

    # Gather result rows
    rows = upper_df.collect()
    expected_rows = expected_df.collect()

    # Compare dataframes row by row
    for row_num, row in enumerate(rows):
        assert row == expected_rows[row_num]


def test_round_numbers(spark_session) -> None:

    # Set up data
    columns = ["Name", "Amount"]
    data = [["Juliet", 2.5], ["Will", 3.1]]
    expected_columns = ["Name", "Amount", "Rounded Amount"]
    expected_data = [["Juliet", 2.5, 3.0], ["Will", 3.1, 3.0]]

    # Create dataframes
    df: DataFrame = spark_session.createDataFrame(data, columns)
    expected_df: DataFrame = spark_session.createDataFrame(expected_data, expected_columns)

    # Apply Transformations
    rounded_df = job.round_numbers(df)

    # Gather result rows
    rows = rounded_df.collect()
    expected_rows = expected_df.collect()

    # Compare dataframes row by row
    for row_num, row in enumerate(rows):
        assert row == expected_rows[row_num]
