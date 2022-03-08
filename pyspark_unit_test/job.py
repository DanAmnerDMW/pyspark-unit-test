from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def to_uppercase(df: DataFrame) -> DataFrame:
    for column in df.columns:
        df = df.withColumn(column, F.upper(F.col(column)))
    return df


def round_numbers(df: DataFrame) -> DataFrame:
    return df.withColumn("Rounded Amount", F.round(F.col("Amount")))
