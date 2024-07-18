from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, regexp_replace
import pyspark.sql.functions as F


def count_missing_string_values(df: DataFrame, column_name: str) -> int:
    """Return count of null or empty or whitespace.

    Args:
        df (DataFrame): dataframe to perform counts on
        column_name (str): column to perform counts on

    Returns:
        int: count of not empty string values within column
    """

    return (df.where((col(column_name).isNull())
                     | (regexp_replace(col(column_name), r'\s', lit('')) == lit('')))
              .count())

def get_basic_counts(df: DataFrame, 
                     column_name: str, 
                     schema_name: str, 
                     table_name: str,
                     column_type: str, 
                     env: str,
                     as_of: datetime) -> DataFrame:
    """Provides single entry of counts over input dataframe's column.

    Args:
        df (DataFrame): dataframe to perform counts on
        column_name (str): column to perform counts on
        schema_name (str): original schema of table
        table_name (str): original table name
        column_type (str): type of column with regard to count 
        env (str): the original environment of the table
        as_of (datetime): TODO: #

    Returns:
        DataFrame: One row dataframe containing basic dq counts
    """
    return df.select(
                lit(schema_name).alias("schema"),
                lit(table_name).alias("table"),
                lit(column_name).alias("col_name"),
                lit(column_type).alias("col_type"),
                lit(env).alias("env"),
                F.count(col("*")).alias("rows"),
                F.count(column_name).alias("non_null_col"),
                (col("rows") - col("non_null_col")).alias("null_col"),
                F.count_distinct(column_name).alias("distinct_col"),
                lit(as_of).cast("TIMESTAMP").alias("as_of"))

def count_multiple_values(df: DataFrame, id_column: str, name_column: str) -> int:
    """Counts number of id column values that have multiple values in name column. 

    Args:
        df (DataFrame): dataframe to perform counts on
        id_column (str): grouping column
        name_column (str): value column

    Returns:
        int: number of id column values that have multiple values in name column
    """
    return (df.groupBy(id_column)
              .agg(F.count_distinct(name_column).alias("names_count"))
              .where(col("names_count") > lit(1))
              .count())
