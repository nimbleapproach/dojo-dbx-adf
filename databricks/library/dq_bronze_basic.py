import os
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Row, SparkSession
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

from library.dq_counts import (
    count_missing_string_values,
    count_multiple_values,
    get_basic_counts,
)

def get_env() -> str:
    """Returns environment name.

    Returns:
        str: environment name
    """
    return os.environ["__ENVIRONMENT__"]


def dq_load_bronze_stats(spark: SparkSession, 
                         src_full_table_name: str, 
                         column_id: str, 
                         column_attr: list, 
                         recreate: bool) -> str:
    """Populate the DQ table with basic stats

    The function populates the basic statistics against of a table. It will get total rows, distinct
    counts and counts of missing values etc

    the function will load the stats into dq.bronze_dq

    Args:
        src_full_table_name (str): name of the intended table you wish to perform Basic DQ checks
                                    eg [schema.table_name]
        column_id (str): column you class as the unique identifier to the table
        column_attr (list): a list of all the columns you would perform statistics against
        recreate (bool): remove existing stats

    Returns:
        str: Message reporting on success.
    """    

    df_src_table = spark.table(f"{src_full_table_name}")
    src_schema_name, src_table_name = src_full_table_name.split(".")

    TARGET_TABLE_NAME = "bronze_dq"
    TARGET_TABLE_SCHEMA = "dq"
    TARGET = f"silver_{get_env()}.{TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME}"

    AS_OF = datetime.now(timezone.utc)

    #remove previous stats for source table 
    if recreate:
        spark.sql(
                f"""delete from {TARGET}
                    where schema ='{src_schema_name}'
                    AND table ='{src_table_name}'
                    AND env ='bronze'
                """
            )
    
    # set up the result df with target schema
    target_df_schema = spark.table(TARGET).schema
    target_df = spark.createDataFrame([], schema=target_df_schema)
    
    # the ID must exists in the source table. once we have checked if exists then continue to process the ID col
    if column_id in df_src_table.columns:       
        basic_counts = get_basic_counts(df_src_table, 
                                        column_name=column_id,
                                        table_name=src_table_name,
                                        schema_name=src_schema_name,
                                        env="bronze",
                                        column_type= "column_id",
                                        as_of=AS_OF)
        entry = basic_counts.select(
                    col("*"),
                    lit(None).alias("missing_names"),
                    lit(None).alias("multiple_names"),
                )
        target_df = target_df.union(entry)
    else:
        return 'WARNING, The column_id is not found in the table. Please provide a column that exists, ensure it is an ID.'
    
    # loop through the list of column you wish to produce basic DQ stats against
    for name_col in column_attr:
        # check is column exist in the source table
        if name_col in df_src_table.columns:
            basic_counts = get_basic_counts(df_src_table, 
                                    column_name=name_col,
                                    table_name=src_table_name,
                                    schema_name=src_schema_name,
                                    env="bronze",
                                    column_type= "column_attr",
                                    as_of=AS_OF)

            missing_names_count = count_missing_string_values(df_src_table, name_col)
            multiple_name_count = count_multiple_values(df_src_table, id_column=column_id, name_column=name_col)

            entry = basic_counts.select(
                col("*"),
                lit(missing_names_count).alias("missing_names"),
                lit(multiple_name_count).alias("multiple_names"),
            )

            target_df = target_df.union(entry)
        else:
            # handle column that are missing from the source table, set col_type to column_missing
            # the count columns to null
            missing_col = spark.createDataFrame(
                [(
                    src_schema_name,
                    src_table_name,
                    name_col,
                    "column_missing",
                    "bronze",
                    None,
                    None,
                    None,
                    None,
                    AS_OF,
                    None,
                    None
                ),]
                , schema=target_df_schema
            )
            target_df = target_df.union(missing_col)

    # write stats about table once
    target_df.write.mode("append").option("mergeSchema", "true").saveAsTable(TARGET)

    return 'Success'