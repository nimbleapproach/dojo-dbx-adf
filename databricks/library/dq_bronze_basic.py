from pyspark.sql import DataFrame
from pyspark.sql import Row 
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F
import os
from pyspark.sql.functions import col, lit, regexp_replace

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT



def count_missing_string_values(df: DataFrame, column_name: str) -> int:
    """ Return count of null or empty or whitespace. """
    
    # handle missing column, type mismatch if needed
    return (df.where((col(column_name).isNull())
                     | (regexp_replace(col(column_name), r'\s', lit('')) == lit('')))
                     .count())

def dq_load_bronze_stats(src_full_table_name: str, column_id: str, column_attr: list, recreate: bool):
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
    """

    df_src_table = spark.table(f"{src_full_table_name}")
    # split out the source schema and the source table name
    src_schema_name = src_full_table_name.split(".")[0]
    src_table_name = src_full_table_name.split(".")[-1]

    TARGET_TABLE_NAME = "bronze_dq"
    TARGET_TABLE_SCHEMA = "dq"
    TARGET = f"silver_{ENVIRONMENT}.{TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME}"
    
    #remove previous stats for source table 
    if recreate:
        spark.sql(
                f"delete from {TARGET} \
                    where \
                    schema ='{src_schema_name}' \
                    AND table ='{src_table_name}' \
                    AND env ='bronze'"
            )
    
    if column_id in df_src_table.columns:
        # the ID must exists in the source table. once we have checked if exists then continue to process the ID col
        basic_counts = df_src_table.select(
            lit(src_schema_name).alias("schema"),
            lit(src_table_name).alias("table"),
            lit(column_id).alias("col_name"),
            lit("column_id").alias("col_type"),
            lit("bronze").alias("env"),
            F.count(col("*")).alias("rows"),
            F.count(column_id).alias("non_null_col"),
            (col("rows") - col("non_null_col")).alias("null_col"),
            F.count_distinct(column_id).alias("distinct_col"),
            F.current_timestamp().alias("as_of"),
        )

        basic_counts.write.mode("append").option("mergeSchema", "true").saveAsTable(TARGET)
        # end of processing the id col
    else:
        return 'WARNING, The column_id is not found in the table. Please provide a column that exists, ensure it is an ID.'
    # loop through the list of column you wish to produce basic DQ stats against
    for name_col in column_attr:
        # check is column exist in the source table
        if name_col in df_src_table.columns:
            basic_counts = df_src_table.select(
                lit(src_schema_name).alias("schema"),
                lit(src_table_name).alias("table"),
                lit(name_col).alias("col_name"),
                lit("column_attr").alias("col_type"),
                lit("bronze").alias("env"),
                F.count(col("*")).alias("rows"),
                F.count(name_col).alias("non_null_col"),
                (col("rows") - col("non_null_col")).alias("null_col"),
                F.count_distinct(name_col).alias("distinct_col"),
                F.current_timestamp().alias("as_of"),
            )

            missing_names_count = dq_counts.count_missing_string_values(
                df_src_table, name_col
            )

            multiple_name_count = (
                df_src_table.groupBy(column_id)
                .agg(F.count_distinct(name_col).alias("names_count"))
                .where(col("names_count") > lit(1))
                .count()
            )

            entry = basic_counts.select(
                col("*"),
                lit(missing_names_count).alias("missing_names"),
                lit(multiple_name_count).alias("multiple_names"),
            )

            entry.write.mode("append").option("mergeSchema", "true").saveAsTable(TARGET)
        else:
            # handle column that are missing from the source table, set col_type to column_missing
            missing_col = spark.createDataFrame([
                Row(
                    schema=src_schema_name,
                    table=src_table_name,
                    col_name=name_col,
                    col_type="column_missing",
                    env="bronze",
                    as_of=F.current_timestamp()
                )
            ])
            missing_col.write.mode("append").option("mergeSchema", "true").saveAsTable(TARGET)