# Databricks notebook source
# MAGIC %md
# MAGIC ##### Notebook to produce Basic DQ metrics on a list of NUVIAS CRM tables

# COMMAND ----------

# MAGIC %run ../library/nb-enable-imports

# COMMAND ----------

import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"bronze_{ENVIRONMENT}")

# COMMAND ----------
try:
    EXPLORE_BRONZE = bool(dbutils.widgets.get("wg_explorebronze") == 'False')
except:
    dbutils.widgets.dropdown(name = "wg_explorebronze", defaultValue = 'False', choices =  ['False','True'])
    EXPLORE_BRONZE = bool(dbutils.widgets.get("wg_explorebronze") == 'False')


# COMMAND ----------

database_name = "nuav_prod_sqlbyod"
tables = spark.catalog.listTables(database_name)

# explore a database for all the tables that have been recently reloaded.
if EXPLORE_BRONZE=="True":
    for table in tables:
        table_name = table.name
        df = spark.table(f"{database_name}.{table_name}")
        df = df.where("Sys_Bronze_InsertDateTime_UTC>'2024-07-01'")
        if df.count() > 0:
            print(f"Recently Loaded at....{database_name}.{table_name}")

# COMMAND ----------

from pyspark.sql.functions import col, lit
import pyspark.sql.functions as F

from library import dq_counts

# COMMAND ----------


def dq_build_raw(src_full_table_name: str, column_id: str, column_attr: list):
    """Populate the DQ table with basic stats

    The function populates the basic statistics against of a table. It will get total rows, distinct
    counts and counts of missing values etc

    the function will load the stats into dq.bronze_dq

    Args:
        src_full_table_name (str): name of the intended table you wish to perform Basic DQ checks
                                    eg [schema.table_name]
        column_id (str): column you class as the unique identifier to the table
        column_attr (list): a list of all the columns you would perform statistics against
    """

    df_src_table = spark.table(f"{src_full_table_name}")
    # split out the source schema and the source table name
    src_schema_name = src_full_table_name.split(".")[0]
    src_table_name = src_full_table_name.split(".")[-1]

    TARGET_TABLE_NAME = "bronze_dq"
    TARGET_TABLE_SCHEMA = "dq"
    TARGET = f"silver_{ENVIRONMENT}.{TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME}"

    # first process the id col
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

    # loop through the list of column you wish to produce basic DQ stats against
    for name_col in column_attr:
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


# COMMAND ----------

# Define the source tables you wish to perform Basic DQ Chesk againsts
data = [
    ("nuav_prod_sqlbyod.ara_dim_customer", "CustomerID", ["CustomerName"]),
    (
        "nuav_prod_sqlbyod.dbo_purchpurchaseorderlinestaging",
        "ITEMNUMBER",
        ["PURCHASEREBATEVENDORGROUPID"],
    ),
    (
        "nuav_prod_sqlbyod.dbo_vendvendorgroupstaging",
        "VENDORGROUPID",
        ["VENDORGROUPID", "DESCRIPTION", "DATAAREAID"],
    ),
    (
        "nuav_prod_sqlbyod.dbo_vendvendorv2staging",
        "VENDORACCOUNTNUMBER",
        ["VENDORACCOUNTNUMBER", "VENDORGROUPID", "DATAAREAID"],
    ),
    (
        "nuav_prod_sqlbyod.pbi_customer",
        "CUSTOMERID",
        ["CUSTOMERNAME", "LASTTRANSACTIONDATE", "CUSTOMERSTAGE", "ENTITY"],
    ),
    (
        "nuav_prodtrans_sqlbyod.dbo_vendvendorv2staging",
        "VENDORACCOUNTNUMBER",
        ["VENDORGROUPID", "DATAAREAID"],
    ),
]

# Create a DataFrame
df_dq_list = spark.createDataFrame(
    data, ["full_table_name", "key_column", "attribute_columns"]
)

# COMMAND ----------


TARGET_TABLE_NAME = "bronze_dq"
TARGET_TABLE_SCHEMA = "dq"
TARGET = f"silver_{ENVIRONMENT}.{TARGET_TABLE_SCHEMA}.{TARGET_TABLE_NAME}"


# Verify that the table exists
if spark.catalog.tableExists(TARGET):
    for row in df_dq_list.select("full_table_name").distinct().collect():
        # remove exists stats held within the bronze_dq table for the tables that are about to be processed
        table_name = row["full_table_name"]
        spark.sql(
            f"delete from {TARGET} where COALESCE(schema ,'.',table) ='{table_name}'"
        )


for row in df_dq_list.collect():
    # create paramaters that are being passed into the dq_build_raw function
    table_name = row["full_table_name"]
    key = row["key_column"]
    attributes = row["attribute_columns"]
    print(f"dq_build_raw... {table_name} key {key} attributes {attributes}")
    # execute the function to load the stats for the source table
    dq_build_raw(src_full_table_name=table_name, column_id=key, column_attr=attributes)
