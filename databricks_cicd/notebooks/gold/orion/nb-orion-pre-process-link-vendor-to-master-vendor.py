# Databricks notebook source
# MAGIC %run ./nb-orion-common

# COMMAND ----------

# Importing Libraries
import os
import re

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, when, levenshtein, lit, length, greatest,concat, row_number, format_number, sum, date_format, count,to_timestamp
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from datetime import date

from typing import List
from pyspark.sql.window import Window 

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

# MAGIC %md
# MAGIC ## VEDNOR TO MASTER VENDOR PROCESS

# COMMAND ----------

# First get the source system table
df_source_system = spark.table("gold_dev.orion.dim_source_system").alias("ss")

# Join vendor with source system
df_vendor_with_source = spark.table(f"{catalog}.{schema}.dim_vendor").alias("v") \
    .filter(F.col("is_current") == True) \
    .join(
        df_source_system,
        F.col("v.source_system_fk") == F.col("ss.source_system_pk"),
        "inner"
    ) \
    .select(
        "v.*",  # all columns from vendor
        F.col("ss.source_entity")  # just the source_entity from source system
    )

# COMMAND ----------

# DBTITLE 1,stage Vendor Groups table
key_cols=["vendor_code"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
priority_col="source_entity"
priority_map = {
    "UK": 1,
    "DE": 2,
    "CH": 3,
    "NO": 4,
    "DN": 5,
    "SE": 6,
    "FI": 7,
    "NL": 8,
    "BE": 9,
    "FR": 10,
    "DK": 11,
    "AE": 12,
    "AT": 13
}
df_vendor_to_master_vendor_dq = map_parent_child_keys(df_vendor_with_source, key_cols, order_cols, priority_col, priority_map).alias("vmv")

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_vendor_to_master_vendor_dq_pre1 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_vendor_to_master_vendor_dq
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_vendor_to_master_vendor_dq_pre1"))

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_vendor_to_master_vendor_pre1
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


df_vendor_to_master_vendor = (df_vendor_to_master_vendor_dq
    .withColumn("start_datetime", to_timestamp(lit('1990-01-01')))
    .withColumn("end_datetime", to_timestamp(lit('9999-12-31')))
    .withColumn("master_vendor_fk", lit(-1))
    .select ("master_vendor_fk","vendor_pk","vendor_code","vendor_name_internal","is_parent","parent_id","Sys_Gold_InsertedDateTime_UTC","Sys_Gold_ModifiedDateTime_UTC","source_system_fk","start_datetime","end_datetime","is_current"))


# Multiple column rename:
columns_to_rename = {
    "vendor_pk": "vendor_fk", 
    "parent_id": "parent_vendor_fk"
}

df_vendor_to_master_vendor = rename_columns(df_vendor_to_master_vendor, columns_to_rename)


# COMMAND ----------

display(df_with_parent_path)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# First rename columns as specified
df_vendor_to_master_vendor = rename_columns(df_vendor_to_master_vendor, columns_to_rename)

# Start with initial DataFrame
df_with_parents = df_vendor_to_master_vendor \
    .withColumn("tree_level", F.lit(0))

# Add first parent level
df_with_parents = df_with_parents \
    .alias("c") \
    .join(df_vendor_to_master_vendor.alias("vmv"), 
          F.col("c.parent_vendor_fk") == F.col("vmv.vendor_fk"),
          "left") \
    .select(
        "c.*",
        F.col("vmv.vendor_fk").alias("parent_level_1")
    )

# Add subsequent parent levels
for i in range(2, 10):  # Get parents up to level 9
    df_with_parents = df_with_parents \
        .alias("c") \
        .join(df_vendor_to_master_vendor.alias("vmv"), 
              F.col(f"parent_level_{i-1}") == F.col("vmv.vendor_fk"),
              "left") \
        .select(
            "c.*",
            F.col("vmv.vendor_fk").alias(f"parent_level_{i}")
        )

# Get the ultimate parent
final_df = df_with_parents \
    .withColumn(
        "master_vendor_fk",
        F.when(F.col("is_parent"), F.col("vendor_fk"))
         .otherwise(F.coalesce(
             *[F.col(f"parent_level_{i}") for i in range(1, 10)][::-1]
         ))
    ) \
    .select(
        "master_vendor_fk",
        "vendor_fk",
        "vendor_code",
        "vendor_name_internal",
        "is_parent",
        "parent_vendor_fk",
        "Sys_Gold_InsertedDateTime_UTC",
        "Sys_Gold_ModifiedDateTime_UTC",
        "source_system_fk",
        "start_datetime",
        "end_datetime",
        "is_current"
    )

# COMMAND ----------

# display(final_df.filter(F.col("vendor_code") == "1000"))
# display(final_df)

# COMMAND ----------


#clean column names
df_vendor_to_master_vendor = clean_column_names(df_vendor_to_master_vendor)


# Then write the data to the table
(df_vendor_to_master_vendor.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_vendor_to_master_vendor_pre1"))




# COMMAND ----------

df_vendor_to_master_vendor  =spark.table(f"{catalog}.{schema}.link_vendor_to_master_vendor_pre1").alias("vmv")

# COMMAND ----------

display(df_vendor_to_master_vendor.filter(F.col("vendor_name_internal").isin('n-able MSP')))

# COMMAND ----------

df_vendor_to_master_vendor.display()
