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
# MAGIC ## VEDNOR TO VENDOR GROUP PROCESS

# COMMAND ----------

df_vendor_map = spark.table(f"silver_{ENVIRONMENT}.masterdata.vendor_mapping").alias("vm").filter(F.col("sys_silver_iscurrent") == True)
df_vendor = spark.table(f"{catalog}.{schema}.dim_vendor").alias("v").filter(F.col("is_current") == True)
df_vendor_group = spark.table(f"{catalog}.{schema}.dim_vendor_group").alias("vg").filter(F.col("is_current") == True)

# COMMAND ----------

# DBTITLE 1,stage Vendor Groups table
key_cols=["VendorCode", "VendorGroup"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_vendor_to_vendor_group_dq = check_duplicate_keys(df_vendor_map, key_cols, order_cols)

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_vendor_to_vendor_group_dq 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_vendor_to_vendor_group_dq
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_vendor_to_vendor_group_dq"))

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_vendor_to_vendor_group_stg
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


df_vendor_map_unique = (df_vendor_to_vendor_group_dq
    .filter(F.col('row_number') == 1) 
    .withColumn("Sys_Gold_InsertedDateTime_UTC", to_timestamp(lit('1990-01-01')))
    .withColumn("Sys_Gold_ModifiedDateTime_UTC", to_timestamp(lit('1990-01-01')))
    .withColumn("start_datetime", to_timestamp(lit('1990-01-01')))
    .withColumn("end_datetime", to_timestamp(lit('9999-12-31')))
    .withColumn("is_current", lit(1))
    .withColumn("source_system", lit('Managed Datasets'))
    .select ("SID","VendorCode","VendorGroup","Sys_Bronze_InsertDateTime_UTC","Sys_Silver_InsertDateTime_UTC","Sys_Silver_ModifedDateTime_UTC","Sys_Gold_InsertedDateTime_UTC","Sys_Gold_ModifiedDateTime_UTC","start_datetime","end_datetime","is_current", "source_system"))


# Multiple column rename:
columns_to_rename = {
    "VendorGroup": "vendor_group_code", 
    "VendorCode": "vendor_code", 
    "SID": "local_link_id"
}

df_vendor_map_unique = rename_columns(df_vendor_map_unique, columns_to_rename)

#clean column names
df_vendor_map_unique = clean_column_names(df_vendor_map_unique)

# df_vendor_map_with_keys = df_vendor_map_unique.alias("vm")\
#     .join(df_vendor, ["vendor_code"], "left")\
#     .join(df_vendor_group, ["vendor_group_code"], "left")\
#     .select(
#         "vm.*",
#         F.col("vendor_pk").alias("vendor_fk"),
#         F.col("v.source_system_fk").alias("source_system_fk"),
#         F.col("vendor_group_pk").alias("vendor_group_fk")
#     )


# Then write the data to the table
(df_vendor_map_unique.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_vendor_to_vendor_group_stg"))




# COMMAND ----------

df_vendor_to_vendor_group_dq  =spark.table(f"{catalog}.{schema}.link_vendor_to_vendor_group_dq").alias("vgdq")

# COMMAND ----------

df_vendor_master_unique  =spark.table(f"{catalog}.{schema}.link_vendor_to_vendor_group_stg").alias("vg")
