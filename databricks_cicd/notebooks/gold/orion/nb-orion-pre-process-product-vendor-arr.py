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
from pyspark.sql.functions import col, lower, when, levenshtein, lit, length, greatest,concat, row_number, format_number, sum, date_format, count
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
  
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

from datetime import date
from dateutil.relativedelta import relativedelta

# Widget for full year processing
dbutils.widgets.text("full_year", "0", "Data period")
full_year_process = dbutils.widgets.get("full_year")
full_year_process = {"0": False, "1": True}.get(full_year_process, False)  

# Widget for month period
dbutils.widgets.text("month_period", "YYYY-MM", "Month period")
month_period_process = dbutils.widgets.get("month_period")

# Check if a valid month period was provided
if month_period_process == "YYYY-MM":
    # No valid month provided, use previous month
    today = date.today()
    first_of_this_month = date(today.year, today.month, 1)
    last_month_date = first_of_this_month - relativedelta(months=1)
    month_period_process = last_month_date.strftime('%Y-%m')
else:
    # Validate the provided month period format
    try:
        # Try to parse the provided date
        from datetime import datetime
        datetime.strptime(month_period_process, '%Y-%m')
        # If successful, keep the provided value
    except ValueError:
        # If invalid format, use previous month
        today = date.today()
        first_of_this_month = date(today.year, today.month, 1)
        last_month_date = first_of_this_month - relativedelta(months=1)
        month_period_process = last_month_date.strftime('%Y-%m')

print(f"Processing {month_period_process} sales analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ##SKU VENDOR PROCESS

# COMMAND ----------

from pyspark.sql import functions as F
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import os

def get_sales_analysis_v2(period=None, full_year=False):
    """
    Analyze sales data for a specific period.
    
    Parameters:
    period: str, optional - Format 'YYYY-MM' or 'YYYY' (e.g., '2024-05' or '2024')
    full_year: bool, optional - If True, analyzes entire year regardless of period format
    
    Returns:
    pyspark.sql.DataFrame: Analysis results
    """
    ENVIRONMENT = os.getenv('ENVIRONMENT', 'dev')
    BASE_PATH = f"silver_{ENVIRONMENT}.igsql03"
    
    if period is None:
        today = date.today()
        first_of_this_month = date(today.year, today.month, 1)
        last_month_date = first_of_this_month - relativedelta(months=1)
        period = last_month_date.strftime('%Y-%m')
    
    # Handle period formatting
    if full_year or len(period.split('-')) == 1:
        year = period.split('-')[0] if '-' in period else period
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31"
    else:
        start_date = f"{period}-01"
        end_date = (datetime.strptime(start_date, '%Y-%m-%d') + relativedelta(months=1) - relativedelta(days=1)).strftime('%Y-%m-%d')
    
    # Rest of the function remains the same...
    invoice_lines = spark.table(f"{BASE_PATH}.sales_invoice_line").alias("invoice_lines")
    invoice_headers = spark.table(f"{BASE_PATH}.sales_invoice_header").alias("invoice_headers")
    cr_memo_lines = spark.table(f"{BASE_PATH}.sales_cr_memo_line").alias("cr_memo_lines")
    cr_memo_headers = spark.table(f"{BASE_PATH}.sales_cr_memo_header").alias("cr_memo_headers")
    items = spark.table(f"{BASE_PATH}.item").alias("items")
    
    # Process invoice sales
    invoice_sales = (
        invoice_lines
        .join(
            invoice_headers, 
            (invoice_lines["DocumentNo_"] == invoice_headers["No_"]) &
            (invoice_lines["sys_databaseName"] == invoice_headers["sys_databaseName"])
        )
        .where((F.col("invoice_headers.PostingDate").between(start_date, end_date)) &
               (F.col("invoice_lines.Sys_Silver_IsCurrent") == 1) &
               (F.col("invoice_headers.Sys_Silver_IsCurrent") == 1))
        .select(
            F.col("invoice_lines.No_").alias("sku"),
            F.col("invoice_lines.sys_databaseName").alias("database_name"),
            F.col("invoice_lines.ShortcutDimension1Code"),
            F.when(F.col("invoice_headers.CurrencyFactor") > 0,
                  F.col("invoice_lines.Amount") / F.col("invoice_headers.CurrencyFactor"))
             .otherwise(F.col("invoice_lines.Amount"))
             .alias("SalesLCYLTM")
        )
    )
    
    # Process credit memo sales
    cr_memo_sales = (
        cr_memo_lines
        .join(
            cr_memo_headers, 
            (cr_memo_lines["DocumentNo_"] == cr_memo_headers["No_"]) &
            (cr_memo_lines["sys_databaseName"] == cr_memo_headers["sys_databaseName"])
        )
        .where((F.col("cr_memo_headers.PostingDate").between(start_date, end_date)) &
               (F.col("cr_memo_lines.Sys_Silver_IsCurrent") == 1) &
               (F.col("cr_memo_headers.Sys_Silver_IsCurrent") == 1))
        .select(
            F.col("cr_memo_lines.No_").alias("sku"),
            F.col("cr_memo_lines.sys_databaseName").alias("database_name"),
            F.col("cr_memo_lines.ShortcutDimension1Code"),
            (F.when(F.col("cr_memo_headers.CurrencyFactor") > 0,
                   F.col("cr_memo_lines.Amount") / F.col("cr_memo_headers.CurrencyFactor"))
             .otherwise(F.col("cr_memo_lines.Amount")) * -1)
             .alias("SalesLCYLTM")
        )
    )
    
    sales_ltm = invoice_sales.union(cr_memo_sales)
    
    result = (
        sales_ltm
        .groupBy("sku", "ShortcutDimension1Code", "database_name")
        .agg(F.sum("SalesLCYLTM").alias("SalesLCYLTM"))
        .join(
            items, 
            (F.col("sku") == F.col("items.No_")) &
            (F.col("items.Sys_Silver_IsCurrent") ==1) &
            (F.col("database_name") == F.col("items.sys_databaseName")),
            "left"
        )
        .select(
            F.col("sku"),
            F.col("items.GlobalDimension1Code").alias("VendorCodeItem"),
            F.col("ShortcutDimension1Code").alias("VendorCodePostedDocLine"),
            F.col("items.Description"),
            F.col("items.Description2"),
            F.col("items.Description3"),
            F.col("items.Description4"),
            F.col("items.Type"),
            F.col("items.InventoryPostingGroup"),
            F.col("items.itemdisc_group"),
            F.col("items.VendorNo_"),
            F.col("items.VendorItemNo_"),
            F.col("items.No_Series"),
            F.col("items.GlobalDimension2Code"),
            F.col("items.ManufacturerCode"),
            F.col("items.ManufacturerItemNo_"),
            F.col("items.ItemTrackingCode"),
            F.col("items.LifeCycleFormula"),
            F.col("items.Inactive"),
            F.col("items.EndUserType"),
            F.col("items.ProductType"),
            F.col("items.LicenseType"),
            F.col("items.Status"),
            F.col("items.ProductGroupCode"),
            F.col("items.Subscription"),
            F.col("items.Createdon"),
            F.col("SalesLCYLTM"),
            F.col("items.Sys_Bronze_InsertDateTime_UTC"),
            F.col("items.Sys_Silver_InsertDateTime_UTC"),
            F.col("items.Sys_Silver_IsCurrent"),
            F.col("database_name").alias("sys_databaseName")
        )
        .where(F.col("SalesLCYLTM") != 0)
    )
    
    return result.replace(float('NaN'), None)

# COMMAND ----------


df_link_product_to_vendor_stg_pre_1 = get_sales_analysis_v2(month_period_process, full_year=full_year_process)

# COMMAND ----------


# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_product_to_vendor_stg_pre_1 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_link_product_to_vendor_stg_pre_1
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_product_to_vendor_stg_pre_1"))

# COMMAND ----------

df_link_product_to_vendor_stg_pre_1 = spark.table(f"{catalog}.{schema}.link_product_to_vendor_stg_pre_1")

# COMMAND ----------

# DBTITLE 1,stage igsql03 for dupes: include databasename
#
# this is used for investigation purpose only 
# 

key_cols=["ManufacturerItemNo_","VendorCodeItem", "sys_databasename"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_link_product_to_vendor_database_dq = check_duplicate_keys(df_link_product_to_vendor_stg_pre_1, key_cols, order_cols)

df_link_product_to_vendor_database_dq = df_link_product_to_vendor_database_dq.replace({'NaN': None})


# COMMAND ----------

# DBTITLE 1,mark records that are dupes based on local skus & vendor_name
key_cols=["sku","VendorCodeItem"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_link_local_product_to_vendor_dq = check_duplicate_keys(df_link_product_to_vendor_stg_pre_1, key_cols, order_cols)

df_link_local_product_to_vendor_dq = df_link_local_product_to_vendor_dq.replace({'NaN': None})

# Create a dq table with column defaults enabled
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_local_product_to_vendor_dq 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_link_local_product_to_vendor_dq
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_local_product_to_vendor_dq"))


# Create a clean table with column defaults enabled
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_local_product_to_vendor_unique 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_link_local_product_to_vendor_dq
    .filter(F.col('row_number') == 1)
    .drop(F.col('row_number'))
    .withColumn("has_duplicates", when(F.col('occurrence') > 1, "Y").otherwise(F.lit( "N")))
    .drop(F.col('occurrence'))
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_local_product_to_vendor_unique"))

# COMMAND ----------

df_link_product_to_vendor_stg_pre_1 = spark.table(f"{catalog}.{schema}.link_product_to_vendor_stg_pre_1")

#key_cols=["ManufacturerItemNo_","VendorCodeItem"]
key_cols=["ManufacturerItemNo_","VendorCodeItem"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_link_product_to_vendor_stg_pre_1_dq = check_duplicate_keys(df_link_product_to_vendor_stg_pre_1, key_cols, order_cols)

df_link_product_to_vendor_stg_pre_1_dq = df_link_product_to_vendor_stg_pre_1_dq.replace({'NaN': None})

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_product_to_vendor_dq 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_link_product_to_vendor_stg_pre_1_dq
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_product_to_vendor_dq"))

# COMMAND ----------



# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_product_to_vendor_stg_pre_2
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


df_link_product_to_vendor_stg_pre_2 = (df_link_product_to_vendor_stg_pre_1_dq
    .filter(F.col('row_number') == 1)
    .drop(F.col('row_number'))
    .withColumn("has_duplicates", when(F.col('occurrence') > 1, "Y").otherwise(F.lit( "N")))
    .drop(F.col('occurrence')))

# Then write the data to the table

(df_link_product_to_vendor_stg_pre_2.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.link_product_to_vendor_stg_pre_2"))




# COMMAND ----------

df_vendor_master  =spark.table(f"{catalog}.{schema}.arr_vendor_group_dq").alias("vgdq")

# COMMAND ----------

# DBTITLE 1,will the dupes cause issues with the stock vendor mapping?
result = (
    df_link_product_to_vendor_stg_pre_1_dq
    .join(
        df_vendor_master,
        df_link_product_to_vendor_stg_pre_1_dq['VendorCodeItem'] == df_vendor_master['vendor_group_code'],
        'inner'
    )
    .select('VendorCodeItem', 'VendorGroup') 
    .distinct()
    .groupBy('VendorCodeItem')
    .agg(F.count(col('VendorCodeItem')).alias('occurrence'))
    .orderBy(F.desc('occurrence'))
)

display(result)

# COMMAND ----------

df_link_product_to_vendor_stg_pre_2 = spark.table(f"{catalog}.{schema}.link_product_to_vendor_stg_pre_2")



# only pick the first unique vendor group
df_link_product_to_vendor_stg = (
   df_link_product_to_vendor_stg_pre_2
   .join(
       df_vendor_master,
       df_link_product_to_vendor_stg_pre_2['VendorCodeItem'] == df_vendor_master_unique['VendorCode'],
       'left'
   ).select(
       df_link_product_to_vendor_stg_pre_2['*'],  # All columns from df_link_product_to_vendor_stg_pre_2
       df_vendor_master['vendor_group_pk'].alias('vendor_group_id'),  # Renamed sid to vendor_group_id
       df_vendor_master['VendorGroup'],  # VendorGroup from vendor master
       df_vendor_master['has_duplicates'].alias('has_vendor_master_duplicates'),  # has_duplicates for vendormaster
       df_link_product_to_vendor_stg_pre_2['has_duplicates'].alias('has_sku_vendor_duplicates')  # has_duplicates for vendormaster
   )
)

# Multiple column rename:
columns_to_rename = {
    "sku": "local_sku",
    "ManufacturerItemNo_": "sku",
    "VendorGroup": "vendor_name"
}

df_link_product_to_vendor_stg = rename_columns(df_link_product_to_vendor_stg, columns_to_rename)

#clean column names
df_link_product_to_vendor_stg = clean_column_names(df_link_product_to_vendor_stg)

# Reorder columns more elegantly
columns = df_link_product_to_vendor_stg.columns
desired_columns = ['sku', 'vendorcodeitem', 'vendor_name', 'has_vendor_master_duplicates','has_sku_vendor_duplicates']
desired_order = desired_columns + [col for col in columns if col not in desired_columns]
df_link_product_to_vendor_stg = df_link_product_to_vendor_stg.select(desired_order)

#remove where sku and vendorcodeitem are null
df_link_product_to_vendor_stg = df_link_product_to_vendor_stg.filter(F.col('vendorcodeitem').isNotNull() & F.col('sku').isNotNull())

df_link_product_to_vendor_stg = df_link_product_to_vendor_stg.replace({'NaN': None})
# display(df_link_product_to_vendor_stg)

# First, enable the allowColumnDefaults feature
# spark.sql(f"ALTER TABLE {catalog}.{schema}.link_product_to_vendor SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")

# Write the cleaned DataFrame to Delta table
df_link_product_to_vendor_stg.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.link_product_to_vendor_stg")

# COMMAND ----------

# DBTITLE 1,mark records that are dupes absed on manufacturer skus & vendor_name
df_source_stg = spark.table(f"{catalog}.{schema}.link_product_to_vendor_stg")

key_cols=["sku","vendor_name"]
order_cols=["sys_databasename", "Sys_Bronze_InsertDateTime_UTC"]
df_manufacturer_auto = check_duplicate_keys(df_source_stg, key_cols, order_cols)

df_manufacturer_auto = df_manufacturer_auto.replace({'NaN': None})



# display(df_manufacturer_auto)


# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_manufacturer_auto_dq
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_manufacturer_auto
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_manufacturer_auto_dq"))

# COMMAND ----------

df_source_stg = spark.table(f"{catalog}.{schema}.link_product_to_vendor_stg")

key_cols=["local_sku","vendor_name"]
order_cols=["sys_databasename", "Sys_Bronze_InsertDateTime_UTC"]
df_auto = check_duplicate_keys(df_source_stg, key_cols, order_cols)

df_auto = df_auto.replace({'NaN': None})
# display(df_auto.filter(F.col('local_sku') =='XS136Z12ZZRCAA-SP'))


# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_auto_dq
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_auto
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_auto_dq"))
