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

from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import warnings

# Widget for full year processing
dbutils.widgets.text("full_year", "0", "Data period")
full_year_process = dbutils.widgets.get("full_year")
full_year_process = {"0": 0, "1": 1}.get(full_year_process, 0) 

# disabled retro processing
# df_final = spark.table(f"{catalog}.{schema}.globaltransactions_arr")
# if df_final.count() == 0:
#     warnings.warn("⚠️ No data found in target table. Full data load required.")
#     full_year_process=2


# Widget for month period
dbutils.widgets.text("month_period", "YYYY-MM", "Month period")
month_period_process = dbutils.widgets.get("month_period")

# Check if a valid month period was provided
if month_period_process == "YYYY-MM":
    # No valid month provided, use previous month
    today = date.today()
    if today.day > 10:
        month_period_process = today.strftime('%Y-%m')
    else:
        last_month_date = date(today.year, today.month, 1) - relativedelta(months=1)
        month_period_process = last_month_date.strftime('%Y-%m')
else:
    # Validate the provided month period format
    try:
        datetime.strptime(month_period_process, '%Y-%m')
        # If successful, keep the provided value
    except ValueError:
        # If invalid format, use previous month
        today = date.today()
        if today.day > 10:
            month_period_process = today.strftime('%Y-%m')
        else:
            last_month_date = date(today.year, today.month, 1) - relativedelta(months=1)
            month_period_process = last_month_date.strftime('%Y-%m')

print(f"Processing {month_period_process} sales analysis")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Some Specific ARR Functions

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
    
    # disabled retro processing
    # elif full_year_process==2:
    #     today = date.today()
    #     two_years_ago = today - relativedelta(years=2)
    #     year = two_years_ago.strftime('%Y')
    #     start_date = f"{year}-01-01"
    #     end_date = f"{year}-12-31"
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

# DBTITLE 1,return dupe record

# Optional: Get specific duplicates for certain key values
def get_specific_duplicates(df, manufacturer_item, vendor_name):
    """
    Get duplicate records for a specific key combination
    """
    return df.filter(
        (F.col("sku") == manufacturer_item) & 
        (F.col("Vendor_Name") == vendor_name)
    ).orderBy("sku", "Vendor_Name")

# Example:
# specific_dupes = get_specific_duplicates(duplicate_records, "SKU123", "Vendor1")
# specific_dupes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##SKU VENDOR PROCESS

# COMMAND ----------


df_sku_vendor = get_sales_analysis_v2(month_period_process, full_year=full_year_process)

# COMMAND ----------


# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_sku_vendor_stg 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_sku_vendor
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_sku_vendor_stg"))

# COMMAND ----------

df_sku_vendor = spark.table(f"{catalog}.{schema}.arr_sku_vendor_stg")

# COMMAND ----------

# DBTITLE 1,stage igsql03 for dupes: include databasename
#
# this is used for investigation purpose only 
# 

key_cols=["ManufacturerItemNo_","VendorCodeItem", "sys_databasename"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_sku_vendor_database_dq = check_duplicate_keys(df_sku_vendor, key_cols, order_cols)

df_sku_vendor_database_dq = df_sku_vendor_database_dq.replace({'NaN': None})


# COMMAND ----------

#
# this is used for investigation purpose only 
#  

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_sku_vendor_database_dq 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_sku_vendor_database_dq
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_sku_vendor_database_dq"))

# COMMAND ----------

#
# this is used for investigation purpose only 
#  

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_sku_vendor_database 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


df_sku_vendor_database_unique = (df_sku_vendor_database_dq
    .filter(F.col('row_number') == 1)
    .drop(F.col('row_number'))
    .withColumn("has_duplicates", when(F.col('occurrence') > 1, "Y").otherwise(F.lit( "N")))
    .drop(F.col('occurrence')))

# Then write the data to the table

(df_sku_vendor_database_unique.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_sku_vendor_database"))




# COMMAND ----------

# DBTITLE 1,mark records that are dupes based on local skus & vendor_name
key_cols=["sku","VendorCodeItem"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_local_sku_vendor_dq = check_duplicate_keys(df_sku_vendor, key_cols, order_cols)

df_local_sku_vendor_dq = df_local_sku_vendor_dq.replace({'NaN': None})

# Create a dq table with column defaults enabled
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_local_sku_vendor_dq 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_local_sku_vendor_dq
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_local_sku_vendor_dq"))


# Create a clean table with column defaults enabled
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_local_sku_vendor_unique 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_local_sku_vendor_dq
    .filter(F.col('row_number') == 1)
    .drop(F.col('row_number'))
    .withColumn("has_duplicates", when(F.col('occurrence') > 1, "Y").otherwise(F.lit( "N")))
    .drop(F.col('occurrence'))
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_local_sku_vendor_unique"))

# COMMAND ----------

# DBTITLE 1,stage igsql03 for dupes: exclude databasename
#key_cols=["ManufacturerItemNo_","VendorCodeItem"]
key_cols=["ManufacturerItemNo_","VendorCodeItem"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_sku_vendor_dq = check_duplicate_keys(df_sku_vendor, key_cols, order_cols)

df_sku_vendor_dq = df_sku_vendor_dq.replace({'NaN': None})

# COMMAND ----------

df_sku_vendor_dq.filter(col("sku")=='XS136Z12ZZRCAA-SP').display()

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_sku_vendor_dq 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_sku_vendor_dq
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_sku_vendor_dq"))

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_sku_vendor 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


df_sku_vendor_unique = (df_sku_vendor_dq
    .filter(F.col('row_number') == 1)
    .drop(F.col('row_number'))
    .withColumn("has_duplicates", when(F.col('occurrence') > 1, "Y").otherwise(F.lit( "N")))
    .drop(F.col('occurrence')))

# Then write the data to the table

(df_sku_vendor_unique.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_sku_vendor"))




# COMMAND ----------

# MAGIC %md
# MAGIC ## VEDNOR GROUP PROCESS

# COMMAND ----------

df_vendor_master = spark.table(f"silver_{ENVIRONMENT}.masterdata.vendor_mapping").alias("vm").filter(F.col("sys_silver_iscurrent") == True)


# COMMAND ----------

# DBTITLE 1,stage Vendor Groups table
key_cols=["VendorCode"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_vendor_master_dq = check_duplicate_keys(df_vendor_master, key_cols, order_cols)

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_vendor_group_dq 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

# Then write the data to the table
(df_vendor_master_dq
    .filter(F.col('occurrence') > 1)
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_vendor_group_dq"))

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_vendor_group 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


df_vendor_master_unique = (df_vendor_master_dq
    .filter(F.col('row_number') == 1)
    .drop(F.col('row_number'))
    .withColumn("has_duplicates", when(F.col('occurrence') > 1, "Y").otherwise(F.lit( "N")))
    .drop(F.col('occurrence')))

# Then write the data to the table

(df_vendor_master_unique.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.{schema}.arr_vendor_group"))




# COMMAND ----------

df_vendor_master_dq  =spark.table(f"{catalog}.{schema}.arr_vendor_group_dq").alias("vgdq")

# COMMAND ----------

df_vendor_master_unique  =spark.table(f"{catalog}.{schema}.arr_vendor_group").alias("vg")

# COMMAND ----------

# display(df_vendor_master_unique)

# COMMAND ----------

df_sku_vendor_dq  =spark.table(f"{catalog}.{schema}.arr_sku_vendor_dq").alias("svdq")

# COMMAND ----------

df_sku_vendor_unique  =spark.table(f"{catalog}.{schema}.arr_sku_vendor").alias("sv")

# COMMAND ----------

# lets get records with duplicates
result = (
   df_sku_vendor_dq
   .join(
       df_vendor_master_dq,
       df_sku_vendor_dq['VendorCodeItem'] == df_vendor_master_dq['VendorCode'], 
       'inner'
   )
)

# display(result)

# COMMAND ----------

# display(df_sku_vendor_dq.select('VendorCodeItem').distinct())

# COMMAND ----------

# DBTITLE 1,will the dupes cause issues with the stock vendor mapping?
result = (
    df_sku_vendor_dq
    .join(
        df_vendor_master_dq,
        df_sku_vendor_dq['VendorCodeItem'] == df_vendor_master_dq['VendorCode'],
        'inner'
    )
    .select('VendorCodeItem', 'VendorGroup') 
    .distinct()
    .groupBy('VendorCodeItem')
    .agg(F.count(col('VendorCodeItem')).alias('occurrence'))
    .orderBy(F.desc('occurrence'))
)

# display(result)

# COMMAND ----------

# only pick the first unique vendor group
df_arr_auto = (
   df_sku_vendor_unique
   .join(
       df_vendor_master_unique,
       df_sku_vendor_unique['VendorCodeItem'] == df_vendor_master_unique['VendorCode'],
       'left'
   ).select(
       df_sku_vendor_unique['*'],  # All columns from df_sku_vendor_unique
       df_vendor_master_unique['sid'].alias('vendor_group_id'),  # Renamed sid to vendor_group_id
       df_vendor_master_unique['VendorGroup'],  # VendorGroup from vendor master
       df_vendor_master_unique['has_duplicates'].alias('has_vendor_master_duplicates'),  # has_duplicates for vendormaster
       df_sku_vendor_unique['has_duplicates'].alias('has_sku_vendor_duplicates')  # has_duplicates for vendormaster
   )
)

# Multiple column rename:
columns_to_rename = {
    "sku": "local_sku",
    "ManufacturerItemNo_": "sku",
    "VendorGroup": "vendor_name"
}

df_arr_auto = rename_columns(df_arr_auto, columns_to_rename)

#clean column names
df_arr_auto = clean_column_names(df_arr_auto)

# Reorder columns more elegantly
columns = df_arr_auto.columns
desired_columns = ['sku', 'vendorcodeitem', 'vendor_name', 'has_vendor_master_duplicates','has_sku_vendor_duplicates']
desired_order = desired_columns + [col for col in columns if col not in desired_columns]
df_arr_auto = df_arr_auto.select(desired_order)

#remove where sku and vendorcodeitem are null
df_arr_auto = df_arr_auto.filter(F.col('vendorcodeitem').isNotNull() & F.col('sku').isNotNull())

df_arr_auto = df_arr_auto.replace({'NaN': None})
# display(df_arr_auto)

# First, enable the allowColumnDefaults feature
# spark.sql(f"ALTER TABLE {catalog}.{schema}.arr_auto_stg SET TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')")

# Write the cleaned DataFrame to Delta table
df_arr_auto.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.arr_auto_stg")

# COMMAND ----------

# DBTITLE 1,mark records that are dupes absed on manufacturer skus & vendor_name
df_source_stg = spark.table(f"{catalog}.{schema}.arr_auto_stg")

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

df_source_stg = spark.table(f"{catalog}.{schema}.arr_auto_stg")

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

# COMMAND ----------

# # Create a clean table with column defaults enabled from the start
# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_auto 
#     USING delta
#     TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
# """)


df_auto_unique = (df_auto
    .filter(F.col('row_number') == 1)
    .drop(F.col('row_number'))
    .withColumn("has_sku_master_vendor_duplicates", when(F.col('occurrence') > 1, "Y").otherwise(F.lit( "N")))
    .drop(F.col('occurrence')))

# Then write the data to the table

# Reorder columns more elegantly
columns = df_auto_unique.columns
desired_columns = ['sku', 'vendorcodeitem', 'vendor_name','has_sku_master_vendor_duplicates', 'has_vendor_master_duplicates','has_sku_vendor_duplicates']
desired_order = desired_columns + [col for col in columns if col not in desired_columns]
df_auto_unique = df_auto_unique.select(desired_order)


# First create a temporary view of the empty DataFrame
df_auto_unique.limit(0).createOrReplaceTempView("temp_auto_unique")

# Create the table using the temp view
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_auto_0
USING delta
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults' = 'supported'
) AS
SELECT * FROM temp_auto_unique
""")

# Write the data
df_auto_unique.write \
    .mode("overwrite") \
    .format("delta") \
    .option("mergeSchema", "true") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.arr_auto_0")


    

# COMMAND ----------

df_source = spark.table(f"{catalog}.{schema}.arr_auto_0")

# display(df_source)

# COMMAND ----------


# Example usage:
#key_cols=["sku", "Vendor_Name","sys_databasename"]
key_cols=["sku", "Vendor_Name"]
duplicate_records = check_duplicate_keys(df_source, key_cols)


# COMMAND ----------

# MAGIC %md
# MAGIC ## CURRENT ARR PROCESS

# COMMAND ----------

# Convert to PySpark DataFrame
df_source_manual = spark.table(f"silver_dev.masterdata.datanowarr")
df_source_manual = df_source_manual.filter(col("Sys_Silver_IsCurrent") == True)
# Example usage:
key_cols=["SKU", "Vendor_Name"]
duplicate_records = check_duplicate_keys(df_source_manual, key_cols)

# COMMAND ----------

# check if description field is different for skus
description_counts = df_source.groupBy("sku") \
    .agg(F.countDistinct("Description").alias("description_count"))

# Get SKUs with multiple descriptions (description_count > 1)
skus_with_diff_descriptions = description_counts \
    .filter(F.col("description_count") > 1) \
    .select("sku")

# Join back to original DataFrame to get all fields for these SKUs
result = df_source.join(skus_with_diff_descriptions, "sku")

# Sort the results by SKU and Description for better readability
result = result.orderBy("sku", "Description")

# Show the results
# result.display()

# COMMAND ----------

# DBTITLE 1,common transformations



def dq_transform(
    df,
    composite_key: List[str] = None,
    keep_duplicates: bool = False
):
    """
    Identify and optionally remove duplicate records based on a composite key.
    Adds a count of occurrences for each unique composite key combination.
    
    Args:
        df: Spark DataFrame to process
        composite_key: List of column names to use as the composite key for determining duplicates.
                      If None, defaults to ['sku', 'vendor_name', 'item_tracking_code']
        keep_duplicates: If False, removes duplicate records keeping only the first occurrence.
                        If True, keeps all records but adds a row number column. (default: False)
    
    Returns:
        Spark DataFrame with:
        - Unique records (if keep_duplicates=False) or all records (if keep_duplicates=True)
        - occurrence_count column showing total matches for each composite key combination
    
    Example:
        >>> result = dq_transform(my_dataframe)
        >>> result_with_custom_key = dq_transform(my_dataframe, composite_key=['order_id', 'product_id'])
        >>> result_with_dupes = dq_transform(my_dataframe, keep_duplicates=True)
    """
    # Use default composite key if none provided
    if composite_key is None:
        composite_key = [
            'sku',
            'vendor_name',
            'item_tracking_code'
        ]
    
    # Create window specifications
    window_spec = Window.partitionBy(composite_key).orderBy(composite_key[0])
    count_window = Window.partitionBy(composite_key)
    
    # Add row numbers and occurrence count within each partition
    result = (df
        .withColumn('row_num', row_number().over(window_spec))
        .withColumn('occurrence_count', count('*').over(count_window))
    )
    
    # If keep_duplicates is False, filter to keep only unique records
    if not keep_duplicates:
        result = result.filter(col('row_num') == 1).drop('row_num','occurrence_count')
    else:
        result = result.filter(col('occurrence_count') > 1)
    return result



# Utility function to parse months from "Life Cycle Formula" column
def parse_months(df):
    return df.withColumn(
        "months",
        F.when(F.col("lifecycleformula").isNotNull(),
               F.regexp_extract(F.col("lifecycleformula"), "^(\d+)", 1).cast("int"))
         .otherwise(0)
    )


# Define the transformation logic
def default_columns(df):
    return df.withColumn("matched_type", F.lit("mt=auto")) \
    .withColumn("duration", F.lit("Not Assigned")) \
    .withColumn("frequency", F.lit("Not Assigned")) \
    .withColumn("Consumption", F.lit("Not Assigned")) \
    .withColumn("Type", F.lit("Not Assigned")) \
    .withColumn("Mapping_type_Duration", F.lit("Not Assigned")) \
    .withColumn("Mapping_type_Billing", F.lit("Not Assigned")) \
    .withColumn("MRRratio", F.lit(0)) \
    .withColumn("months", F.lit(0)) \
    .withColumn("description", F.concat_ws(" ", F.col("Description"), F.col("Description2"),
                            F.col("Description3"), F.col("Description4")))


# # Transformations based on `months` column
# def apply_month_based_transformations(df):
#     return df.withColumn("duration", 
#                          F.when(F.col("months") > 1, (F.col("months") / 12).cast("string") + " YR")
#                           .when(F.col("months") == 1, "1M")
#                           .otherwise("Perpetual")) \
#              .withColumn("Mapping_type_Duration", 
#                          F.when(F.col("months") > 1, "Sure Mapping")
#                           .when(F.col("months") == 1, "Sure Mapping")
#                           .otherwise("Other Mapping")) \
#              .withColumn("frequency", 
#                          F.when(F.col("months") > 1, "Upfront")
#                           .when(F.col("months") == 1, "Monthly")
#                           .otherwise("Upfront")) \
#              .withColumn("Consumption", 
#                          F.when(F.col("months") == 1, "Flexible")
#                           .otherwise("Capacity"))


# Transformations for duration and MRR ratio handling
def apply_final_transformations(df):
    # First convert description to uppercase
    df = df.withColumn("description", F.upper(F.col("description")))
    # Duration and Mapping_type_Duration
    df = df.withColumn(
        "duration",
        F.when(F.col("description").rlike(r'(?i).*1 YEAR*'), "1 YR")
         .when(F.col("description").rlike(r'(?i).*2 YEAR*'), "2 YR")
         .when(F.col("description").rlike(r'(?i).*3 YEAR*'), "3 YR")
         .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("description").rlike(r'(?i).*1 YEAR*'), "Sure Mapping")
         .when(F.col("description").rlike(r'(?i).*2 YEAR*'), "Sure Mapping")
         .when(F.col("description").rlike(r'(?i).*3 YEAR*'), "Sure Mapping")
         .otherwise(F.col("Mapping_type_Duration"))
    )

    # Type
    df = df.withColumn(
        "Type",
        F.when(F.col("description").rlike(r'(?i).*SUBSCRIPTION*|.*SUBSCR*'), "SW Subscription")
         .otherwise(F.col("Type"))
    )

    # MRR Ratio
    df = df.withColumn(
        "MRRratio",
        F.when(F.col("duration").rlike(r'.*YR'), 12 * F.regexp_extract(F.col("duration"), r"(\d*\.?\d+)", 1).cast("float"))
         .otherwise(0)
    )

    # Duration in years (rounded and formatted)
    df = df.withColumn(
        "duration",
        F.when(F.col("duration").rlike(r'.*YR'),
            F.concat(
                F.format_number(
                    F.regexp_extract(F.col("duration"), r"(\d*\.?\d+)", 1).cast("float"),
                    2
                ),
                F.lit(" YR")
            )
        ).otherwise(F.col("duration"))
    )
        
    return df



# COMMAND ----------

# DBTITLE 1,Vendor-specific transformations

# Vendor-specific transformations
def apply_vendor_transformations(df):

    # Transformations for "Software" and "Professional Service"
    df = df.withColumn("Type",
                F.when((F.col("producttype") == "Software") & F.col("duration").contains("YR"), "SW Subscription")
                 .when((F.col("producttype") == "Software") & F.col("duration").contains("M"), "SW Subscription")
                 .when(F.col("producttype") == "Professional Service", "Professional services"))

    
    # A10Networks-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "A10Networks") & 
            (F.col("sku") == "GOLD SUPPORT 4 YEAR"), 
            "4 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("vendor_name") == "A10Networks") & 
            (F.col("sku") == "GOLD SUPPORT 4 YEAR"), 
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("vendor_name") == "A10Networks") & 
            (F.col("sku") == "GOLD SUPPORT 4 YEAR"), 
            "Capacity"
        ).otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "A10Networks") & 
            (F.col("sku") == "GOLD SUPPORT 4 YEAR"), 
            "Vendor support"
        ).otherwise(F.col("Type"))
    )

    # Arbor Networks-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(
            (F.col("vendor_name") == "Arbor Networks") & 
            F.col("sku").contains("MNT-"), 
            "Upfront"
        ).when(
            F.col("vendor_name") == "Arbor Networks", 
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("vendor_name") == "Arbor Networks") & 
            F.col("sku").contains("MNT-"), 
            "Capacity"
        ).when(
            F.col("vendor_name") == "Arbor Networks", 
            "Capacity"
        ).otherwise(F.col("consumption"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            F.col("vendor_name") == "Arbor Networks", 
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Arbor Networks") & 
            F.col("sku").contains("MNT-"), 
            "Vendor support"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Arbor Networks") & 
            F.col("sku").contains("-2YR"), 
            "2 YR"
        ).when(
            (F.col("vendor_name") == "Arbor Networks") & 
            F.col("sku").contains("-3YR"), 
            "3 YR"
        ).when(
            (F.col("vendor_name") == "Arbor Networks") & 
            F.col("sku").contains("-4YR"), 
            "4 YR"
        ).when(
            (F.col("vendor_name") == "Arbor Networks") & 
            F.col("sku").contains("-5YR"), 
            "5 YR"
        ).when(
            F.col("vendor_name") == "Arbor Networks", 
            "1 YR"
        ).otherwise(F.col("duration"))
    )

    # Fortinet-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(F.col("vendor_name") == "Fortinet", 
            F.when(F.col("sku").rlike(r".*-02-24"), "2 YR")
                .when(F.col("sku").rlike(r".*-02-12"), "1 YR")
                .when(F.col("sku").rlike(r".*-02-36"), "3 YR")
                .when(F.col("sku").rlike(r".*-02-48"), "4 YR")
                .when(F.col("sku").rlike(r".*-02-60"), "5 YR")
                .otherwise("Perpetual"))
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "Fortinet", 
            F.when(F.col("duration").contains("YR"), "Sure Mapping")
                .otherwise("Other Mapping"))
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Fortinet", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Fortinet", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Fortinet", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Type",
        F.when(F.col("vendor_name") == "Fortinet", 
            F.when(F.col("sku").rlike(r".*-247-02-.*|.*-928-02-.*|.*-950-02-.*|.*-963-02-.*|.*-248-02-.*|.*-936-02-.*|.*-211-02-.*|.*-809-02-.*|.*-258-02-.*|.*-916-02-.*|.*-314-02-.*|.*-585-02-.*|.*-812-02-.*"), "Vendor support")
                .when(F.col("sku").rlike(r".*-108-02-.*|.*-131-02-.*|.*-189-02-.*|.*-651-02-.*|.*-159-02-.*|.*-647-02-.*|.*-423-02-.*|.*-160-02-.*"), "SW Subscription")
                .when(F.col("sku").rlike(r".*-714-02-.*"), "Professional services")
                .when(F.col("sku").rlike(r"FTM-ELIC-.*|.*-VM-BASE"), "SW Perpetual")
                .otherwise(F.col("Type")))
        .otherwise(F.col("Type"))
    )

    # Checkpoint-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(
            F.col("vendor_name") == "Checkpoint", 
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(
            F.col("vendor_name") == "Checkpoint", 
            "Capacity"
        ).otherwise(F.col("consumption"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            F.col("vendor_name") == "Checkpoint", 
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").rlike("CPAP|CPAC")), 
            "Perpetual"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").contains("-2Y")), 
            "2 YR"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").contains("-3Y")), 
            "3 YR"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").contains("-4Y")), 
            "4 YR"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").contains("-5Y")), 
            "5 YR"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").contains("-1Y")), 
            "1 YR"
        ).when(
            F.col("vendor_name") == "Checkpoint", 
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").rlike("CPAP|CPAC")), 
            "Sure Mapping"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").rlike("-[1-5]Y")), 
            "Sure Mapping"
        ).when(
            F.col("vendor_name") == "Checkpoint", 
            "Other Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").rlike("CPAP|CPAC")), 
            "Hardware"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("sku").rlike("CPES|CPCES")), 
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("producttype") == "Support"), 
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("producttype") == "Courseware"), 
            "Training"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("producttype") == "Professional Service"), 
            "Professional services"
        ).when(
            (F.col("vendor_name") == "Checkpoint") & 
            (F.col("producttype") == "Software") & 
            (F.col("duration").contains("YR")), 
            "SW Subscription"
        ).when(
            F.col("vendor_name") == "Checkpoint", 
            "SW Perpetual"
        ).otherwise(F.col("Type"))
    )

    # Sophos-Specific Transformations
    df = df.withColumn(
        "months",
        F.when((F.col("vendor_name") == "Sophos") & (F.col("itemtrackingcode").isNotNull()),
            F.regexp_extract(F.col("lifecycleformula"), "^(\d+)", 1).cast("int"))
        .otherwise(F.col("months"))
    ).withColumn(
        "duration",
        F.when(F.col("vendor_name") == "Sophos",
            F.when(F.col("months") > 1, F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR")))
                .when(F.col("months") == 1, "1M")
                .otherwise("Perpetual"))
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "Sophos",
            F.when(F.col("months") > 1, "Sure Mapping")
                .when(F.col("months") == 1, "Sure Mapping")
                .otherwise("Other Mapping"))
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Sophos",
            F.when(F.col("months") > 1, "Upfront")
                .when(F.col("months") == 1, "Monthly")
                .otherwise("Upfront"))
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Sophos",
            F.when(F.col("months") == 1, "Flexible")
                .otherwise("Capacity"))
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when(F.col("vendor_name") == "Sophos",
            F.when((F.col("producttype") == "Software") & F.col("duration").contains("YR"), "SW Subscription")
                .when((F.col("producttype") == "Software") & F.col("duration").contains("M"), "SW Subscription")
                .when(F.col("producttype") == "Professional Service", "Professional services"))
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("vendor_name") == "Sophos") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    )

    ### Watchguard-Specific Transformations ###
    df = df.withColumn(
        "duration",
        F.when((F.col("vendor_name") == "Watchguard") & (F.col("description").rlike(r"(?i).*1-Year.*|.*1 -Year.*|.*1 Year.*|.*1-yr.*")), "1 YR")
         .when((F.col("vendor_name") == "Watchguard") & (F.col("description").rlike(r"(?i).*3-Year.*|.*3 Year.*|.*3-yr.*|.*3 -Year.*")), "3 YR")
         .when((F.col("vendor_name") == "Watchguard") & (F.col("description").rlike(r"(?i).*FireboxV.*MSSP Appliance.*")), "3 YR")
         .when((F.col("vendor_name") == "Watchguard") & (F.col("description").rlike(r"(?i).*IPSec VPN Client .*")), "Perpetual")
         .otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when((F.col("vendor_name") == "Watchguard") & (F.col("description").rlike(r"(?i).*Total Security Suite.*|.*Standard Support.*|.*Basic Security Suite.*")), "Vendor support")
         .when((F.col("vendor_name") == "Watchguard") & (F.col("description").rlike(r"(?i).*Panda Endpoint Protection Plus.*")), "SW Subscription")
         .when((F.col("vendor_name") == "Watchguard") & (F.col("description").rlike(r"(?i).*VPN Client.*")), "SW Perpetual")
         .otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when((F.col("vendor_name") == "Watchguard") & F.col("sku").rlike(r"WG\d{4}|WGT49023-EU"), "Perpetual")
         .otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when((F.col("vendor_name") == "Watchguard") & F.col("sku").rlike(r"WG\d{4}|WGT49023-EU"), "Hardware")
         .otherwise(F.col("Type"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Watchguard", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Watchguard", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Watchguard", "Capacity").otherwise(F.col("consumption"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("vendor_name") == "Watchguard") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
         .otherwise(F.col("Mapping_type_Duration"))
    )

    # Hardware transformation for Watchguard
    df = df.withColumn("Type",
            F.when((F.col("vendor_name") == "Watchguard") & F.col("sku").rlike("WG\\d{4}|WGT49023-EU"), "Hardware")
             .otherwise(F.col("Type"))
    )

    # DDN-specific transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "DDN") & F.col("sku").rlike(r"SUP-.*-(\d+)YR"),
            F.concat(F.regexp_extract(F.col("sku"), r"SUP-.*-(\d+)YR", 1), F.lit(" YR"))
        ).when(
            (F.col("vendor_name") == "DDN") & F.col("sku").rlike(r"REINSTATE-BASIC-VM"),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "DDN") & F.col("sku").rlike(r"SUP-.*"),
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "DDN") & F.col("sku").rlike(r"REINSTATE-BASIC-VM"),
            "SW Perpetual"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "DDN", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "DDN", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "DDN", "Capacity").otherwise(F.col("consumption"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("vendor_name") == "DDN") & (F.col("duration")!='Not Assigned'),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    )


   
    # Entrust-specific transformations
    entrust_regex_map = [
        (r"NC-.*-PR|NC-.*-ST|SUP-ESSENTIALS-PLAT|SUP-IDG-PLAT-PT", "1 YR", "Vendor support"),
        (r"ECS-ADVA-.*|ECS-EVMD-.*|ECS-FCMG-.*|ECS-QWCE-.*|ECS-STDZ-.*|NC-.*-SUB|SMSPC-R-MFA-.*-12|TBL-IAS-ENT-NU-.*-12|SMSPC-R-PRM-.*-12", "1 YR", "SW Subscription"),
        (r"SMSPC-R-MFA-.*-36", "3 YR", "SW Subscription"),
        (r"EI-IDG-SOF-LIC.*|EI-IDG-SOFT-UC.*|EI-INF-BUN.*|EI-NP-ENT-UC.*|NC-AC3120L\(EU\+10\)-E.*|ENC-AC3134C.*|NC-FE1637L-E.*|NC-OP3169-CIOP-E.*|TBL-IAS-MOB-ST-SDK.*|NC-AC3134C|NC-AC3120L\(EU\+10\)-E", "Perpetual", "SW Perpetual"),
        (r"NC-M-010114-L-EU.*", "Perpetual", "Hardware"),
        (r"NC-PS-EDS-DEV.*|PS-HSM-CUSTOM.*", "Perpetual", "Professional Service")
    ]

    for pattern, duration_val, type_val in entrust_regex_map:
        df = df.withColumn(
            "duration",
            F.when((F.col("vendor_name") == "Entrust") & F.col("sku").rlike(pattern), duration_val)
             .otherwise(F.col("duration"))
        ).withColumn(
            "Type",
            F.when((F.col("vendor_name") == "Entrust") & F.col("sku").rlike(pattern), type_val)
             .otherwise(F.col("Type"))
        )

    # Additional Entrust defaults
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Entrust", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Entrust", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Entrust", "Capacity").otherwise(F.col("consumption"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("vendor_name") == "Entrust") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
         .otherwise(F.col("Mapping_type_Duration"))
    )


    # Juniper-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^PAR-.*|^SVC-.*"),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^SUB-.*-(\d+)Y.*"),
            F.concat(F.regexp_extract(F.col("sku"), r"^SUB-.*-(\d+)Y.*", 1), F.lit(" YR"))
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^S-.*-P|JS-NETDIR-10|JS-SECDIR-10|ME-VM-OC-PROXY|ME-ADV-XCH-WW|EX4650-PFL"),
            "Perpetual"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^S-.*-3"),
            "3 YR"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^S-.*-1"),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^S-.*-5"),
            "5 YR"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^JUNIPER-RENEWAL"),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "Juniper") & (F.col("description").rlike(r"(?i).*1-Year subscr LIC.*")),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"PD-9001GR-AT-AC|EX4100-F-12-RME|SRX320-RMK0|EX-4PST-RMK|SRX320-WALL-KIT0|CBL-JNP-SG4-EU|CBL-PWR-10AC-STR-EU"),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^PAR-.*|^SVC-.*|JUNIPER-RENEWAL"),
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^SUB-.*|^S-.*-(\d+)|.*1-Year subscr LIC.*"),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"^S-.*-P|JS-NETDIR-10|JS-SECDIR-10|ME-VM-OC-PROXY|ME-ADV-XCH-WW|EX4650-PFL"),
            "SW Perpetual"
        ).when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"PD-9001GR-AT-AC|EX4100-F-12-RME|SRX320-RMK0|EX-4PST-RMK|SRX320-WALL-KIT0|CBL-JNP-SG4-EU|CBL-PWR-10AC-STR-EU"),
            "Hardware"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Juniper", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Juniper", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Juniper", "Capacity").otherwise(F.col("consumption"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("vendor_name") == "Juniper") & (F.col("duration") == "1 YR") & (F.col("sku").rlike(r'^JUNIPER-RENEWAL|^PAR-.*|^SVC-.*')), "Other Mapping")
        .when((F.col("vendor_name") == "Juniper") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .when(
            (F.col("vendor_name") == "Juniper") & F.col("sku").rlike(r"PD-9001GR-AT-AC|EX4100-F-12-RME|SRX320-RMK0|EX-4PST-RMK|SRX320-WALL-KIT0|CBL-JNP-SG4-EU|CBL-PWR-10AC-STR-EU"),
            "Sure mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    )

    # Extreme Networks-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Extreme Networks") & F.col("sku").rlike(r"9.*"),
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "Extreme Networks") & F.col("sku").rlike(r"5000-MACSEC-LIC-P"),
            "SW Perpetual"
        ).when(
            (F.col("vendor_name") == "Extreme Networks") & F.col("sku").rlike(r"XIQ-PIL-S-C-PWP-DELAY|TR-EVENT-PASS-USER-CONF"),
            "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Extreme Networks") & F.col("sku").rlike(r"9.*") & (F.col("description").rlike(r"(?i).*12 Months.*")),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "Extreme Networks") & F.col("sku").rlike(r"9.*") & (F.col("description").rlike(r"(?i).*36 Months.*")),
            "3 YR"
        ).when(
            (F.col("vendor_name") == "Extreme Networks") & F.col("sku").rlike(r"9.*") & (F.col("description").rlike(r"(?i).*60 Months.*")),
            "5 YR"
        ).when(
            (F.col("vendor_name") == "Extreme Networks") & F.col("sku").rlike(r"5000-MACSEC-LIC-P"),
            "Perpetual"
        ).when(
            (F.col("vendor_name") == "Extreme Networks") & F.col("sku").rlike(r"XIQ-PIL-S-C-PWP-DELAY|TR-EVENT-PASS-USER-CONF"),
            "1 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            ((F.col("vendor_name") == "Extreme Networks") & (F.col("duration")!='Not Assigned')),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Extreme Networks", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Extreme Networks", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Extreme Networks", "Capacity").otherwise(F.col("consumption"))
    )

    # Riverbed-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Riverbed") & F.col("sku").rlike(r"(?i).*MNT-.*"),
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "Riverbed") & F.col("sku").rlike(r"^LIC-.*|^ATNY-.*"),
            "SW Perpetual"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Riverbed") & F.col("sku").rlike(r"(?i).*MNT-.*"),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "Riverbed") & F.col("sku").rlike(r"^LIC-.*|^ATNY-.*"),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("vendor_name") == "Riverbed") & F.col("sku").rlike(r"(?i).*MNT-.*"),
            "Other Mapping"
        ).when(
            (F.col("vendor_name") == "Riverbed") & F.col("sku").rlike(r"^LIC-.*|^ATNY-.*"),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Riverbed", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Riverbed", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Riverbed", "Capacity").otherwise(F.col("consumption"))
    )

    # WithSecure-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "WithSecure") & (F.col("description").rlike(r"(?i).*3 years.*")),
            "3 YR"
        ).when(
            (F.col("vendor_name") == "WithSecure") & (F.col("description").rlike(r"(?i).*1 year.*")),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "WithSecure") & (F.col("description").rlike(r"(?i).*2 year.*")),
            "2 YR"
        ).when(
            (F.col("vendor_name") == "WithSecure") & (F.col("description").rlike(r"(?i).*5 year.*")),
            "5 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("vendor_name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("vendor_name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            (F.col("vendor_name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("vendor_name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "Capacity"
        ).otherwise(F.col("consumption"))
    )

    # Acronis-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Acronis") & 
            (F.col("description").rlike(".*3 years.*|.*3 Year.*")),
            "3 YR"
        ).when(
            (F.col("vendor_name") == "Acronis") & 
            (F.col("description").rlike(".*1 year.*|.*1 Year.*")),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "Acronis") & 
            (F.col("description").rlike(".*2 year.*")),
            "2 YR"
        ).when(
            (F.col("vendor_name") == "Acronis") & 
            (F.col("description").rlike(".*5 year.*|.*5 Year.*")),
            "5 YR"
        ).when(
            (F.col("vendor_name") == "Acronis") & 
            (F.col("description").rlike(".*Physical Data Shipping to Cloud.*")),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when((F.col("vendor_name") == "Acronis") & (F.col("duration")=='Perpetual'),
            "SW Perpetual"
        ).when((F.col("vendor_name") == "Acronis") & (F.col("duration")!='Not Assigned'),
           "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("vendor_name") == "Acronis") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("vendor_name") == "Acronis") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("vendor_name") == "Acronis") & (F.col("duration")!='Not Assigned'),"Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when((F.col("vendor_name") == "Acronis") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("consumption"))
    )

    # Arcserve-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") > 1)
            , F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR"))
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") == 1),
            "1M"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") == 0),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") > 0),
            "Sure Mapping"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") == 0),
            "Other Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") > 1),
            "Upfront"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") == 1),
            "Monthly"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") == 0),
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            F.col("vendor_name") == "Arcserve",
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") > 1),
            "Capacity"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") == 1),
            "Flexible"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("months") == 0),
            "Capacity"
        ).otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Arcserve") & (F.col("producttype") == "Software") & F.col("duration").contains("YR"),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("producttype") == "Managed Services") & F.col("duration").contains("YR"),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("producttype") == "Software") & F.col("duration").contains("M"),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("producttype") == "Software"),
            "SW Perpetual"
        ).when(
            (F.col("vendor_name") == "Arcserve") & (F.col("producttype") == "Professional Service"),
            "Professional services"
        ).otherwise(F.col("Type"))
    )

    # Barracuda-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") > 1)
            , F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR"))
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") == 1),
            "1M"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") == 0),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") > 0),
            "Sure Mapping"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") == 0),
            "Other Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") > 1),
            "Upfront"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") == 1),
            "Monthly"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") == 0),
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            F.col("vendor_name") == "Barracuda",
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") > 1),
            "Capacity"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") == 1),
            "Flexible"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("months") == 0),
            "Capacity"
        ).otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Barracuda") & (F.col("producttype") == "Software") & F.col("duration").contains("YR"),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("producttype") == "Managed Services") & F.col("duration").contains("YR"),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("producttype") == "Software") & F.col("duration").contains("M"),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("producttype") == "Software"),
            "SW Perpetual"
        ).when(
            (F.col("vendor_name") == "Barracuda") & (F.col("producttype") == "Professional Service"),
            "Professional services"
        ).otherwise(F.col("Type"))
    )

  

    # Bitdefender-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("months").cast("int") > 1),
            F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR"))
        ).when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("months").cast("int") == 1),
            "1M"
        ).when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("months").cast("int") == 0),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "Bitdefender", 
            F.when(F.col("duration") == "Perpetual", "Other Mapping")
            .otherwise("Sure Mapping"))
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("duration") == "1M"),
            "Monthly"
        ).when(
            F.col("vendor_name") == "Bitdefender",
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Bitdefender", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("duration") == "1M"),
            "Flexible"
        ).when(
            F.col("vendor_name") == "Bitdefender",
            "Capacity"
        ).otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("producttype") == "Software") & 
            (F.col("duration").rlike("YR")),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("producttype") == "Extras") &
            (F.col("duration").rlike("YR")),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("producttype") == "Software") &
            (F.col("duration") == "1M"),
            "SW Subscription"
        ).when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("producttype") == "Software"),
            "SW Perpetual"
        ).when(
            (F.col("vendor_name") == "Bitdefender") &
            (F.col("producttype") == "Professional Service"),
            "Professional services"
        ).otherwise(F.col("Type"))
    )

    # SonicWall-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "SonicWall") &
            (F.col("description").rlike(r"(?i).*SUPPORT.*3YR.*|.*Support.*3 Years.*")),
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "SonicWall") &
            (F.col("description").rlike(r"(?i).*SUPPORT.*1YR.*|.*Support.*1 Year.*")),
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "SonicWall") &
            (F.col("description").rlike(r"(?i).*STATEFUL HA UPGRADE FOR.*|.*Virtual Appliance.*")),
            "SW Perpetual"
        ).when(
            (F.col("vendor_name") == "SonicWall"),
            "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("vendor_name") == "SonicWall", 
            F.when(F.col("description").rlike(r"(?i).*3YR.*|.*3 Years.*"), "3 YR")
            .when(F.col("description").rlike(r"(?i).*1YR.*|.*1 Year.*"), "1 YR")
            .when(F.col("description").rlike(r"(?i).*2 Year.*"), "2 YR")
            .when(F.col("description").rlike(r"(?i).*4 Year.*"), "4 YR")
            .when(F.col("description").rlike(r"(?i).*5 Year.*|.*5 YR.*"), "5 YR")
            .when(F.col("description").rlike(r"(?i).*STATEFUL HA UPGRADE FOR.*|.*Virtual Appliance.*"), "Perpetual")
            .otherwise(F.col("duration"))).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", F.when((F.col("vendor_name") == "SonicWall") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("vendor_name") == "SonicWall", "Upfront")
                    .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("vendor_name") == "SonicWall", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("vendor_name") == "SonicWall", "Capacity")
                        .otherwise(F.col("consumption"))
    )

    # Armis-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("vendor_name") == "Armis", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("vendor_name") == "Armis", "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "Armis", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Armis", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Armis", "Other Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Armis", "Capacity")
        .otherwise(F.col("consumption"))
    )

    # Blackberry-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("vendor_name") == "Blackberry", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("vendor_name") == "Blackberry", "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "Blackberry", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Blackberry", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Blackberry", "Other Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "Blackberry", "Capacity")
        .otherwise(F.col("consumption"))
    )

    # Cambium-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "Cambium") &
            (F.col("sku").rlike(r"(?i).*RNW-1.*")),
            "Vendor support"
        ).when(
            (F.col("vendor_name") == "Cambium") &
            (F.col("sku").rlike(r"(?i).*SUB-.*-3")),
            "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "Cambium") &
            (F.col("sku").rlike(r"(?i).*RNW-1.*")),
            "1 YR"
        ).when(
            (F.col("vendor_name") == "Cambium") &
            (F.col("sku").rlike(r"(?i).*SUB-.*-3")),
            "3 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", F.when(F.col("vendor_name") == "Cambium", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("vendor_name") == "Cambium", "Upfront")
                    .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("vendor_name") == "Cambium", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("vendor_name") == "Cambium", "Capacity")
                        .otherwise(F.col("consumption"))
    )

    # CarbonBlack-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("vendor_name") == "CarbonBlack") &
            (F.col("sku").rlike(r"(?i).*-1Y-EU-R-.*")),
            "Vendor support"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("vendor_name") == "CarbonBlack") &
            (F.col("sku").rlike(r"(?i).*-1Y-EU-R-.*")),
            "1 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", F.when(F.col("vendor_name") == "CarbonBlack", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("vendor_name") == "CarbonBlack", "Upfront")
                    .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("vendor_name") == "CarbonBlack", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("vendor_name") == "CarbonBlack", "Capacity")
                        .otherwise(F.col("consumption"))
    )

    # Cato Networks-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("vendor_name") == "Cato Networks",
            F.when(F.col("sku").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*"), "SW Subscription")
            .when(F.col("sku").rlike(r"(?i).*CATO-DR-KIT.*|.*CATO-DEPLOYMENT-FEE.*"), "Hardware")
            .when(F.col("sku").rlike(r"(?i).*CAN-RENEW-QTR.*"), "Vendor Support")
            .when(F.col("sku").rlike(r"(?i).*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "SW Subscription")
            .otherwise(F.col("Type"))
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("vendor_name") == "Cato Networks",
            F.when(F.col("sku").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*|.*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "3 YR")
            .when(F.col("sku").rlike(r"(?i).*CAN-RENEW-QTR.*"), "3M")
            .when(F.col("sku").rlike(r"(?i).*CATO-DR-KIT.*|.*CATO-DEPLOYMENT-FEE.*"), "Perpetual")
            .otherwise(F.col("duration"))
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "Cato Networks",
            F.when(F.col("sku").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*|.*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "Ratio mapping")
            .when(F.col("sku").rlike(r"(?i).*CAN-RENEW-QTR.*|.*CATO-DR-KIT.*|.*CATO-DEPLOYMENT-FEE.*"), "Other Mapping")
            .otherwise(F.col("Mapping_type_Duration"))
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Cato Networks",
            F.when(F.col("sku").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*|.*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "Monthly")
            .otherwise(lit("Upfront"))
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Cato Networks",
            F.when(F.col("sku").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*|.*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "Other Mapping")
            .otherwise("Sure Mapping")
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("vendor_name") == "Cato Networks", "Capacity").otherwise(F.col("consumption"))
    )


    # conpal-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("vendor_name") == "conpal",
            F.when(F.col("sku").rlike(r"(?i).*USC-12.*|.*USC-24.*|.*USC-36.*"), "Vendor Support")
            .when(F.col("sku").rlike(r"(?i).*LIC-PP.*"), "SW Perpetual")
            .otherwise(F.col("Type"))
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("vendor_name") == "conpal",
            F.when(F.col("sku").rlike(r"(?i).*USC-12.*"), "1 YR")
            .when(F.col("sku").rlike(r"(?i).*USC-24.*"), "2 YR")
            .when(F.col("sku").rlike(r"(?i).*USC-36.*"), "3 YR")
            .when(F.col("sku").rlike(r"(?i).*LIC-PP.*"), "Perpetual")
            .when(F.col("sku").rlike(r"(?i).*USC-01.*"), "1 M")
            .otherwise(F.col("duration"))
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "conpal", "Sure Mapping").otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "conpal",
            F.when(F.col("sku").rlike(r"(?i).*USC-01.*"), "Monthly")
            .otherwise(lit("Upfront"))
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "conpal", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "conpal",
            F.when(F.col("sku").rlike(r"(?i).*USC-01.*"), "Flexible")
            .otherwise("Capacity")
        ).otherwise(F.col("consumption"))
    )

    # CloudFlare-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("vendor_name") == "CloudFlare",
            F.when(F.col("sku").rlike(r"(?i).*CLOUDFLARE-CONTRACT.*"), "SW Subscription")
            .otherwise(F.col("Type"))
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("vendor_name") == "CloudFlare",
            F.when(F.col("sku").rlike(r"(?i).*CLOUDFLARE-CONTRACT.*"), "Not Assigned")
            .otherwise(F.col("duration"))
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "CloudFlare", "Other Mapping").otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("vendor_name") == "CloudFlare", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("vendor_name") == "CloudFlare", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("vendor_name") == "CloudFlare", "Capacity").otherwise(F.col("consumption"))
    )

    # CyberArk-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(F.col("vendor_name") == "CyberArk",
            F.when(F.col("sku").rlike(r"(?i).*EPM-TARGET-WRK-SAAS.*"), "Not Assigned")
            .otherwise(F.col("duration"))
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when((F.col("vendor_name") == "CyberArk") & (F.col("duration")!='Not Assigned'), "SW Subscription").otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("vendor_name") == "CyberArk") & (F.col("duration")!='Not Assigned'), "Other Mapping").otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("vendor_name") == "CyberArk", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("vendor_name") == "CyberArk", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("vendor_name") == "CyberArk", "Capacity").otherwise(F.col("consumption"))
    )

    # Cybereason-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("vendor_name") == "Cybereason", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Cybereason", "25.44 Monthly coef")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("vendor_name") == "Cybereason", "Other Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "duration", 
        F.when(F.col("vendor_name") == "Cybereason", "2.26 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "months", 
        F.when(F.col("vendor_name") == "Cybereason", "27.12")
        .otherwise(F.col("months"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "Cybereason", "Ratio mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Cybereason", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Cybereason") & 
            (F.col("sku").rlike(r'(?i).*CR-TS-.*') | F.col("sku").rlike(r'(?i).*CR-TRI-.*')), "Professional Service")
        .otherwise(F.col("Type"))
    ).withColumn(
        "frequency", 
        F.when((F.col("vendor_name") == "Cybereason") & 
            (F.col("sku").rlike(r'(?i).*CR-TS-.*') | F.col("sku").rlike(r'(?i).*CR-TRI-.*')), lit("Upfront"))
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("vendor_name") == "Cybereason") & 
            (F.col("sku").rlike(r'(?i).*CR-TS-.*') | F.col("sku").rlike(r'(?i).*CR-TRI-.*')), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Cybereason") & 
            (F.col("sku").rlike(r'(?i).*CR-TS-.*') | F.col("sku").rlike(r'(?i).*CR-TRI-.*')), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Cybereason") & 
            (F.col("sku").rlike(r'(?i).*CR-TS-.*') | F.col("sku").rlike(r'(?i).*CR-TRI-.*')), "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "months", 
        F.when((F.col("vendor_name") == "Cybereason") & 
            (F.col("sku").rlike(r'(?i).*CR-TS-.*') | F.col("sku").rlike(r'(?i).*CR-TRI-.*')), "0")
        .otherwise(F.col("months"))
    )

    # Deepinstinct-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Deepinstinct", "Monthly")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("vendor_name") == "Deepinstinct", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Deepinstinct", "Flexible")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("vendor_name") == "Deepinstinct", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("vendor_name") == "Deepinstinct", "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "Deepinstinct", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", 
        F.when((F.col("vendor_name") == "Deepinstinct") & 
            (F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), lit("Monthly"))
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("vendor_name") == "Deepinstinct") & 
            (F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "Deepinstinct") & 
            (F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "Flexible")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Deepinstinct") & 
            (F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Deepinstinct") & 
            (F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Deepinstinct") & 
            (F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("sku").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # EXAGRID-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "EXAGRID") & 
            (F.col("sku").rlike(r'^EX-3YR-MS-S')), "3 YR")
            .when((F.col("vendor_name") == "EXAGRID") & 
            (F.col("sku").rlike(r'^EX-10GBE-OPTICAL')), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency", 
        F.when((F.col("vendor_name") == "EXAGRID") & 
            (F.col("duration")=="3 YR"), "Upfront")
        .when((F.col("vendor_name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("vendor_name") == "EXAGRID")  & 
            (F.col("duration")=="3 YR"), "Sure Mapping")
        .when((F.col("vendor_name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "EXAGRID")  & 
            (F.col("duration")=="3 YR"), "Capacity")
        .when((F.col("vendor_name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "EXAGRID")  & 
            (F.col("duration")=="3 YR"), "Vendor Support")
        .when((F.col("vendor_name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Hardware")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "EXAGRID")  & 
            (F.col("duration")=="3 YR"), "Sure Mapping")
        .when((F.col("vendor_name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Forcepoint-Specific Transformations
    df = df.withColumn(
        "months", 
        F.when(F.col("vendor_name") == "Forcepoint", 
               F.regexp_extract(F.col("lifecycleformula"), "^(\d+)", 1).cast("int"))
        .otherwise(F.col("months"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Forcepoint") & (F.col("months") > 1)
            , F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR")))
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 1), "1M")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 0), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Forcepoint") & (F.col("months") > 1), "Sure Mapping")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 1), "Sure Mapping")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 0), "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", 
        F.when((F.col("vendor_name") == "Forcepoint") & (F.col("months") > 1), "Upfront")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 1), "Monthly")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 0), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("vendor_name") == "Forcepoint") & (F.col("months") > 1), "Sure Mapping")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 1), "Sure Mapping")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 0), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "Forcepoint") & (F.col("months") > 1), "Capacity")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 1), "Flexible")
        .when((F.col("vendor_name") == "Forcepoint") & (F.col("months") == 0), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Forcepoint") & 
            ((F.col("producttype") == "Software") & F.col("duration").rlike("YR")), "SW Subscription")
        .when((F.col("vendor_name") == "Forcepoint") & 
            ((F.col("producttype") == "Extras") & F.col("duration").rlike("YR")), "SW Subscription")
        .when((F.col("vendor_name") == "Forcepoint") & 
            ((F.col("producttype") == "Software") & F.col("duration").rlike("M")), "SW Subscription")
        .when((F.col("vendor_name") == "Forcepoint") & 
            (F.col("producttype") == "Software"), "SW Perpetual")
        .when((F.col("vendor_name") == "Forcepoint") & 
            (F.col("producttype") == "Professional Service"), "Professional services")
        .otherwise(F.col("Type"))
    )
    # GFI-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when((F.col("vendor_name") == "GFI") & 
            (F.col("description").rlike(r'(?i).*1 Year.*')) & 
            (F.col("description").rlike(r'(?i).*subscription.*')), "1 YR")
        .when((F.col("vendor_name") == "GFI") & 
            (F.col("description").rlike(r'(?i).*2 Year.*')) & 
            (F.col("description").rlike(r'(?i).*subscription.*')), "2 YR")
        .when((F.col("vendor_name") == "GFI") & 
            (F.col("description").rlike(r'(?i).*3 Year.*')) & 
            (F.col("description").rlike(r'(?i).*subscription.*')), "3 YR")
        .when((F.col("vendor_name") == "GFI") & 
            (F.col("description").rlike(r'(?i).*GFI 6000 Fax Pages inbound or outbound LOCAL in one year.*')), "1 YR")
        .when((F.col("vendor_name") == "GFI") & 
            (F.col("itemtrackingcode").rlike(r'(?i).*FMO-SS500-OFS-1Y.*')), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("vendor_name") == "GFI") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("vendor_name") == "GFI") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("vendor_name") == "GFI") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when((F.col("vendor_name") == "GFI") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when((F.col("vendor_name") == "GFI") & 
            (F.col("itemtrackingcode").rlike(r'(?i).*FMO-SS500-OFS-1Y.*')), "Vendor Support")
        .when((F.col("vendor_name") == "GFI") & (F.col("duration")!='Not Assigned'), "SW Subscription")
        .otherwise(F.col("Type"))
    )
    # Kaspersky-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when((F.col("vendor_name") == "Kaspersky") & 
            (F.col("description").rlike(r'(?i).*12 Months.*')), "1 YR")
        .when((F.col("vendor_name") == "Kaspersky") & 
            (F.col("description").rlike(r'(?i).*24 Month.*')), "2 YR")
        .when((F.col("vendor_name") == "Kaspersky") & 
            (F.col("description").rlike(r'(?i).*36 Month.*')), "3 YR")
        .when((F.col("vendor_name") == "Kaspersky") & 
            (F.col("description").rlike(r'(?i).*60 Months.*')), "5 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("vendor_name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("vendor_name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("vendor_name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when((F.col("vendor_name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when((F.col("vendor_name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "SW Subscription")
        .otherwise(F.col("Type"))
    )
    # GitHub-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "GITHUB", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "GITHUB", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "GITHUB", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when(F.col("vendor_name") == "GITHUB", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("vendor_name") == "GITHUB", "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("vendor_name") == "GITHUB", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # HP-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when((F.col("vendor_name") == "HP") & (F.col("description").rlike(r'(?i).*1yr.*')), "1 YR")
        .when((F.col("vendor_name") == "HP") & (F.col("description").rlike(r'(?i).*5-year.*')), "5 YR")
        .when((F.col("vendor_name") == "HP") & (F.col("description").rlike(r'(?i).*5y.*')), "5 YR")
        .when((F.col("vendor_name") == "HP") & (F.col("description").rlike(r'(?i).*5Y.*')), "5 YR")
        .when((F.col("vendor_name") == "HP") & (F.col("description").rlike(r'(?i).*3Y.*')), "3 YR")
        .when((F.col("vendor_name") == "HP") & (F.col("description").rlike(r'(?i).*4y.*')), "4 YR")
        .when((F.col("vendor_name") == "HP") & (F.col("description").rlike(r'(?i).*4Y.*')), "4 YR")
        .when((F.col("vendor_name") == "HP"), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "HP", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "HP", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "HP", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when(F.col("vendor_name") == "HP", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("vendor_name") == "HP") & (F.col("duration")=='Perpetual'), "Other Mapping")
        .when((F.col("vendor_name") == "HP") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Progress-Specific Transformations
    df = df.withColumn(
        "frequency", 
        F.when(F.col("vendor_name") == "Progress", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("vendor_name") == "Progress", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Progress", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*1 Year Support.*'), "Vendor Support")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*3 Year Support.*'), "Vendor Support")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*Standard Support.*'), "Vendor Support")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*3 Years Service.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*1 Year Service.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*1 Year Enterprise Subscription.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*Subscription 1 Year Enterprise.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*1 Year Subscription.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*LICENSE W.*O SUPPORT.*'), "SW Perpetual")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*3 Year Extended Support.*'), "Vendor Support")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*Extended Support 1 Year.*'), "Vendor Support")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*2 Year Extended Support.*'), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*1 Year Support.*'), "1 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*3 Year Support.*'), "3 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*Standard Support.*'), "1 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*3 Years Service.*'), "3 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*1 Year Service.*'), "1 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*1 Year Enterprise Subscription.*'), "1 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*Subscription 1 Year Enterprise.*'), "1 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*1 Year Subscription.*'), "1 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*LICENSE W.*O SUPPORT.*'), "Perpetual")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*3 Year Extended Support.*'), "3 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*Extended Support 1 Year.*'), "1 YR")
        .when((F.col("vendor_name") == "Progress") & 
            F.col("description").rlike(r'(?i).*2 Year Extended Support.*'), "2 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("vendor_name") == "Progress") &
               (F.col("description").rlike(r'(?i).*Standard Support.*')), "Other Mapping")
        .when((F.col("vendor_name") == "Progress") & 
              (F.col("duration")!='Not Assigned') , "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # HornetSecurity-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "HornetSecurity", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "HornetSecurity", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "HornetSecurity", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("vendor_name") == "HornetSecurity", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "HornetSecurity") & 
            F.col("sku").rlike(r'(?i).*REN-SMA24.*'), "1 YR")
        .when((F.col("vendor_name") == "HornetSecurity") & 
            F.col("sku").rlike(r'(?i).*VMSE-1-999.*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "HornetSecurity", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # IMPERVA-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "IMPERVA", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "IMPERVA", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("vendor_name") == "IMPERVA", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("vendor_name") == "IMPERVA", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "IMPERVA") & 
            F.col("sku").rlike(r'(?i).*SS-WAF-X451-P-R-SL2.*'), "1 YR")
        .when((F.col("vendor_name") == "IMPERVA") & 
            F.col("sku").rlike(r'(?i).*SB-WAF-TRS-45-R-TR0.*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "IMPERVA") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # IRONSCALES-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "IRONSCALES", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "IRONSCALES", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "IRONSCALES", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("vendor_name") == "IRONSCALES", "SW TBD")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("vendor_name") == "IRONSCALES", "Not Assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "IRONSCALES", "Not Assigned")
        .otherwise(F.col("Mapping_type_Duration"))
    )



    # Ivanti-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Ivanti", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Ivanti", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Ivanti", 
            F.when(F.col("description").rlike(r'(?i).*MSP.*'), "Flexible")
            .otherwise("Capacity"))
        .otherwise(F.col("consumption"))
    ).withColumn(
        "duration", 
        F.when(F.col("vendor_name") == "Ivanti", "Not Assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "Ivanti", "Not Assigned")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Ivanti") & 
            F.col("description").rlike(r'(?i).*Subscription.*'), "SW Subscription")
        .otherwise(F.col("Type"))
    )

    # LogRhythm-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "LOGRHYTHM", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("vendor_name") == "LOGRHYTHM", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "LOGRHYTHM", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "LOGRHYTHM") & 
            F.col("description").rlike(r'(?i).*Training.*'), "Professional Service")
        .when((F.col("vendor_name") == "LOGRHYTHM") & 
            F.col("description").rlike(r'(?i).*Software Subscription.*'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "LOGRHYTHM") & 
            F.col("description").rlike(r'(?i).*Training.*'), "Perpetual")
        .when((F.col("vendor_name") == "LOGRHYTHM") & 
            F.col("description").rlike(r'(?i).*Software Subscription.*'), "1.8 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "LOGRHYTHM") & 
            F.col("description").rlike(r'(?i).*Training.*'), "Sure Mapping")
        .when((F.col("vendor_name") == "LOGRHYTHM") & 
            F.col("description").rlike(r'(?i).*Software Subscription.*'), "Ratio Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Macmon-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Macmon", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Macmon", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Macmon", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-LI-.*'), "SW Perpetual")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-W1-.*'), "Vendor Support")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-W2-.*'), "Vendor Support")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'MMSMB-VA250-RNW'), "SW Subscription")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'MMMSP-.*'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-LI-.*'), "Perpetual")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-W1-.*'), "1 YR")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-W2-.*'), "2 YR")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'MMSMB-VA250-RNW'), "3 YR")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'MMMSP-.*'), "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-LI-.*'), "Sure Mapping")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-W1-.*'), "Sure Mapping")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'(?i).*-W2-.*'), "Sure Mapping")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'MMSMB-VA250-RNW'), "Sure Mapping")
        .when((F.col("vendor_name") == "Macmon") & 
            F.col("sku").rlike(r'MMMSP-.*'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # n-able-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "n-able") & 
            (F.col("sku").rlike(r'(?i).*-1Y')), "1 YR")
        .when((F.col("vendor_name") == "n-able") & 
            (F.col("description").rlike(r'(?i).*Annual Maintenance Renewal.*')), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("vendor_name") == "n-able") & 
            (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("vendor_name") == "n-able") & 
            (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "n-able") & 
            (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "n-able") & 
            (F.col("sku").rlike(r'(?i).*-1Y')), "SW Subscription")
        .when((F.col("vendor_name") == "n-able") & 
            (F.col("description").rlike(r'(?i).*Annual Maintenance Renewal.*')), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "n-able") & 
            (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Netwrix-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Netwrix") & 
            (F.col("sku").rlike(r'(?i).*-CC-1M')), "1 M")
        .when((F.col("vendor_name") == "Netwrix") & 
            (F.col("sku").rlike(r'(?i).*-CC-1Y')), "1 YR")
        .when((F.col("vendor_name") == "Netwrix") & 
            (F.col("sku").rlike(r'(?i).*-CC-3Y')), "3 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("vendor_name") == "Netwrix") & 
            (F.col("sku").rlike(r'(?i).*-CC-1M')), "Monthly")
        .when((F.col("vendor_name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("vendor_name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "Netwrix") & 
            (F.col("sku").rlike(r'(?i).*-CC-1M')), "Flexible")
        .when((F.col("vendor_name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Versa Networks-Specific Transformations
    df = df.withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'(?i).*-CC-1M')), "Flexible")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PRIME.*-5YR')), "Capacity")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'CLDSVC-.*-5YR.*')), "Capacity")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'CLDSVC-.*-3YR.*')), "Capacity")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PRIME.*-3YR')), "Capacity")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PREM.*-3YR')), "Capacity")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'SUP-.*-5YR')), "Capacity")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PDD-HOURLY-20-A|PDD-DAILY-A')), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'(?i).*-CC-1M')), "Vendor Support")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PRIME.*-5YR')), "SW Subscription")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'CLDSVC-.*-5YR.*')), "SW Subscription")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'CLDSVC-.*-3YR.*')), "SW Subscription")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PRIME.*-3YR')), "SW Subscription")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PREM.*-3YR')), "SW Subscription")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'SUP-.*-5YR')), "Vendor Support")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PDD-HOURLY-20-A|PDD-DAILY-A')), "Professional Service")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'(?i).*-CC-1M')), "1 M")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PRIME.*-5YR')), "5 YR")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'CLDSVC-.*-5YR.*')), "5 YR")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'CLDSVC-.*-3YR.*')), "3 YR")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PRIME.*-3YR')), "3 YR")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PREM.*-3YR')), "3 YR")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'SUP-.*-5YR')), "5 YR")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PDD-HOURLY-20-A|PDD-DAILY-A')), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("vendor_name") == "Versa Networks") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Versa Networks") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'(?i).*-CC-1M')), "Monthly")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PRIME.*-5YR')), "Upfront")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'CLDSVC-.*-5YR.*')), "Upfront")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'CLDSVC-.*-3YR.*')), "Upfront")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PRIME.*-3YR')), "Upfront")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PREM.*-3YR')), "Upfront")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'SUP-.*-5YR')), "Upfront")
        .when((F.col("vendor_name") == "Versa Networks") & 
            (F.col("sku").rlike(r'PDD-HOURLY-20-A|PDD-DAILY-A')), "Upfront")
        .otherwise(F.col("frequency"))
    )

    # Veritas-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Veritas", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Veritas", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Veritas", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*36MO.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*12MO.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*24MO.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Veritas") & 
            (F.col("description").rlike(r'(?i).*24 MONTHS.*') | F.col("description").rlike(r'(?i).*RENEWALS.*')), "Vendor Support")
        .when((F.col("vendor_name") == "Veritas") & 
            (F.col("description").rlike(r'(?i).*12MO.*') | F.col("description").rlike(r'(?i).*RENEWALS.*')), "Vendor Support")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*FREIGHT SERVICE.*|.*FEE SERVICE.*'), "SW Perpetual")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*36MO.*'), "3 YR")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*12MO.*'), "1 YR")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*24MO.*'), "2 YR")
        .when((F.col("vendor_name") == "Veritas") & 
            (F.col("description").rlike(r'(?i).*24 MONTHS.*') | F.col("description").rlike(r'(?i).*RENEWALS.*')), "2 YR")
        .when((F.col("vendor_name") == "Veritas") & 
            (F.col("description").rlike(r'(?i).*12MO.*') | F.col("description").rlike(r'(?i).*RENEWALS.*')), "1 YR")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*FREIGHT SERVICE.*|.*FEE SERVICE.*'), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*36MO.*'), "Sure Mapping")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*12MO.*'), "Sure Mapping")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*24MO.*'), "Sure Mapping")
        .when((F.col("vendor_name") == "Veritas") & 
            (F.col("description").rlike(r'(?i).*24 MONTHS.*') | F.col("description").rlike(r'(?i).*RENEWALS.*')), "Sure Mapping")
        .when((F.col("vendor_name") == "Veritas") & 
            (F.col("description").rlike(r'(?i).*12MO.*') | F.col("description").rlike(r'(?i).*RENEWALS.*')), "Sure Mapping")
        .when((F.col("vendor_name") == "Veritas") & 
            F.col("description").rlike(r'(?i).*FREIGHT SERVICE.*|.*FEE SERVICE.*'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Vendors of Complementary Products-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("vendor_name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("vendor_name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("vendor_name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Vectra Networks-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Vectra Networks", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Vectra Networks", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Vectra Networks", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Vectra Networks") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*') & (F.col("description").rlike(r'(?i).*1 YEAR.*')), "SW Subscription")
        .when((F.col("vendor_name") == "Vectra Networks") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*') & (F.col("description").rlike(r'(?i).*3 YEAR.*')), "SW Subscription")
        .when((F.col("vendor_name") == "Vectra Networks") & 
            F.col("sku").rlike(r'V-NDR-CLOUD-STANDARD'), "SW Subscription")
        .when((F.col("vendor_name") == "Vectra Networks") & 
            F.col("sku").rlike(r'VN-DETECT-NET'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Vectra Networks") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*') & (F.col("description").rlike(r'(?i).*1 YEAR.*')), "1 YR")
        .when((F.col("vendor_name") == "Vectra Networks") & 
            F.col("description").rlike(r'(?i).*SUBSCRIPTION.*') & (F.col("description").rlike(r'(?i).*3 YEAR.*')), "3 YR")
        .when((F.col("vendor_name") == "Vectra Networks") & 
            F.col("sku").rlike(r'V-NDR-CLOUD-STANDARD'), "Not Assigned")
        .when((F.col("vendor_name") == "Vectra Networks") & 
            F.col("sku").rlike(r'VN-DETECT-NET'), "Not Assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "Vectra Networks", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Varonis-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Varonis", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Varonis", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Varonis", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("vendor_name") == "Varonis", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("vendor_name") == "Varonis", "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "Varonis", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # TXOne Network-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "TXOne Network", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "TXOne Network", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "TXOne Network", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("vendor_name") == "TXOne Network", "Not assigned")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("vendor_name") == "TXOne Network", "Not assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "TXOne Network", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Tufin-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Tufin", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Tufin", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Tufin", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Tufin") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Professional Service")
        .when((F.col("vendor_name") == "Tufin") & 
            F.col("sku").rlike(r'TF-PS-DEPLOY-ST-PLUS'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Tufin") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Perpetual")
        .when((F.col("vendor_name") == "Tufin") & 
            F.col("sku").rlike(r'TF-PS-DEPLOY-ST-PLUS'), "Not assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Tufin") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Sure Mapping")
        .when((F.col("vendor_name") == "Tufin") & 
            F.col("sku").rlike(r'TF-PS-DEPLOY-ST-PLUS'), "Not assigned")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Trustwave-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Trustwave", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Trustwave", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Trustwave", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Trustwave") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Professional Service")
        .when((F.col("vendor_name") == "Trustwave") & 
            F.col("sku").rlike(r'MPL-MM-CLD-ESSENTIALS'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Trustwave") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Perpetual")
        .when((F.col("vendor_name") == "Trustwave") & 
            F.col("sku").rlike(r'MPL-MM-CLD-ESSENTIALS'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Trustwave") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Sure Mapping")
        .when((F.col("vendor_name") == "Trustwave") & 
            F.col("sku").rlike(r'MPL-MM-CLD-ESSENTIALS'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Tripwire-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Tripwire", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Tripwire", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Tripwire", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Tripwire") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Professional Service")
        .when((F.col("vendor_name") == "Tripwire") & 
            F.col("description").rlike(r'(?i).*SUPPORT RENEWAL.*'), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Tripwire") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Perpetual")
        .when((F.col("vendor_name") == "Tripwire") & 
            F.col("description").rlike(r'(?i).*SUPPORT RENEWAL.*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Tripwire") & 
            F.col("sku").rlike(r'TF-TOS-TCSE-CUSTOM'), "Sure Mapping")
        .when((F.col("vendor_name") == "Tripwire") & 
            F.col("description").rlike(r'(?i).*SUPPORT RENEWAL.*'), "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Trendmicro-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,36 MONTH.*')), "3 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,12 MONTH.*')), "1 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,24 MONTH.*')), "2 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,07 MONTH.*')), "0.583333333 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,13 MONTH.*')), "1.0883333333 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,27 MONTH.*')), "2.25 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,08 MONTH.*')), "0.66666666666667 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,03 MONTH.*')), "0.25 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,06 MONTH.*')), "0.5 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,02 MONTH.*')), "0.1666666666666667 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,18 MONTH.*')), "1.5 YR")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,01 MONTH.*')), "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,01 MONTH.*')), "Monthly")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("vendor_name") == "Trendmicro") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "Trendmicro") & (F.col("description").rlike(r'(?i).*LICENSE,01 MONTH.*')), "Flexible")
        .when((F.col("vendor_name") == "Trendmicro") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Trendmicro") & (F.col("duration")!='Not Assigned'),  "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Trendmicro") & (F.col("duration")!='Not Assigned'),  "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Trellix/Skyhigh-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh"), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh"), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh"), "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh") & 
            F.col("description").rlike(r'(?i).*1YR SUBSCRIPTION.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh") & 
            F.col("description").rlike(r'(?i).*1YR THRIVE.*'), "SW Subscription")
        .when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh") & 
            F.col("description").rlike(r'(?i).*EXTENDED.*SUPPORT 1YR.*'), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh") & 
            F.col("description").rlike(r'(?i).*1YR SUBSCRIPTION.*'), "1 YR")
        .when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh") & 
            F.col("description").rlike(r'(?i).*1YR THRIVE.*'), "1 YR")
        .when((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh") & 
            F.col("description").rlike(r'(?i).*EXTENDED.*SUPPORT 1YR.*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(((F.col("vendor_name") == "Trellix") | (F.col("vendor_name") == "Skyhigh"))  & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Thales-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Thales", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Thales", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Thales", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*THALES CLIENT SERVICES.*')), "Professional Service")
        .when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*SAFENET TRISTED ACCESS.*SUPPORT.*12 MO.*')), "SW Subscription")
        .when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*12 Mon. Subscr.*')), "SW Subscription")
        .when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*Basic, 12 Months.*')), "SW Subscription")
        .when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*SETUP FEE.*')), "SW Perpetual")
        .when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*ACTIVE USER.*MONTH.*')), "SW Subscription")
        .when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*CIPHERTRUST FLEX CONNECTOR.*')), "SW Perpetual")
        .when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*CIPHERTRUST FLEX ABILI.*')), "SW Perpetual")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Thales") & (F.col("description").rlike(r'(?i).*THALES CLIENT SERVICES.*')), "1 YR")
        .when((F.col("vendor_name") == "Thales")& (F.col("description").rlike(r'(?i).*SAFENET TRISTED ACCESS.*SUPPORT.*12 MO.*')), "1 YR")
        .when((F.col("vendor_name") == "Thales")& (F.col("description").rlike(r'(?i).*12 Mon. Subscr.*')), "1 YR")
        .when((F.col("vendor_name") == "Thales")& (F.col("description").rlike(r'(?i).*Basic, 12 Months.*')), "1 YR")
        .when((F.col("vendor_name") == "Thales")& (F.col("description").rlike(r'(?i).*SETUP FEE.*')), "Perpetual")
        .when((F.col("vendor_name") == "Thales")& (F.col("description").rlike(r'(?i).*ACTIVE USER.*MONTH.*')), "1 M")
        .when((F.col("vendor_name") == "Thales")& (F.col("description").rlike(r'(?i).*CIPHERTRUST FLEX CONNECTOR.*')), "Perpetual")
        .when((F.col("vendor_name") == "Thales")& (F.col("description").rlike(r'(?i).*CIPHERTRUST FLEX ABILI.*')), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Thales") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Teamviewer-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Teamviewer", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Teamviewer", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Teamviewer", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Teamviewer") & 
            (F.col("description").rlike(r'(?i).*2 YEARS SUBSCRIPTION.*')), "2 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Teamviewer") & (F.col("duration")!='Not Assigned'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Teamviewer") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # SentinelOne-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "SentinelOne", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "SentinelOne", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "SentinelOne", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "SentinelOne") & 
            (F.col("description").rlike(r'(?i).*12 M.*')), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "SentinelOne") & 
            (F.col("description").rlike(r'(?i).*12 M.*')), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "SentinelOne") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Rapid7-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Rapid7", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Rapid7", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Rapid7", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Rapid7") & 
            ((F.col("description").rlike(r'(?i).*1 YEAR*') | F.col("description").rlike(r'(?i).*ONE YEAR*'))), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Rapid7") & 
            ((F.col("description").rlike(r'(?i).*1 YEAR*')) | (F.col("description").rlike(r'(?i).*ONE YEAR*'))), "1 YR")
        .when((F.col("vendor_name") == "Rapid7") & (F.col("description").rlike(r'(?i).*PER MONTH*')), "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Rapid7") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", 
        F.when((F.col("vendor_name") == "Rapid7") & (F.col("description").rlike(r'(?i).*PER MONTH*')), "Monthly")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("vendor_name") == "Rapid7") & (F.col("description").rlike(r'(?i).*PER MONTH*')), "Flexible")
        .otherwise(F.col("consumption"))
    )
    # Ruckus-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Ruckus") & 
            F.col("description").rlike(r'(?i).*-12'), "1 YR")
        .when((F.col("vendor_name") == "Ruckus") & 
            F.col("description").rlike(r'(?i).*36 MONTHS*'), "3 YR")
        .when((F.col("vendor_name") == "Ruckus") & 
            F.col("description").rlike(r'(?i).*12 MONTHS*'), "1 YR")
        .when((F.col("vendor_name") == "Ruckus") & 
            F.col("description").rlike(r'(?i).*PERPETUAL*'), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Ruckus", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Ruckus", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Ruckus", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Ruckus") & (F.col("duration")=='duration'), "SW Perpetual")
        .when((F.col("vendor_name") == "Ruckus") & (F.col("duration")!='Not Assigned'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Ruckus") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # OneSpan-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "OneSpan", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "OneSpan", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "OneSpan", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "OneSpan") & 
            ((F.col("description").rlike(r'(?i).*MAINTENANCE*') | F.col("description").rlike(r'(?i).*MAINT.*SUPPORT*'))), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "OneSpan") & 
            ((F.col("description").rlike(r'(?i).*MAINTENANCE*') | F.col("description").rlike(r'(?i).*MAINT.*SUPPORT*'))), "3 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("vendor_name") == "OneSpan", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # SEPPMail-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "SEPPMail", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "SEPPMail", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "SEPPMail", "Capacity")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "SEPPMail") & 
            F.col("description").rlike(r'(?i).*5 JAHRE*'), "SW Subscription")
        .when((F.col("vendor_name") == "SEPPMail") & 
            F.col("description").rlike(r'(?i).*1 JAHR*'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "SEPPMail") & 
            F.col("description").rlike(r'(?i).*5 JAHRE*'), "5 YR")
        .when((F.col("vendor_name") == "SEPPMail") & 
            F.col("description").rlike(r'(?i).*1 JAHR*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "SEPPMail") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Parallels-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("vendor_name") == "Parallels", "Monthly")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("vendor_name") == "Parallels", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("vendor_name") == "Parallels", "Flexible")
        .otherwise(F.col("consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("vendor_name") == "Parallels") & 
            F.col("description").rlike(r'(?i).*USERS.*MONTH*'), "SW Subscription")
        .when((F.col("vendor_name") == "Parallels") & 
            F.col("description").rlike(r'(?i).*1 YEAR*'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("vendor_name") == "Parallels") & 
            F.col("description").rlike(r'(?i).*USERS.*MONTH*'), "1 M")
        .when((F.col("vendor_name") == "Parallels") & 
            F.col("description").rlike(r'(?i).*1 YEAR*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("vendor_name") == "Parallels") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )


    # Hardware-Specific Transformations
    ## overides all others
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("producttype") == "Hardware") | 
            (F.col("vendor_name") == "PROLABS") | 
            (F.col("vendor_name") == "RACKMOUNT"), 
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("producttype") == "Hardware") | 
            (F.col("vendor_name") == "PROLABS") | 
            (F.col("vendor_name") == "RACKMOUNT"), 
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("producttype") == "Hardware") | 
            (F.col("vendor_name") == "PROLABS") | 
            (F.col("vendor_name") == "RACKMOUNT"), 
            "Capacity"
        ).otherwise(F.col("consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("producttype") == "Hardware") | 
            (F.col("vendor_name") == "PROLABS") | 
            (F.col("vendor_name") == "RACKMOUNT"), 
            "Hardware"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("producttype") == "Hardware") | 
            (F.col("vendor_name") == "PROLABS") | 
            (F.col("vendor_name") == "RACKMOUNT"), 
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            (F.col("producttype") == "Hardware") | 
            (F.col("vendor_name") == "PROLABS") | 
            (F.col("vendor_name") == "RACKMOUNT"), 
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    )



    return df





# COMMAND ----------

# # Write the cleaned DataFrame to Delta table
# df_source_1.write \
#     .mode("overwrite") \
#     .format("delta") \
#     .option("mergeSchema", "true") \
#     .option("overwriteSchema", "true") \
#     .option("columnMapping", "name") \
#     .option("delta.columnMapping.mode", "name") \
#     .saveAsTable(f"{catalog}.{schema}.arr_x")

# zorder_cols = ["Consolidated Vendor Name", "Manufacturer Item No_", "description"]
# zorder_by = ", ".join(f"`{col}`" for col in zorder_cols)
# spark.sql(f"OPTIMIZE {catalog}.{schema}.arr_x ZORDER BY ({zorder_by})")
   

# df_source_1 = spark.table(f"{catalog}.{schema}.arr_x")
 

# COMMAND ----------

# Check for pattern matching
#df_1.select("Manufacturer Item No_").where(F.col("Manufacturer Item No_").rlike("^S-.*-P$")).show()

# COMMAND ----------

# DBTITLE 1,initialise the cleansed data
# Convert to PySpark DataFrame
df_source = spark.table(f"{catalog}.{schema}.arr_auto_0")
 

# COMMAND ----------

# DBTITLE 1,levenshtein value for sku and local_sku
df_source_lev=df_source.filter(col('local_sku')!=col('sku')).withColumn("levenshtein", levenshtein(col(f'local_sku'), col(f'sku')) )
# display(df_source_lev.select('levenshtein','local_sku','*'))


# COMMAND ----------

# DBTITLE 1,begin automation matching


# Apply transformations in sequence
composite_key = [
            'sku',
            'vendor_name'
        ]
##clear dupe records from source
df_arr_auto_1 = dq_transform(df_source, composite_key, keep_duplicates=False)
df_arr_auto_1 = default_columns(df_arr_auto_1)

df_arr_auto_1 = parse_months(df_arr_auto_1)
df_arr_auto_1 = apply_vendor_transformations(df_arr_auto_1)
df_arr_auto_1 = apply_final_transformations(df_arr_auto_1)

df_arr_auto_1 = df_arr_auto_1.filter(col('sku').isNotNull())
df_arr_auto_1 = clean_column_names(df_arr_auto_1)

# Reorder columns more elegantly
columns = df_arr_auto_1.columns
desired_columns = ['sku', 'vendorcodeitem', 'vendor_name',
'matched_type',
'duration',
'frequency',
'consumption',
'mapping_type_duration',
'mapping_type_billing',
'mrrratio',
'months',
'type',
'itemtrackingcode',
'local_sku',
'description',
'vendor_group_id',
'has_sku_master_vendor_duplicates',
'has_vendor_master_duplicates',
'has_sku_vendor_duplicates',
'has_duplicates',
'sys_databasename',
'sys_bronze_insertdatetime_utc',
'sys_silver_insertdatetime_utc',
'sys_silver_iscurrent'
]
desired_order = desired_columns #+ [col for col in columns if col not in desired_columns]
df_arr_auto_1 = df_arr_auto_1.select(desired_order)



# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_auto_1 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


# COMMAND ----------

# DBTITLE 1,write matched arr skus to table arr_auto_1
# Write the cleaned DataFrame to Delta table
df_arr_auto_1.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.arr_auto_1")

# COMMAND ----------

# display(df_arr_auto_1)

# COMMAND ----------

# DBTITLE 1,load in pierre file
import pandas as pd
!pip install openpyxl --quiet

# File path in DBFS
pierre_file_path = "/Workspace/Users/akhtar.miah@infinigate.com/ARR_Output_2024_11_21_11_41_59.xlsx"

pandas_pierre_df = pd.read_excel(pierre_file_path)
# Convert to PySpark DataFrame
pierre_df = spark.createDataFrame(pandas_pierre_df)


# Multiple column rename:
pierre_columns_to_rename = {
'Commitment Duration (in months)' : 'duration',
'Billing frequency' : 'frequency',
'Consumption Model' : 'consumption',
'Mapping type Duration' : 'mapping_type_duration',
'Mapping type Billing' : 'mapping_type_billing',
'MRR ratio' : 'mrrratio'
}

pierre_df = rename_columns(pierre_df, pierre_columns_to_rename)

#clean column names
pierre_df = clean_column_names(pierre_df)



# Write the cleaned DataFrame to Delta table
pierre_df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.arr_pierre_0")

# COMMAND ----------

# arr_pierre_0: pierres file generated via python
df_arr_pierre_0= spark.table(f"{catalog}.{schema}.arr_pierre_0")
df_arr_auto_1= spark.table(f"{catalog}.{schema}.arr_auto_1")


# MRR Ratio
df_arr_pierre_0 = df_arr_pierre_0.withColumn(
    "mrrratio",
    F.when(F.col("duration").rlike(r'.*YR'), 12 * F.regexp_extract(F.col("duration"), r"(\d*\.?\d+)", 1).cast("float"))
    .otherwise(0)
)

# Duration in years (rounded and formatted)
df_arr_pierre_0 = df_arr_pierre_0.withColumn(
    "duration",
    F.when(F.col("duration").rlike(r'.*YR'),
        F.concat(
            F.format_number(
                F.regexp_extract(F.col("duration"), r"(\d*\.?\d+)", 1).cast("float"),
                2
            ),
            F.lit(" YR")
        )
    ).otherwise(F.col("duration"))
)

# mapping_type_billing set to "Sure Mapping"
df_arr_pierre_0 = df_arr_pierre_0.withColumn("mapping_type_billing",
    F.when(F.lower(F.col("mapping_type_billing")) == 'sure mapping',
        F.lit("Sure Mapping")
    ).when(F.lower(F.col("mapping_type_billing")) == 'other mapping',
        F.lit("Other Mapping")  
    ).otherwise(F.col("mapping_type_billing")))


# mapping_type_duration set to "Sure Mapping" and "Other Mapping"
df_arr_pierre_0 = df_arr_pierre_0.withColumn("mapping_type_duration",
    F.when(F.lower(F.col("mapping_type_duration")) == 'other mapping',
        F.lit("Other Mapping")
    ).when(F.lower(F.col("mapping_type_duration")) == 'sure mapping',  
        F.lit("Sure Mapping")
    ).otherwise(F.col("mapping_type_duration")))




# df_arr_pierre_0.printSchema()
# df_arr_auto_1.printSchema()


# COMMAND ----------

# First, let's count total rows in each DataFrame
df1_count = df_arr_pierre_0.count()
df2_count = df_arr_auto_1.count()

print(f"Total rows in df_arr_pierre_0: {df1_count}")
print(f"Total rows in df_arr_auto_1: {df2_count}")

# Compare schema differences
schema_diff = set(df_arr_pierre_0.columns) ^ set(df_arr_auto_1.columns)
if schema_diff:
    print("\nColumns that differ between DataFrames:")
    print(schema_diff)

# Find records in df1 that are not in df2
df1_not_in_df2 = df_arr_pierre_0.join(
    df_arr_auto_1,
    (df_arr_pierre_0.sku == df_arr_auto_1.sku) & 
    (df_arr_pierre_0.vendor_name == df_arr_auto_1.vendor_name),
    "left_anti"
)

# Find records in df2 that are not in df1
df2_not_in_df1 = df_arr_auto_1.join(
    df_arr_pierre_0,
    (df_arr_auto_1.sku == df_arr_pierre_0.sku) & 
    (df_arr_auto_1.vendor_name == df_arr_pierre_0.vendor_name),
    "left_anti"
)

# Find exact matches (records that are identical in both DataFrames)
exact_matches = df_arr_pierre_0.join(
    df_arr_auto_1,
    (df_arr_pierre_0.sku == df_arr_auto_1.sku) & 
    (df_arr_pierre_0.vendor_name == df_arr_auto_1.vendor_name),
    "inner"
)

print(f"\nRecords in df_arr_pierre_0 but not in df_arr_auto_1: {df1_not_in_df2.count()}")
print(f"Records in df_arr_auto_1 but not in df_arr_pierre_0: {df2_not_in_df1.count()}")
print(f"Exact matches (based on SKU (manufacturer_item_no_) and Vendor): {exact_matches.count()}")

# COMMAND ----------

# df1_not_in_df2.display()

# COMMAND ----------

# df2_not_in_df1.display()

# COMMAND ----------

# display(df_arr_auto_1)

# COMMAND ----------

columns_to_rename = { 
    "item_tracking_code": "itemtrackingcode"
}


df_arr_pierre_0 = rename_columns(df_arr_pierre_0, columns_to_rename)

# Define composite key
composite_key = [
    'sku',
    'vendor_name'
    ,'itemtrackingcode'
]

# Define fields to compare - need to include mrrratio since we want to compare it
fields_to_compare = [
    'duration',
    'frequency',
    'consumption',
    'mapping_type_duration',
    'mapping_type_billing',
    'mrrratio'  # Added this since we want to compare it with tolerance
]

# Define numeric fields tolerance
numeric_fields_tolerance = {
    'mrrratio': 0.05  # 5% tolerance
}

# Define additional fields
additional_fields = [
    'matched_type',
    'country', 
    'consumption_model', 
    'description'
]

# Call the function
compare_df = compare_dataframes(
    df1=df_arr_pierre_0, 
    df2=df_arr_auto_1, 
    composite_key=composite_key, 
    fields_to_compare=fields_to_compare, 
    additional_fields=additional_fields,
    numeric_tolerance=numeric_fields_tolerance
)

# COMMAND ----------


compare_df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.arr_comparison")

# COMMAND ----------

# compare_df.filter(col('comparison_status')=='DIFFERENT_VALUES').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Map local skus to their manufacturer sku
# MAGIC Manufacturer SKUs have been mapped using the automation process.  This does not account for the local skus which are occasionally used within the transaction table, and in certain situations they are paired to their MasterSKU (ManufacturerSKU). Populating the arr_sku table with local skus will fill satisfy these scenarios
# MAGIC a match_type field will help classify the mapping types used.
# MAGIC - match_type auto: means the skus have been mapped using the mapping process
# MAGIC - match_type child: means the local skus have been mapped to their manufacturing sku that was processed in the automated stage
# MAGIC - match_type manual: means the skus have been mapped through the manual data sheet master.datanowarr
# MAGIC

# COMMAND ----------

# DBTITLE 1,map local sku to their manufacturer sku
df_sku_vendor_local =spark.table(f"{catalog}.{schema}.arr_sku_vendor_dq").alias("svdq")
df_sku_vendor_local= df_sku_vendor_local.filter((col('VendorCodeItem').isNotNull()) 
                                                & (col('ManufacturerItemNo_') == lit('A320TCHNE'))
                                                ).withColumn("matched_type", lit("mt=local"))

key_cols=["sku","VendorCodeItem"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_sku_vendor_local = check_duplicate_keys(df_sku_vendor_local, key_cols, order_cols)

df_sku_vendor_local.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lets compare auto arr with the manual mapping

# COMMAND ----------

# DBTITLE 1,bring in manual file to compare it to the automated mapping process
  
df_arr_datanow_0= spark.table(f"silver_{ENVIRONMENT}.masterdata.datanowarr")
df_arr_datanow_0= df_arr_datanow_0 \
    .withColumn("matched_type", lit("mt=manual")) \
    .withColumn("sys_databasename", lit("Managed Datasets")) \
    .filter(col("sys_silver_iscurrent") == True)
# Multiple column rename:
columns_to_rename = { 
    "sku": "sku",
    "Vendor_Name": "vendor_name",
    "billing_frequency": "frequency",
    "commitment_duration2": "duration",
    "Commitment_Duration_in_months":"months",
    "consumption_model": "consumption",
    "MRR_Ratio": "mrrratio",
    'Product_Type':"type",
    "Mapping_Type_Duration" : "mapping_type_duration",
    "Mapping_Type_Billing" : "mapping_type_billing",
    "Sys_Silver_IsCurrent": "sys_silver_iscurrent"
}

df_arr_datanow_0 = rename_columns(df_arr_datanow_0, columns_to_rename)
# Apply the function to clean column names
df_arr_datanow_0 = clean_column_names(df_arr_datanow_0)




# COMMAND ----------

# MRR Ratio
df_arr_datanow_0 = df_arr_datanow_0.withColumn(
    "mrrratio",
    F.when(F.col("duration").rlike(r'.*YR'), 12 * F.regexp_extract(F.col("duration"), r"(\d*\.?\d+)", 1).cast("float"))
    .otherwise(0)
)

# Duration in years (rounded and formatted)
df_arr_datanow_0 = df_arr_datanow_0.withColumn(
    "duration",
    F.when(F.col("duration").rlike(r'.*YR'),
        F.concat(
            F.format_number(
                F.regexp_extract(F.col("duration"), r"(\d*\.?\d+)", 1).cast("float"),
                2
            ),
            F.lit(" YR")
        )
    ).otherwise(F.col("duration"))
)

# mapping_type_billing set to "Sure Mapping"
df_arr_datanow_0 = df_arr_datanow_0.withColumn("mapping_type_billing",
    F.when(F.lower(F.col("mapping_type_billing")) == 'sure mapping',
        F.lit("Sure Mapping")
    ).when(F.lower(F.col("mapping_type_billing")) == 'other mapping',
        F.lit("Other Mapping")  
    ).otherwise(F.col("mapping_type_billing")))


# mapping_type_duration set to "Sure Mapping" and "Other Mapping"
df_arr_datanow_0 = df_arr_datanow_0.withColumn("mapping_type_duration",
    F.when(F.lower(F.col("mapping_type_duration")) == 'other mapping',
        F.lit("Other Mapping")
    ).when(F.lower(F.col("mapping_type_duration")) == 'sure mapping',  
        F.lit("Sure Mapping")
    ).otherwise(F.col("mapping_type_duration")))

#map the vendoritemcode using the vendorcode 
#!!DO THIS LATER ONCE WE CREATE A VENDORGROUP TABLE. arr_vendor_group is actually a link table not a dimension
# df_vendor_master_unique = spark.table(f"{catalog}.{schema}.arr_vendor_group")
# # only pick the first unique vendor group
# df_arr_datanow_0 = (
#    df_arr_datanow_0
#    .join(
#        df_vendor_master_unique,
#        df_arr_datanow_0['vendor_name'] == df_vendor_master_unique['VendorGroup'],
#        'left'
#    ).select(
#        df_arr_datanow_0['*'],  # All columns from df_sku_vendor_unique
#        df_vendor_master_unique['sid'].alias('vendor_group_id'),  # Renamed sid to vendor_group_id
#        df_vendor_master_unique['VendorCode'].alias('VendorCodeItem'),  # VendorGroup from vendor master
#        df_vendor_master_unique['has_duplicates'].alias('has_vendor_master_duplicates'),  # has_duplicates for vendormaster
#    )
# )


# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_datanow_0 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


df_arr_datanow_0.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.arr_datanow_0")



# COMMAND ----------

display(df_arr_datanow_0.filter(col("sku")=='001-000001696'))

# COMMAND ----------

display(df_arr_datanow_0.filter(col("sku")=='95504-AP310I-WR'))

# COMMAND ----------

key_cols=["sku","VendorCodeItem"]
order_cols=["Sys_Bronze_InsertDateTime_UTC"]
df_sku_vendor_dq = check_duplicate_keys(df_sku_vendor, key_cols, order_cols)

df_sku_vendor_child=df_sku_vendor_dq.filter(col("row_number") == lit(1))

# COMMAND ----------

display(df_sku_vendor_child.filter(col("sku")=='XS136Z12ZZRCAA-SP'))

# COMMAND ----------

# Load the auto DataFrame
df_arr_auto_1 = spark.table(f"{catalog}.{schema}.arr_auto_1")
df_arr_vendor_group = spark.table(f"{catalog}.{schema}.arr_vendor_group")
# Add parent_sku column to df_arr_auto_1 with null values, at the front
df_arr_auto_1 = df_arr_auto_1.select(
    lit(None).alias("parent_sku"),
    "*"
)


# Add parent_sku column to df_arr_auto_1 with null values
df_arr_auto_1 = df_arr_auto_1.withColumn("parent_sku", lit(None))

# Perform the anti join to find missing records
df_arr_auto_2 = (
    df_sku_vendor_child.alias("local")
    .join(
        df_arr_vendor_group.alias("vg"),
        on=[
            (col("vg.VendorCode") == col("local.VendorCodeItem"))
        ],
        how="inner"
    )
    .join(
        df_arr_auto_1.alias("auto"),
        on=[
            (col("local.sku") == col("auto.sku")) & 
            (col("auto.vendorcodeitem") == col("local.VendorCodeItem"))
        ],
        how="left_anti"
    )
    .join(
        df_arr_auto_1.alias("auto2"),
        on=[
            (col("local.ManufacturerItemNo_") == col("auto2.sku")) &
            (col("local.sku") != col("auto2.sku")) & 
            (col("auto2.vendor_name") == col("vg.VendorGroup"))
        ],
        how="inner"
    )
)

# Get all columns from df_arr_auto_1 except sku and local_sku
columns_auto2 = [col_name for col_name in df_arr_auto_1.columns 
                if col_name not in ["local_sku", "sku", "parent_sku", "matched_type"]]

# Create intermediate DataFrame with new columns and proper structure
df_arr_auto_2 = (
    df_arr_auto_2
    .select(
        col("local.sku").alias("sku"),
        col("auto2.sku").alias("parent_sku"),
        col("local.vendorcodeitem").alias("new_vendorcodeitem"),
        *[col(f"auto2.{c}").alias(c) for c in columns_auto2]
    )
    .withColumn("local_sku", lit(None))
    .withColumn("new_matched_type", lit("mt=child"))
    .withColumn("vendorcodeitem", coalesce(col("new_vendorcodeitem"), col("vendorcodeitem"), lit("NaN")))
    .drop("matched_type", "new_vendorcodeitem")
    .withColumnRenamed("new_matched_type", "matched_type")
)

# Get updated columns list including parent_sku
columns_auto_1 = df_arr_auto_1.columns

# Add any missing columns with null values
for col_name in columns_auto_1:
    if col_name not in df_arr_auto_2.columns:
        df_arr_auto_2 = df_arr_auto_2.withColumn(col_name, lit(None))

# Create final union with exact column ordering
df_arr_auto_2 = df_arr_auto_1.unionByName(
    df_arr_auto_2.select(*columns_auto_1),
    allowMissingColumns=True
)

# Display the child matches
# df_arr_auto_2.filter(col("matched_type") == "mt=child").display()


# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_auto_2 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")

df_arr_auto_2.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.arr_auto_2")


df_arr_auto_2 = spark.table(f"{catalog}.{schema}.arr_auto_2")

# COMMAND ----------


 
# First do the anti join to find missing records
df_arr_auto_3 = (
    df_arr_datanow_0
    .join(
        df_arr_auto_2,
        on=["sku", "vendor_name"],
        how="left_anti"
    )
)
columns_auto_2 = df_arr_auto_2.columns
# Then add any missing columns and select
for missing_col in columns_auto_1:
    if missing_col not in df_arr_auto_3.columns:
        df_arr_auto_3 = df_arr_auto_3.withColumn(missing_col, lit(None))

# Create final union
df_arr_auto_3 = df_arr_auto_2.union(
    df_arr_auto_3.select(columns_auto_1)
)

# df_arr_auto_3.filter(col("matched_type")=="mt=child").display() 

# COMMAND ----------

key_cols=["sku","vendor_name"]
order_cols=["sys_bronze_insertdatetime_utc"]
df_arr_auto_3_dq = check_duplicate_keys(df_arr_auto_3, key_cols, order_cols)

df_arr_auto_3_dq.filter((F.col('occurrence') > 1) & (F.col('matched_type')== 'mt=manual')).display() 

# COMMAND ----------

# Create a clean table with column defaults enabled from the start
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_auto_3 
    USING delta
    TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
""")


# COMMAND ----------


# df_arr_auto_3.write \
#   .mode("overwrite") \
#   .format("delta") \
#   .option("mergeSchema", "true") \
#   .option("overwriteSchema", "true") \
#   .saveAsTable(f"{catalog}.{schema}.arr_auto_3")

# COMMAND ----------

#df_vendor_master_unique.filter(col("VendorCode")=='SO_UTM').display()
#df_sku_vendor_unique.filter(col("sku")=='XS136Z12ZZRCAA-SP').display()
df_arr_auto_3.filter(col("sku")=='XS136Z12ZZRCAA-SP').display()


# COMMAND ----------

from delta.tables import DeltaTable
target_table=f"{catalog}.{schema}.arr_auto_3"
# Define the columns to update/insert (excluding business keys)
update_columns_manual = {
    col: f"source.{col}" for col in df_arr_auto_3.columns 
    if col not in ["sku", "vendor_name"]
}
update_columns = {
    col: f"source.{col}" for col in df_arr_auto_3.columns 
    if col not in ["sku", "vendorcodeitem", "vendor_name"]
}

# Define all columns for insert
insert_columns = {col: f"source.{col}" for col in df_arr_auto_3.columns}

# Perform the merge
deltaTable = DeltaTable.forName(spark, target_table)

# first update the records where target matched_type is manual
merge_statement = (
    deltaTable.alias("target")
    .merge(
        source=df_arr_auto_3.alias("source").filter(col("matched_type")!='mt=manual'),
        condition=(
            (col("target.sku") == col("source.sku")) &
            (col("target.vendorcodeitem").isNull())  &
            (col("target.vendor_name") == col("source.vendor_name"))
        )
    )
    .whenMatchedUpdate(set=update_columns_manual)
    .execute()
)


# # then merge the records where matched_type is manual
merge_statement = (
    deltaTable.alias("target")
    .merge(
        source=df_arr_auto_3.alias("source"),
        condition=(
            (col("target.sku") == col("source.sku")) &
            (coalesce(col("target.vendorcodeitem"), lit('NaN')) == coalesce(col("source.vendorcodeitem"), lit('NaN'))) &
            (col("target.vendor_name") == col("source.vendor_name"))
        )
    )
    .whenMatchedUpdate(set=update_columns)
    .whenNotMatchedInsert(values=insert_columns)
    .execute()
)




# COMMAND ----------

# Load the auto DataFrame
df_arr_auto_3 = spark.table(f"{catalog}.{schema}.arr_auto_3")

# COMMAND ----------


# MRR Ratio

# Define composite key
composite_key = [
    'sku',
    'vendor_name'
]

# Define fields to compare - need to include mrrratio since we want to compare it
fields_to_compare = [
    'duration',
    'frequency',
    'consumption',
    'mapping_type_duration',
    'mapping_type_billing',
    'mrrratio'  # Added this since we want to compare it with tolerance
]

# Define numeric fields tolerance
numeric_fields_tolerance = {
    'mrrratio': 0.05  # 5% tolerance
}

# Define additional fields
additional_fields = [
    'matched_type',
    'country', 
    'consumption_model', 
]

df_arr_datanow_0 = df_arr_datanow_0.withColumn('vendor_name', lower(col('vendor_name')))
df_arr_auto_3 = df_arr_auto_3.withColumn('vendor_name', lower(col('vendor_name')))

# Call the function
compare_df = compare_dataframes(
    df1=df_arr_auto_3, 
    df2=df_arr_datanow_0, 
    composite_key=composite_key, 
    fields_to_compare=fields_to_compare, 
    additional_fields=additional_fields,
    numeric_tolerance=numeric_fields_tolerance
)


compare_df.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.arr_comparison_v2")

# COMMAND ----------

compare_df.filter(col('comparison_status')=='ONLY_IN_DF1').display()

# COMMAND ----------

compare_df.filter(col('comparison_status')=='DIFFERENT_VALUES').display()

# COMMAND ----------

composite_key = [
    'comparison_status',
    'vendor_name'
]
# Count duplicates
dupe_counts = compare_df.groupBy(*composite_key) \
    .agg(F.count("*").alias("_count"))

# Get records with duplicates
dupe_keys = dupe_counts \
    .filter(F.col("_count") > 1) \
    .select(*composite_key, "_count")

display(dupe_keys)
