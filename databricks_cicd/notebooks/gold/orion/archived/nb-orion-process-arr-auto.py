# Databricks notebook source
# MAGIC %run ../nb-orion-common

# COMMAND ----------

# Importing Libraries
import os
import re

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lower, when, levenshtein, lit, length, greatest,concat, row_number, format_number, sum, date_format
from datetime import datetime, date
from dateutil.relativedelta import relativedelta


# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

!pip install openpyxl --quiet

# COMMAND ----------

# %restart_python

# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT DISTINCT 
# MAGIC     sl.No_ as sku,
# MAGIC     sl.ShortcutDimension1Code,
# MAGIC     sum(case when sih.CurrencyFactor > 0 then sl.Amount/sih.CurrencyFactor else sl.Amount end) as SalesLCYLTM,
# MAGIC     Gen_Prod_PostingGroup
# MAGIC   FROM silver_{ENVIRONMENT}.igsql03.sales_invoice_line sl
# MAGIC   INNER JOIN silver_{ENVIRONMENT}.igsql03.sales_invoice_header sih
# MAGIC     ON sl.DocumentNo_ = sih.No_ 
# MAGIC   WHERE sih.PostingDate BETWEEN '2024-05-01' AND '2024-05-31'
# MAGIC   and sl.Sys_Silver_IsCurrent =1
# MAGIC   and sih.Sys_Silver_IsCurrent =1
# MAGIC   GROUP BY all

# COMMAND ----------

# DBTITLE 1,load from igsql03 silver layer
from pyspark.sql import functions as F
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import os

def get_sales_analysis(year_month=None):
    """
    Analyze sales data for a specific year-month or default to last month.
    
    Parameters:
    year_month: str, optional - Format 'YYYY-MM' (e.g., '2024-05'). If None, uses last month
    
    Returns:
    pyspark.sql.DataFrame: Analysis results
    """ 
    # Define base table path
    BASE_PATH = f"silver_{ENVIRONMENT}.igsql03"
    
    # If no year_month provided, use last month
    if year_month is None:
        today = date.today()
        first_of_this_month = date(today.year, today.month, 1)
        last_month_date = first_of_this_month - relativedelta(months=1)
        year_month = last_month_date.strftime('%Y-%m')
    
    # Convert year_month to start and end dates
    start_date = f"{year_month}-01"
    end_date = (datetime.strptime(start_date, '%Y-%m-%d') + relativedelta(months=1) - relativedelta(days=1)).strftime('%Y-%m-%d')
    
    # Read tables with dynamic environment
    invoice_lines = spark.table(f"{BASE_PATH}.sales_invoice_line").alias("invoice_lines")
    invoice_headers = spark.table(f"{BASE_PATH}.sales_invoice_header").alias("invoice_headers")
    cr_memo_lines = spark.table(f"{BASE_PATH}.sales_cr_memo_line").alias("cr_memo_lines")
    cr_memo_headers = spark.table(f"{BASE_PATH}.sales_cr_memo_header").alias("cr_memo_headers")
    items = spark.table(f"{BASE_PATH}.item").alias("items")
    
    # Process invoice sales
    invoice_sales = (
        invoice_lines
        .join(invoice_headers, 
              invoice_lines["DocumentNo_"] == invoice_headers["No_"])
        .where((F.col("invoice_headers.PostingDate").between(start_date, end_date)) &
               (F.col("invoice_lines.Sys_Silver_IsCurrent") == 1) &
               (F.col("invoice_headers.Sys_Silver_IsCurrent") == 1))
        .select(
            F.col("invoice_lines.No_").alias("sku"),
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
        .join(cr_memo_headers, 
              cr_memo_lines["DocumentNo_"] == cr_memo_headers["No_"])
        .where((F.col("cr_memo_headers.PostingDate").between(start_date, end_date)) &
               (F.col("cr_memo_lines.Sys_Silver_IsCurrent") == 1) &
               (F.col("cr_memo_headers.Sys_Silver_IsCurrent") == 1))
        .select(
            F.col("cr_memo_lines.No_").alias("sku"),
            F.col("cr_memo_lines.ShortcutDimension1Code"),
            (F.when(F.col("cr_memo_headers.CurrencyFactor") > 0,
                   F.col("cr_memo_lines.Amount") / F.col("cr_memo_headers.CurrencyFactor"))
             .otherwise(F.col("cr_memo_lines.Amount")) * -1)
             .alias("SalesLCYLTM")
        )
    )
    
    # Union invoice and credit memo sales
    sales_ltm = invoice_sales.union(cr_memo_sales)
    
    # Final aggregation with items join
    result = (
        sales_ltm
        .groupBy("sku", "ShortcutDimension1Code")
        .agg(F.sum("SalesLCYLTM").alias("SalesLCYLTM"))
        .join(items, F.col("sku") == F.col("items.No_"), "left")
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
            F.col("SalesLCYLTM")
        )
        .where(F.col("SalesLCYLTM") != 0)
    )
    result = result.replace({'NaN': None})
    return result

# COMMAND ----------


df_oct_2024 = get_sales_analysis("2024-10")

# COMMAND ----------

display(df_oct_2024)

# COMMAND ----------

df_vendor_master = spark.table(f"silver_{ENVIRONMENT}.masterdata.vendor_mapping").alias("vm").filter(F.col("sys_silver_iscurrent") == True)


# COMMAND ----------

from pyspark.sql.window import Window

df_vendor_master_unique = (
    df_vendor_master
    .withColumn('row_num', F.row_number().over(
        Window.partitionBy('VendorCode').orderBy(F.desc('Sys_Bronze_InsertDateTime_UTC'))
    ))
    .withColumn('occurrence_count', F.count('*').over(Window.partitionBy('VendorCode')))
)

# COMMAND ----------

# First get records with duplicates
result = (
   df_oct_2024
   .join(
       df_vendor_master_unique.filter(F.col('occurrence_count') > 1),
       df_oct_2024['VendorCodeItem'] == df_vendor_master_unique['VendorCode'], 
       'left'
   )
)

display(result)

# COMMAND ----------

# only pick the first unique vendor group
df_arr_auto = (
   df_oct_2024
   .join(
       df_vendor_master_unique.filter(F.col('row_num') == 1),
       df_oct_2024['VendorCodeItem'] == df_vendor_master_unique['VendorCode'],
       'left'
   ).select(
       df_oct_2024['*'],  # All columns from df_oct_2024
       df_vendor_master_unique['sid'].alias('vendor_group_id'),  # Renamed sid to vendor_group_id
       df_vendor_master_unique['VendorGroup']  # VendorGroup from vendor master
   )
)

# Reorder columns more elegantly
columns = df_arr_auto.columns
desired_order = ['sku', 'VendorCodeItem', 'VendorGroup'] + [col for col in columns if col not in ['sku', 'VendorCodeItem', 'VendorGroup']]
df_arr_auto = df_arr_auto.select(desired_order)

display(df_arr_auto)

# COMMAND ----------

# DBTITLE 1,load from file

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
  .saveAsTable(f"{catalog}.{schema}.pierre_arr_0")

# COMMAND ----------


# File path in DBFS
file_path = "/Workspace/Users/akhtar.miah@infinigate.com/2024_05_incremental.xlsx"

pandas_df = pd.read_excel(file_path)

# COMMAND ----------

# Convert to PySpark DataFrame
df_source = spark.createDataFrame(pandas_df)


# display(df_source)


# COMMAND ----------

def check_duplicate_keys(df, key_cols=["Manufacturer Item No_", "Consolidated Vendor Name"]):
    """
    Check for duplicate composite keys and provide detailed analysis
    
    Parameters:
    df: DataFrame to check
    key_cols: List of column names that form the composite key
    """
    # Get total record count
    total_records = df.count()
    
    # Count duplicates
    dupe_counts = df.groupBy(*key_cols) \
        .agg(F.count("*").alias("dupe_count"))
    
    # Get records with duplicates
    dupe_keys = dupe_counts \
        .filter(F.col("dupe_count") > 1) \
        .select(*key_cols, "dupe_count")
        
    # Join back to get full records with duplicates
    dupes_with_details = df.join(dupe_keys, key_cols) \
        .orderBy(*key_cols)
    
    # Get summary statistics
    num_unique_keys = dupe_counts.count()
    num_duplicate_keys = dupe_keys.count()
    
    # Print summary
    print(f"\nDuplicate Key Analysis:")
    print(f"Total Records: {total_records}")
    print(f"Unique Keys: {num_unique_keys}")
    print(f"Keys with Duplicates: {num_duplicate_keys}")
    
    if num_duplicate_keys > 0:
        # Show distribution of duplicate counts
        print("\nDistribution of Duplicates:")
        dupe_distribution = dupe_counts \
            .filter(F.col("dupe_count") > 1) \
            .groupBy("dupe_count") \
            .agg(F.count("*").alias("frequency")) \
            .orderBy("dupe_count")
        dupe_distribution.display()
        
        # Show the actual duplicate records
        print("\nDetailed Duplicate Records:")
        dupes_with_details.display()
        
        # Optional: Save to a table
        # dupes_with_details.write.mode("overwrite").saveAsTable("duplicate_records")
        
    return dupes_with_details

# Example usage:
duplicate_records = check_duplicate_keys(df_source)

# Optional: Get specific duplicates for certain key values
def get_specific_duplicates(df, manufacturer_item, vendor_name):
    """
    Get duplicate records for a specific key combination
    """
    return df.filter(
        (F.col("Manufacturer Item No_") == manufacturer_item) & 
        (F.col("Consolidated Vendor Name") == vendor_name)
    ).orderBy("Manufacturer Item No_", "Consolidated Vendor Name")

# Example:
# specific_dupes = get_specific_duplicates(duplicate_records, "SKU123", "Vendor1")
# specific_dupes.display()

# COMMAND ----------

# check if description field is different for skus
description_counts = df_source.groupBy("Manufacturer Item No_") \
    .agg(F.countDistinct("Description").alias("description_count"))

# Get SKUs with multiple descriptions (description_count > 1)
skus_with_diff_descriptions = description_counts \
    .filter(F.col("description_count") > 1) \
    .select("Manufacturer Item No_")

# Join back to original DataFrame to get all fields for these SKUs
result = df_source.join(skus_with_diff_descriptions, "Manufacturer Item No_")

# Sort the results by SKU and Description for better readability
result = result.orderBy("Manufacturer Item No_", "Description")

# Show the results
result.display()

# COMMAND ----------

# DBTITLE 1,common transformations

from pyspark.sql import functions as F

# Define unique records
from typing import List
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

from typing import List
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, count

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
        F.when(F.col("Life Cycle Formula").isNotNull(),
               F.regexp_replace(F.col("Life Cycle Formula"), "_x0005_", "").cast("int"))
         .otherwise(0)
    )


# Define the transformation logic
def default_columns(df):
    return df.withColumn("match_type", F.lit("")) \
    .withColumn("duration", F.lit("Not Assigned")) \
    .withColumn("frequency", F.lit("Not Assigned")) \
    .withColumn("Consumption", F.lit("Not Assigned")) \
    .withColumn("Type", F.lit("Not Assigned")) \
    .withColumn("Mapping_type_Duration", F.lit("Not Assigned")) \
    .withColumn("Mapping_type_Billing", F.lit("Not Assigned")) \
    .withColumn("MRRratio", F.lit(0)) \
    .withColumn("months", F.lit(0)) \
    .withColumn("description", F.concat_ws(" ", F.col("Description"), F.col("Description 2"),
                            F.col("Description 3"), F.col("Description 4")))


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
                F.when((F.col("Product Type") == "Software") & F.col("duration").contains("YR"), "SW Subscription")
                 .when((F.col("Product Type") == "Software") & F.col("duration").contains("M"), "SW Subscription")
                 .when(F.col("Product Type") == "Professional Service", "Professional services"))

    
    # A10Networks-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "A10Networks") & 
            (F.col("Manufacturer Item No_") == "GOLD SUPPORT 4 YEAR"), 
            "4 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("Consolidated Vendor Name") == "A10Networks") & 
            (F.col("Manufacturer Item No_") == "GOLD SUPPORT 4 YEAR"), 
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("Consolidated Vendor Name") == "A10Networks") & 
            (F.col("Manufacturer Item No_") == "GOLD SUPPORT 4 YEAR"), 
            "Capacity"
        ).otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "A10Networks") & 
            (F.col("Manufacturer Item No_") == "GOLD SUPPORT 4 YEAR"), 
            "Vendor support"
        ).otherwise(F.col("Type"))
    )

    # Arbor Networks-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arbor Networks") & 
            F.col("Manufacturer Item No_").contains("MNT-"), 
            "Upfront"
        ).when(
            F.col("Consolidated Vendor Name") == "Arbor Networks", 
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arbor Networks") & 
            F.col("Manufacturer Item No_").contains("MNT-"), 
            "Capacity"
        ).when(
            F.col("Consolidated Vendor Name") == "Arbor Networks", 
            "Capacity"
        ).otherwise(F.col("Consumption"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            F.col("Consolidated Vendor Name") == "Arbor Networks", 
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arbor Networks") & 
            F.col("Manufacturer Item No_").contains("MNT-"), 
            "Vendor support"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arbor Networks") & 
            F.col("Manufacturer Item No_").contains("-2YR"), 
            "2 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arbor Networks") & 
            F.col("Manufacturer Item No_").contains("-3YR"), 
            "3 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arbor Networks") & 
            F.col("Manufacturer Item No_").contains("-4YR"), 
            "4 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arbor Networks") & 
            F.col("Manufacturer Item No_").contains("-5YR"), 
            "5 YR"
        ).when(
            F.col("Consolidated Vendor Name") == "Arbor Networks", 
            "1 YR"
        ).otherwise(F.col("duration"))
    )

    # Fortinet-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "Fortinet", 
            F.when(F.col("Manufacturer Item No_").rlike(r".*-02-24"), "2 YR")
                .when(F.col("Manufacturer Item No_").rlike(r".*-02-12"), "1 YR")
                .when(F.col("Manufacturer Item No_").rlike(r".*-02-36"), "3 YR")
                .when(F.col("Manufacturer Item No_").rlike(r".*-02-48"), "4 YR")
                .when(F.col("Manufacturer Item No_").rlike(r".*-02-60"), "5 YR")
                .otherwise("Perpetual"))
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "Fortinet", 
            F.when(F.col("duration").contains("YR"), "Sure Mapping")
                .otherwise("Other Mapping"))
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Fortinet", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Fortinet", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Fortinet", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "Fortinet", 
            F.when(F.col("Manufacturer Item No_").rlike(r".*-247-02-.*|.*-928-02-.*|.*-950-02-.*|.*-963-02-.*|.*-248-02-.*|.*-936-02-.*|.*-211-02-.*|.*-809-02-.*|.*-258-02-.*|.*-916-02-.*|.*-314-02-.*|.*-585-02-.*|.*-812-02-.*"), "Vendor support")
                .when(F.col("Manufacturer Item No_").rlike(r".*-108-02-.*|.*-131-02-.*|.*-189-02-.*|.*-651-02-.*|.*-159-02-.*|.*-647-02-.*|.*-423-02-.*|.*-160-02-.*"), "SW Subscription")
                .when(F.col("Manufacturer Item No_").rlike(r".*-714-02-.*"), "Professional services")
                .when(F.col("Manufacturer Item No_").rlike(r"FTM-ELIC-.*|.*-VM-BASE"), "SW Perpetual")
                .otherwise(F.col("Type")))
        .otherwise(F.col("Type"))
    )

    # Checkpoint-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(
            F.col("Consolidated Vendor Name") == "Checkpoint", 
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(
            F.col("Consolidated Vendor Name") == "Checkpoint", 
            "Capacity"
        ).otherwise(F.col("Consumption"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            F.col("Consolidated Vendor Name") == "Checkpoint", 
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").rlike("CPAP|CPAC")), 
            "Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").contains("-2Y")), 
            "2 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").contains("-3Y")), 
            "3 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").contains("-4Y")), 
            "4 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").contains("-5Y")), 
            "5 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").contains("-1Y")), 
            "1 YR"
        ).when(
            F.col("Consolidated Vendor Name") == "Checkpoint", 
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").rlike("CPAP|CPAC")), 
            "Sure Mapping"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").rlike("-[1-5]Y")), 
            "Sure Mapping"
        ).when(
            F.col("Consolidated Vendor Name") == "Checkpoint", 
            "Other Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").rlike("CPAP|CPAC")), 
            "Hardware"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Manufacturer Item No_").rlike("CPES|CPCES")), 
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Product Type") == "Support"), 
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Product Type") == "Courseware"), 
            "Training"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Product Type") == "Professional Service"), 
            "Professional services"
        ).when(
            (F.col("Consolidated Vendor Name") == "Checkpoint") & 
            (F.col("Product Type") == "Software") & 
            (F.col("duration").contains("YR")), 
            "SW Subscription"
        ).when(
            F.col("Consolidated Vendor Name") == "Checkpoint", 
            "SW Perpetual"
        ).otherwise(F.col("Type"))
    )

    # Sophos-Specific Transformations
    df = df.withColumn(
        "months",
        F.when((F.col("Consolidated Vendor Name") == "Sophos") & (F.col("Life Cycle Formula").isNotNull()),
            F.regexp_replace(F.col("Life Cycle Formula"), "_x0005_", "").cast("int"))
        .otherwise(F.col("months"))
    ).withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "Sophos",
            F.when(F.col("months") > 1, F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR")))
                .when(F.col("months") == 1, "1M")
                .otherwise("Perpetual"))
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "Sophos",
            F.when(F.col("months") > 1, "Sure Mapping")
                .when(F.col("months") == 1, "Sure Mapping")
                .otherwise("Other Mapping"))
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Sophos",
            F.when(F.col("months") > 1, "Upfront")
                .when(F.col("months") == 1, "Monthly")
                .otherwise("Upfront"))
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Sophos",
            F.when(F.col("months") == 1, "Flexible")
                .otherwise("Capacity"))
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "Sophos",
            F.when((F.col("Product Type") == "Software") & F.col("duration").contains("YR"), "SW Subscription")
                .when((F.col("Product Type") == "Software") & F.col("duration").contains("M"), "SW Subscription")
                .when(F.col("Product Type") == "Professional Service", "Professional services"))
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("Consolidated Vendor Name") == "Sophos") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    )

    ### Watchguard-Specific Transformations ###
    df = df.withColumn(
        "duration",
        F.when((F.col("Consolidated Vendor Name") == "Watchguard") & (F.col("Description").rlike(r"(?i).*1-Year.*|.*1 -Year.*|.*1 Year.*|.*1-yr.*")), "1 YR")
         .when((F.col("Consolidated Vendor Name") == "Watchguard") & (F.col("Description").rlike(r"(?i).*3-Year.*|.*3 Year.*|.*3-yr.*|.*3 -Year.*")), "3 YR")
         .when((F.col("Consolidated Vendor Name") == "Watchguard") & (F.col("Description").rlike(r"(?i).*FireboxV.*MSSP Appliance.*")), "3 YR")
         .when((F.col("Consolidated Vendor Name") == "Watchguard") & (F.col("Description").rlike(r"(?i).*IPSec VPN Client .*")), "Perpetual")
         .otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when((F.col("Consolidated Vendor Name") == "Watchguard") & (F.col("Description").rlike(r"(?i).*Total Security Suite.*|.*Standard Support.*|.*Basic Security Suite.*")), "Vendor support")
         .when((F.col("Consolidated Vendor Name") == "Watchguard") & (F.col("Description").rlike(r"(?i).*Panda Endpoint Protection Plus.*")), "SW Subscription")
         .when((F.col("Consolidated Vendor Name") == "Watchguard") & (F.col("Description").rlike(r"(?i).*VPN Client.*")), "SW Perpetual")
         .otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when((F.col("Consolidated Vendor Name") == "Watchguard") & F.col("Manufacturer Item No_").rlike(r"WG\d{4}|WGT49023-EU"), "Perpetual")
         .otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when((F.col("Consolidated Vendor Name") == "Watchguard") & F.col("Manufacturer Item No_").rlike(r"WG\d{4}|WGT49023-EU"), "Hardware")
         .otherwise(F.col("Type"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Watchguard", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Watchguard", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Watchguard", "Capacity").otherwise(F.col("Consumption"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("Consolidated Vendor Name") == "Watchguard") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
         .otherwise(F.col("Mapping_type_Duration"))
    )

    # Hardware transformation for Watchguard
    df = df.withColumn("Type",
            F.when((F.col("Consolidated Vendor Name") == "Watchguard") & F.col("Manufacturer Item No_").rlike("WG\\d{4}|WGT49023-EU"), "Hardware")
             .otherwise(F.col("Type"))
    )

    # DDN-specific transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "DDN") & F.col("Manufacturer Item No_").rlike(r"SUP-.*-(\d+)YR"),
            F.concat(F.regexp_extract(F.col("Manufacturer Item No_"), r"SUP-.*-(\d+)YR", 1), F.lit(" YR"))
        ).when(
            (F.col("Consolidated Vendor Name") == "DDN") & F.col("Manufacturer Item No_").rlike(r"REINSTATE-BASIC-VM"),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "DDN") & F.col("Manufacturer Item No_").rlike(r"SUP-.*"),
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "DDN") & F.col("Manufacturer Item No_").rlike(r"REINSTATE-BASIC-VM"),
            "SW Perpetual"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "DDN", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "DDN", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "DDN", "Capacity").otherwise(F.col("Consumption"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "DDN") & (F.col("duration")!='Not Assigned'),
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
            F.when((F.col("Consolidated Vendor Name") == "Entrust") & F.col("Manufacturer Item No_").rlike(pattern), duration_val)
             .otherwise(F.col("duration"))
        ).withColumn(
            "Type",
            F.when((F.col("Consolidated Vendor Name") == "Entrust") & F.col("Manufacturer Item No_").rlike(pattern), type_val)
             .otherwise(F.col("Type"))
        )

    # Additional Entrust defaults
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Entrust", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Entrust", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Entrust", "Capacity").otherwise(F.col("Consumption"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("Consolidated Vendor Name") == "Entrust") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
         .otherwise(F.col("Mapping_type_Duration"))
    )


    # Juniper-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^PAR-.*|^SVC-.*"),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^SUB-.*-(\d+)Y.*"),
            F.concat(F.regexp_extract(F.col("Manufacturer Item No_"), r"^SUB-.*-(\d+)Y.*", 1), F.lit(" YR"))
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^S-.*-P|JS-NETDIR-10|JS-SECDIR-10|ME-VM-OC-PROXY|ME-ADV-XCH-WW|EX4650-PFL"),
            "Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^S-.*-3"),
            "3 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^S-.*-1"),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^S-.*-5"),
            "5 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^JUNIPER-RENEWAL"),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & (F.col("Description").rlike(r"(?i).*1-Year subscr LIC.*")),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"PD-9001GR-AT-AC|EX4100-F-12-RME|SRX320-RMK0|EX-4PST-RMK|SRX320-WALL-KIT0|CBL-JNP-SG4-EU|CBL-PWR-10AC-STR-EU"),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^PAR-.*|^SVC-.*|JUNIPER-RENEWAL"),
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^SUB-.*|^S-.*-(\d+)|.*1-Year subscr LIC.*"),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"^S-.*-P|JS-NETDIR-10|JS-SECDIR-10|ME-VM-OC-PROXY|ME-ADV-XCH-WW|EX4650-PFL"),
            "SW Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"PD-9001GR-AT-AC|EX4100-F-12-RME|SRX320-RMK0|EX-4PST-RMK|SRX320-WALL-KIT0|CBL-JNP-SG4-EU|CBL-PWR-10AC-STR-EU"),
            "Hardware"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Juniper", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Juniper", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Juniper", "Capacity").otherwise(F.col("Consumption"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Juniper") & (F.col("duration") == "1 YR") & (F.col("Manufacturer Item No_").rlike(r'^JUNIPER-RENEWAL|^PAR-.*|^SVC-.*')), "Other Mapping")
        .when((F.col("Consolidated Vendor Name") == "Juniper") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .when(
            (F.col("Consolidated Vendor Name") == "Juniper") & F.col("Manufacturer Item No_").rlike(r"PD-9001GR-AT-AC|EX4100-F-12-RME|SRX320-RMK0|EX-4PST-RMK|SRX320-WALL-KIT0|CBL-JNP-SG4-EU|CBL-PWR-10AC-STR-EU"),
            "Sure mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    )

    # Extreme Networks-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Extreme Networks") & F.col("Manufacturer Item No_").rlike(r"9.*"),
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "Extreme Networks") & F.col("Manufacturer Item No_").rlike(r"5000-MACSEC-LIC-P"),
            "SW Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "Extreme Networks") & F.col("Manufacturer Item No_").rlike(r"XIQ-PIL-S-C-PWP-DELAY|TR-EVENT-PASS-USER-CONF"),
            "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Extreme Networks") & F.col("Manufacturer Item No_").rlike(r"9.*") & (F.col("Description").rlike(r"(?i).*12 Months.*")),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Extreme Networks") & F.col("Manufacturer Item No_").rlike(r"9.*") & (F.col("Description").rlike(r"(?i).*36 Months.*")),
            "3 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Extreme Networks") & F.col("Manufacturer Item No_").rlike(r"9.*") & (F.col("Description").rlike(r"(?i).*60 Months.*")),
            "5 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Extreme Networks") & F.col("Manufacturer Item No_").rlike(r"5000-MACSEC-LIC-P"),
            "Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "Extreme Networks") & F.col("Manufacturer Item No_").rlike(r"XIQ-PIL-S-C-PWP-DELAY|TR-EVENT-PASS-USER-CONF"),
            "1 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            ((F.col("Consolidated Vendor Name") == "Extreme Networks") & (F.col("duration")!='Not Assigned')),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Extreme Networks", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Extreme Networks", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Extreme Networks", "Capacity").otherwise(F.col("Consumption"))
    )

    # Riverbed-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Riverbed") & F.col("Manufacturer Item No_").rlike(r"(?i).*MNT-.*"),
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "Riverbed") & F.col("Manufacturer Item No_").rlike(r"^LIC-.*|^ATNY-.*"),
            "SW Perpetual"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Riverbed") & F.col("Manufacturer Item No_").rlike(r"(?i).*MNT-.*"),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Riverbed") & F.col("Manufacturer Item No_").rlike(r"^LIC-.*|^ATNY-.*"),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Riverbed") & F.col("Manufacturer Item No_").rlike(r"(?i).*MNT-.*"),
            "Other Mapping"
        ).when(
            (F.col("Consolidated Vendor Name") == "Riverbed") & F.col("Manufacturer Item No_").rlike(r"^LIC-.*|^ATNY-.*"),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Riverbed", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Riverbed", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Riverbed", "Capacity").otherwise(F.col("Consumption"))
    )

    # WithSecure-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("Description").rlike(r"(?i).*3 years.*")),
            "3 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("Description").rlike(r"(?i).*1 year.*")),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("Description").rlike(r"(?i).*2 year.*")),
            "2 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("Description").rlike(r"(?i).*5 year.*")),
            "5 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("Consolidated Vendor Name") == "WithSecure") & (F.col("duration")!='Not Assigned'),
            "Capacity"
        ).otherwise(F.col("Consumption"))
    )

    # Acronis-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Acronis") & 
            (F.col("description").rlike(".*3 years.*|.*3 Year.*")),
            "3 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Acronis") & 
            (F.col("description").rlike(".*1 year.*|.*1 Year.*")),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Acronis") & 
            (F.col("description").rlike(".*2 year.*")),
            "2 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Acronis") & 
            (F.col("description").rlike(".*5 year.*|.*5 Year.*")),
            "5 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Acronis") & 
            (F.col("description").rlike(".*Physical Data Shipping to Cloud.*")),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when((F.col("Consolidated Vendor Name") == "Acronis") & (F.col("duration")=='Perpetual'),
            "SW Perpetual"
        ).when((F.col("Consolidated Vendor Name") == "Acronis") & (F.col("duration")!='Not Assigned'),
           "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("Consolidated Vendor Name") == "Acronis") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("Consolidated Vendor Name") == "Acronis") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("Consolidated Vendor Name") == "Acronis") & (F.col("duration")!='Not Assigned'),"Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when((F.col("Consolidated Vendor Name") == "Acronis") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("Consumption"))
    )

    # Arcserve-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") > 1)
            , F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR"))
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") == 1),
            "1M"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") == 0),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") > 0),
            "Sure Mapping"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") == 0),
            "Other Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") > 1),
            "Upfront"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") == 1),
            "Monthly"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") == 0),
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            F.col("Consolidated Vendor Name") == "Arcserve",
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") > 1),
            "Capacity"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") == 1),
            "Flexible"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("months") == 0),
            "Capacity"
        ).otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("Product Type") == "Software") & F.col("duration").contains("YR"),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("Product Type") == "Managed Services") & F.col("duration").contains("YR"),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("Product Type") == "Software") & F.col("duration").contains("M"),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("Product Type") == "Software"),
            "SW Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "Arcserve") & (F.col("Product Type") == "Professional Service"),
            "Professional services"
        ).otherwise(F.col("Type"))
    )

    # Barracuda-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") > 1)
            , F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR"))
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") == 1),
            "1M"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") == 0),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") > 0),
            "Sure Mapping"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") == 0),
            "Other Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") > 1),
            "Upfront"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") == 1),
            "Monthly"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") == 0),
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            F.col("Consolidated Vendor Name") == "Barracuda",
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") > 1),
            "Capacity"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") == 1),
            "Flexible"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("months") == 0),
            "Capacity"
        ).otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("Product Type") == "Software") & F.col("duration").contains("YR"),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("Product Type") == "Managed Services") & F.col("duration").contains("YR"),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("Product Type") == "Software") & F.col("duration").contains("M"),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("Product Type") == "Software"),
            "SW Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "Barracuda") & (F.col("Product Type") == "Professional Service"),
            "Professional services"
        ).otherwise(F.col("Type"))
    )

  

    # Bitdefender-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("months").cast("int") > 1),
            F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR"))
        ).when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("months").cast("int") == 1),
            "1M"
        ).when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("months").cast("int") == 0),
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "Bitdefender", 
            F.when(F.col("duration") == "Perpetual", "Other Mapping")
            .otherwise("Sure Mapping"))
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("duration") == "1M"),
            "Monthly"
        ).when(
            F.col("Consolidated Vendor Name") == "Bitdefender",
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Bitdefender", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("duration") == "1M"),
            "Flexible"
        ).when(
            F.col("Consolidated Vendor Name") == "Bitdefender",
            "Capacity"
        ).otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("Product Type") == "Software") & 
            (F.col("duration").rlike("YR")),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("Product Type") == "Extras") &
            (F.col("duration").rlike("YR")),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("Product Type") == "Software") &
            (F.col("duration") == "1M"),
            "SW Subscription"
        ).when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("Product Type") == "Software"),
            "SW Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "Bitdefender") &
            (F.col("Product Type") == "Professional Service"),
            "Professional services"
        ).otherwise(F.col("Type"))
    )

    # SonicWall-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "SonicWall") &
            (F.col("description").rlike(r"(?i).*SUPPORT.*3YR.*|.*Support.*3 Years.*")),
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "SonicWall") &
            (F.col("description").rlike(r"(?i).*SUPPORT.*1YR.*|.*Support.*1 Year.*")),
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "SonicWall") &
            (F.col("description").rlike(r"(?i).*STATEFUL HA UPGRADE FOR.*|.*Virtual Appliance.*")),
            "SW Perpetual"
        ).when(
            (F.col("Consolidated Vendor Name") == "SonicWall"),
            "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "SonicWall", 
            F.when(F.col("description").rlike(r"(?i).*3YR.*|.*3 Years.*"), "3 YR")
            .when(F.col("description").rlike(r"(?i).*1YR.*|.*1 Year.*"), "1 YR")
            .when(F.col("description").rlike(r"(?i).*2 Year.*"), "2 YR")
            .when(F.col("description").rlike(r"(?i).*4 Year.*"), "4 YR")
            .when(F.col("description").rlike(r"(?i).*5 Year.*|.*5 YR.*"), "5 YR")
            .when(F.col("description").rlike(r"(?i).*STATEFUL HA UPGRADE FOR.*|.*Virtual Appliance.*"), "Perpetual")
            .otherwise(F.col("duration"))).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", F.when((F.col("Consolidated Vendor Name") == "SonicWall") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("Consolidated Vendor Name") == "SonicWall", "Upfront")
                    .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("Consolidated Vendor Name") == "SonicWall", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("Consolidated Vendor Name") == "SonicWall", "Capacity")
                        .otherwise(F.col("Consumption"))
    )

    # Armis-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "Armis", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "Armis", "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "Armis", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Armis", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Armis", "Other Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Armis", "Capacity")
        .otherwise(F.col("Consumption"))
    )

    # Blackberry-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "Blackberry", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "Blackberry", "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "Blackberry", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Blackberry", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Blackberry", "Other Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "Blackberry", "Capacity")
        .otherwise(F.col("Consumption"))
    )

    # Cambium-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "Cambium") &
            (F.col("Manufacturer Item No_").rlike(r"(?i).*RNW-1.*")),
            "Vendor support"
        ).when(
            (F.col("Consolidated Vendor Name") == "Cambium") &
            (F.col("Manufacturer Item No_").rlike(r"(?i).*SUB-.*-3")),
            "SW Subscription"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "Cambium") &
            (F.col("Manufacturer Item No_").rlike(r"(?i).*RNW-1.*")),
            "1 YR"
        ).when(
            (F.col("Consolidated Vendor Name") == "Cambium") &
            (F.col("Manufacturer Item No_").rlike(r"(?i).*SUB-.*-3")),
            "3 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", F.when(F.col("Consolidated Vendor Name") == "Cambium", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("Consolidated Vendor Name") == "Cambium", "Upfront")
                    .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("Consolidated Vendor Name") == "Cambium", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("Consolidated Vendor Name") == "Cambium", "Capacity")
                        .otherwise(F.col("Consumption"))
    )

    # CarbonBlack-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(
            (F.col("Consolidated Vendor Name") == "CarbonBlack") &
            (F.col("Manufacturer Item No_").rlike(r"(?i).*-1Y-EU-R-.*")),
            "Vendor support"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(
            (F.col("Consolidated Vendor Name") == "CarbonBlack") &
            (F.col("Manufacturer Item No_").rlike(r"(?i).*-1Y-EU-R-.*")),
            "1 YR"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", F.when(F.col("Consolidated Vendor Name") == "CarbonBlack", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("Consolidated Vendor Name") == "CarbonBlack", "Upfront")
                    .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("Consolidated Vendor Name") == "CarbonBlack", "Sure Mapping")
                                .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("Consolidated Vendor Name") == "CarbonBlack", "Capacity")
                        .otherwise(F.col("Consumption"))
    )

    # Cato Networks-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "Cato Networks",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*"), "SW Subscription")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*CATO-DR-KIT.*|.*CATO-DEPLOYMENT-FEE.*"), "Hardware")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*CAN-RENEW-QTR.*"), "Vendor Support")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "SW Subscription")
            .otherwise(F.col("Type"))
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "Cato Networks",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*|.*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "3 YR")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*CAN-RENEW-QTR.*"), "3M")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*CATO-DR-KIT.*|.*CATO-DEPLOYMENT-FEE.*"), "Perpetual")
            .otherwise(F.col("duration"))
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "Cato Networks",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*|.*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "Ratio mapping")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*CAN-RENEW-QTR.*|.*CATO-DR-KIT.*|.*CATO-DEPLOYMENT-FEE.*"), "Other Mapping")
            .otherwise(F.col("Mapping_type_Duration"))
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Cato Networks",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*|.*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "Monthly")
            .otherwise(lit("Upfront"))
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Cato Networks",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*CATO-SUBSCRIPTION.*|.*CATO-SITES.*|.*CATO-SDP-USERS.*|.*CATO-SN-SDP.*|.*CATO INTRUSION PREVENTION SYSTEM.*|.*CATO-NEXTGEN-ANTI-MALWARE.*|.*CATO-SSE-SITES.*|.*CATO-IPS.*|.*CATO-ANTI-MALWARE.*|.*CATO-CASB.*|.*CATO-REMOTE-BROWSER.*|.*CATO SDP USERS.*|.*CATO-SN-EUR-100MBPS-R.*"), "Other Mapping")
            .otherwise("Sure Mapping")
        ).otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("Consolidated Vendor Name") == "Cato Networks", "Capacity").otherwise(F.col("Consumption"))
    )


    # conpal-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "conpal",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*USC-12.*|.*USC-24.*|.*USC-36.*"), "Vendor Support")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*LIC-PP.*"), "SW Perpetual")
            .otherwise(F.col("Type"))
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "conpal",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*USC-12.*"), "1 YR")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*USC-24.*"), "2 YR")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*USC-36.*"), "3 YR")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*LIC-PP.*"), "Perpetual")
            .when(F.col("Manufacturer Item No_").rlike(r"(?i).*USC-01.*"), "1 M")
            .otherwise(F.col("duration"))
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "conpal", "Sure Mapping").otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "conpal",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*USC-01.*"), "Monthly")
            .otherwise(lit("Upfront"))
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "conpal", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "conpal",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*USC-01.*"), "Flexible")
            .otherwise("Capacity")
        ).otherwise(F.col("Consumption"))
    )

    # CloudFlare-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "CloudFlare",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*CLOUDFLARE-CONTRACT.*"), "SW Subscription")
            .otherwise(F.col("Type"))
        ).otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "CloudFlare",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*CLOUDFLARE-CONTRACT.*"), "Not Assigned")
            .otherwise(F.col("duration"))
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "CloudFlare", "Other Mapping").otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("Consolidated Vendor Name") == "CloudFlare", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("Consolidated Vendor Name") == "CloudFlare", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("Consolidated Vendor Name") == "CloudFlare", "Capacity").otherwise(F.col("Consumption"))
    )

    # CyberArk-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "CyberArk",
            F.when(F.col("Manufacturer Item No_").rlike(r"(?i).*EPM-TARGET-WRK-SAAS.*"), "Not Assigned")
            .otherwise(F.col("duration"))
        ).otherwise(F.col("duration"))
    ).withColumn(
        "Type",
        F.when((F.col("Consolidated Vendor Name") == "CyberArk") & (F.col("duration")!='Not Assigned'), "SW Subscription").otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("Consolidated Vendor Name") == "CyberArk") & (F.col("duration")!='Not Assigned'), "Other Mapping").otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", F.when(F.col("Consolidated Vendor Name") == "CyberArk", "Upfront").otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", F.when(F.col("Consolidated Vendor Name") == "CyberArk", "Sure Mapping").otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", F.when(F.col("Consolidated Vendor Name") == "CyberArk", "Capacity").otherwise(F.col("Consumption"))
    )

    # Cybereason-Specific Transformations
    df = df.withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "Cybereason", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Cybereason", "Monthly")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("Consolidated Vendor Name") == "Cybereason", "Other Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "duration", 
        F.when(F.col("Consolidated Vendor Name") == "Cybereason", "2.26 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "Cybereason", "Ratio mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Cybereason", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Cybereason") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TS-.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TRI-.*')), "Professional Service")
        .otherwise(F.col("Type"))
    ).withColumn(
        "frequency", 
        F.when((F.col("Consolidated Vendor Name") == "Cybereason") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TS-.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TRI-.*')), lit("Upfront"))
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("Consolidated Vendor Name") == "Cybereason") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TS-.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TRI-.*')), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Cybereason") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TS-.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TRI-.*')), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Cybereason") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TS-.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*CR-TRI-.*')), "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Deepinstinct-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Deepinstinct", "Monthly")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("Consolidated Vendor Name") == "Deepinstinct", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Deepinstinct", "Flexible")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("Consolidated Vendor Name") == "Deepinstinct", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("Consolidated Vendor Name") == "Deepinstinct", "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "Deepinstinct", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", 
        F.when((F.col("Consolidated Vendor Name") == "Deepinstinct") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), lit("Monthly"))
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("Consolidated Vendor Name") == "Deepinstinct") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "Deepinstinct") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "Flexible")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Deepinstinct") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Deepinstinct") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Deepinstinct") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-STD.*') | F.col("Manufacturer Item No_").rlike(r'(?i).*DI-EP-CL-SUB-PRM.*')), "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # EXAGRID-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "EXAGRID") & 
            (F.col("Manufacturer Item No_").rlike(r'^EX-3YR-MS-S')), "3 YR")
            .when((F.col("Consolidated Vendor Name") == "EXAGRID") & 
            (F.col("Manufacturer Item No_").rlike(r'^EX-10GBE-OPTICAL')), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency", 
        F.when((F.col("Consolidated Vendor Name") == "EXAGRID") & 
            (F.col("duration")=="3 YR"), "Upfront")
        .when((F.col("Consolidated Vendor Name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("Consolidated Vendor Name") == "EXAGRID")  & 
            (F.col("duration")=="3 YR"), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "EXAGRID")  & 
            (F.col("duration")=="3 YR"), "Capacity")
        .when((F.col("Consolidated Vendor Name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "EXAGRID")  & 
            (F.col("duration")=="3 YR"), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Hardware")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "EXAGRID")  & 
            (F.col("duration")=="3 YR"), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "EXAGRID") & 
            (F.col("duration")=="Perpetual"), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Forcepoint-Specific Transformations
    df = df.withColumn(
        "months", 
        F.when(F.col("Consolidated Vendor Name") == "Forcepoint", 
            F.regexp_replace(F.col("Life Cycle Formula"), "_x0005_", "").cast("int"))
        .otherwise(F.col("months"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") > 1)
            , F.concat(F.round((F.col("months") / 12) ,4).cast("string"),
            F.lit(" YR")))
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 1), "1M")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 0), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") > 1), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 1), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 0), "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", 
        F.when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") > 1), "Upfront")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 1), "Monthly")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 0), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") > 1), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 1), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 0), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") > 1), "Capacity")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 1), "Flexible")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & (F.col("months") == 0), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Forcepoint") & 
            ((F.col("Product Type") == "Software") & F.col("duration").rlike("YR")), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & 
            ((F.col("Product Type") == "Extras") & F.col("duration").rlike("YR")), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & 
            ((F.col("Product Type") == "Software") & F.col("duration").rlike("M")), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & 
            (F.col("Product Type") == "Software"), "SW Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Forcepoint") & 
            (F.col("Product Type") == "Professional Service"), "Professional services")
        .otherwise(F.col("Type"))
    )
    # GFI-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Description").rlike(r'(?i).*1 Year.*')) & 
            (F.col("Description").rlike(r'(?i).*subscription.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Description").rlike(r'(?i).*2 Year.*')) & 
            (F.col("Description").rlike(r'(?i).*subscription.*')), "2 YR")
        .when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Description").rlike(r'(?i).*3 Year.*')) & 
            (F.col("Description").rlike(r'(?i).*subscription.*')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Description").rlike(r'(?i).*GFI 6000 Fax Pages inbound or outbound LOCAL in one year.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Item Tracking Code").rlike(r'(?i).*FMO-SS500-OFS-1Y.*')), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("Consolidated Vendor Name") == "GFI") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("Consolidated Vendor Name") == "GFI") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("Consolidated Vendor Name") == "GFI") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when((F.col("Consolidated Vendor Name") == "GFI") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Item Tracking Code").rlike(r'(?i).*FMO-SS500-OFS-1Y.*')), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "GFI") & (F.col("duration")!='Not Assigned'), "SW Subscription")
        .otherwise(F.col("Type"))
    )
    # Kaspersky-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when((F.col("Consolidated Vendor Name") == "Kaspersky") & 
            (F.col("Description").rlike(r'(?i).*12 Months.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Kaspersky") & 
            (F.col("Description").rlike(r'(?i).*24 Month.*')), "2 YR")
        .when((F.col("Consolidated Vendor Name") == "Kaspersky") & 
            (F.col("Description").rlike(r'(?i).*36 Month.*')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Kaspersky") & 
            (F.col("Description").rlike(r'(?i).*60 Months.*')), "5 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("Consolidated Vendor Name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("Consolidated Vendor Name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("Consolidated Vendor Name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when((F.col("Consolidated Vendor Name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when((F.col("Consolidated Vendor Name") == "Kaspersky") & (F.col("duration")!='Not Assigned'), "SW Subscription")
        .otherwise(F.col("Type"))
    )
    # GitHub-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "GITHUB", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "GITHUB", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "GITHUB", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "GITHUB", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration",
        F.when(F.col("Consolidated Vendor Name") == "GITHUB", "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(F.col("Consolidated Vendor Name") == "GITHUB", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # HP-Specific Transformations
    df = df.withColumn(
        "duration",
        F.when((F.col("Consolidated Vendor Name") == "HP") & (F.col("Description").rlike(r'(?i).*1yr.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "HP") & (F.col("Description").rlike(r'(?i).*5-year.*')), "5 YR")
        .when((F.col("Consolidated Vendor Name") == "HP") & (F.col("Description").rlike(r'(?i).*5y.*')), "5 YR")
        .when((F.col("Consolidated Vendor Name") == "HP") & (F.col("Description").rlike(r'(?i).*5Y.*')), "5 YR")
        .when((F.col("Consolidated Vendor Name") == "HP") & (F.col("Description").rlike(r'(?i).*3Y.*')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "HP") & (F.col("Description").rlike(r'(?i).*4y.*')), "4 YR")
        .when((F.col("Consolidated Vendor Name") == "HP") & (F.col("Description").rlike(r'(?i).*4Y.*')), "4 YR")
        .when((F.col("Consolidated Vendor Name") == "HP"), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "HP", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "HP", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "HP", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when(F.col("Consolidated Vendor Name") == "HP", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("Consolidated Vendor Name") == "HP") & (F.col("duration")=='Perpetual'), "Other Mapping")
        .when((F.col("Consolidated Vendor Name") == "HP") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Progress-Specific Transformations
    df = df.withColumn(
        "frequency", 
        F.when(F.col("Consolidated Vendor Name") == "Progress", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("Consolidated Vendor Name") == "Progress", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Progress", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*1 Year Support.*'), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*3 Year Support.*'), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*Standard Support.*'), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*3 Years Service.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*1 Year Service.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*1 Year Enterprise Subscription.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*Subscription 1 Year Enterprise.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*1 Year Subscription.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*LICENSE W.*O SUPPORT.*'), "SW Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*3 Year Extended Support.*'), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*Extended Support 1 Year.*'), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*2 Year Extended Support.*'), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*1 Year Support.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*3 Year Support.*'), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*Standard Support.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*3 Years Service.*'), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*1 Year Service.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*1 Year Enterprise Subscription.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*Subscription 1 Year Enterprise.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*1 Year Subscription.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*LICENSE W.*O SUPPORT.*'), "Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*3 Year Extended Support.*'), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*Extended Support 1 Year.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
            F.col("Description").rlike(r'(?i).*2 Year Extended Support.*'), "2 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when((F.col("Consolidated Vendor Name") == "Progress") &
               (F.col("Description").rlike(r'(?i).*Standard Support.*')), "Other Mapping")
        .when((F.col("Consolidated Vendor Name") == "Progress") & 
              (F.col("duration")!='Not Assigned') , "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # HornetSecurity-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "HornetSecurity", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "HornetSecurity", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "HornetSecurity", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("Consolidated Vendor Name") == "HornetSecurity", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "HornetSecurity") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*REN-SMA24.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "HornetSecurity") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*VMSE-1-999.*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "HornetSecurity", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # IMPERVA-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "IMPERVA", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "IMPERVA", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption",
        F.when(F.col("Consolidated Vendor Name") == "IMPERVA", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("Consolidated Vendor Name") == "IMPERVA", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "IMPERVA") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*SS-WAF-X451-P-R-SL2.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "IMPERVA") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*SB-WAF-TRS-45-R-TR0.*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "IMPERVA") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # IRONSCALES-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "IRONSCALES", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "IRONSCALES", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "IRONSCALES", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("Consolidated Vendor Name") == "IRONSCALES", "SW TBD")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("Consolidated Vendor Name") == "IRONSCALES", "Not Assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "IRONSCALES", "Not Assigned")
        .otherwise(F.col("Mapping_type_Duration"))
    )



    # Ivanti-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Ivanti", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Ivanti", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Ivanti", 
            F.when(F.col("Description").rlike(r'(?i).*MSP.*'), "Flexible")
            .otherwise("Capacity"))
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "duration", 
        F.when(F.col("Consolidated Vendor Name") == "Ivanti", "Not Assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "Ivanti", "Not Assigned")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Ivanti") & 
            F.col("Description").rlike(r'(?i).*Subscription.*'), "SW Subscription")
        .otherwise(F.col("Type"))
    )

    # LogRhythm-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "LOGRHYTHM", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("Consolidated Vendor Name") == "LOGRHYTHM", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "LOGRHYTHM", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "LOGRHYTHM") & 
            F.col("Description").rlike(r'(?i).*Training.*'), "Professional Service")
        .when((F.col("Consolidated Vendor Name") == "LOGRHYTHM") & 
            F.col("Description").rlike(r'(?i).*Software Subscription.*'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "LOGRHYTHM") & 
            F.col("Description").rlike(r'(?i).*Training.*'), "Perpetual")
        .when((F.col("Consolidated Vendor Name") == "LOGRHYTHM") & 
            F.col("Description").rlike(r'(?i).*Software Subscription.*'), "1.8 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "LOGRHYTHM") & 
            F.col("Description").rlike(r'(?i).*Training.*'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "LOGRHYTHM") & 
            F.col("Description").rlike(r'(?i).*Software Subscription.*'), "Ratio Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Macmon-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Macmon", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Macmon", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Macmon", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-LI-.*'), "SW Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-W1-.*'), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-W2-.*'), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'MMSMB-VA250-RNW'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'MMMSP-.*'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-LI-.*'), "Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-W1-.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-W2-.*'), "2 YR")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'MMSMB-VA250-RNW'), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'MMMSP-.*'), "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-LI-.*'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-W1-.*'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'(?i).*-W2-.*'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'MMSMB-VA250-RNW'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Macmon") & 
            F.col("Manufacturer Item No_").rlike(r'MMMSP-.*'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # n-able-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "n-able") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-1Y')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "n-able") & 
            (F.col("Description").rlike(r'(?i).*Annual Maintenance Renewal.*')), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("Consolidated Vendor Name") == "n-able") & 
            (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("Consolidated Vendor Name") == "n-able") & 
            (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "n-able") & 
            (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "n-able") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-1Y')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "n-able") & 
            (F.col("Description").rlike(r'(?i).*Annual Maintenance Renewal.*')), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "n-able") & 
            (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Netwrix-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Netwrix") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-1M')), "1 M")
        .when((F.col("Consolidated Vendor Name") == "Netwrix") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-1Y')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Netwrix") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-3Y')), "3 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("Consolidated Vendor Name") == "Netwrix") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-1M')), "Monthly")
        .when((F.col("Consolidated Vendor Name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when((F.col("Consolidated Vendor Name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "Netwrix") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-1M')), "Flexible")
        .when((F.col("Consolidated Vendor Name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Netwrix") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Versa Networks-Specific Transformations
    df = df.withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-1M')), "Flexible")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PRIME.*-5YR')), "Capacity")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'CLDSVC-.*-5YR.*')), "Capacity")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'CLDSVC-.*-3YR.*')), "Capacity")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PRIME.*-3YR')), "Capacity")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PREM.*-3YR')), "Capacity")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'SUP-.*-5YR')), "Capacity")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PDD-HOURLY-20-A|PDD-DAILY-A')), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-1M')), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PRIME.*-5YR')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'CLDSVC-.*-5YR.*')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'CLDSVC-.*-3YR.*')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PRIME.*-3YR')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PREM.*-3YR')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'SUP-.*-5YR')), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PDD-HOURLY-20-A|PDD-DAILY-A')), "Professional Service")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-1M')), "1 M")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PRIME.*-5YR')), "5 YR")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'CLDSVC-.*-5YR.*')), "5 YR")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'CLDSVC-.*-3YR.*')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PRIME.*-3YR')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PREM.*-3YR')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'SUP-.*-5YR')), "5 YR")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PDD-HOURLY-20-A|PDD-DAILY-A')), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("Consolidated Vendor Name") == "Versa Networks") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Versa Networks") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'(?i).*-CC-1M')), "Monthly")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PRIME.*-5YR')), "Upfront")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'CLDSVC-.*-5YR.*')), "Upfront")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'CLDSVC-.*-3YR.*')), "Upfront")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PRIME.*-3YR')), "Upfront")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PREM.*-3YR')), "Upfront")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'SUP-.*-5YR')), "Upfront")
        .when((F.col("Consolidated Vendor Name") == "Versa Networks") & 
            (F.col("Manufacturer Item No_").rlike(r'PDD-HOURLY-20-A|PDD-DAILY-A')), "Upfront")
        .otherwise(F.col("frequency"))
    )

    # Veritas-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Veritas", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Veritas", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Veritas", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*36MO.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*12MO.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*24MO.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            (F.col("Description").rlike(r'(?i).*24 MONTHS.*') | F.col("Description").rlike(r'(?i).*RENEWALS.*')), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            (F.col("Description").rlike(r'(?i).*12MO.*') | F.col("Description").rlike(r'(?i).*RENEWALS.*')), "Vendor Support")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*FREIGHT SERVICE.*|.*FEE SERVICE.*'), "SW Perpetual")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*36MO.*'), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*12MO.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*24MO.*'), "2 YR")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            (F.col("Description").rlike(r'(?i).*24 MONTHS.*') | F.col("Description").rlike(r'(?i).*RENEWALS.*')), "2 YR")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            (F.col("Description").rlike(r'(?i).*12MO.*') | F.col("Description").rlike(r'(?i).*RENEWALS.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*FREIGHT SERVICE.*|.*FEE SERVICE.*'), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*36MO.*'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*12MO.*'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*24MO.*'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            (F.col("Description").rlike(r'(?i).*24 MONTHS.*') | F.col("Description").rlike(r'(?i).*RENEWALS.*')), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            (F.col("Description").rlike(r'(?i).*12MO.*') | F.col("Description").rlike(r'(?i).*RENEWALS.*')), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Veritas") & 
            F.col("Description").rlike(r'(?i).*FREIGHT SERVICE.*|.*FEE SERVICE.*'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Vendors of Complementary Products-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing", 
        F.when(F.col("Consolidated Vendor Name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("Consolidated Vendor Name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("Consolidated Vendor Name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "Vendors of Complementary Products", "Not assigned")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Vectra Networks-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Vectra Networks", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Vectra Networks", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Vectra Networks", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Vectra Networks") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*') & (F.col("Description").rlike(r'(?i).*1 YEAR.*')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Vectra Networks") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*') & (F.col("Description").rlike(r'(?i).*3 YEAR.*')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Vectra Networks") & 
            F.col("Manufacturer Item No_").rlike(r'V-NDR-CLOUD-STANDARD'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Vectra Networks") & 
            F.col("Manufacturer Item No_").rlike(r'VN-DETECT-NET'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Vectra Networks") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*') & (F.col("Description").rlike(r'(?i).*1 YEAR.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Vectra Networks") & 
            F.col("Description").rlike(r'(?i).*SUBSCRIPTION.*') & (F.col("Description").rlike(r'(?i).*3 YEAR.*')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Vectra Networks") & 
            F.col("Manufacturer Item No_").rlike(r'V-NDR-CLOUD-STANDARD'), "Not Assigned")
        .when((F.col("Consolidated Vendor Name") == "Vectra Networks") & 
            F.col("Manufacturer Item No_").rlike(r'VN-DETECT-NET'), "Not Assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "Vectra Networks", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Varonis-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Varonis", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Varonis", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Varonis", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("Consolidated Vendor Name") == "Varonis", "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("Consolidated Vendor Name") == "Varonis", "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "Varonis", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # TXOne Network-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "TXOne Network", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "TXOne Network", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "TXOne Network", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when(F.col("Consolidated Vendor Name") == "TXOne Network", "Not assigned")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when(F.col("Consolidated Vendor Name") == "TXOne Network", "Not assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "TXOne Network", "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Tufin-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Tufin", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Tufin", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Tufin", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Tufin") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Professional Service")
        .when((F.col("Consolidated Vendor Name") == "Tufin") & 
            F.col("Manufacturer Item No_").rlike(r'TF-PS-DEPLOY-ST-PLUS'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Tufin") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Tufin") & 
            F.col("Manufacturer Item No_").rlike(r'TF-PS-DEPLOY-ST-PLUS'), "Not assigned")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Tufin") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Tufin") & 
            F.col("Manufacturer Item No_").rlike(r'TF-PS-DEPLOY-ST-PLUS'), "Not assigned")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Trustwave-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Trustwave", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Trustwave", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Trustwave", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Trustwave") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Professional Service")
        .when((F.col("Consolidated Vendor Name") == "Trustwave") & 
            F.col("Manufacturer Item No_").rlike(r'MPL-MM-CLD-ESSENTIALS'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Trustwave") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Trustwave") & 
            F.col("Manufacturer Item No_").rlike(r'MPL-MM-CLD-ESSENTIALS'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Trustwave") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Trustwave") & 
            F.col("Manufacturer Item No_").rlike(r'MPL-MM-CLD-ESSENTIALS'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Tripwire-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Tripwire", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Tripwire", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Tripwire", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Tripwire") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Professional Service")
        .when((F.col("Consolidated Vendor Name") == "Tripwire") & 
            F.col("Description").rlike(r'(?i).*SUPPORT RENEWAL.*'), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Tripwire") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Tripwire") & 
            F.col("Description").rlike(r'(?i).*SUPPORT RENEWAL.*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Tripwire") & 
            F.col("Manufacturer Item No_").rlike(r'TF-TOS-TCSE-CUSTOM'), "Sure Mapping")
        .when((F.col("Consolidated Vendor Name") == "Tripwire") & 
            F.col("Description").rlike(r'(?i).*SUPPORT RENEWAL.*'), "Other Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Trendmicro-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,36 MONTH.*')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,12 MONTH.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,24 MONTH.*')), "2 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,07 MONTH.*')), "0.583333333 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,13 MONTH.*')), "1.0883333333 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,27 MONTH.*')), "2.25 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,08 MONTH.*')), "0.66666666666667 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,03 MONTH.*')), "0.25 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,06 MONTH.*')), "0.5 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,02 MONTH.*')), "0.1666666666666667 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,18 MONTH.*')), "1.5 YR")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,01 MONTH.*')), "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,01 MONTH.*')), "Monthly")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("duration")!='Not Assigned'), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("Description").rlike(r'(?i).*LICENSE,01 MONTH.*')), "Flexible")
        .when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("duration")!='Not Assigned'), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("duration")!='Not Assigned'),  "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Trendmicro") & (F.col("duration")!='Not Assigned'),  "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Trellix/Skyhigh-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh"), "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh"), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh"), "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh") & 
            F.col("Description").rlike(r'(?i).*1YR SUBSCRIPTION.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh") & 
            F.col("Description").rlike(r'(?i).*1YR THRIVE.*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh") & 
            F.col("Description").rlike(r'(?i).*EXTENDED.*SUPPORT 1YR.*'), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh") & 
            F.col("Description").rlike(r'(?i).*1YR SUBSCRIPTION.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh") & 
            F.col("Description").rlike(r'(?i).*1YR THRIVE.*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh") & 
            F.col("Description").rlike(r'(?i).*EXTENDED.*SUPPORT 1YR.*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(((F.col("Consolidated Vendor Name") == "Trellix") | (F.col("Consolidated Vendor Name") == "Skyhigh"))  & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )

    # Thales-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Thales", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Thales", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Thales", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*THALES CLIENT SERVICES.*')), "Professional Service")
        .when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*SAFENET TRISTED ACCESS.*SUPPORT.*12 MO.*')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*12 Mon. Subscr.*')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*Basic, 12 Months.*')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*SETUP FEE.*')), "SW Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*ACTIVE USER.*MONTH.*')), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*CIPHERTRUST FLEX CONNECTOR.*')), "SW Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*CIPHERTRUST FLEX ABILI.*')), "SW Perpetual")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("Description").rlike(r'(?i).*THALES CLIENT SERVICES.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Thales")& (F.col("Description").rlike(r'(?i).*SAFENET TRISTED ACCESS.*SUPPORT.*12 MO.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Thales")& (F.col("Description").rlike(r'(?i).*12 Mon. Subscr.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Thales")& (F.col("Description").rlike(r'(?i).*Basic, 12 Months.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Thales")& (F.col("Description").rlike(r'(?i).*SETUP FEE.*')), "Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Thales")& (F.col("Description").rlike(r'(?i).*ACTIVE USER.*MONTH.*')), "1 M")
        .when((F.col("Consolidated Vendor Name") == "Thales")& (F.col("Description").rlike(r'(?i).*CIPHERTRUST FLEX CONNECTOR.*')), "Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Thales")& (F.col("Description").rlike(r'(?i).*CIPHERTRUST FLEX ABILI.*')), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Thales") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Teamviewer-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Teamviewer", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Teamviewer", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Teamviewer", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Teamviewer") & 
            (F.col("Description").rlike(r'(?i).*2 YEARS SUBSCRIPTION.*')), "2 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Teamviewer") & (F.col("duration")!='Not Assigned'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Teamviewer") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # SentinelOne-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "SentinelOne", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "SentinelOne", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "SentinelOne", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "SentinelOne") & 
            (F.col("Description").rlike(r'(?i).*12 M.*')), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "SentinelOne") & 
            (F.col("Description").rlike(r'(?i).*12 M.*')), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "SentinelOne") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Rapid7-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Rapid7", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Rapid7", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Rapid7", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Rapid7") & 
            ((F.col("Description").rlike(r'(?i).*1 YEAR*') | F.col("Description").rlike(r'(?i).*ONE YEAR*'))), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Rapid7") & 
            ((F.col("Description").rlike(r'(?i).*1 YEAR*')) | (F.col("Description").rlike(r'(?i).*ONE YEAR*'))), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Rapid7") & (F.col("Description").rlike(r'(?i).*PER MONTH*')), "1 M")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Rapid7") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "frequency", 
        F.when((F.col("Consolidated Vendor Name") == "Rapid7") & (F.col("Description").rlike(r'(?i).*PER MONTH*')), "Monthly")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption", 
        F.when((F.col("Consolidated Vendor Name") == "Rapid7") & (F.col("Description").rlike(r'(?i).*PER MONTH*')), "Flexible")
        .otherwise(F.col("Consumption"))
    )
    # Ruckus-Specific Transformations
    df = df.withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Ruckus") & 
            F.col("Description").rlike(r'(?i).*-12'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Ruckus") & 
            F.col("Description").rlike(r'(?i).*36 MONTHS*'), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "Ruckus") & 
            F.col("Description").rlike(r'(?i).*12 MONTHS*'), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "Ruckus") & 
            F.col("Description").rlike(r'(?i).*PERPETUAL*'), "Perpetual")
        .otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Ruckus", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Ruckus", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Ruckus", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Ruckus") & (F.col("duration")=='duration'), "SW Perpetual")
        .when((F.col("Consolidated Vendor Name") == "Ruckus") & (F.col("duration")!='Not Assigned'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Ruckus") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # OneSpan-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "OneSpan", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "OneSpan", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "OneSpan", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "OneSpan") & 
            ((F.col("Description").rlike(r'(?i).*MAINTENANCE*') | F.col("Description").rlike(r'(?i).*MAINT.*SUPPORT*'))), "Vendor Support")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "OneSpan") & 
            ((F.col("Description").rlike(r'(?i).*MAINTENANCE*') | F.col("Description").rlike(r'(?i).*MAINT.*SUPPORT*'))), "3 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when(F.col("Consolidated Vendor Name") == "OneSpan", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # SEPPMail-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "SEPPMail", "Upfront")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "SEPPMail", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "SEPPMail", "Capacity")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "SEPPMail") & 
            F.col("Description").rlike(r'(?i).*5 JAHRE*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "SEPPMail") & 
            F.col("Description").rlike(r'(?i).*1 JAHR*'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "SEPPMail") & 
            F.col("Description").rlike(r'(?i).*5 JAHRE*'), "5 YR")
        .when((F.col("Consolidated Vendor Name") == "SEPPMail") & 
            F.col("Description").rlike(r'(?i).*1 JAHR*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "SEPPMail") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )
    # Parallels-Specific Transformations
    df = df.withColumn(
        "frequency",
        F.when(F.col("Consolidated Vendor Name") == "Parallels", "Monthly")
        .otherwise(F.col("frequency"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(F.col("Consolidated Vendor Name") == "Parallels", "Sure Mapping")
        .otherwise(F.col("Mapping_type_Billing"))
    ).withColumn(
        "Consumption", 
        F.when(F.col("Consolidated Vendor Name") == "Parallels", "Flexible")
        .otherwise(F.col("Consumption"))
    ).withColumn(
        "Type", 
        F.when((F.col("Consolidated Vendor Name") == "Parallels") & 
            F.col("Description").rlike(r'(?i).*USERS.*MONTH*'), "SW Subscription")
        .when((F.col("Consolidated Vendor Name") == "Parallels") & 
            F.col("Description").rlike(r'(?i).*1 YEAR*'), "SW Subscription")
        .otherwise(F.col("Type"))
    ).withColumn(
        "duration", 
        F.when((F.col("Consolidated Vendor Name") == "Parallels") & 
            F.col("Description").rlike(r'(?i).*USERS.*MONTH*'), "1 M")
        .when((F.col("Consolidated Vendor Name") == "Parallels") & 
            F.col("Description").rlike(r'(?i).*1 YEAR*'), "1 YR")
        .otherwise(F.col("duration"))
    ).withColumn(
        "Mapping_type_Duration", 
        F.when((F.col("Consolidated Vendor Name") == "Parallels") & (F.col("duration")!='Not Assigned'), "Sure Mapping")
        .otherwise(F.col("Mapping_type_Duration"))
    )


    # Hardware-Specific Transformations
    ## overides all others
    df = df.withColumn(
        "duration",
        F.when(
            (F.col("Product Type") == "Hardware") | 
            (F.col("Consolidated Vendor Name") == "PROLABS") | 
            (F.col("Consolidated Vendor Name") == "RACKMOUNT"), 
            "Perpetual"
        ).otherwise(F.col("duration"))
    ).withColumn(
        "frequency",
        F.when(
            (F.col("Product Type") == "Hardware") | 
            (F.col("Consolidated Vendor Name") == "PROLABS") | 
            (F.col("Consolidated Vendor Name") == "RACKMOUNT"), 
            "Upfront"
        ).otherwise(F.col("frequency"))
    ).withColumn(
        "Consumption",
        F.when(
            (F.col("Product Type") == "Hardware") | 
            (F.col("Consolidated Vendor Name") == "PROLABS") | 
            (F.col("Consolidated Vendor Name") == "RACKMOUNT"), 
            "Capacity"
        ).otherwise(F.col("Consumption"))
    ).withColumn(
        "Type",
        F.when(
            (F.col("Product Type") == "Hardware") | 
            (F.col("Consolidated Vendor Name") == "PROLABS") | 
            (F.col("Consolidated Vendor Name") == "RACKMOUNT"), 
            "Hardware"
        ).otherwise(F.col("Type"))
    ).withColumn(
        "Mapping_type_Duration",
        F.when(
            (F.col("Product Type") == "Hardware") | 
            (F.col("Consolidated Vendor Name") == "PROLABS") | 
            (F.col("Consolidated Vendor Name") == "RACKMOUNT"), 
            "Sure Mapping"
        ).otherwise(F.col("Mapping_type_Duration"))
    ).withColumn(
        "Mapping_type_Billing",
        F.when(
            (F.col("Product Type") == "Hardware") | 
            (F.col("Consolidated Vendor Name") == "PROLABS") | 
            (F.col("Consolidated Vendor Name") == "RACKMOUNT"), 
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
#     .saveAsTable(f"{catalog}.{schema}.pierre_arr_x")

# zorder_cols = ["Consolidated Vendor Name", "Manufacturer Item No_", "description"]
# zorder_by = ", ".join(f"`{col}`" for col in zorder_cols)
# spark.sql(f"OPTIMIZE {catalog}.{schema}.pierre_arr_x ZORDER BY ({zorder_by})")
   

# df_source_1 = spark.table(f"{catalog}.{schema}.pierre_arr_x")
 

# COMMAND ----------

# Check for pattern matching
#df_1.select("Manufacturer Item No_").where(F.col("Manufacturer Item No_").rlike("^S-.*-P$")).show()

# COMMAND ----------

df_source_1=df_source.filter(col('Manufacturer Item No_')!=col('sku')).withColumn("levenshtein", levenshtein(col(f'Manufacturer Item No_'), col(f'sku')) )
display(df_source_1.select('levenshtein','Manufacturer Item No_','*'))


# COMMAND ----------

# DBTITLE 1,Apply transformations to 1 record
df_source_1=df_source.filter(col('Manufacturer Item No_')=='LANSSREN250-2999')

df_1 = default_columns(df_source_1)
# Apply transformations to 1 record
df_1 = parse_months(df_1)

# df_1 = apply_vendor_transformations(df_1)
# df_1 = apply_final_transformations(df_1) 
df_1 = df_1.withColumn(
        "duration",
        F.when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Description").rlike(r'(?i).*1 Year.*')) & 
            (F.col("Description").rlike(r'(?i).*subscription.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Description").rlike(r'(?i).*2 Year.*')) & 
            (F.col("Description").rlike(r'(?i).*subscription.*')), "2 YR")
        .when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Description").rlike(r'(?i).*3 Year.*')) & 
            (F.col("Description").rlike(r'(?i).*subscription.*')), "3 YR")
        .when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Description").rlike(r'(?i).*GFI 6000 Fax Pages inbound or outbound LOCAL in one year.*')), "1 YR")
        .when((F.col("Consolidated Vendor Name") == "GFI") & 
            (F.col("Item Tracking Code").rlike(r'(?i).*FMO-SS500-OFS-1Y.*')), "1 YR")
        .otherwise(F.col("duration")))

display(df_1.select(
    "sku",
    "Manufacturer Item No_",
    "Consolidated Vendor Name"
            ,'duration','Manufacturer Item No_','Mapping_type_Duration','months','Consolidated Vendor Name','Product Type','description','frequency','consumption', 'Type', 'match_type'))


# COMMAND ----------

# DBTITLE 1,Apply transformations to 1 record, step 2

#df_X = apply_final_transformations(df_1) 
# display(df_X.withColumn(
#         "x",
#     F.when(F.col("duration").rlike(r'.*YR'),
#         F.concat(
#             F.round(
#                 F.regexp_extract(F.col("duration"), r"(\d+)", 1).cast("float"), 
#                 2
#             ).cast("string"),
#             F.lit(" YR")
#         )))
# )

#display(df_X.select(col('duration')))

# COMMAND ----------

# DBTITLE 1,data quality process
# Multiple column rename:
columns_to_rename = {
    "sku": "local_sku",
    "Manufacturer Item No_": "sku",
    "Consolidated Vendor Name": "vendor_name"
}

df = rename_columns(df_source, columns_to_rename)

# Apply the function to clean column names
df_cleaned = clean_column_names(df)



# Apply transformations in sequence
composite_key = [
            'sku',
            'vendor_name',
            'item_tracking_code'
        ]
        
##add dupes to exception dq table
df_dq = dq_transform(df_cleaned, composite_key, keep_duplicates=True)

# Write the cleaned DataFrame to Delta table
df_dq.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.pierre_arr_dq")



# Apply transformations in sequence
composite_key = [
            'sku',
            'vendor_name' 
        ]
        
##add dupes to exception dq table
df_dq = dq_transform(df_cleaned, composite_key, keep_duplicates=True)

# Write the cleaned DataFrame to Delta table
df_dq.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.pierre_arr_dq_0")




# COMMAND ----------



# Apply transformations in sequence
composite_key = [
           'Manufacturer Item No_',
           'Consolidated Vendor Name',
           'Item Tracking Code'
        ]
##clear dupe records from source
df = dq_transform(df_source, composite_key, keep_duplicates=False)
df = default_columns(df)

df = parse_months(df)
df = apply_vendor_transformations(df)
df = apply_final_transformations(df)

# Multiple column rename:
columns_to_rename = {
    "sku": "local_sku",
    "Manufacturer Item No_": "sku",
    "Consolidated Vendor Name": "vendor_name"
}

df = rename_columns(df, columns_to_rename)

# Apply the function to clean column names
df_cleaned = clean_column_names(df)
df_cleaned = df_cleaned.filter(col('sku').isNotNull())

# Write the cleaned DataFrame to Delta table
df_cleaned.write \
  .mode("overwrite") \
  .format("delta") \
  .option("mergeSchema", "true") \
  .option("overwriteSchema", "true") \
  .saveAsTable(f"{catalog}.{schema}.pierre_arr_1")

# COMMAND ----------

# example where pierres script create ambigious arr
display(df_pierre_arr_0.filter(col('sku')=='LANSSREN250-2999'))

# COMMAND ----------

df_pierre_arr_0= spark.table(f"{catalog}.{schema}.pierre_arr_0")
# MRR Ratio
df_pierre_arr_0 = df_pierre_arr_0.withColumn(
    "mrrratio",
    F.when(F.col("duration").rlike(r'.*YR'), 12 * F.regexp_extract(F.col("duration"), r"(\d*\.?\d+)", 1).cast("float"))
    .otherwise(0)
)

# Duration in years (rounded and formatted)
df_pierre_arr_0 = df_pierre_arr_0.withColumn(
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
df_pierre_arr_0 = df_pierre_arr_0.withColumn("mapping_type_billing",
    F.when(F.lower(F.col("mapping_type_billing")) == 'sure mapping',
        F.lit("Sure Mapping")
    ).when(F.lower(F.col("mapping_type_billing")) == 'other mapping',
        F.lit("Other Mapping")  
    ).otherwise(F.col("mapping_type_billing")))


# mapping_type_duration set to "Sure Mapping" and "Other Mapping"
df_pierre_arr_0 = df_pierre_arr_0.withColumn("mapping_type_duration",
    F.when(F.lower(F.col("mapping_type_duration")) == 'other mapping',
        F.lit("Other Mapping")
    ).when(F.lower(F.col("mapping_type_duration")) == 'sure mapping',  
        F.lit("Sure Mapping")
    ).otherwise(F.col("mapping_type_duration")))



df_pierre_arr_1= spark.table(f"{catalog}.{schema}.pierre_arr_1")

print(df_pierre_arr_0.count())
print(df_pierre_arr_1.count())

# df_pierre_arr_0.printSchema()
# df_pierre_arr_1.printSchema()


# COMMAND ----------

# First, let's count total rows in each DataFrame
df1_count = df_pierre_arr_0.count()
df2_count = df_pierre_arr_1.count()

print(f"Total rows in df_pierre_arr_0: {df1_count}")
print(f"Total rows in df_pierre_arr_1: {df2_count}")

# Compare schema differences
schema_diff = set(df_pierre_arr_0.columns) ^ set(df_pierre_arr_1.columns)
if schema_diff:
    print("\nColumns that differ between DataFrames:")
    print(schema_diff)

# Find records in df1 that are not in df2
df1_not_in_df2 = df_pierre_arr_0.join(
    df_pierre_arr_1,
    (df_pierre_arr_0.sku == df_pierre_arr_1.sku) & 
    (df_pierre_arr_0.vendor_name == df_pierre_arr_1.vendor_name),
    "left_anti"
)

# Find records in df2 that are not in df1
df2_not_in_df1 = df_pierre_arr_1.join(
    df_pierre_arr_0,
    (df_pierre_arr_1.sku == df_pierre_arr_0.sku) & 
    (df_pierre_arr_1.vendor_name == df_pierre_arr_0.vendor_name),
    "left_anti"
)

# Find exact matches (records that are identical in both DataFrames)
exact_matches = df_pierre_arr_0.join(
    df_pierre_arr_1,
    (df_pierre_arr_0.sku == df_pierre_arr_1.sku) & 
    (df_pierre_arr_0.vendor_name == df_pierre_arr_1.vendor_name),
    "inner"
)

print(f"\nRecords in df_pierre_arr_0 but not in df_pierre_arr_1: {df1_not_in_df2.count()}")
print(f"Records in df_pierre_arr_1 but not in df_pierre_arr_0: {df2_not_in_df1.count()}")
print(f"Exact matches (based on SKU (manufacturer_item_no_) and Vendor): {exact_matches.count()}")

# COMMAND ----------

df1_not_in_df2.display()

# COMMAND ----------

df2_not_in_df1.display()

# COMMAND ----------

display(df_pierre_arr_1.filter(col('sku')=='SP87ZZ12ZZRCAA'))

display(df_pierre_arr_0.filter(F.col('sku') =='SP87ZZ12ZZRCAA'))

# COMMAND ----------


display(df_pierre_arr_1.filter(F.col('sku') =='KCONN50-249'))

display(df_pierre_arr_0.filter(F.col('sku') =='KCONN50-249'))


# COMMAND ----------

display(df_pierre_arr_0.filter(F.col('duration').rlike(r'\d+\.\d{1}\s*YR')))
#display(df_pierre_arr_0.filter(F.col('duration') =='1.00 YR'))


# COMMAND ----------

# Define composite key
composite_key = [
    'sku',
    'vendor_name'
    ,'item_tracking_code'
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
    'match_type',
    'country', 
    'consumption_model', 
    'description',
    'description_2', 
    'description_3', 
    'description_4'
]

# Call the function
compare_df = compare_dataframes(
    df1=df_pierre_arr_0, 
    df2=df_pierre_arr_1, 
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
  .saveAsTable(f"{catalog}.{schema}.pierre_arr_comparison")

# COMMAND ----------

compare_df.filter(col('comparison_status')=='DIFFERENT_VALUES').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lets compare auto arr with manual

# COMMAND ----------

 
df_datanow_arr_0= spark.table(f"{catalog}.obt.datanowarr")
# Multiple column rename:
columns_to_rename = { 
    "sku": "sku",
    "Vendor Name": "vendor_name",
    "billing_frequency": "frequency",
    "commitment_duration2": "duration",
    "consumption_model": "consumption",
    "mrr_ratio": "mrrratio"
    #"Mapping type Duration" : "mapping_type_duration"
}

df_datanow_arr_0 = rename_columns(df_datanow_arr_0, columns_to_rename)

# Apply the function to clean column names
df_datanow_arr_0 = clean_column_names(df_datanow_arr_0)

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
    'match_type',
    'country', 
    'consumption_model', 
]

# Call the function
compare_df = compare_dataframes(
    df1=df_pierre_arr_0, 
    df2=df_datanow_arr_0, 
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
  .saveAsTable(f"{catalog}.{schema}.pierre_arr_comparison_v2")

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
