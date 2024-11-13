# Databricks notebook source
# Importing Libraries
import os

# COMMAND ----------

ENVIRONMENT = os.environ["__ENVIRONMENT__"]
ENVIRONMENT

# COMMAND ----------


spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")


# COMMAND ----------

catalog = spark.catalog.currentCatalog()
schema = 'orion'

# COMMAND ----------

# REMOVE ONCE SOLUTION IS LIVE
if ENVIRONMENT == 'dev':
    spark.sql(f"""
              DROP VIEW IF EXISTS {catalog}.{schema}.vw_link_product_to_vendor_arr_staging
              """)

# COMMAND ----------

!pip install openpyxl --quiet

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps

# COMMAND ----------


# File path in DBFS
file_path = "/Workspace/Users/akhtar.miah@infinigate.com/2024_05_incremental.xlsx"

df = pd.read_excel(file_path)

# COMMAND ----------

display(df)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import re

# Utility function to parse months from row[18] column
def parse_months(df):
    return df.withColumn(
        "months",
        F.when(F.col("row[18]").isNotNull(), F.regexp_replace(F.col("row[18]"), "_x0005_", "").cast("int")).otherwise(0)
    )

# Transformations based on `months` column
def apply_month_based_transformations(df):
    return df.withColumn("duration", F.when(F.col("months") > 1, (F.col("months") / 12).cast("string") + " YR")
                                .when(F.col("months") == 1, "1M")
                                .otherwise("Perpetual")) \
             .withColumn("Mapping_type_Duration", F.when(F.col("months") > 1, "Sure mapping")
                                                     .when(F.col("months") == 1, "Sure mapping")
                                                     .otherwise("other mapping")) \
             .withColumn("frequency", F.when(F.col("months") > 1, "Upfront")
                                       .when(F.col("months") == 1, "Monthly")
                                       .otherwise("Upfront")) \
             .withColumn("Consumption", F.when(F.col("months") == 1, "Flexible").otherwise("Capacity"))

# Product-specific transformations for each vendor
def apply_vendor_transformations(df):
    # Add transformations specific to "Software" and "Professional Service" categories
    df = df.withColumn("Type", 
                F.when((F.col("row[21]") == "Software") & F.col("duration").contains("YR"), "SW Subscription")
                 .when((F.col("row[21]") == "Software") & F.col("duration").contains("M"), "SW Subscription")
                 .when(F.col("row[21]") == "Professional Service", "Professional services")
    )

    # Watchguard-specific transformations
    df = df.withColumn("duration", 
            F.when(F.col("row[3]") == "Watchguard", 
                F.when(F.col("description").rlike("1-Year|1 -Year|1 Year|1-yr"), "1 YR")
                 .when(F.col("description").rlike("3-Year|3 Year|3-yr|3 -Year"), "3 YR")
                 .when(F.col("description").rlike("FireboxV.*MSSP Appliance"), "3 YR")
                 .when(F.col("description").rlike("IPSec VPN Client"), "Perpetual"))
    ) \
    .withColumn("Type", 
            F.when(F.col("row[3]") == "Watchguard", 
                F.when(F.col("description").rlike("Total Security Suite|Standard Support|Basic Security Suite"), "Vendor support")
                 .when(F.col("description").rlike("Panda Endpoint Protection Plus"), "SW Subscription")
                 .when(F.col("description").rlike("VPN Client"), "SW Perpetual"))
    )

    # Hardware transformation based on `row[16]` pattern
    df = df.withColumn("Type",
            F.when((F.col("row[3]") == "Watchguard") & F.col("row[16]").rlike("WG\\d{4}|WGT49023-EU"), "Hardware")
             .otherwise(F.col("Type"))
    )

    # DDN-specific transformations
    df = df.withColumn("duration",
            F.when((F.col("row[3]") == "DDN") & F.col("row[16]").rlike("SUP-.*-(\\d+)YR"), F.regexp_extract(F.col("row[16]"), "SUP-.*-(\\d+)YR", 1) + " YR")
             .when((F.col("row[3]") == "DDN") & F.col("row[16]").rlike("REINSTATE-BASIC-VM"), "Perpetual")
    ) \
    .withColumn("Type",
            F.when((F.col("row[3]") == "DDN") & F.col("row[16]").rlike("SUP-.*"), "Vendor support")
             .when((F.col("row[3]") == "DDN") & F.col("row[16]").rlike("REINSTATE-BASIC-VM"), "SW Perpetual")
    )

    # Entrust-specific transformations (similar approach as above for other vendors)
    df = df.withColumn("duration",
            F.when((F.col("row[3]") == "Entrust") & F.col("row[16]").rlike("NC-.*-PR|NC-.*-ST|SUP-ESSENTIALS-PLAT"), "1 YR")
             .when((F.col("row[3]") == "Entrust") & F.col("row[16]").rlike("ECS-ADVA|SMSPC-R-MFA-.*-12"), "1 YR")
             .when((F.col("row[3]") == "Entrust") & F.col("row[16]").rlike("SMSPC-R-MFA-.*-36"), "3 YR")
    ) \
    .withColumn("Type",
            F.when((F.col("row[3]") == "Entrust") & F.col("row[16]").rlike("NC-.*-PR|SUP-ESSENTIALS-PLAT"), "Vendor support")
             .when((F.col("row[3]") == "Entrust") & F.col("row[16]").rlike("ECS-ADVA|SMSPC-R-MFA-.*-12"), "SW Subscription")
             .when((F.col("row[3]") == "Entrust") & F.col("row[16]").rlike("NC-M-010114-L-EU.*"), "Hardware")
    )

    # Repeat similar transformations for each vendor: Juniper, Extreme Networks, Riverbed, WithSecure, Acronis, etc.

    return df

# Apply transformations in sequence
df = parse_months(df)
df = apply_month_based_transformations(df)
df = apply_vendor_transformations(df)

# Show the resulting dataframe
df.show()

