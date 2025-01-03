# Databricks notebook source
# Importing Libraries
import os
spark = spark  # noqa

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
              DROP TABLE IF EXISTS {catalog}.{schema}.arr_auto_3
              """)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.arr_auto_3 (
  parent_sku STRING,
  sku STRING,
  vendorcodeitem STRING,
  vendor_name STRING,
  matched_type STRING,
  duration STRING,
  frequency STRING,
  consumption STRING,
  mapping_type_duration STRING,
  mapping_type_billing STRING,
  mrrratio FLOAT,
  months STRING,
  type STRING,
  itemtrackingcode STRING,
  local_sku STRING,
  description STRING,
  vendor_group_id BIGINT COMMENT 'Surrogate Key',
  has_sku_master_vendor_duplicates STRING,
  has_vendor_master_duplicates STRING,
  has_sku_vendor_duplicates STRING,
  has_duplicates STRING,
  sys_databasename STRING,
  sys_bronze_insertdatetime_utc TIMESTAMP COMMENT 'The timestamp when this entry landed in bronze.',
  sys_silver_insertdatetime_utc TIMESTAMP DEFAULT current_timestamp() COMMENT 'The timestamp when this entry landed in silver.',
  sys_silver_iscurrent BOOLEAN COMMENT 'Flag if this is the current version.')
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
;
""")
