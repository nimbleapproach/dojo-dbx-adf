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
              DROP TABLE IF EXISTS {catalog}.{schema}.link_vendor_to_vendor_group_stg
              """)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_vendor_to_vendor_group_stg (
  local_link_id BIGINT COMMENT 'Surrogate Key',
  vendor_code STRING COMMENT 'The code of the vendor',
  vendor_group_code STRING COMMENT 'The name of the vendor group.',
  sys_bronze_insertdatetime_utc TIMESTAMP COMMENT 'The timestamp when this entry landed in bronze.',
  sys_silver_insertdatetime_utc TIMESTAMP DEFAULT current_timestamp() COMMENT 'The timestamp when this entry landed in silver.',
  sys_silver_modifeddatetime_utc TIMESTAMP DEFAULT current_timestamp() COMMENT 'The timestamp when this entry was last modifed in silver.',
  sys_gold_inserteddatetime_utc TIMESTAMP,
  sys_gold_modifieddatetime_utc TIMESTAMP,
  start_datetime TIMESTAMP,
  end_datetime TIMESTAMP,
  is_current INT,
  source_system STRING)
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
""")
