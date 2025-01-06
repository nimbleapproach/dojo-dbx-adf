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
              DROP TABLE IF EXISTS {catalog}.{schema}.link_vendor_to_master_vendor_pre1
              """)

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.link_vendor_to_master_vendor_pre1 (
  master_vendor_fk INT,
  vendor_fk BIGINT,
  vendor_code STRING COMMENT 'Vendor Code',
  vendor_name_internal STRING COMMENT 'The internal name text of the vendor',
  is_parent BOOLEAN,
  parent_vendor_fk BIGINT,
  sys_gold_inserteddatetime_utc TIMESTAMP COMMENT 'The timestamp when this record was inserted into gold',
  sys_gold_modifieddatetime_utc TIMESTAMP COMMENT 'The timestamp when this record was last updated in gold',
  source_system_fk BIGINT COMMENT 'The ID from the Source System Dimension',
  start_datetime TIMESTAMP,
  end_datetime TIMESTAMP,
  is_current INT COMMENT 'Flag to indicate if this is the active dimension record per code')
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
;
""")
