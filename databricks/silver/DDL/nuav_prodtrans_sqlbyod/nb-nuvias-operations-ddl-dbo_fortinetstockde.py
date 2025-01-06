# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA nuav_prodtrans_sqlbyod;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dbo_fortinetstockde
# MAGIC   (
# MAGIC     SID BIGINT GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC   , ID INT
# MAGIC       COMMENT 'TODO'
# MAGIC   , STATUS STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , ITEMID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SKU STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SEARCHNAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , B2B STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , COO STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , COVERAGEGROUP STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , ECCN STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , COMMODITYCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , AVAILABLEPHYSICAL INT
# MAGIC       COMMENT 'TODO'
# MAGIC   , ORDEREDINTOTAL INT
# MAGIC       COMMENT 'TODO'
# MAGIC   , ONORDER INT
# MAGIC       COMMENT 'TODO'
# MAGIC   , TOTALAVAILABLE INT
# MAGIC       COMMENT 'TODO'
# MAGIC   , ENTITY STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , Sys_Bronze_InsertDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this entry landed in bronze.'
# MAGIC   , Sys_Silver_InsertDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry landed in silver.'
# MAGIC   , Sys_Silver_ModifedDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry was last modifed in silver.'
# MAGIC   , Sys_Silver_HashKey BIGINT NOT NULL
# MAGIC       COMMENT 'HashKey over all but Sys columns.'
# MAGIC   , Sys_Silver_IsCurrent BOOLEAN
# MAGIC   , CONSTRAINT dbo_fortinetstockde PRIMARY KEY(ID, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the data from dbo_fortinetstockde. \n'
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_fortinetstockde ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_fortinetstockde ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_fortinetstockde ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver_dev.nuav_prodtrans_sqlbyod.dbo_fortinetstockde OWNER TO `az_edw_data_engineers_ext_db`
