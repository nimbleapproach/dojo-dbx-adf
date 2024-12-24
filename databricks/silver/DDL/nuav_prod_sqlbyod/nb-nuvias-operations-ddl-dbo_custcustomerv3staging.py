# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA nuav_prod_sqlbyod;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dbo_custcustomerv3staging
# MAGIC   (
# MAGIC     SID BIGINT GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC   , ADDRESSCOUNTRYREGIONISOCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , CUSTOMERACCOUNT STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , ORGANIZATIONNAME STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , ADDRESSZIPCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , DATAAREAID STRING
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
# MAGIC   , CONSTRAINT dbo_custcustomerv3staging_pk PRIMARY KEY(CUSTOMERACCOUNT, DATAAREAID, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the data from dbo_custcustomerv3staging. \n'
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_custcustomerv3staging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_custcustomerv3staging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_custcustomerv3staging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_custcustomerv3staging OWNER TO `az_edw_data_engineers_ext_db`
