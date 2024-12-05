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
# MAGIC CREATE OR REPLACE TABLE dbo_sag_logisticspostaladdressbasestaging
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,COUNTRYREGIONID STRING 
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ZIPCODE	STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DESCRIPTION STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,LOCATIONRECID BIGINT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ADDRESSRECID BIGINT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Sys_Bronze_InsertDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this entry landed in bronze.'
# MAGIC     ,Sys_Silver_InsertDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry landed in silver.'
# MAGIC     ,Sys_Silver_ModifedDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry was last modifed in silver.'
# MAGIC     ,Sys_Silver_HashKey BIGINT NOT NULL
# MAGIC       COMMENT 'HashKey over all but Sys columns.'
# MAGIC     ,Sys_Silver_IsCurrent BOOLEAN
# MAGIC
# MAGIC ,CONSTRAINT dbo_sag_logisticspostaladdressbasestaging_pk PRIMARY KEY(LOCATIONRECID, ADDRESSRECID, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for dbo_sag_logisticspostaladdressbasestaging. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_logisticspostaladdressbasestaging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_logisticspostaladdressbasestaging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_logisticspostaladdressbasestaging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver_dev.nuav_prod_sqlbyod.dbo_sag_logisticspostaladdressbasestaging OWNER TO `az_edw_data_engineers_ext_db`
