# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA vuzion_budget;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE desc
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,desc STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Description STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Product STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Product_Category STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Revenue_Type STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Revenue_Category STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Vendor STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Sys_SheetName STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Sys_FileName STRING
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
# MAGIC       COMMENT 'Flag if this is the current version.'
# MAGIC     ,Sys_Silver_IsDeleted BOOLEAN
# MAGIC       COMMENT 'Flag if this is the deleted version.'
# MAGIC ,CONSTRAINT vuzion_budget_desc_pk PRIMARY KEY(Description, Sys_FileName, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ardoc. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (Description, Sys_FileName)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE desc ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE desc ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE desc ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
