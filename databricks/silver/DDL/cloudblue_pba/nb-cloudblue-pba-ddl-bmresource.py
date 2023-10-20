# Databricks notebook source
# MAGIC %python
# MAGIC import os
# MAGIC
# MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA cloudblue_pba;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bmresource
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,resourceID	INT	
# MAGIC         COMMENT 'Primary key'
# MAGIC     ,name	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,description	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,additive	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,measurable	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,postApprovalRequired	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,measureUnit	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,AccountID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,GateID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,maxValue	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,minValue	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,displayType	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,increment	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,allowedValuesList	STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,MaxControl	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Identifier	STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,UserArc	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DateArc	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,BMResourceType	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,ApsUUID	STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,MPNumber	STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Manufacturer	STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,ManufacturerName	STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,ApsAppUUID	STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,consumptionType	INT
# MAGIC         COMMENT 'TODO'
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
# MAGIC ,CONSTRAINT bmresource_pk PRIMARY KEY(resourceID,DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for plan. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (resourceID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE bmresource ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE bmresource ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE bmresource ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
