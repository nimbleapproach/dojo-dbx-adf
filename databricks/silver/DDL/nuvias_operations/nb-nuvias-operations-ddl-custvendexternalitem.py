# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA nuvias_operations;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE custvendexternalitem
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,_SysRowId STRING NOT NULL
# MAGIC       COMMENT 'Technical Key'
# MAGIC     ,ItemId	STRING	NOT NULL                    
# MAGIC       COMMENT  'Business key'
# MAGIC     ,Description STRING
# MAGIC       COMMENT  'TODO'
# MAGIC     ,DataAreaId	STRING	NOT NULL   	                    
# MAGIC       COMMENT  'Identifier of source data area'
# MAGIC     ,ModuleType STRING	
# MAGIC       COMMENT  'TODO'
# MAGIC     ,ExternalItemId STRING	
# MAGIC       COMMENT  'TODO'
# MAGIC     ,RECID STRING	
# MAGIC       COMMENT  'TODO'
# MAGIC     ,DataLakeModified_DateTime	TIMESTAMP   
# MAGIC       COMMENT  'TODO'                 
# MAGIC     ,LastProcessedChange_DateTime	TIMESTAMP	
# MAGIC       COMMENT  'TODO'
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
# MAGIC ,CONSTRAINT custvendexternalitem_pk PRIMARY KEY(ItemId,DataAreaId,DataLakeModified_DateTime)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for custvendexternalitem. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (ItemId,DataAreaId)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE custvendexternalitem ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE custvendexternalitem ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE custvendexternalitem ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
