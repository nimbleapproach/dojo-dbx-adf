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
# MAGIC CREATE OR REPLACE TABLE subscrparam
# MAGIC   ( 
# MAGIC         SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,subscriptionID	INT	
# MAGIC       COMMENT 'Primary key'
# MAGIC     ,Status	INT	
# MAGIC       COMMENT 'been installed, provisioned'
# MAGIC     ,OrderedAmount	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,resourceID	INT	
# MAGIC       COMMENT 'FK'
# MAGIC     ,Amount	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,CancellationFeeDescr	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,IncludedValue	DECIMAL
# MAGIC       COMMENT 'TODO'
# MAGIC     ,RelativeStatus	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,DateArc	INT	
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
# MAGIC ,CONSTRAINT subscriptionparam_pk PRIMARY KEY(subscriptionID,DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for subscriptionparam. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (subscriptionID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE subscrparam ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE subscrparam ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE subscrparam ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
