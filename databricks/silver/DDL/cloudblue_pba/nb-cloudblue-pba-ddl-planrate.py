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
# MAGIC CREATE OR REPLACE TABLE planrate
# MAGIC   ( 
# MAGIC         SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,PlanRateID	INT	
# MAGIC       COMMENT 'PK'
# MAGIC     ,PlanID	INT	
# MAGIC       COMMENT 'FK'
# MAGIC     ,resourceID	INT	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,recurringFee	DECIMAL	
# MAGIC       COMMENT 'period fee'
# MAGIC     ,SetupFeeDescr	STRING	
# MAGIC       COMMENT 'can only grab the EN description'
# MAGIC     ,setupFee	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,costForAdditional	DECIMAL	
# MAGIC       COMMENT 'if they have a measureable resource'
# MAGIC     ,MSRPcostForAdditional	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,MSRPrecurringFee	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,MSRPsetupFee	DECIMAL	
# MAGIC       COMMENT 'TODO'
# MAGIC     ,IsVisible	INT	
# MAGIC       COMMENT '1 is fault'
# MAGIC     ,RecurrFeeDescr	STRING	
# MAGIC       COMMENT 'can only grab the EN description'
# MAGIC     ,OveruseFeeDescr	STRING	
# MAGIC       COMMENT 'can only grab the EN description'
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
# MAGIC ,CONSTRAINT planrate_pk PRIMARY KEY(PlanRateID,DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for planrate. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (PlanRateID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE planrate ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE planrate ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE planrate ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
