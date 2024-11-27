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
# MAGIC CREATE OR REPLACE TABLE plan
# MAGIC   ( 
# MAGIC         SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,PlanID	INT	
# MAGIC         COMMENT 'Primary key'
# MAGIC     ,name	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,shortDescription	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,ServTermID	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,serviceTemplateID	INT	
# MAGIC         COMMENT 'template plan id for vendor'
# MAGIC     ,RecurringType	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,IsAutoRenew	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,longDescription	STRING	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,AccountID	INT	
# MAGIC         COMMENT 'FK, tells which resellers have the plan'
# MAGIC     ,BillingPeriod	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,BillingPeriodType	INT	
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Copied_From_PlanID	INT	
# MAGIC         COMMENT 'where plan is delegated from'
# MAGIC     ,DateArc	INT	
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
# MAGIC     ,Sys_Silver_IsDeleted BOOLEAN
# MAGIC       COMMENT 'Flag if this is the deleted version.'
# MAGIC ,CONSTRAINT planpk PRIMARY KEY(PlanID,DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for plan. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (PlanID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE plan ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE plan ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE plan ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
