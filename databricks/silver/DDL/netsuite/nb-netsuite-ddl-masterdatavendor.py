# Databricks notebook source
# DBTITLE 1,Define MasterDataVendor at Silver
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC import os
# MAGIC
# MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA netsuite;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE masterdatavendor
# MAGIC
# MAGIC   ( SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,Vendor_ID	 string NOT NULL     
# MAGIC       COMMENT 'BUSINESS KEY / Unique ID for each Vendor'
# MAGIC     ,Vendor_Name	    string    
# MAGIC       COMMENT 'Name of the Vendor'
# MAGIC     ,Contract_Start_Date	TIMESTAMP
# MAGIC       COMMENT 'Date of when Vendor joined'
# MAGIC     ,Contract_End_Date	TIMESTAMP  
# MAGIC       COMMENT 'Date of when Vendor has left'
# MAGIC     ,IFG_Mapping	string   
# MAGIC       COMMENT 'Name of the Vendor'
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
# MAGIC     ,Sys_Silver_IsDeleted BOOLEAN
# MAGIC ,CONSTRAINT masterdatavendor_pk PRIMARY KEY(Vendor_ID,Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for masterdatavendor. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (Vendor_ID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE masterdatavendor ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE masterdatavendor ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE masterdatavendor ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
