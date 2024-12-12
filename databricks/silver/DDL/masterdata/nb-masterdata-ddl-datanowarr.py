# Databricks notebook source
# MAGIC %python
# MAGIC import os
# MAGIC
# MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA masterdata;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE datanowarr
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,Vendor_Name STRING
# MAGIC       COMMENT 'Name of the Vendor.'
# MAGIC     ,SKU	STRING
# MAGIC       COMMENT 'Globally Identifier for a Product.'
# MAGIC     ,Commitment_Duration_in_months	STRING
# MAGIC       COMMENT 'One of Monthly,1 YR,2 YR,3 YR,5 YR,Perpetual,Other'
# MAGIC     ,Commitment_Duration2	STRING
# MAGIC       COMMENT 'One of Monthly,1 YR,2 YR,3 YR,5 YR,Perpetual,Other'
# MAGIC     ,Billing_Frequency	STRING
# MAGIC       COMMENT 'One of Monthly,Quarterly,Yearly,Upfront,Yearly (in arrear)'
# MAGIC     ,Billing_Frequency2	STRING
# MAGIC       COMMENT 'One of Monthly,Quarterly,Yearly,Upfront,Yearly (in arrear)'
# MAGIC     ,Consumption_Model	STRING
# MAGIC       COMMENT 'One of Capacity,Flexible'
# MAGIC     ,Product_Type	STRING
# MAGIC       COMMENT 'One of Hardware,SW Subscription,SW Perpetual,Vendor support,Training,Professional services,Logistics,Rebates'
# MAGIC     ,Mapping_Type_Duration	STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Mapping_Type_Billing	STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,MRR_Ratio	STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Commitment_Duration_Value	STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Billing_Frequency_Value2	STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,MRR	STRING
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
# MAGIC ,CONSTRAINT datanowarr_pk PRIMARY KEY(Vendor_Name,SKU,Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for account. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (Vendor_Name,SKU)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE datanowarr ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE datanowarr ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE datanowarr ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
