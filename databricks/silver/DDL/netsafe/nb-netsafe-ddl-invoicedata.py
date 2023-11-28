# Databricks notebook source
# MAGIC %python
# MAGIC import os
# MAGIC
# MAGIC ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA netsafe;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE invoicedata
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,Invoice_Number STRING
# MAGIC       COMMENT 'TODO'  
# MAGIC     ,LineNo INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Invoice_Date STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Sales_Order_Number STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Order_Type STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Item_ID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SKU STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Item_Type STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Customer_Name STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,QTY INT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Transaction_Currency STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Revenue_Transaction_Currency DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Cost_Transaction_Currency DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Margin_Transaction_Currency DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Exchange_Rate FLOAT
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Revenue_GBP DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Cost_GBP DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Margin_GBP DECIMAL(38,18)
# MAGIC       COMMENT 'TODO'    
# MAGIC     ,Vendor_ID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Vendor_Name STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Account_Manager STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Quote_ID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Customer_Account STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,SKU_Description STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Country STRING
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
# MAGIC ,CONSTRAINT invoicedata_pk PRIMARY KEY(Invoice_Number, LineNo, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ardoc. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (Invoice_Number, LineNo)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE invoicedata ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
