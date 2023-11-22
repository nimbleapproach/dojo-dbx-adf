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
# MAGIC CREATE OR REPLACE TABLE ardoc
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC       GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC     ,DocID BIGINT NOT NULL 
# MAGIC       COMMENT 'Business Key'
# MAGIC     ,DocNum STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DocType	BIGINT
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Status	INT
# MAGIC         COMMENT 'TODO'
# MAGIC     ,CurrencyID	STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Total_Code STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,Total_Value FLOAT
# MAGIC         COMMENT 'TODO'
# MAGIC     ,TaxTotal_Code STRING
# MAGIC         COMMENT 'TODO'
# MAGIC     ,TaxTotal_Value FLOAT
# MAGIC         COMMENT 'TODO'
# MAGIC     ,DateArc INT NOT NULL
# MAGIC       COMMENT 'Water mark'
# MAGIC     ,Vendor_AccountID	BIGINT
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,Customer_AccountID	BIGINT
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,DocDATE	INT
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,DueDate	INT
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,CloseDate	INT
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,Description string
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,Acct	string
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
# MAGIC ,CONSTRAINT ardoc_pk PRIMARY KEY(DocID, DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for ardoc. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (DocID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE ardoc ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ardoc ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ardoc ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
