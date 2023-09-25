# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

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
# MAGIC     DocID BIGINT NOT NULL 
# MAGIC       COMMENT 'Business Key'
# MAGIC     ,DocNum	    string
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,DocType	BIGINT
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,CurrencyID	string
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,Total_Value float
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,Vendor_AccountID	BIGINT
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,Customer_AccountID	BIGINT
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,DocDATE	timestamp
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,DueDate	timestamp
# MAGIC         COMMENT 'TODO'    
# MAGIC     ,CloseDate	timestamp
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
# MAGIC ,CONSTRAINT ardoc_pk PRIMARY KEY(DocID)
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
