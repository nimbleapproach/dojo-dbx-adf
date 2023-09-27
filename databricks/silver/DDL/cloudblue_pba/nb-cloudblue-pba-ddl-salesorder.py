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
# MAGIC CREATE OR REPLACE TABLE salesorder
# MAGIC   ( 
# MAGIC         SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,OrderID	INT NOT NULL
# MAGIC       COMMENT 'Business Key' 
# MAGIC     ,DateArc TIMESTAMP NOT NULL
# MAGIC       COMMENT 'WATERMARK'
# MAGIC     ,OrderNbr	STRING
# MAGIC       COMMENT   'TODO' 
# MAGIC     ,OrderDate	INT
# MAGIC       COMMENT   'TODO'    
# MAGIC     ,OrderTypeID	STRING
# MAGIC        COMMENT   'TODO' 
# MAGIC     ,CurrencyID	STRING
# MAGIC       COMMENT   'TODO'    
# MAGIC     ,TaxTotal_Value	DECIMAL
# MAGIC       COMMENT   'TODO'    
# MAGIC     ,Vendor_AccountID	INT
# MAGIC       COMMENT   'TODO'    
# MAGIC     ,ReasonID	INT
# MAGIC       COMMENT   'TODO'    
# MAGIC     ,ExpDate	INT
# MAGIC       COMMENT   'TODO'    
# MAGIC     ,MerchTotal_Value	DECIMAL
# MAGIC       COMMENT   'TODO'    
# MAGIC     ,Total_Value	DECIMAL
# MAGIC       COMMENT   'TODO'      
# MAGIC     ,OrderStatusID	STRING
# MAGIC       COMMENT   'TODO'      
# MAGIC     ,Customer_AccountID	INT
# MAGIC       COMMENT   'TODO'    
# MAGIC     ,Descr	STRING
# MAGIC       COMMENT   'TODO'    
# MAGIC     
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
# MAGIC ,CONSTRAINT salesorder_pk PRIMARY KEY(OrderID,DateArc)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for salesorder. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (OrderID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE salesorder ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE salesorder ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE salesorder ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
