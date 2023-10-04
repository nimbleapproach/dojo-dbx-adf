# Databricks notebook source
# DBTITLE 1,Define MasterDataSKU at Silver
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
# MAGIC CREATE OR REPLACE TABLE masterdatasku
# MAGIC
# MAGIC   (  SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,SKU_ID	        STRING	 NOT NULL 
# MAGIC     COMMENT    'BUSINESS KEY / Unique SKU ID'
# MAGIC     ,Description	  STRING	  
# MAGIC       COMMENT 'Description of the item'
# MAGIC     ,Vendor_Name	  STRING	  
# MAGIC       COMMENT 'Vendor Name'
# MAGIC     ,Vendor_Family	STRING	  
# MAGIC       COMMENT 'Further breakdown of vendor name'
# MAGIC     ,Item_Category	STRING	  
# MAGIC       COMMENT 'Classification of the individual item'
# MAGIC     ,Type	          STRING	  
# MAGIC       COMMENT 'Column to identify Inventory/Non- inventory item'
# MAGIC     ,Billing_Duration String
# MAGIC       COMMENT 'TODO'
# MAGIC     ,Date_Created	  TIMESTAMP	
# MAGIC       COMMENT 'Date the SKU was created'
# MAGIC     ,Last_Modified	TIMESTAMP	NOT NULL
# MAGIC       COMMENT 'Date the SKU was last Modified'
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
# MAGIC ,CONSTRAINT masterdatasku_pk PRIMARY KEY(SKU_ID,Last_Modified)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for masterdatasku. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (SKU_ID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE masterdatasku ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE masterdatasku ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE masterdatasku ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
