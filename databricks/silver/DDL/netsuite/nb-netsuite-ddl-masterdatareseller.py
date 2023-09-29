# Databricks notebook source
# DBTITLE 1,Define MasterDataReseller at Silver
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
# MAGIC CREATE OR REPLACE TABLE masterdatareseller
# MAGIC   ( SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,Reseller_Name	string NOT NULL     
# MAGIC       COMMENT 'Name of the End User. Also used as the Unique ID'
# MAGIC     ,Date_Created	TIMESTAMP NOT NULL
# MAGIC       COMMENT 'Date the End User was created'
# MAGIC     ,Last_Modified	TIMESTAMP NOT NULL
# MAGIC       COMMENT 'Date the End User was last Modified'
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
# MAGIC ,CONSTRAINT masterdatareseller_pk PRIMARY KEY(Reseller_Name, Last_Modified)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for masterdatareseller. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (Reseller_Name, Last_Modified)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE masterdatareseller ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE masterdatareseller ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE masterdatareseller ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
