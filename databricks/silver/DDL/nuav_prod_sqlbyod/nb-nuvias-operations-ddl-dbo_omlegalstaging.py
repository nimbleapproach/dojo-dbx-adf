# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"silver_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA nuav_prod_sqlbyod;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE dbo_omlegalstaging
# MAGIC   ( 
# MAGIC     SID bigint
# MAGIC         GENERATED ALWAYS AS IDENTITY
# MAGIC         COMMENT 'Surrogate Key'
# MAGIC     ,SYNCSTARTDATETIME TIMESTAMP NOT NULL
# MAGIC       COMMENT 'TODO'
# MAGIC     ,NAME STRING 
# MAGIC       COMMENT 'TODO'
# MAGIC     ,NAMEALIAS	STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,LEGALENTITYID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,ADDRESSCOUNTRYREGIONISOCODE STRING
# MAGIC       COMMENT 'TODO'
# MAGIC     ,PARTYNUMBER	STRING	
# MAGIC       COMMENT 'TODO'
# MAGIC     -- ,DataLakeModified_DateTime	TIMESTAMP	
# MAGIC     --   COMMENT 'TODO'
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
# MAGIC
# MAGIC     ,Sys_Silver_IsCurrent BOOLEAN
# MAGIC
# MAGIC ,CONSTRAINT dbo_omlegalstaging_pk PRIMARY KEY(PARTYNUMBER,LEGALENTITYID,SYNCSTARTDATETIME)
# MAGIC   )
# MAGIC COMMENT 'This table contains the line data for dbo_omlegalstaging. \n' 
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC CLUSTER BY (PARTYNUMBER,LEGALENTITYID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_omlegalstaging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_omlegalstaging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_omlegalstaging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');
