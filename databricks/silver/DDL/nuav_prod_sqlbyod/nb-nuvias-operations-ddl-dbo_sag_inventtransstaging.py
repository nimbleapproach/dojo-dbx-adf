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
# MAGIC CREATE OR REPLACE TABLE dbo_sag_inventtransstaging
# MAGIC   (
# MAGIC    SID BIGINT GENERATED ALWAYS AS IDENTITY
# MAGIC        COMMENT 'Surrogate Key'
# MAGIC   , INVENTTRANSID STRING
# MAGIC        COMMENT 'TODO'
# MAGIC   , DATAAREAID STRING
# MAGIC        COMMENT 'TODO'
# MAGIC   , ITEMID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , RECID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , DATEPHYSICAL TIMESTAMP
# MAGIC         COMMENT 'TODO'
# MAGIC   , DATEFINANCIAL TIMESTAMP
# MAGIC         COMMENT 'TODO'
# MAGIC   , QTY DECIMAL
# MAGIC         COMMENT 'TODO'
# MAGIC   , INVOICEID STRING
# MAGIC         COMMENT 'TODO'
# MAGIC   , INVENTSERIALID STRING
# MAGIC         COMMENT 'TODO'
# MAGIC   , STATUSISSUE INT
# MAGIC         COMMENT 'TODO'
# MAGIC   , STATUSRECEIPT INT
# MAGIC         COMMENT 'TODO'
# MAGIC   , INVOICERETURNED INT
# MAGIC         COMMENT 'TODO'
# MAGIC   , PACKINGSLIPRETURNED INT
# MAGIC         COMMENT 'TODO'
# MAGIC   , INVENTDIMID STRING
# MAGIC         COMMENT 'TODO'
# MAGIC   , PACKINGSLIPID STRING
# MAGIC         COMMENT 'TODO'
# MAGIC   , Sys_Bronze_InsertDateTime_UTC TIMESTAMP
# MAGIC       COMMENT 'The timestamp when this entry landed in bronze.'
# MAGIC   , Sys_Silver_InsertDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry landed in silver.'
# MAGIC   , Sys_Silver_ModifedDateTime_UTC TIMESTAMP
# MAGIC       DEFAULT current_timestamp()
# MAGIC       COMMENT 'The timestamp when this entry was last modifed in silver.'
# MAGIC   , Sys_Silver_HashKey BIGINT NOT NULL
# MAGIC       COMMENT 'HashKey over all but Sys columns.'
# MAGIC   , Sys_Silver_IsCurrent BOOLEAN
# MAGIC   , CONSTRAINT dbo_sag_inventtransstaging_pk PRIMARY KEY(RECID, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the data from dbo_sag_inventtransstaging. \n'
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE dbo_sag_inventtransstaging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_inventtransstaging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_inventtransstaging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_inventtransstaging OWNER TO `az_edw_data_engineers_ext_db`
