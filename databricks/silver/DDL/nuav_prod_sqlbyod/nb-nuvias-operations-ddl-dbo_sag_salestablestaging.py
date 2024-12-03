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
# MAGIC CREATE OR REPLACE TABLE dbo_sag_salestablestaging
# MAGIC   (
# MAGIC     SID BIGINT GENERATED ALWAYS AS IDENTITY
# MAGIC       COMMENT 'Surrogate Key'
# MAGIC   , SALESID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , DATAAREAID STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , CUSTACCOUNT STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , INVOICEACCOUNT STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , DELIVERYPOSTALADDRESS BIGINT
# MAGIC       COMMENT 'TODO'
# MAGIC   , CUSTOMERREF STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SAG_EUADDRESS_EMAIL STRING
# MAGIC       COMMENT 'TODO'   
# MAGIC   , SAG_EUADDRESS_CONTACT STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SAG_EUADDRESS_COUNTRY  STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SAG_EUADDRESS_POSTCODE  STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SAG_EUADDRESS_CITY  STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SAG_EUADDRESS_STREET1  STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SAG_EUADDRESS_STREET2  STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SAG_EUADDRESS_NAME  STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SALESNAME  STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , PURCHORDERFORMNUM STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , SAG_EUADDRESS_COUNTY  STRING
# MAGIC       COMMENT 'TODO'
# MAGIC   , CURRENCYCODE  STRING
# MAGIC       COMMENT 'TODO'
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
# MAGIC   , CONSTRAINT dbo_sag_salestablestaging_pk PRIMARY KEY(SALESID, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC COMMENT 'This table contains the data from dbo_sag_salestablestaging. \n'
# MAGIC TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_salestablestaging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_salestablestaging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_salestablestaging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE silver_dev.nuav_prod_sqlbyod.dbo_sag_salestablestaging OWNER TO `az_edw_data_engineers_ext_db`
