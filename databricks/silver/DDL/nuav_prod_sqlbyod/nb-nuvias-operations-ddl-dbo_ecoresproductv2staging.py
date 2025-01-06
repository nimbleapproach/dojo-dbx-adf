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
# MAGIC create or replace table dbo_ecoresproductv2staging
# MAGIC   (
# MAGIC     SID                               bigint       generated always as identity    comment 'Surrogate Key'
# MAGIC   , recid                             bigint                                       comment 'TODO'
# MAGIC   , productnumber                     string                                       comment 'The unique identifier for a product in the system.'
# MAGIC   , trackingdimensiongroupname        string                                       comment 'The name of the tracking dimension group for the product.'
# MAGIC   , Sys_Bronze_InsertDateTime_UTC     timestamp                                    comment 'The timestamp when this entry landed in bronze.'
# MAGIC   , Sys_Silver_InsertDateTime_UTC     timestamp   default current_timestamp()      comment 'The timestamp when this entry landed in silver.'
# MAGIC   , Sys_Silver_ModifedDateTime_UTC    timestamp   default current_timestamp()      comment 'The timestamp when this entry was last modifed in silver.'
# MAGIC   , Sys_Silver_HashKey                bigint      not null                         comment 'HashKey over all but Sys columns.'
# MAGIC   , Sys_Silver_IsCurrent              boolean
# MAGIC    , constraint dbo_ecoresproductv2staging_pk primary key(recid, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC comment 'This table contains the data from dbo_ecoresproductv2staging.'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_ecoresproductv2staging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_ecoresproductv2staging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_ecoresproductv2staging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_ecoresproductv2staging OWNER TO `az_edw_data_engineers_ext_db`
