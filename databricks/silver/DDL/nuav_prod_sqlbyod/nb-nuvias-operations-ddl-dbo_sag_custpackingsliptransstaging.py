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
# MAGIC create or replace table dbo_sag_custpackingsliptransstaging
# MAGIC   (
# MAGIC     SID                               bigint       generated always as identity    comment 'Surrogate Key'
# MAGIC   , recid                             bigint                                       comment 'TODO'
# MAGIC   , inventtransid                     string                                       comment 'TODO'
# MAGIC   , packingslipid                     string                                       comment 'TODO'
# MAGIC   , dataareaid                        string                                       comment 'Identifier for the data area'
# MAGIC   , deliverydate                      timestamp                                    comment 'The date on which the delivery is scheduled to be made.'
# MAGIC   , dlvterm                           string                                       comment 'Delivery term code'
# MAGIC   , origsalesid                       string                                       comment 'The original sales ID associated with the transaction.'
# MAGIC   , salesid                           string                                       comment 'Unique identifier for a sales transaction'
# MAGIC   , itemid                            string                                       comment 'Identifier for the item'
# MAGIC   , qty                               decimal(32,6)                                comment 'Quantity of items'
# MAGIC   , weight                            decimal(32,12)                               comment 'The weight of the item in a specific unit of measurement.'
# MAGIC   , inventdimid                       string                                       comment 'Identifier for the inventory dimension of the item'
# MAGIC   , Sys_Bronze_InsertDateTime_UTC     timestamp                                    comment 'The timestamp when this entry landed in bronze.'
# MAGIC   , Sys_Silver_InsertDateTime_UTC     timestamp   default current_timestamp()      comment 'The timestamp when this entry landed in silver.'
# MAGIC   , Sys_Silver_ModifedDateTime_UTC    timestamp   default current_timestamp()      comment 'The timestamp when this entry was last modifed in silver.'
# MAGIC   , Sys_Silver_HashKey                bigint      not null                         comment 'HashKey over all but Sys columns.'
# MAGIC   , Sys_Silver_IsCurrent              boolean
# MAGIC    , constraint dbo_sag_custpackingsliptransstaging_pk primary key(recid, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC comment 'This table contains the data from dbo_sag_custpackingsliptransstaging.'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_custpackingsliptransstaging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_custpackingsliptransstaging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_custpackingsliptransstaging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_custpackingsliptransstaging OWNER TO `az_edw_data_engineers_ext_db`
