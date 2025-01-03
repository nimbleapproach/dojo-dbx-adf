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
# MAGIC create or replace table dbo_sag_saleslinev2staging
# MAGIC   (
# MAGIC     SID                             bigint generated always as identity             comment 'Surrogate Key'
# MAGIC   , inventtransid                   string                                          comment 'TODO'
# MAGIC   , dataareaid                      string                                          comment 'TODO'
# MAGIC   , salesid                         string                                          comment 'TODO'
# MAGIC   , sag_vendorreferencenumber       string                                          comment 'TODO'
# MAGIC   , sag_resellervendorid            string                                          comment 'TODO'
# MAGIC   , currencycode                    string                                          comment 'TODO'
# MAGIC   , sag_purchprice                  decimal(32, 6)                                  comment 'TODO'
# MAGIC   , salesstatus                     int                                             comment 'TODO'
# MAGIC   , sag_shipanddebit                int                                             comment 'TODO'
# MAGIC   , shippingdaterequested           timestamp                                       comment 'The requested date for shipping the item.'
# MAGIC   , receiptdaterequested            timestamp                                       comment 'The date when the receipt was requested.'
# MAGIC   , itemid                          string                                          comment 'TODO'
# MAGIC   , inventreftransid                string                                          comment 'TODO'
# MAGIC   , sag_vendorstandardcost          decimal(32, 6)                                  comment 'TODO'
# MAGIC   , salesprice                      decimal(32, 6)                                  comment 'TODO'
# MAGIC   , salesqty                        decimal(32,6)                                   comment 'TODO'
# MAGIC   , lineamount                      decimal(32,6)                                   comment 'TODO'
# MAGIC   , sag_navlinenum                  string                                          comment 'TODO'
# MAGIC   , sag_ngs1pobuyprice              decimal(32,6)                                   comment 'TODO'
# MAGIC   , sag_ngs1standardbuyprice        decimal(32,6)                                   comment 'TODO'
# MAGIC   , sag_unitcostinquotecurrency     decimal(32,6)                                   comment 'TODO'
# MAGIC   , linenum                         decimal(32,16)                                  comment 'TODO'
# MAGIC   , qtyordered                      decimal(32,6)                                   comment 'The quantity of items ordered.'
# MAGIC   , Sys_Bronze_InsertDateTime_UTC   timestamp                                       comment 'The timestamp when this entry landed in bronze.'
# MAGIC   , Sys_Silver_InsertDateTime_UTC   timestamp      default current_timestamp()      comment 'The timestamp when this entry landed in silver.'
# MAGIC   , Sys_Silver_ModifedDateTime_UTC  timestamp      default current_timestamp()      comment 'The timestamp when this entry was last modifed in silver.'
# MAGIC   , Sys_Silver_HashKey              bigint not null                                 comment 'HashKey over all but Sys columns.'
# MAGIC   , Sys_Silver_IsCurrent            boolean
# MAGIC   , constraint dbo_sag_saleslinev2staging_pk primary key(inventtransid, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC comment 'This table contains the data from dbo_sag_saleslinev2staging.'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_saleslinev2staging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_saleslinev2staging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_saleslinev2staging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_saleslinev2staging OWNER TO `az_edw_data_engineers_ext_db`
