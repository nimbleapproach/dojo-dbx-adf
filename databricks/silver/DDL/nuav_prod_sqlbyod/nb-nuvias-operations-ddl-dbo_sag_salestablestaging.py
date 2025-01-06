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
# MAGIC create or replace table dbo_sag_salestablestaging
# MAGIC   (
# MAGIC     SID                             bigint generated always as identity    comment 'Surrogate Key'
# MAGIC   , salesid                         string                                 comment 'TODO'
# MAGIC   , dataareaid                      string                                 comment 'TODO'
# MAGIC   , custaccount                     string                                 comment 'TODO'
# MAGIC   , invoiceaccount                  string                                 comment 'TODO'
# MAGIC   , deliverypostaladdress           bigint                                 comment 'TODO'
# MAGIC   , customerref                     string                                 comment 'TODO'
# MAGIC   , sag_euaddress_email             string                                 comment 'TODO'
# MAGIC   , sag_euaddress_contact           string                                 comment 'TODO'
# MAGIC   , sag_euaddress_country           string                                 comment 'TODO'
# MAGIC   , sag_euaddress_postcode          string                                 comment 'TODO'
# MAGIC   , sag_euaddress_city              string                                 comment 'TODO'
# MAGIC   , sag_euaddress_street1           string                                 comment 'TODO'
# MAGIC   , sag_euaddress_street2           string                                 comment 'TODO'
# MAGIC   , sag_euaddress_name              string                                 comment 'TODO'
# MAGIC   , salesname                       string                                 comment 'TODO'
# MAGIC   , purchorderformnum               string                                 comment 'TODO'
# MAGIC   , sag_euaddress_county            string                                 comment 'TODO'
# MAGIC   , currencycode                    string                                 comment 'TODO'
# MAGIC   , salesordertype                  string                                 comment 'TODO'
# MAGIC   , sag_createddatetime             timestamp                              comment 'TODO'
# MAGIC   , sag_navsonumber                 string                                 comment 'TODO'
# MAGIC   , sag_navponumber                 string                                 comment 'TODO'
# MAGIC   , nuv_navcustomerref              string                                 comment 'TODO'
# MAGIC   , sag_reselleremailaddress        string                                 comment 'TODO'
# MAGIC   , nuv_rsaddress_contact           string                                 comment 'TODO'
# MAGIC   , sag_cpqaccountmanager           string                                 comment 'The account manager responsible for the CPQ (Configure, Price, Quote) process.'
# MAGIC   , sag_cpqsalestaker               string                                 comment 'The column represents the sales taker associated with the sale in the SAG CPQ system.'
# MAGIC   , sagtransactionnumber            string                                 comment 'Quote Reference'
# MAGIC   , customer                        string                                 comment 'The name or identifier of the customer associated with the sale'
# MAGIC   , fixedexchrate                   decimal(32,16)                         comment 'The fixed exchange rate for the transaction.'
# MAGIC   , Sys_Bronze_InsertDateTime_UTC   timestamp                              comment 'The timestamp when this entry landed in bronze.'
# MAGIC   , Sys_Silver_InsertDateTime_UTC   timestamp  default current_timestamp() comment 'The timestamp when this entry landed in silver.'
# MAGIC   , Sys_Silver_ModifedDateTime_UTC  timestamp  default current_timestamp() comment 'The timestamp when this entry was last modifed in silver.'
# MAGIC   , Sys_Silver_HashKey              bigint     not null                    comment 'HashKey over all but Sys columns.'
# MAGIC   , Sys_Silver_IsCurrent            boolean
# MAGIC   , constraint dbo_sag_salestablestaging_pk primary key(salesid, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC comment 'This table contains the data from dbo_sag_salestablestaging.'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_salestablestaging ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_salestablestaging ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_sag_salestablestaging ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_sag_salestablestaging OWNER TO `az_edw_data_engineers_ext_db`
