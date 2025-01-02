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
# MAGIC create or replace table dbo_v_distinctitems
# MAGIC   (
# MAGIC      SID bigint      generated always as identity                                      comment 'Surrogate Key'
# MAGIC     ,ItemID	                           string                                          comment 'TODO'
# MAGIC     ,CompanyID	                       string                                          comment 'TODO'
# MAGIC     ,ItemName                          string                                          comment 'TODO'
# MAGIC     ,ItemDescription                   string                                          comment 'TODO'
# MAGIC     ,ItemModelGroupID                  string                                          comment 'TODO'
# MAGIC     ,ItemModelGroupName                string                                          comment 'TODO'
# MAGIC     ,ItemGroupID                       string                                          comment 'TODO'
# MAGIC     ,ItemGroupName                     string                                          comment 'TODO'
# MAGIC     ,PrimaryVendorID                   string                                          comment 'TODO'
# MAGIC     ,PrimaryVendorName                 string                                          comment 'TODO'
# MAGIC     ,Practice                          string                                          comment 'Practice associated with the item'
# MAGIC     ,Sys_Bronze_InsertDateTime_UTC     timestamp                                       comment 'The timestamp when this entry landed in bronze.'
# MAGIC     ,Sys_Silver_InsertDateTime_UTC     timestamp      default current_timestamp()      comment 'The timestamp when this entry landed in silver.'
# MAGIC     ,Sys_Silver_ModifedDateTime_UTC    timestamp      default current_timestamp()      comment 'The timestamp when this entry was last modifed in silver.'
# MAGIC     ,Sys_Silver_HashKey                bigint         not null                         comment 'HashKey over all but Sys columns.'
# MAGIC     ,Sys_Silver_IsCurrent              boolean                                         comment 'Flag if this is the current version.'
# MAGIC     ,Sys_Silver_IsDeleted              boolean                                         comment 'Flag if this is the deleted version.'
# MAGIC     ,constraint dbo_v_distinctitems_pk primary key(ItemID,CompanyID,Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC comment 'This table contains the line data for ara_dim_company.'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported')
# MAGIC cluster by (ItemID,CompanyID)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_v_distinctitems ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_v_distinctitems ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE dbo_v_distinctitems ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE dbo_v_distinctitems OWNER TO `az_edw_data_engineers_ext_db`
