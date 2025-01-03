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
# MAGIC create or replace table ara_so_po_id_list
# MAGIC   (
# MAGIC     SID                              bigint generated always as identity        comment 'Surrogate Key'
# MAGIC   , salestableid_local               string                                     comment 'Local identifier for the sales table'
# MAGIC   , saleslineid_local                string                                     comment 'Local identifier for the sales line'
# MAGIC   , purchtableid_local               string                                     comment 'Local identifier for the purchase table'
# MAGIC   , purchlineid_local                string                                     comment 'Local identifier for the purchase line'
# MAGIC   , hdr                              string                                     comment 'Header information'
# MAGIC   , salestableid_intercomp           string                                     comment 'Intercompany identifier for the sales table'
# MAGIC   , saleslineid_intercomp            string                                     comment 'Intercompany identifier for the sales line'
# MAGIC   , purchtableid_intercomp           string                                     comment 'Intercompany identifier for the purchase table'
# MAGIC   , purchlineid_intercomp            string                                     comment 'Intercompany identifier for the purchase line'
# MAGIC   , Sys_Bronze_InsertDateTime_UTC    timestamp                                  comment 'The timestamp when this entry landed in bronze.'
# MAGIC   , Sys_Silver_InsertDateTime_UTC    timestamp      default current_timestamp() comment 'The timestamp when this entry landed in silver.'
# MAGIC   , Sys_Silver_ModifedDateTime_UTC   timestamp      default current_timestamp() comment 'The timestamp when this entry was last modifed in silver.'
# MAGIC   , Sys_Silver_HashKey               bigint         not null                    comment 'HashKey over all but Sys columns.'
# MAGIC   , Sys_Silver_IsCurrent             boolean
# MAGIC   , constraint ara_so_po_id_list_pk primary key(saleslineid_local, Sys_Bronze_InsertDateTime_UTC)
# MAGIC   )
# MAGIC comment 'This table contains the data from ara_so_po_id_list.'
# MAGIC tblproperties ('delta.feature.allowColumnDefaults' = 'supported');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE ara_so_po_id_list ADD CONSTRAINT dateWithinRange_Bronze_InsertDateTime CHECK (Sys_Bronze_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ara_so_po_id_list ADD CONSTRAINT dateWithinRange_Silver_InsertDateTime CHECK (Sys_Silver_InsertDateTime_UTC > '1900-01-01');
# MAGIC ALTER TABLE ara_so_po_id_list ADD CONSTRAINT dateWithinRange_Silver_ModifedDateTime CHECK (Sys_Silver_ModifedDateTime_UTC > '1900-01-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE ara_so_po_id_list OWNER TO `az_edw_data_engineers_ext_db`
