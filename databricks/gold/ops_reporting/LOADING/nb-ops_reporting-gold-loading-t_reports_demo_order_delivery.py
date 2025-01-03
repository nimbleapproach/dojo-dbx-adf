# Databricks notebook source
import os

ENVIRONMENT = os.environ["__ENVIRONMENT__"]

# COMMAND ----------

spark.catalog.setCurrentCatalog(f"gold_{ENVIRONMENT}")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA if not EXISTS ops_reporting;
# MAGIC USE SCHEMA ops_reporting;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace table t_reports_outbound_shipments as select * from v_reports_outbound_shipments;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE t_reports_outbound_shipments OWNER TO `az_edw_data_engineers_ext_db`
